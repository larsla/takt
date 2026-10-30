// Package metrics reports tally metrics to a Prometheus registry, the way
// Cadence's server is usually set up. Tally's own Prometheus reporter for
// v3 needs an old fork of client_golang, so this is a small replacement.
package metrics

import (
	"io"
	"log/slog"
	"math"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/uber-go/tally"
)

// TimerBuckets are the histogram buckets, in seconds, used for timers. They
// reach a minute so long polls fit.
var TimerBuckets = []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60}

// Prometheus is a tally scope backed by a Prometheus registry.
type Prometheus struct {
	Scope    tally.Scope
	Handler  http.Handler
	Registry *prom.Registry
	closer   io.Closer
}

// NewPrometheus creates a root scope that reports into a new registry. The
// registry also holds the Go runtime and process collectors.
func NewPrometheus(log *slog.Logger) *Prometheus {
	reg := prom.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	r := &reporter{reg: reg, log: log, vecs: map[string]*vec{}}
	scope, closer := tally.NewRootScope(tally.ScopeOptions{
		CachedReporter: r,
		Separator:      "_",
		SanitizeOptions: &tally.SanitizeOptions{
			NameCharacters:       tally.ValidCharacters{Ranges: tally.AlphanumericRange, Characters: []rune{'_'}},
			KeyCharacters:        tally.ValidCharacters{Ranges: tally.AlphanumericRange, Characters: []rune{'_'}},
			ValueCharacters:      tally.ValidCharacters{Ranges: tally.AlphanumericRange, Characters: []rune{'_', '-', '.', '/', ':', '@'}},
			ReplacementCharacter: '_',
		},
	}, time.Second)
	return &Prometheus{
		Scope:    scope,
		Handler:  promhttp.HandlerFor(reg, promhttp.HandlerOpts{}),
		Registry: reg,
		closer:   closer,
	}
}

// Close flushes and stops the scope.
func (p *Prometheus) Close() error { return p.closer.Close() }

type kind int

const (
	kindCounter kind = iota
	kindGauge
	kindHistogram
)

// vec is one registered metric family. Every use of a name must have the
// same tag keys, as Prometheus requires.
type vec struct {
	kind    kind
	keys    []string
	counter *prom.CounterVec
	gauge   *prom.GaugeVec
	hist    *prom.HistogramVec
}

type reporter struct {
	reg  *prom.Registry
	log  *slog.Logger
	mu   sync.Mutex
	vecs map[string]*vec
}

func (*reporter) Capabilities() tally.Capabilities { return caps{} }
func (*reporter) Flush()                           {}

type caps struct{}

func (caps) Reporting() bool { return true }
func (caps) Tagging() bool   { return true }

// get returns the family for name, registering it on first use. It returns
// nil when name was registered with another kind or other tag keys.
func (r *reporter) get(name string, k kind, tags map[string]string, buckets []float64) (*vec, []string) {
	keys := make([]string, 0, len(tags))
	for key := range tags {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	values := make([]string, len(keys))
	for i, key := range keys {
		values[i] = tags[key]
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if v, ok := r.vecs[name]; ok {
		if v.kind != k || !slices.Equal(v.keys, keys) {
			r.log.Warn("metric reused with other tags, dropping", "name", name, "tags", strings.Join(keys, ","), "registered", strings.Join(v.keys, ","))
			return nil, nil
		}
		return v, values
	}
	v := &vec{kind: k, keys: keys}
	var c prom.Collector
	switch k {
	case kindCounter:
		v.counter = prom.NewCounterVec(prom.CounterOpts{Name: name, Help: name + " counter"}, keys)
		c = v.counter
	case kindGauge:
		v.gauge = prom.NewGaugeVec(prom.GaugeOpts{Name: name, Help: name + " gauge"}, keys)
		c = v.gauge
	case kindHistogram:
		v.hist = prom.NewHistogramVec(prom.HistogramOpts{Name: name, Help: name + " histogram", Buckets: buckets}, keys)
		c = v.hist
	}
	if err := r.reg.Register(c); err != nil {
		r.log.Warn("metric registration failed", "name", name, "err", err)
		return nil, nil
	}
	r.vecs[name] = v
	return v, values
}

func (r *reporter) AllocateCounter(name string, tags map[string]string) tally.CachedCount {
	v, values := r.get(name, kindCounter, tags, nil)
	if v == nil {
		return noop{}
	}
	return counter{v.counter.WithLabelValues(values...)}
}

func (r *reporter) AllocateGauge(name string, tags map[string]string) tally.CachedGauge {
	v, values := r.get(name, kindGauge, tags, nil)
	if v == nil {
		return noop{}
	}
	return gauge{v.gauge.WithLabelValues(values...)}
}

func (r *reporter) AllocateTimer(name string, tags map[string]string) tally.CachedTimer {
	v, values := r.get(name, kindHistogram, tags, TimerBuckets)
	if v == nil {
		return noop{}
	}
	return timer{v.hist.WithLabelValues(values...)}
}

func (r *reporter) AllocateHistogram(name string, tags map[string]string, buckets tally.Buckets) tally.CachedHistogram {
	bounds := buckets.AsValues()
	if _, ok := buckets.(tally.DurationBuckets); ok {
		bounds = make([]float64, 0, buckets.Len())
		for _, d := range buckets.AsDurations() {
			bounds = append(bounds, d.Seconds())
		}
	}
	bounds = slices.DeleteFunc(slices.Clone(bounds), func(f float64) bool { return math.IsInf(f, 0) })
	v, values := r.get(name, kindHistogram, tags, bounds)
	if v == nil {
		return noop{}
	}
	return histogram{v.hist.WithLabelValues(values...)}
}

type noop struct{}

func (noop) ReportCount(int64)                                        {}
func (noop) ReportGauge(float64)                                      {}
func (noop) ReportTimer(time.Duration)                                {}
func (noop) ReportSamples(int64)                                      {}
func (noop) ValueBucket(float64, float64) tally.CachedHistogramBucket { return noop{} }
func (noop) DurationBucket(time.Duration, time.Duration) tally.CachedHistogramBucket {
	return noop{}
}

type counter struct{ c prom.Counter }

func (c counter) ReportCount(v int64) { c.c.Add(float64(v)) }

type gauge struct{ g prom.Gauge }

func (g gauge) ReportGauge(v float64) { g.g.Set(v) }

type timer struct{ o prom.Observer }

func (t timer) ReportTimer(d time.Duration) { t.o.Observe(d.Seconds()) }

// histogram receives tally's per-bucket sample counts and observes each
// sample at its bucket's upper bound (the lower bound for the last bucket).
type histogram struct{ o prom.Observer }

func (h histogram) ValueBucket(lower, upper float64) tally.CachedHistogramBucket {
	if math.IsInf(upper, 1) {
		upper = lower
	}
	return bucket{h.o, upper}
}

func (h histogram) DurationBucket(lower, upper time.Duration) tally.CachedHistogramBucket {
	if upper == time.Duration(math.MaxInt64) {
		upper = lower
	}
	return bucket{h.o, upper.Seconds()}
}

type bucket struct {
	o     prom.Observer
	value float64
}

func (b bucket) ReportSamples(n int64) {
	for range n {
		b.o.Observe(b.value)
	}
}
