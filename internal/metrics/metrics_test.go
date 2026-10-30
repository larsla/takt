package metrics

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/uber-go/tally"
)

func TestPrometheus(t *testing.T) {
	var logs bytes.Buffer
	p := NewPrometheus(slog.New(slog.NewTextHandler(&logs, nil)))
	s := p.Scope.Tagged(map[string]string{"domain": "d.1/x"})
	s.Counter("takt-requests").Inc(2)
	s.Gauge("size").Update(7)
	s.Timer("latency").Record(30 * time.Millisecond)
	s.Histogram("count", tally.ValueBuckets{1, 10, 100}).RecordValue(5)
	s.Histogram("count", tally.ValueBuckets{1, 10, 100}).RecordValue(500)
	s.Histogram("wait", tally.DurationBuckets{time.Second, time.Minute}).RecordDuration(2 * time.Second)
	// Same name with other tags or kind: dropped with a warning.
	p.Scope.Tagged(map[string]string{"other": "x"}).Counter("takt-requests").Inc(1)
	p.Scope.Gauge("latency").Update(1)
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	p.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		`takt_requests{domain="d.1/x"} 2`,
		`size{domain="d.1/x"} 7`,
		`latency_bucket{domain="d.1/x",le="0.05"} 1`,
		`count_bucket{domain="d.1/x",le="10"} 1`,
		`count_count{domain="d.1/x"} 2`,
		`wait_bucket{domain="d.1/x",le="60"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %s in\n%s", want, body)
		}
	}
	if strings.Count(logs.String(), "metric reused with other tags") != 2 {
		t.Errorf("want two conflict warnings, got %s", logs.String())
	}
}
