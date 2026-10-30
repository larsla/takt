package sdk

import (
	"container/list"
	"sync"
	"sync/atomic"

	"github.com/uber-go/tally"

	"github.com/larsla/takt/api"
)

// A worker keeps the replayer of each workflow run it handled, with its
// goroutines parked, so the next decision task for that run only processes
// new events instead of replaying the whole history. The worker also polls
// its own sticky task list and tells the server about it when it completes
// a decision task, so the run's next decision task comes back to it. A
// task that reaches another worker anyway (after the sticky timeout) is
// replayed in full there.

var stickyCacheSize atomic.Int64

func init() { stickyCacheSize.Store(10000) }

// SetStickyWorkflowCacheSize sets how many workflow runs each worker keeps
// in memory. Call it before creating workers.
func SetStickyWorkflowCacheSize(cacheSize int) { stickyCacheSize.Store(int64(cacheSize)) }

type stickyEntry struct {
	runID       string
	exec        *api.WorkflowExecution
	r           *replayer
	consumed    int   // events of the history the replayer has seen
	lastStarted int64 // started event ID of the decision task it answered
	sent        []*api.Decision
}

type stickyCache struct {
	mu    sync.Mutex
	max   int
	order *list.List
	byRun map[string]*list.Element
	scope tally.Scope
	// onEvict is called, outside the lock, with entries dropped to make
	// room.
	onEvict func([]*stickyEntry)
}

func newStickyCache(size int) *stickyCache {
	return &stickyCache{max: size, order: list.New(), byRun: map[string]*list.Element{}, scope: tally.NoopScope}
}

func (c *stickyCache) hit() {
	if c != nil {
		c.scope.Counter(MetricStickyCacheHit).Inc(1)
	}
}

func (c *stickyCache) miss() {
	if c != nil {
		c.scope.Counter(MetricStickyCacheMiss).Inc(1)
	}
}

// take removes and returns the entry of a run, if cached.
func (c *stickyCache) take(runID string) *stickyEntry {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.byRun[runID]
	if !ok {
		return nil
	}
	c.order.Remove(el)
	delete(c.byRun, runID)
	return el.Value.(*stickyEntry)
}

// put caches an entry, evicting the least recently used ones over the limit.
func (c *stickyCache) put(e *stickyEntry) {
	if c == nil || c.max <= 0 {
		e.r.close()
		return
	}
	c.mu.Lock()
	if old, ok := c.byRun[e.runID]; ok {
		c.order.Remove(old)
		old.Value.(*stickyEntry).r.close()
	}
	c.byRun[e.runID] = c.order.PushFront(e)
	var evicted []*stickyEntry
	for c.order.Len() > c.max {
		last := c.order.Back()
		c.order.Remove(last)
		ev := last.Value.(*stickyEntry)
		delete(c.byRun, ev.runID)
		evicted = append(evicted, ev)
	}
	size := c.order.Len()
	c.mu.Unlock()
	c.scope.Gauge(MetricStickyCacheSize).Update(float64(size))
	if len(evicted) > 0 {
		c.scope.Counter(MetricStickyCacheEvict).Inc(int64(len(evicted)))
	}
	for _, ev := range evicted {
		ev.r.close()
	}
	if len(evicted) > 0 && c.onEvict != nil {
		c.onEvict(evicted)
	}
}

// drop removes e if it is still cached, and closes it. It reports whether
// it did; another decision task may have taken e meanwhile.
func (c *stickyCache) drop(e *stickyEntry) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	el, ok := c.byRun[e.runID]
	if !ok || el.Value.(*stickyEntry) != e {
		c.mu.Unlock()
		return false
	}
	c.order.Remove(el)
	delete(c.byRun, e.runID)
	c.mu.Unlock()
	e.r.close()
	return true
}

// clear empties the cache and returns what it held.
func (c *stickyCache) clear() []*stickyEntry {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	entries := make([]*stickyEntry, 0, len(c.byRun))
	for _, el := range c.byRun {
		entries = append(entries, el.Value.(*stickyEntry))
	}
	c.byRun = map[string]*list.Element{}
	c.order.Init()
	c.mu.Unlock()
	for _, e := range entries {
		e.r.close()
	}
	return entries
}

func (c *stickyCache) len() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}
