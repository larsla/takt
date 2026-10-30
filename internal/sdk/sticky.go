package sdk

import (
	"container/list"
	"sync"
	"sync/atomic"

	"github.com/larsla/takt/api"
)

// A worker keeps the replayer of each workflow run it handled, with its
// goroutines parked, so the next decision task for that run only processes
// new events instead of replaying the whole history. takt has no sticky
// task lists: a task that reaches another worker is replayed in full there.

var stickyCacheSize atomic.Int64

func init() { stickyCacheSize.Store(10000) }

// SetStickyWorkflowCacheSize sets how many workflow runs each worker keeps
// in memory. Call it before creating workers.
func SetStickyWorkflowCacheSize(cacheSize int) { stickyCacheSize.Store(int64(cacheSize)) }

type stickyEntry struct {
	runID       string
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
}

func newStickyCache(size int) *stickyCache {
	return &stickyCache{max: size, order: list.New(), byRun: map[string]*list.Element{}}
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
	c.mu.Unlock()
	for _, ev := range evicted {
		ev.r.close()
	}
}

func (c *stickyCache) clear() {
	if c == nil {
		return
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
}

func (c *stickyCache) len() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}
