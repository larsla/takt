package sdk

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/larsla/takt/internal/server"
	"github.com/larsla/takt/internal/servertest"
	"github.com/larsla/takt/internal/transport"
)

// longWorkflow takes many decision tasks and uses most features, so a
// resumed replayer has to get all of them right.
func longWorkflow(ctx Context, steps int) (string, error) {
	ctx = WithActivityOptions(ctx, shortActivity)
	ctx = WithLocalActivityOptions(ctx, LocalActivityOptions{ScheduleToCloseTimeout: 10 * time.Second})
	var parts []string
	for i := range steps {
		var s string
		switch i % 4 {
		case 0:
			if err := ExecuteActivity(ctx, greet, fmt.Sprint(i)).Get(ctx, &s); err != nil {
				return "", err
			}
		case 1:
			if err := ExecuteLocalActivity(ctx, greet, fmt.Sprint(i)).Get(ctx, &s); err != nil {
				return "", err
			}
		case 2:
			if err := SideEffect(ctx, func(Context) interface{} { return fmt.Sprint(i) }).Get(&s); err != nil {
				return "", err
			}
			if err := Sleep(ctx, time.Second); err != nil {
				return "", err
			}
		case 3:
			GetSignalChannel(ctx, "step").Receive(ctx, &s)
		}
		if v := GetVersion(ctx, "v", DefaultVersion, 1); v != 1 {
			return "", fmt.Errorf("version %d", v)
		}
		parts = append(parts, s)
	}
	return strings.Join(parts, ","), nil
}

func runLong(t *testing.T, e *stackEnv, id string, steps int) string {
	t.Helper()
	run := e.run(id, longWorkflow, steps)
	go func() {
		for range steps / 4 {
			for e.ctx.Err() == nil {
				if err := e.client.SignalWorkflow(e.ctx, id, "", "step", "sig"); err == nil {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			time.Sleep(300 * time.Millisecond)
		}
	}()
	var result string
	if err := run.Get(e.ctx, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestStickyCache(t *testing.T) {
	t.Parallel()
	const steps = 8
	want := "Hello 0!,Hello 1!,2,sig,Hello 4!,Hello 5!,6,sig"

	e := newStackEnv(t)
	cached := e.worker(WorkerOptions{}, func(w *Worker) {
		w.RegisterWorkflow(longWorkflow)
		w.RegisterActivity(greet)
	})
	if got := runLong(t, e, "sticky", steps); got != want {
		t.Fatalf("cached result %q", got)
	}
	if n := cached.fullReplays.Load(); n != 1 {
		t.Fatalf("cached worker replayed the full history %d times, want 1", n)
	}
	cached.Stop()
	if cached.cache.len() != 0 {
		t.Fatal("stopping the worker should empty the cache")
	}

	e2 := newStackEnv(t)
	uncached := e2.worker(WorkerOptions{DisableStickyExecution: true}, func(w *Worker) {
		w.RegisterWorkflow(longWorkflow)
		w.RegisterActivity(greet)
	})
	if got := runLong(t, e2, "no-sticky", steps); got != want {
		t.Fatalf("uncached result %q", got)
	}
	if n := uncached.fullReplays.Load(); n < 4 {
		t.Fatalf("uncached worker replayed only %d times", n)
	}
}

func TestStickyCacheEviction(t *testing.T) {
	c := newStickyCache(2)
	mk := func(id string) *stickyEntry {
		return &stickyEntry{runID: id, r: newReplayer(replayerConfig{registry: newRegistry(nil)})}
	}
	c.put(mk("a"))
	c.put(mk("b"))
	c.put(mk("a"))
	c.put(mk("c"))
	if c.len() != 2 || c.take("b") != nil || c.take("a") == nil || c.take("c") == nil {
		t.Fatal("least recently used entry should be evicted")
	}
	(*stickyCache)(nil).put(mk("x"))
	newStickyCache(0).put(mk("y"))
	if (*stickyCache)(nil).take("x") != nil || (*stickyCache)(nil).len() != 0 {
		t.Fatal("nil cache")
	}
}

// Two servers and two workers share one NATS. Decision tasks land on both
// workers, so cached state is often stale and must be detected.
func TestReplicasAndWorkers(t *testing.T) {
	t.Parallel()
	e := newStackEnv(t)
	second, err := server.Start(context.Background(), server.Config{NC: e.stack.NC, JS: e.stack.JS})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Stop()
	register := func(w *Worker) {
		w.RegisterWorkflow(longWorkflow)
		w.RegisterActivity(greet)
	}
	e.worker(WorkerOptions{Identity: "w1"}, register)
	other, err := transport.New(e.stack.NC, transport.Options{PollTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	w2 := NewWorker(other, servertest.Domain, testTaskList, WorkerOptions{Identity: "w2"})
	register(w2)
	if err := w2.Start(); err != nil {
		t.Fatal(err)
	}
	defer w2.Stop()

	const n = 6
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			if got := runLong(t, e, fmt.Sprintf("replica-%d", i), 8); !strings.HasPrefix(got, "Hello 0!") {
				t.Errorf("workflow %d: %q", i, got)
			}
		})
	}
	wg.Wait()
}
