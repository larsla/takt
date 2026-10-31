package sdk

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/uber-go/tally"

	"github.com/larsla/takt/api"
	"github.com/larsla/takt/internal/servertest"
	"github.com/larsla/takt/internal/transport"
)

// gauge returns the value of the task list gauge name for tl, or -1.
func gauge(scope tally.TestScope, name, tl string, t api.TaskListType) float64 {
	for _, g := range scope.Snapshot().Gauges() {
		tags := g.Tags()
		if g.Name() == name && tags["tasklist"] == tl && tags["tasklisttype"] == string(t) {
			return g.Value()
		}
	}
	return -1
}

// A task queued while nobody polls its task list shows in the gauges and
// runs once a worker starts.
func TestBacklogUntilWorkerStarts(t *testing.T) {
	t.Parallel()
	scope := tally.NewTestScope("", nil)
	stack := servertest.New(t, servertest.Options{Metrics: scope})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	c := NewClient(stack.Client, servertest.Domain, nil)
	const tl = "late-workers"
	run, err := c.ExecuteWorkflow(ctx, StartWorkflowOptions{
		ID: "backlog", TaskList: tl, ExecutionStartToCloseTimeout: time.Minute,
	}, helloWorkflow, "late")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	if err := stack.Server.Engine().ReportTaskLists(ctx); err != nil {
		t.Fatal(err)
	}
	if n := gauge(scope, "takt_tasklist_backlog", tl, api.TaskListTypeDecision); n != 1 {
		t.Fatalf("backlog %v, want 1", n)
	}
	if age := gauge(scope, "takt_tasklist_backlog_age_seconds", tl, api.TaskListTypeDecision); age < 1 {
		t.Fatalf("backlog age %v, want at least 1s", age)
	}
	if n := gauge(scope, "takt_tasklist_pollers", tl, api.TaskListTypeDecision); n != 0 {
		t.Fatalf("pollers %v, want 0", n)
	}

	w := NewWorker(stack.Client, servertest.Domain, tl, WorkerOptions{})
	w.RegisterWorkflow(helloWorkflow)
	w.RegisterActivity(greet)
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	var out string
	if err := run.Get(ctx, &out); err != nil || out != "Hello late!" {
		t.Fatalf("result %q, err %v", out, err)
	}
	if err := stack.Server.Engine().ReportTaskLists(ctx); err != nil {
		t.Fatal(err)
	}
	if n := gauge(scope, "takt_tasklist_backlog", tl, api.TaskListTypeDecision); n != 0 {
		t.Fatalf("backlog after the run %v, want 0", n)
	}
	if n := gauge(scope, "takt_tasklist_pollers", tl, api.TaskListTypeDecision); n < 1 {
		t.Fatalf("pollers %v, want the worker", n)
	}
}

// A run whose workflow type no worker registered shows why in describe.
func TestUnregisteredWorkflowTypeShows(t *testing.T) {
	t.Parallel()
	e := newStackEnv(t)
	e.worker(WorkerOptions{}, func(w *Worker) { w.RegisterWorkflow(helloWorkflow) })
	run := e.run("unregistered-type", "nobody-registers-this")
	deadline := time.Now().Add(20 * time.Second)
	for {
		desc, err := e.client.DescribeWorkflowExecution(e.ctx, run.GetID(), run.GetRunID())
		if err != nil {
			t.Fatal(err)
		}
		if pd := desc.PendingDecision; pd != nil && pd.LastFailureCause != "" {
			if !strings.Contains(string(pd.LastFailureDetails), "unable to find workflow type: nobody-registers-this") {
				t.Fatalf("details %q", pd.LastFailureDetails)
			}
			if pd.LastFailureIdentity == "" || pd.Attempt < 1 {
				t.Fatalf("pending decision %+v", pd)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no decision failure in %+v", desc.PendingDecision)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// A worker that dies mid-poll, as when its pod is killed, doesn't strand
// tasks queued after it: the next worker on the task list runs them.
func TestTaskAfterWorkerKilled(t *testing.T) {
	t.Parallel()
	stack := servertest.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	const tl = "killed-workers"

	nc, err := stack.NATS.Connect()
	if err != nil {
		t.Fatal(err)
	}
	doomed, err := transport.New(nc, transport.Options{PollTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	w := NewWorker(doomed, servertest.Domain, tl, WorkerOptions{})
	w.RegisterWorkflow(helloWorkflow)
	w.RegisterActivity(greet)
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	nc.Close() // the pod is gone; its pollers never come back
	defer w.Stop()

	c := NewClient(stack.Client, servertest.Domain, nil)
	run, err := c.ExecuteWorkflow(ctx, StartWorkflowOptions{
		ID: "after-kill", TaskList: tl, ExecutionStartToCloseTimeout: time.Minute,
	}, helloWorkflow, "again")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	w2 := NewWorker(stack.Client, servertest.Domain, tl, WorkerOptions{})
	w2.RegisterWorkflow(helloWorkflow)
	w2.RegisterActivity(greet)
	if err := w2.Start(); err != nil {
		t.Fatal(err)
	}
	defer w2.Stop()
	start := time.Now()
	var out string
	if err := run.Get(ctx, &out); err != nil || out != "Hello again!" {
		t.Fatalf("result %q, err %v", out, err)
	}
	t.Logf("ran %v after the new worker started", time.Since(start).Round(time.Millisecond))
}
