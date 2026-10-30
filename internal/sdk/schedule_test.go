package sdk

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/larsla/takt/api"
	"github.com/larsla/takt/internal/servertest"
)

// cronCounter counts its runs through the last completion result.
func cronCounter(ctx Context) (int, error) {
	n := 0
	if HasLastCompletionResult(ctx) {
		if err := GetLastCompletionResult(ctx, &n); err != nil {
			return 0, err
		}
	}
	if GetInfo(ctx).CronSchedule == nil {
		return 0, errors.New("cron schedule missing from info")
	}
	return n + 1, nil
}

func TestCronSchedule(t *testing.T) {
	t.Parallel()
	e := newStackEnv(t)
	e.worker(WorkerOptions{}, func(w *Worker) { w.RegisterWorkflow(cronCounter) })
	opts := e.options("cron")
	opts.CronSchedule = "@every 1s"
	if _, err := e.client.StartWorkflow(e.ctx, opts, cronCounter); err != nil {
		t.Fatal(err)
	}
	query := "WorkflowID = 'cron' AND CloseStatus = 'CONTINUED_AS_NEW'"
	deadline := time.Now().Add(20 * time.Second)
	for {
		resp, err := e.client.CountWorkflow(e.ctx, &api.CountWorkflowExecutionsRequest{Query: query})
		if err != nil {
			t.Fatal(err)
		}
		if resp.Count >= 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d cron runs finished", resp.Count)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err := e.client.TerminateWorkflow(e.ctx, "cron", "", "enough", nil); err != nil {
		t.Fatal(err)
	}
	list, err := e.client.ListWorkflow(e.ctx, &api.ListWorkflowExecutionsRequest{Query: query + " ORDER BY StartTime ASC"})
	if err != nil {
		t.Fatal(err)
	}
	// Each run reports one more than the run before.
	for i, info := range list.Executions[:3] {
		it := e.client.GetWorkflowHistory(e.ctx, "cron", info.Execution.RunID, false, api.HistoryEventFilterTypeCloseEvent)
		ev, err := it.Next()
		if err != nil {
			t.Fatal(err)
		}
		a := ev.WorkflowExecutionContinuedAsNewEventAttributes
		if a == nil || a.Initiator != api.ContinueAsNewInitiatorCronSchedule || string(a.LastCompletionResult) != fmt.Sprintf("%d\n", i+1) {
			t.Fatalf("run %d closed with %+v", i, ev)
		}
	}
}

func flakyWorkflow(ctx Context) (int32, error) {
	if attempt := GetInfo(ctx).Attempt; attempt < 2 {
		return 0, NewCustomError("try-again", attempt)
	}
	return GetInfo(ctx).Attempt, nil
}

func TestWorkflowRetryPolicy(t *testing.T) {
	t.Parallel()
	e := newStackEnv(t)
	e.worker(WorkerOptions{}, func(w *Worker) { w.RegisterWorkflow(flakyWorkflow) })
	opts := e.options("wf-retry")
	opts.RetryPolicy = &RetryPolicy{InitialInterval: time.Second, MaximumAttempts: 5}
	run, err := e.client.ExecuteWorkflow(e.ctx, opts, flakyWorkflow)
	if err != nil {
		t.Fatal(err)
	}
	var attempt int32
	if err := run.Get(e.ctx, &attempt); err != nil || attempt != 2 {
		t.Fatalf("attempt %d, err %v", attempt, err)
	}

	opts = e.options("wf-retry-exhausted")
	opts.RetryPolicy = &RetryPolicy{InitialInterval: time.Second, MaximumAttempts: 2}
	run, err = e.client.ExecuteWorkflow(e.ctx, opts, flakyWorkflow)
	if err != nil {
		t.Fatal(err)
	}
	var custom *CustomError
	if err := run.Get(e.ctx, nil); !errors.As(err, &custom) || custom.Reason() != "try-again" {
		t.Fatalf("expected the last failure, got %v", err)
	}
}

func TestVisibilityQueries(t *testing.T) {
	t.Parallel()
	e := newStackEnv(t)
	e.worker(WorkerOptions{}, func(w *Worker) { w.RegisterWorkflow(failingWorkflow) })
	start := func(id string, wf interface{}, sa map[string]interface{}) {
		t.Helper()
		opts := e.options(id)
		opts.SearchAttributes = sa
		if _, err := e.client.StartWorkflow(e.ctx, opts, wf); err != nil {
			t.Fatal(err)
		}
	}
	start("open-a", "neverRuns", map[string]interface{}{"Customer": "acme", "Amount": 10})
	start("open-b", "neverRuns", map[string]interface{}{"Customer": "globex", "Amount": 99})
	start("failed", failingWorkflow, nil)
	if err := e.client.GetWorkflow(e.ctx, "failed", "").Get(e.ctx, nil); err == nil {
		t.Fatal("expected failure")
	}

	count := func(q string) int64 {
		t.Helper()
		resp, err := e.client.CountWorkflow(e.ctx, &api.CountWorkflowExecutionsRequest{Query: q})
		if err != nil {
			t.Fatalf("%q: %v", q, err)
		}
		return resp.Count
	}
	for q, want := range map[string]int64{
		"":                                 3,
		"CloseTime = missing":              2,
		"CloseStatus = 'FAILED'":           1,
		"CloseStatus = 1":                  1,
		"WorkflowType = 'neverRuns'":       2,
		"Customer = 'acme'":                1,
		"Amount > 50":                      1,
		"Customer = 'acme' OR Amount > 50": 2,
		"WorkflowID = 'failed' AND TaskList = '" + testTaskList + "'": 1,
	} {
		if got := count(q); got != want {
			t.Errorf("%q: count %d, want %d", q, got, want)
		}
	}
	page, err := e.client.ListWorkflow(e.ctx, &api.ListWorkflowExecutionsRequest{PageSize: 2, Query: "ORDER BY StartTime ASC"})
	if err != nil || len(page.Executions) != 2 || page.Executions[0].Execution.WorkflowID != "open-a" || len(page.NextPageToken) == 0 {
		t.Fatalf("first page: %v %+v", err, page)
	}
	rest, err := e.client.ScanWorkflow(e.ctx, &api.ListWorkflowExecutionsRequest{PageSize: 2, Query: "ORDER BY StartTime ASC", NextPageToken: page.NextPageToken})
	if err != nil || len(rest.Executions) != 1 || rest.Executions[0].Execution.WorkflowID != "failed" {
		t.Fatalf("second page: %v %+v", err, rest)
	}
	var br *api.BadRequestError
	if _, err := e.client.CountWorkflow(e.ctx, &api.CountWorkflowExecutionsRequest{Query: "WorkflowType ="}); !errors.As(err, &br) {
		t.Fatalf("bad query: %v", err)
	}
}

func TestRetentionPurge(t *testing.T) {
	t.Parallel()
	// One retention "day" is a millisecond here.
	stack := servertest.New(t, servertest.Options{RetentionUnit: time.Millisecond})
	e := &stackEnv{t: t, stack: stack, client: NewClient(stack.Client, servertest.Domain, nil), ctx: newStackEnv(t).ctx}
	run, err := e.client.ExecuteWorkflow(e.ctx, e.options("old"), "neverRuns")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.client.StartWorkflow(e.ctx, e.options("still-open"), "neverRuns"); err != nil {
		t.Fatal(err)
	}
	if err := e.client.TerminateWorkflow(e.ctx, "old", "", "done", nil); err != nil {
		t.Fatal(err)
	}
	if err := run.Get(e.ctx, nil); err == nil {
		t.Fatal("expected terminated error")
	}
	time.Sleep(20 * time.Millisecond)
	n, err := stack.Server.Engine().PurgeExpired(e.ctx)
	if err != nil || n != 1 {
		t.Fatalf("purged %d, err %v", n, err)
	}
	var nf *api.EntityNotExistsError
	if _, err := e.client.DescribeWorkflowExecution(e.ctx, "old", run.GetRunID()); !errors.As(err, &nf) {
		t.Fatalf("describe purged run: %v", err)
	}
	if _, err := e.client.DescribeWorkflowExecution(e.ctx, "old", ""); !errors.As(err, &nf) {
		t.Fatalf("current pointer should be gone: %v", err)
	}
	resp, err := e.client.CountWorkflow(e.ctx, &api.CountWorkflowExecutionsRequest{})
	if err != nil || resp.Count != 1 {
		t.Fatalf("count after purge: %v %+v", err, resp)
	}
	// The ID can be reused once its run is gone.
	if _, err := e.client.StartWorkflow(e.ctx, e.options("old"), "neverRuns"); err != nil {
		t.Fatal(err)
	}
}
