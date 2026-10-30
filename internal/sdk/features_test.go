package sdk

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/larsla/takt/api"
)

var shortActivity = ActivityOptions{ScheduleToStartTimeout: time.Minute, StartToCloseTimeout: time.Minute}

// signalOrTimerWorkflow waits for a signal or a timer, whichever comes first.
func signalOrTimerWorkflow(ctx Context, wait time.Duration) (string, error) {
	var got string
	timedOut := false
	s := NewSelector(ctx)
	s.AddReceive(GetSignalChannel(ctx, "name"), func(c Channel, more bool) {
		c.Receive(ctx, &got)
	})
	s.AddFuture(NewTimer(ctx, wait), func(f Future) { timedOut = true })
	s.Select(ctx)
	if timedOut {
		return "timeout", nil
	}
	return "signal:" + got, nil
}

func TestSelectorSignalAndTimer(t *testing.T) {
	t.Parallel()
	e := newStackEnv(t)
	e.worker(WorkerOptions{}, func(w *Worker) { w.RegisterWorkflow(signalOrTimerWorkflow) })

	run := e.run("sel-signal", signalOrTimerWorkflow, time.Hour)
	if err := e.client.SignalWorkflow(e.ctx, run.GetID(), "", "name", "bob"); err != nil {
		t.Fatal(err)
	}
	var result string
	if err := run.Get(e.ctx, &result); err != nil || result != "signal:bob" {
		t.Fatalf("result %q, err %v", result, err)
	}

	run = e.run("sel-timer", signalOrTimerWorkflow, time.Second)
	if err := run.Get(e.ctx, &result); err != nil || result != "timeout" {
		t.Fatalf("result %q, err %v", result, err)
	}
}

func childWorkflow(ctx Context, n int) (int, error) {
	if n < 0 {
		return 0, NewCustomError("negative", n)
	}
	return n * 2, nil
}

func parentWorkflow(ctx Context, n int) (string, error) {
	ctx = WithChildOptions(ctx, ChildWorkflowOptions{ExecutionStartToCloseTimeout: time.Minute})
	cf := ExecuteChildWorkflow(ctx, childWorkflow, n)
	var exec WorkflowExecution
	if err := cf.GetChildWorkflowExecution().Get(ctx, &exec); err != nil {
		return "", err
	}
	var doubled int
	if err := cf.Get(ctx, &doubled); err != nil {
		var custom *CustomError
		if errors.As(err, &custom) {
			var detail int
			_ = custom.Details(&detail)
			return fmt.Sprintf("child failed: %s %d", custom.Reason(), detail), nil
		}
		return "", err
	}
	return fmt.Sprintf("%s=%d", exec.ID[:len(GetInfo(ctx).WorkflowExecution.RunID)], doubled), nil
}

func TestChildWorkflow(t *testing.T) {
	t.Parallel()
	e := newStackEnv(t)
	e.worker(WorkerOptions{}, func(w *Worker) {
		w.RegisterWorkflow(parentWorkflow)
		w.RegisterWorkflow(childWorkflow)
	})
	run := e.run("parent-ok", parentWorkflow, 21)
	var result string
	if err := run.Get(e.ctx, &result); err != nil {
		t.Fatal(err)
	}
	if result != run.GetRunID()+"=42" {
		t.Fatalf("result %q", result)
	}
	if err := e.run("parent-fail", parentWorkflow, -1).Get(e.ctx, &result); err != nil || result != "child failed: negative -1" {
		t.Fatalf("result %q, err %v", result, err)
	}
}

// counters count calls per key (usually workflow ID), so parallel and
// repeated tests don't see each other's calls.
var counters sync.Map

func count(key string) int32 {
	n, _ := counters.LoadOrStore(key, new(atomic.Int32))
	return n.(*atomic.Int32).Add(1)
}

func counted(key string) int32 {
	n, ok := counters.Load(key)
	if !ok {
		return 0
	}
	return n.(*atomic.Int32).Load()
}

func resetCount(key string) { counters.Delete(key) }

// replayHeavyWorkflow uses side effects, versions and mutable side effects
// across several decision tasks, so each later task replays the earlier ones.
func replayHeavyWorkflow(ctx Context) (string, error) {
	var a int
	if err := SideEffect(ctx, func(ctx Context) interface{} {
		count("se:" + GetInfo(ctx).WorkflowExecution.ID)
		return 7
	}).Get(&a); err != nil {
		return "", err
	}
	v := GetVersion(ctx, "change-1", DefaultVersion, 2)
	if err := Sleep(ctx, time.Second); err != nil {
		return "", err
	}
	var m string
	if err := MutableSideEffect(ctx, "m", func(Context) interface{} { return "fixed" }, func(x, y interface{}) bool {
		return x.(string) == y.(string)
	}).Get(&m); err != nil {
		return "", err
	}
	v2 := GetVersion(ctx, "change-1", DefaultVersion, 2)
	if err := Sleep(ctx, time.Second); err != nil {
		return "", err
	}
	if !IsReplaying(ctx) && Now(ctx).IsZero() {
		return "", errors.New("zero workflow time")
	}
	return fmt.Sprintf("%d/%d/%d/%s", a, v, v2, m), nil
}

func TestSideEffectsAndVersions(t *testing.T) {
	t.Parallel()
	e := newStackEnv(t)
	resetCount("se:replay-heavy-run")
	e.worker(WorkerOptions{}, func(w *Worker) { w.RegisterWorkflow(replayHeavyWorkflow) })
	var result string
	if err := e.run("replay-heavy-run", replayHeavyWorkflow).Get(e.ctx, &result); err != nil {
		t.Fatal(err)
	}
	if result != "7/2/2/fixed" {
		t.Fatalf("result %q", result)
	}
	if n := counted("se:replay-heavy-run"); n != 1 {
		t.Fatalf("side effect ran %d times, want 1", n)
	}
}

func flakyLocal(ctx context.Context, s string) (string, error) {
	if count("la:"+GetActivityInfo(ctx).WorkflowExecution.ID) == 1 {
		return "", errors.New("first attempt fails")
	}
	return strings.ToUpper(s), nil
}

func localActivityWorkflow(ctx Context) (string, error) {
	ctx = WithLocalActivityOptions(ctx, LocalActivityOptions{
		ScheduleToCloseTimeout: 10 * time.Second,
		RetryPolicy:            &RetryPolicy{InitialInterval: 10 * time.Millisecond, MaximumAttempts: 3},
	})
	var a, b string
	if err := ExecuteLocalActivity(ctx, flakyLocal, "one").Get(ctx, &a); err != nil {
		return "", err
	}
	if err := Sleep(ctx, time.Second); err != nil {
		return "", err
	}
	if err := ExecuteLocalActivity(ctx, flakyLocal, "two").Get(ctx, &b); err != nil {
		return "", err
	}
	return a + b, nil
}

func TestLocalActivity(t *testing.T) {
	t.Parallel()
	e := newStackEnv(t)
	resetCount("la:local")
	e.worker(WorkerOptions{}, func(w *Worker) { w.RegisterWorkflow(localActivityWorkflow) })
	var result string
	if err := e.run("local", localActivityWorkflow).Get(e.ctx, &result); err != nil {
		t.Fatal(err)
	}
	if result != "ONETWO" {
		t.Fatalf("result %q", result)
	}
	if n := counted("la:local"); n != 3 {
		t.Fatalf("local activity ran %d times, want 3", n)
	}
}

func queryWorkflow(ctx Context) error {
	state := "started"
	if err := SetQueryHandler(ctx, "state", func(prefix string) (string, error) {
		return prefix + state, nil
	}); err != nil {
		return err
	}
	GetSignalChannel(ctx, "advance").Receive(ctx, nil)
	state = "advanced"
	GetSignalChannel(ctx, "finish").Receive(ctx, nil)
	return nil
}

func TestQuery(t *testing.T) {
	t.Parallel()
	e := newStackEnv(t)
	e.worker(WorkerOptions{}, func(w *Worker) { w.RegisterWorkflow(queryWorkflow) })
	run := e.run("query", queryWorkflow)

	query := func(want string) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		var got string
		for time.Now().Before(deadline) {
			v, err := e.client.QueryWorkflow(e.ctx, run.GetID(), "", "state", "s=")
			if err == nil {
				if err := v.Get(&got); err != nil {
					t.Fatal(err)
				}
				if got == want {
					return
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("query never returned %q (last %q)", want, got)
	}
	query("s=started")
	if err := e.client.SignalWorkflow(e.ctx, run.GetID(), "", "advance", nil); err != nil {
		t.Fatal(err)
	}
	query("s=advanced")

	v, err := e.client.QueryWorkflow(e.ctx, run.GetID(), "", QueryTypeStackTrace)
	if err != nil {
		t.Fatal(err)
	}
	var trace string
	if err := v.Get(&trace); err != nil || !strings.Contains(trace, "finish.Receive") {
		t.Fatalf("stack trace %q, err %v", trace, err)
	}
	if _, err := e.client.QueryWorkflow(e.ctx, run.GetID(), "", "nope"); err == nil {
		t.Fatal("unknown query type should fail")
	}
	if err := e.client.SignalWorkflow(e.ctx, run.GetID(), "", "finish", nil); err != nil {
		t.Fatal(err)
	}
	if err := run.Get(e.ctx, nil); err != nil {
		t.Fatal(err)
	}
}

func counterWorkflow(ctx Context, n, limit int) (int, error) {
	if n >= limit {
		return n, nil
	}
	return 0, NewContinueAsNewError(ctx, counterWorkflow, n+1, limit)
}

func TestContinueAsNew(t *testing.T) {
	t.Parallel()
	e := newStackEnv(t)
	e.worker(WorkerOptions{}, func(w *Worker) { w.RegisterWorkflow(counterWorkflow) })
	var result int
	if err := e.run("counter", counterWorkflow, 0, 3).Get(e.ctx, &result); err != nil || result != 3 {
		t.Fatalf("result %d, err %v", result, err)
	}
}

func heartbeatingActivity(ctx context.Context) error {
	for {
		RecordActivityHeartbeat(ctx, "working")
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func cancellableWorkflow(ctx Context) (string, error) {
	ctx = WithActivityOptions(ctx, ActivityOptions{
		ScheduleToStartTimeout: time.Minute, StartToCloseTimeout: time.Minute, HeartbeatTimeout: time.Second,
		WaitForCancellation: true,
	})
	err := ExecuteActivity(ctx, heartbeatingActivity).Get(ctx, nil)
	if !IsCanceledError(err) {
		return "", fmt.Errorf("expected canceled activity, got %w", err)
	}
	// Clean up with a context that is not canceled.
	dctx, _ := NewDisconnectedContext(ctx)
	dctx = WithActivityOptions(dctx, shortActivity)
	var cleanup string
	if err := ExecuteActivity(dctx, greet, "cleanup").Get(dctx, &cleanup); err != nil {
		return "", err
	}
	return cleanup, ctx.Err()
}

func TestCancellation(t *testing.T) {
	t.Parallel()
	e := newStackEnv(t)
	e.worker(WorkerOptions{}, func(w *Worker) {
		w.RegisterWorkflow(cancellableWorkflow)
		w.RegisterActivity(heartbeatingActivity)
		w.RegisterActivity(greet)
	})
	run := e.run("cancel", cancellableWorkflow)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		desc, err := e.client.DescribeWorkflowExecution(e.ctx, run.GetID(), "")
		if err == nil && len(desc.PendingActivities) == 1 && desc.PendingActivities[0].State == api.PendingActivityStateStarted {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err := e.client.CancelWorkflow(e.ctx, run.GetID(), ""); err != nil {
		t.Fatal(err)
	}
	err := run.Get(e.ctx, nil)
	if !IsCanceledError(err) {
		t.Fatalf("expected canceled workflow, got %v", err)
	}
	events := historyTypes(t, e, run)
	if !containsType(events, api.EventTypeActivityTaskCanceled) || !containsType(events, api.EventTypeWorkflowExecutionCanceled) {
		t.Fatalf("history %v", events)
	}
}

func historyTypes(t *testing.T, e *stackEnv, run WorkflowRun) []api.EventType {
	t.Helper()
	it := e.client.GetWorkflowHistory(e.ctx, run.GetID(), run.GetRunID(), false, api.HistoryEventFilterTypeAllEvent)
	var out []api.EventType
	for it.HasNext() {
		ev, err := it.Next()
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, ev.EventType)
	}
	return out
}

func containsType(ts []api.EventType, want api.EventType) bool {
	for _, x := range ts {
		if x == want {
			return true
		}
	}
	return false
}

func failingActivity(ctx context.Context) (string, error) {
	n := count("retry:" + GetActivityInfo(ctx).WorkflowExecution.ID)
	if n < 3 {
		return "", NewCustomError("flaky", int(n))
	}
	if n == 3 {
		return "", errors.New("plain error")
	}
	return "", NewCustomError("fatal")
}

func retryWorkflow(ctx Context) (string, error) {
	ctx = WithActivityOptions(ctx, ActivityOptions{
		ScheduleToStartTimeout: time.Minute, StartToCloseTimeout: time.Minute,
		RetryPolicy: &RetryPolicy{InitialInterval: time.Second, MaximumAttempts: 10, NonRetriableErrorReasons: []string{"fatal"}},
	})
	err := ExecuteActivity(ctx, failingActivity).Get(ctx, nil)
	var custom *CustomError
	if !errors.As(err, &custom) {
		return "", fmt.Errorf("expected custom error, got %T %w", err, err)
	}
	return custom.Reason(), nil
}

func TestActivityRetries(t *testing.T) {
	t.Parallel()
	e := newStackEnv(t)
	resetCount("retry:retry")
	e.worker(WorkerOptions{}, func(w *Worker) {
		w.RegisterWorkflow(retryWorkflow)
		w.RegisterActivity(failingActivity)
	})
	var reason string
	if err := e.run("retry", retryWorkflow).Get(e.ctx, &reason); err != nil || reason != "fatal" {
		t.Fatalf("reason %q err %v", reason, err)
	}
	if n := counted("retry:retry"); n != 4 {
		t.Fatalf("activity ran %d times", n)
	}
}

func asyncActivity(ctx context.Context) (string, error) {
	asyncTokens <- GetActivityInfo(ctx).TaskToken
	return "", ErrActivityResultPending
}

var asyncTokens = make(chan []byte, 1)

func asyncWorkflow(ctx Context) (string, error) {
	ctx = WithActivityOptions(ctx, shortActivity)
	var s string
	err := ExecuteActivity(ctx, asyncActivity).Get(ctx, &s)
	return s, err
}

func TestAsyncActivityCompletion(t *testing.T) {
	t.Parallel()
	e := newStackEnv(t)
	e.worker(WorkerOptions{}, func(w *Worker) {
		w.RegisterWorkflow(asyncWorkflow)
		w.RegisterActivity(asyncActivity)
	})
	run := e.run("async", asyncWorkflow)
	token := <-asyncTokens
	if err := e.client.RecordActivityHeartbeat(e.ctx, token, "progress"); err != nil {
		t.Fatal(err)
	}
	if err := e.client.CompleteActivity(e.ctx, token, "done later", nil); err != nil {
		t.Fatal(err)
	}
	var result string
	if err := run.Get(e.ctx, &result); err != nil || result != "done later" {
		t.Fatalf("result %q err %v", result, err)
	}
}

// Versions of the same workflow type with different code, registered by name.
func waitThenGreetV1(ctx Context) (string, error) {
	GetSignalChannel(ctx, "go").Receive(ctx, nil)
	ctx = WithActivityOptions(ctx, shortActivity)
	var s string
	err := ExecuteActivity(ctx, greet, "v1").Get(ctx, &s)
	return s, err
}

func waitThenGreetV2(ctx Context) (string, error) {
	ctx = WithActivityOptions(ctx, shortActivity)
	// Changed code: schedules an activity before waiting.
	_ = ExecuteActivity(ctx, greet, "early")
	GetSignalChannel(ctx, "go").Receive(ctx, nil)
	var s string
	err := ExecuteActivity(ctx, greet, "v2").Get(ctx, &s)
	return s, err
}

func TestNondeterminismPolicies(t *testing.T) {
	t.Parallel()
	for _, policy := range []NonDeterministicWorkflowPolicy{NonDeterministicWorkflowPolicyBlockWorkflow, NonDeterministicWorkflowPolicyFailWorkflow} {
		t.Run(fmt.Sprint(policy), func(t *testing.T) {
			t.Parallel()
			e := newStackEnv(t)
			w1 := e.worker(WorkerOptions{}, func(w *Worker) {
				w.RegisterWorkflowWithOptions(waitThenGreetV1, RegisterWorkflowOptions{Name: "waitThenGreet"})
				w.RegisterActivity(greet)
			})
			run := e.run("nd", "waitThenGreet")
			// Wait for the first decision to finish under v1.
			deadline := time.Now().Add(10 * time.Second)
			for time.Now().Before(deadline) && !containsType(historyTypes(t, e, run), api.EventTypeDecisionTaskCompleted) {
				time.Sleep(50 * time.Millisecond)
			}
			w1.Stop()

			e.worker(WorkerOptions{NonDeterministicWorkflowPolicy: policy}, func(w *Worker) {
				w.RegisterWorkflowWithOptions(waitThenGreetV2, RegisterWorkflowOptions{Name: "waitThenGreet"})
				w.RegisterActivity(greet)
			})
			if err := e.client.SignalWorkflow(e.ctx, run.GetID(), "", "go", nil); err != nil {
				t.Fatal(err)
			}
			if policy == NonDeterministicWorkflowPolicyFailWorkflow {
				err := run.Get(e.ctx, nil)
				var custom *CustomError
				if !errors.As(err, &custom) || custom.Reason() != "NonDeterministicWorkflowPolicyFailWorkflow" {
					t.Fatalf("expected nondeterminism failure, got %v", err)
				}
				return
			}
			deadline = time.Now().Add(10 * time.Second)
			for time.Now().Before(deadline) {
				if containsType(historyTypes(t, e, run), api.EventTypeDecisionTaskFailed) {
					return
				}
				time.Sleep(50 * time.Millisecond)
			}
			t.Fatalf("decision task never failed: %v", historyTypes(t, e, run))
		})
	}
}

func panickingWorkflow(ctx Context) error {
	panic("boom")
}

func TestWorkflowPanic(t *testing.T) {
	t.Parallel()
	e := newStackEnv(t)
	e.worker(WorkerOptions{WorkflowPanicPolicy: FailWorkflow}, func(w *Worker) { w.RegisterWorkflow(panickingWorkflow) })
	err := e.run("panic", panickingWorkflow).Get(e.ctx, nil)
	var pe *PanicError
	if !errors.As(err, &pe) || pe.Error() != "boom" || !strings.Contains(pe.StackTrace(), "panickingWorkflow") {
		t.Fatalf("expected panic error, got %v", err)
	}
}

// coroutinesWorkflow exercises Go, channels, Await and futures.
func coroutinesWorkflow(ctx Context) (int, error) {
	ch := NewChannel(ctx)
	buf := NewBufferedChannel(ctx, 2)
	done := 0
	for i := 1; i <= 3; i++ {
		Go(ctx, func(ctx Context) {
			ch.Send(ctx, i)
			done++
		})
	}
	sum := 0
	for range 3 {
		var v int
		ch.Receive(ctx, &v)
		sum += v
	}
	if err := Await(ctx, func() bool { return done == 3 }); err != nil {
		return 0, err
	}
	buf.SendAsync(10)
	buf.SendAsync(20)
	if buf.SendAsync(30) {
		return 0, errors.New("buffer should be full")
	}
	buf.Close()
	for {
		var v int
		if !buf.Receive(ctx, &v) {
			break
		}
		sum += v
	}
	f, s := NewFuture(ctx)
	Go(ctx, func(ctx Context) {
		_ = Sleep(ctx, time.Second)
		s.SetValue(100)
	})
	var hundred int
	if err := f.Get(ctx, &hundred); err != nil {
		return 0, err
	}
	ok, err := AwaitWithTimeout(ctx, time.Second, func() bool { return false })
	if err != nil || ok {
		return 0, fmt.Errorf("await with timeout: %v %w", ok, err)
	}
	return sum + hundred, nil
}

func TestCoroutinesAndChannels(t *testing.T) {
	t.Parallel()
	e := newStackEnv(t)
	e.worker(WorkerOptions{}, func(w *Worker) { w.RegisterWorkflow(coroutinesWorkflow) })
	var result int
	if err := e.run("coroutines", coroutinesWorkflow).Get(e.ctx, &result); err != nil || result != 136 {
		t.Fatalf("result %d err %v", result, err)
	}
}

func failingWorkflow(ctx Context) error {
	return errors.New("plain failure")
}

func TestWorkflowErrors(t *testing.T) {
	t.Parallel()
	e := newStackEnv(t)
	e.worker(WorkerOptions{}, func(w *Worker) { w.RegisterWorkflow(failingWorkflow) })
	err := e.run("fail", failingWorkflow).Get(e.ctx, nil)
	var ge *GenericError
	if !errors.As(err, &ge) || ge.Error() != "plain failure" {
		t.Fatalf("got %T %v", err, err)
	}
	opts := e.options("timeout")
	opts.ExecutionStartToCloseTimeout = time.Second
	run, err := e.client.ExecuteWorkflow(e.ctx, opts, "unregistered")
	if err != nil {
		t.Fatal(err)
	}
	var te *TimeoutError
	if err := run.Get(e.ctx, nil); !errors.As(err, &te) {
		t.Fatalf("got %T %v", err, err)
	}
	run = e.run("term", "unregistered")
	if err := e.client.TerminateWorkflow(e.ctx, run.GetID(), "", "bye", nil); err != nil {
		t.Fatal(err)
	}
	var term *TerminatedError
	if err := run.Get(e.ctx, nil); !errors.As(err, &term) {
		t.Fatalf("got %T %v", err, err)
	}
}
