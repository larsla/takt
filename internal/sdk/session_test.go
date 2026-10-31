package sdk

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

var sessionOpts = &SessionOptions{ExecutionTimeout: time.Minute, CreationTimeout: 10 * time.Second, HeartbeatTimeout: 2 * time.Second}

// whereActivity returns the task list it ran from.
func whereActivity(ctx context.Context) (string, error) { return GetActivityInfo(ctx).TaskList, nil }

func sessionWorkflow(ctx Context) ([]string, error) {
	ctx = WithActivityOptions(ctx, shortActivity)
	sctx, err := CreateSession(ctx, sessionOpts)
	if err != nil {
		return nil, err
	}
	if _, err := CreateSession(sctx, sessionOpts); !errors.Is(err, errFoundExistingOpenSession) {
		return nil, errors.New("second session in one context should fail")
	}
	var lists []string
	for range 4 {
		var tl string
		if err := ExecuteActivity(sctx, whereActivity).Get(sctx, &tl); err != nil {
			return nil, err
		}
		lists = append(lists, tl)
	}
	info := GetSessionInfo(sctx)
	if info == nil || info.SessionID == "" || info.HostName == "" {
		return nil, errors.New("missing session info")
	}
	var open []*SessionInfo
	if err := SideEffect(ctx, func(Context) interface{} { return getRun(ctx).openSessions() }).Get(&open); err != nil || len(open) != 1 {
		return nil, errors.New("one open session expected")
	}

	// Recreate the session on the same worker from its token.
	token := info.GetRecreateToken()
	CompleteSession(sctx)
	var after string
	if err := ExecuteActivity(sctx, whereActivity).Get(ctx, &after); err == nil {
		return nil, errors.New("activity in a completed session context should be canceled")
	}
	rctx, err := RecreateSession(ctx, token, sessionOpts)
	if err != nil {
		return nil, err
	}
	var again string
	if err := ExecuteActivity(rctx, whereActivity).Get(rctx, &again); err != nil {
		return nil, err
	}
	CompleteSession(rctx)
	lists = append(lists, again)

	// Out of any session, activities use the normal task list.
	if err := ExecuteActivity(ctx, whereActivity).Get(ctx, &after); err != nil {
		return nil, err
	}
	return append(lists, after), nil
}

func TestSession(t *testing.T) {
	t.Parallel()
	e := newStackEnv(t)
	for _, id := range []string{"a", "b"} {
		e.worker(WorkerOptions{EnableSessionWorker: true, Identity: id}, func(w *Worker) {
			w.RegisterWorkflow(sessionWorkflow)
			w.RegisterActivity(whereActivity)
		})
	}
	var lists []string
	if err := e.run("session", sessionWorkflow).Get(e.ctx, &lists); err != nil {
		t.Fatal(err)
	}
	if len(lists) != 6 {
		t.Fatalf("lists %v", lists)
	}
	for _, tl := range lists[:5] {
		if tl != lists[0] || !strings.HasPrefix(tl, "__session:") {
			t.Fatalf("activities left the session's worker: %v", lists)
		}
	}
	if lists[5] != testTaskList {
		t.Fatalf("activity after the session ran from %q", lists[5])
	}
}

// sessionLimitWorkflow opens one session, fails to open a second on a
// worker that hosts one, and opens it once the first completes.
func sessionLimitWorkflow(ctx Context) (string, error) {
	ctx = WithActivityOptions(ctx, shortActivity)
	first, err := CreateSession(ctx, sessionOpts)
	if err != nil {
		return "", err
	}
	short := &SessionOptions{ExecutionTimeout: time.Minute, CreationTimeout: time.Second}
	if _, err := CreateSession(ctx, short); err == nil {
		return "", errors.New("second session should not fit")
	}
	CompleteSession(first)
	second, err := CreateSession(ctx, sessionOpts)
	if err != nil {
		return "", err
	}
	defer CompleteSession(second)
	return "ok", nil
}

func TestSessionLimit(t *testing.T) {
	t.Parallel()
	e := newStackEnv(t)
	e.worker(WorkerOptions{EnableSessionWorker: true, MaxConcurrentSessionExecutionSize: 1}, func(w *Worker) {
		w.RegisterWorkflow(sessionLimitWorkflow)
		w.RegisterActivity(whereActivity)
	})
	var out string
	if err := e.run("session-limit", sessionLimitWorkflow).Get(e.ctx, &out); err != nil || out != "ok" {
		t.Fatalf("result %q, err %v", out, err)
	}
}

// sessionFailWorkflow waits for its session to fail, then checks that an
// activity in it fails without being scheduled.
func sessionFailWorkflow(ctx Context) (string, error) {
	ctx = WithActivityOptions(ctx, shortActivity)
	sctx, err := CreateSession(ctx, sessionOpts)
	if err != nil {
		return "", err
	}
	sctx.Done().Receive(ctx, nil)
	err = ExecuteActivity(sctx, whereActivity).Get(ctx, nil)
	if !errors.Is(err, ErrSessionFailed) {
		return "", err
	}
	return "failed", nil
}

func TestSessionWorkerStops(t *testing.T) {
	t.Parallel()
	e := newStackEnv(t)
	e.worker(WorkerOptions{}, func(w *Worker) { w.RegisterWorkflow(sessionFailWorkflow) })
	sw := e.worker(WorkerOptions{EnableSessionWorker: true, Identity: "session-host"}, func(w *Worker) {
		w.RegisterActivity(whereActivity)
	})
	run := e.run("session-fail", sessionFailWorkflow)
	deadline := time.Now().Add(20 * time.Second)
	for {
		v, err := e.client.QueryWorkflow(e.ctx, run.GetID(), "", QueryTypeOpenSessions)
		var open []SessionInfo
		if err == nil && v.Get(&open) == nil && len(open) == 1 {
			if open[0].HostName == "" {
				t.Fatalf("open sessions %+v", open)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("session never opened: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	sw.Stop()
	var out string
	if err := run.Get(e.ctx, &out); err != nil || out != "failed" {
		t.Fatalf("result %q, err %v", out, err)
	}
}

func TestSessionValidation(t *testing.T) {
	if (WorkerOptions{EnableSessionWorker: true}).Validate() != nil {
		t.Fatal("session workers are supported")
	}
	s := newSessionWorker("tl", 0)
	if cap(s.slots) != defaultMaxConcurrentSessions || s.creationTaskList != "tl"+sessionCreationSuffix {
		t.Fatalf("session worker %+v", s)
	}
	if _, ok := s.activity("other", true); ok {
		t.Fatal("not a session activity")
	}
	// A recreated session on a full worker is refused.
	s = newSessionWorker("tl", 1)
	s.slots <- struct{}{}
	fn, _ := s.activity(sessionCreationActivityName, false)
	err := fn.(func(context.Context, string) error)(context.Background(), "id")
	var ce *CustomError
	if !errors.As(err, &ce) || ce.Reason() != errTooManySessionsMsg {
		t.Fatalf("err %v", err)
	}
	s.complete("unknown")
}
