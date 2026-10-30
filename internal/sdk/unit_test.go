package sdk

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/larsla/takt/api"
)

func TestDefaultConverter(t *testing.T) {
	dc := GetDefaultDataConverter()
	data, err := dc.ToData("a", 1, map[string]int{"k": 2})
	if err != nil {
		t.Fatal(err)
	}
	var s string
	var n int
	var m map[string]int
	if err := dc.FromData(data, &s, &n, &m); err != nil || s != "a" || n != 1 || m["k"] != 2 {
		t.Fatalf("round trip: %v %q %d %v", err, s, n, m)
	}
	raw, _ := dc.ToData([]byte("raw"))
	var back []byte
	if err := dc.FromData(raw, &back); err != nil || string(back) != "raw" {
		t.Fatalf("bytes: %v %q", err, back)
	}
	if _, err := dc.ToData(make(chan int)); err == nil {
		t.Error("channel should not encode")
	}
	if err := dc.FromData([]byte("x"), &n); err == nil {
		t.Error("bad JSON should not decode")
	}

	v := newEncodedValue(nil, nil)
	if v.HasValue() || !errors.Is(v.Get(&s), ErrNoData) {
		t.Error("empty value")
	}
	vs := newEncodedValues(data, nil)
	if !vs.HasValues() || vs.Get(&s) != nil || s != "a" {
		t.Error("values")
	}
	if !errors.Is((&encodedValues{}).Get(&s), ErrNoData) {
		t.Error("empty values")
	}
	d := ErrorDetailsValues{"x", 2}
	if err := d.Get(&s, &n); err != nil || s != "x" || n != 2 {
		t.Fatalf("details: %v", err)
	}
	if !errors.Is(d.Get(&s, &n, &m), ErrTooManyArg) || !errors.Is((ErrorDetailsValues{}).Get(&s), ErrNoData) {
		t.Error("details errors")
	}
	if err := assignValue(1, &s); err == nil {
		t.Error("int is not assignable to string")
	}
	if err := assignValue(1, n); err == nil {
		t.Error("non-pointer target")
	}
	var p *int
	if err := assignValue(nil, &p); err != nil || p != nil {
		t.Error("nil assign")
	}
}

func TestErrorMapping(t *testing.T) {
	dc := GetDefaultDataConverter()
	cases := []struct {
		err    error
		reason string
		check  func(error) bool
	}{
		{NewCustomError("r", "d"), "r", func(e error) bool {
			var c *CustomError
			var d string
			return errors.As(e, &c) && c.HasDetails() && c.Details(&d) == nil && d == "d"
		}},
		{NewCanceledError("why"), errReasonCanceled, func(e error) bool {
			var c *CanceledError
			var d string
			return errors.As(e, &c) && c.Details(&d) == nil && d == "why"
		}},
		{newPanicError("p", "stack"), errReasonPanic, func(e error) bool {
			var p *PanicError
			return errors.As(e, &p) && p.Error() == "p" && p.StackTrace() == "stack"
		}},
		{NewHeartbeatTimeoutError("hb"), errReasonTimeout + " HEARTBEAT", func(e error) bool {
			var te *TimeoutError
			var d string
			return errors.As(e, &te) && te.TimeoutType() == api.TimeoutTypeHeartbeat && te.Details(&d) == nil && d == "hb"
		}},
		{errors.New("plain"), errReasonGeneric, func(e error) bool {
			var g *GenericError
			return errors.As(e, &g) && g.Error() == "plain"
		}},
	}
	for _, c := range cases {
		reason, details := errorDetails(c.err, dc)
		if reason != c.reason {
			t.Errorf("%v: reason %q, want %q", c.err, reason, c.reason)
		}
		if back := errorFromDetails(reason, details, dc); !c.check(back) {
			t.Errorf("%v: round trip gave %T %v", c.err, back, back)
		}
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Error("reserved reason prefix should panic")
			}
		}()
		_ = NewCustomError("cadenceInternal:x")
	}()
	if !IsCanceledError(ErrCanceled) || IsCanceledError(errors.New("x")) {
		t.Error("IsCanceledError")
	}
	if NewCustomError("r").HasDetails() || !errors.Is(NewCustomError("r").Details(), ErrNoData) {
		t.Error("custom without details")
	}
	if (&TerminatedError{}).Error() != "Terminated" || (&UnknownExternalWorkflowExecutionError{}).Error() == "" ||
		(&ContinueAsNewError{}).Error() != "ContinueAsNew" || ErrDeadlineExceeded.Error() == "" {
		t.Error("error strings")
	}
}

type activities struct{}

func (a *activities) Upper(_ context.Context, s string) (string, error) {
	return strings.ToUpper(s), nil
}
func (a *activities) Fail() error { return errors.New("x") }

func TestRegistry(t *testing.T) {
	r := newRegistry(nil)
	r.RegisterWorkflow(helloWorkflow)
	r.RegisterWorkflowWithOptions(childWorkflow, RegisterWorkflowOptions{Name: "child"})
	r.RegisterActivity(greet)
	r.RegisterActivity(&activities{})

	if got := r.workflowName(helloWorkflow); !strings.HasSuffix(got, "sdk.helloWorkflow") {
		t.Errorf("workflow name %q", got)
	}
	if r.workflowName(childWorkflow) != "child" || r.workflowName("x") != "x" {
		t.Error("workflow alias")
	}
	a := &activities{}
	if got := r.activityName(a.Upper); !strings.HasSuffix(got, "(*activities).Upper") {
		t.Errorf("method activity name %q", got)
	}
	prefixed := newRegistry(nil)
	prefixed.RegisterActivityWithOptions(&activities{}, RegisterActivityOptions{Name: "p_"})
	if _, ok := prefixed.getActivity("p_Upper"); !ok || prefixed.activityName(a.Upper) != "p_Upper" {
		t.Error("prefixed method missing")
	}
	short := newRegistry(nil)
	short.RegisterActivityWithOptions(&activities{}, RegisterActivityOptions{EnableShortName: true})
	if _, ok := short.getActivity("Upper"); !ok {
		t.Error("short name missing")
	}
	if _, ok := r.getActivity(r.activityName(a.Fail)); !ok {
		t.Error("method missing")
	}
	if len(r.workflowNames()) != 2 || len(r.activityNames()) != 3 {
		t.Errorf("names %v %v", r.workflowNames(), r.activityNames())
	}
	child := newRegistry(r)
	if _, ok := child.getWorkflow("child"); !ok || child.workflowName(childWorkflow) != "child" {
		t.Error("parent lookup")
	}
	for name, f := range map[string]func(){
		"dup workflow":     func() { r.RegisterWorkflow(helloWorkflow) },
		"bad workflow":     func() { r.RegisterWorkflow(greet) },
		"not func":         func() { r.RegisterWorkflow(1) },
		"dup activity":     func() { r.RegisterActivity(greet) },
		"bad activity":     func() { r.RegisterActivity(func() {}) },
		"nil activity":     func() { r.RegisterActivity(nil) },
		"empty struct":     func() { r.RegisterActivity(&struct{}{}) },
		"wrong 2nd result": func() { r.RegisterActivity(func() (int, int) { return 0, 0 }) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: expected panic", name)
				}
			}()
			f()
		}()
	}
	if err := validateArgs(greet, []interface{}{"a"}, 1); err != nil {
		t.Error(err)
	}
	if validateArgs(greet, []interface{}{1}, 1) == nil || validateArgs(greet, nil, 1) == nil ||
		validateArgs(greet, []interface{}{nil}, 1) == nil {
		t.Error("bad args accepted")
	}
}

func TestContextCancellation(t *testing.T) {
	ctx, cancel := WithCancel(WithValue(background, contextKey("k"), "v"))
	child, childCancel := WithCancel(ctx)
	defer childCancel()
	disc, discCancel := NewDisconnectedContext(ctx)
	defer discCancel()
	called := 0
	onCancel(child, func() { called++ })
	cancel()
	if !errors.Is(child.Err(), ErrCanceled) || called != 1 || child.Value(contextKey("k")) != "v" {
		t.Fatalf("child: %v %d", child.Err(), called)
	}
	if disc.Err() != nil || disc.Value(contextKey("k")) != "v" {
		t.Fatal("disconnected context was canceled")
	}
	onCancel(child, func() { called++ })
	if called != 2 {
		t.Fatal("onCancel on canceled context should run right away")
	}
	ok, more := child.Done().ReceiveAsyncWithMoreFlag(nil)
	if ok || more {
		t.Fatal("done channel should be closed")
	}
	if _, has := background.Deadline(); has || background.Done() != nil || background.Err() != nil {
		t.Fatal("background")
	}
}

func TestDispatcherDeadlockAndStack(t *testing.T) {
	old := deadlockTimeout
	deadlockTimeout = 50 * time.Millisecond
	defer func() { deadlockTimeout = old }()

	d := newDispatcher()
	release := make(chan struct{})
	d.newCoroutine(background, "stuck", func(Context) { <-release })
	err := d.executeUntilAllBlocked()
	if err == nil || !strings.Contains(err.Error(), "deadlock") {
		t.Fatalf("expected deadlock error, got %v", err)
	}
	close(release)

	d = newDispatcher()
	ch := &channelImpl{name: "wait"}
	d.newCoroutine(background, "waiter", func(ctx Context) { ch.Receive(ctx, nil) })
	if err := d.executeUntilAllBlocked(); err != nil {
		t.Fatal(err)
	}
	if st := d.stackTrace(); !strings.Contains(st, "waiter") || !strings.Contains(st, "wait.Receive") {
		t.Fatalf("stack trace %q", st)
	}
	d.close()
	d.close()

	d = newDispatcher()
	d.newCoroutine(background, "", func(Context) { panic("oops") })
	var pe *PanicError
	if err := d.executeUntilAllBlocked(); !errors.As(err, &pe) {
		t.Fatalf("expected panic error, got %v", err)
	}
}

func TestRetryPolicyValidation(t *testing.T) {
	bad := []RetryPolicy{
		{},
		{InitialInterval: time.Second},
		{InitialInterval: time.Second, MaximumAttempts: 1, BackoffCoefficient: 0.5},
		{InitialInterval: time.Second, MaximumAttempts: 1, MaximumInterval: time.Millisecond},
		{InitialInterval: time.Second, MaximumAttempts: -1},
		{InitialInterval: time.Second, ExpirationInterval: -1},
	}
	for _, p := range bad {
		if p.validate() == nil {
			t.Errorf("%+v accepted", p)
		}
	}
	good := &RetryPolicy{InitialInterval: 1500 * time.Millisecond, MaximumAttempts: 3}
	if err := good.validate(); err != nil {
		t.Fatal(err)
	}
	a := good.toAPI()
	if a.InitialIntervalInSeconds != 2 || a.BackoffCoefficient != 2 || a.MaximumAttempts != 3 {
		t.Fatalf("toAPI %+v", a)
	}
	if (*RetryPolicy)(nil).toAPI() != nil || (*RetryPolicy)(nil).validate() != nil {
		t.Fatal("nil policy")
	}
	if d, ok := localBackoff(good, 0, errors.New("x"), time.Now()); !ok || d != good.InitialInterval {
		t.Fatalf("backoff %v %v", d, ok)
	}
	if _, ok := localBackoff(good, 2, errors.New("x"), time.Now()); ok {
		t.Fatal("attempts exhausted")
	}
}

func TestErrorHelpers(t *testing.T) {
	wrap := func(err error) error { return fmt.Errorf("wrapped: %w", err) }
	cases := []struct {
		is  func(error) bool
		err error
	}{
		{IsCustomError, NewCustomError("r")},
		{IsGenericError, &GenericError{err: "g"}},
		{IsTimeoutError, NewHeartbeatTimeoutError()},
		{IsTerminatedError, &TerminatedError{}},
		{IsPanicError, newPanicError("p", "")},
		{IsCanceledError, NewCanceledError()},
		{IsWorkflowExecutionAlreadyStartedError, &api.WorkflowExecutionAlreadyStartedError{}},
	}
	for i, c := range cases {
		if !c.is(wrap(c.err)) || c.is(errors.New("other")) {
			t.Errorf("case %d: %T", i, c.err)
		}
	}

	e := NewCustomError("r", 1, WithNextRetryDelay(1500*time.Millisecond), WithFailureCategory(api.FailureCategoryFatal))
	var n int
	if e.Details(&n) != nil || n != 1 {
		t.Fatalf("details %d", n)
	}
	if e.GetFailureCategory() != api.FailureCategoryFatal || e.GetNextRetryDelay() != 1500*time.Millisecond {
		t.Fatalf("options %v %v", e.GetFailureCategory(), e.GetNextRetryDelay())
	}
	if fo := failureOptions(e); fo == nil || fo.NextRetryIntervalSeconds != 2 || fo.FailureCategory != api.FailureCategoryFatal {
		t.Fatalf("failure options %+v", fo)
	}
	if NewCustomError("r").GetFailureCategory() != api.FailureCategoryStandard || failureOptions(NewCustomError("r")) != nil {
		t.Fatal("defaults")
	}
}

// pollerService answers DescribeTaskList with a fixed set of pollers.
type pollerService struct {
	api.Service
	identities []string
}

func (s *pollerService) DescribeTaskList(ctx context.Context, req *api.DescribeTaskListRequest) (*api.DescribeTaskListResponse, error) {
	resp := &api.DescribeTaskListResponse{}
	for _, id := range s.identities {
		resp.Pollers = append(resp.Pollers, &api.PollerInfo{Identity: id})
	}
	return resp, nil
}

func TestShareTaskListRate(t *testing.T) {
	svc := &pollerService{identities: []string{"me", "other", "third", "fourth"}}
	w := NewWorker(svc, "d", "tl", WorkerOptions{Identity: "me", TaskListActivitiesPerSecond: 8})
	ctx, cancel := context.WithCancel(context.Background())
	w.ctx = ctx
	lim := limiter(8)
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.shareTaskListRate(lim)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for lim.Limit() != 2 {
		if time.Now().After(deadline) {
			t.Fatalf("limit %v, want 2", lim.Limit())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if lim.Burst() != 2 {
		t.Fatalf("burst %d", lim.Burst())
	}
	cancel()
	<-done
}

func TestWorkerOptionsValidate(t *testing.T) {
	if err := (WorkerOptions{}).Validate(); err != nil {
		t.Fatal(err)
	}
	if (WorkerOptions{MaxConcurrentDecisionTaskPollers: 1}).Validate() == nil {
		t.Error("one decision poller with sticky execution should fail")
	}
	if (WorkerOptions{EnableSessionWorker: true}).Validate() == nil {
		t.Error("sessions should fail")
	}
	o := AugmentWorkerOptions(WorkerOptions{})
	if o.MaxConcurrentActivityTaskPollers == 0 || o.Logger == nil {
		t.Errorf("defaults missing: %+v", o)
	}
	w := NewWorker(&pollerService{}, "d", "tl", WorkerOptions{EnableSessionWorker: true})
	w.RegisterActivity(greet)
	if w.Start() == nil {
		t.Error("Start should refuse sessions")
	}
}

func TestLogLifecycle(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	logLifecycle(zap.New(core), &api.PollForDecisionTaskResponse{}, []*api.Decision{
		{DecisionType: api.DecisionTypeCompleteWorkflowExecution},
	})
	if logs.Len() != 2 {
		t.Fatalf("logged %v", logs.All())
	}
}
