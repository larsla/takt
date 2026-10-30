package sdk

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/uber-go/tally"
	"go.uber.org/zap"

	"github.com/larsla/takt/api"
)

// The test environment runs workflows in memory against a mock clock. The
// clock only moves when nothing is running in real time (activities, local
// activities): it then jumps to the next timer. Activities run their real
// code unless mocked with OnActivity.

const (
	defaultTestDomain     = "default-test-domain"
	defaultTestTaskList   = "default-test-tasklist"
	defaultTestWorkflowID = "default-test-workflow-id"
	defaultTestRunID      = "default-test-run-id"
	defaultTestTimeout    = 3 * time.Second
)

// WorkflowTestSuite creates test environments. Embed it in a testify suite.
type WorkflowTestSuite struct {
	logger   *zap.Logger
	scope    tally.Scope
	registry *registry
}

func (s *WorkflowTestSuite) init() {
	if s.registry == nil {
		s.registry = newRegistry(globalRegistry)
	}
	if s.logger == nil {
		s.logger = zap.NewNop()
	}
	if s.scope == nil {
		s.scope = tally.NoopScope
	}
}

// NewTestWorkflowEnvironment creates an environment for one workflow test.
func (s *WorkflowTestSuite) NewTestWorkflowEnvironment() *TestWorkflowEnvironment {
	s.init()
	e := &TestWorkflowEnvironment{}
	e.impl = newTestEnv(s, &e.Mock)
	return e
}

// NewTestActivityEnvironment creates an environment for one activity test.
func (s *WorkflowTestSuite) NewTestActivityEnvironment() *TestActivityEnvironment {
	s.init()
	return &TestActivityEnvironment{impl: newTestEnv(s, &mock.Mock{})}
}

// SetLogger sets the logger of environments created afterwards.
func (s *WorkflowTestSuite) SetLogger(logger *zap.Logger) { s.logger = logger }

// GetLogger returns the suite's logger.
func (s *WorkflowTestSuite) GetLogger() *zap.Logger {
	s.init()
	return s.logger
}

// SetMetricsScope sets the metrics scope of environments created afterwards.
func (s *WorkflowTestSuite) SetMetricsScope(scope tally.Scope) { s.scope = scope }

// RegisterWorkflow registers a workflow for all environments of the suite.
func (s *WorkflowTestSuite) RegisterWorkflow(w interface{}) {
	s.init()
	s.registry.RegisterWorkflowWithOptions(w, RegisterWorkflowOptions{DisableAlreadyRegisteredCheck: true})
}

// RegisterActivity registers an activity for all environments of the suite.
func (s *WorkflowTestSuite) RegisterActivity(a interface{}) {
	s.init()
	s.registry.RegisterActivityWithOptions(a, RegisterActivityOptions{DisableAlreadyRegisteredCheck: true})
}

type mockTimer struct {
	at       time.Time
	seq      int
	fn       func()
	canceled bool
}

type mockClock struct {
	now    time.Time
	timers []*mockTimer
	seq    int
}

func (c *mockClock) add(d time.Duration, fn func()) *mockTimer {
	c.seq++
	t := &mockTimer{at: c.now.Add(d), seq: c.seq, fn: fn}
	c.timers = append(c.timers, t)
	sort.SliceStable(c.timers, func(i, j int) bool {
		if !c.timers[i].at.Equal(c.timers[j].at) {
			return c.timers[i].at.Before(c.timers[j].at)
		}
		return c.timers[i].seq < c.timers[j].seq
	})
	return t
}

// popNext removes and returns the earliest live timer.
func (c *mockClock) popNext() *mockTimer {
	for len(c.timers) > 0 {
		t := c.timers[0]
		c.timers = c.timers[1:]
		if !t.canceled {
			return t
		}
	}
	return nil
}

// testEnv is shared by the root workflow and its children.
type testEnv struct {
	mock     *mock.Mock
	registry *registry
	logger   *zap.Logger
	scope    tally.Scope
	opts     WorkerOptions
	clock    mockClock

	callbacks chan func()
	running   int
	dirty     bool
	timeout   time.Duration
	wfTimeout time.Duration
	workerStp chan struct{}

	workflows []*testWorkflow
	root      *testWorkflow
	delayed   []delayedCallback
	pendingSg []func()

	lastCompletion []byte
	memo           *api.Memo
	searchAttrs    *api.SearchAttributes
	heartbeat      []byte
	taskLists      map[string][]string

	mockMu      sync.Mutex
	lastMatched *MockCallWrapper
	asyncMu     sync.Mutex
	async       map[string]*testActivity
	tokens      map[string]*testActivity

	onActivityStarted       func(*ActivityInfo, context.Context, Values)
	onActivityCompleted     func(*ActivityInfo, Value, error)
	onActivityCanceled      func(*ActivityInfo)
	onActivityHeartbeat     func(*ActivityInfo, Values)
	onChildStarted          func(*WorkflowInfo, Context, Values)
	onChildCompleted        func(*WorkflowInfo, Value, error)
	onChildCanceled         func(*WorkflowInfo)
	onTimerScheduled        func(string, time.Duration)
	onTimerFired            func(string)
	onTimerCanceled         func(string)
	onLocalActivityStarted  func(*ActivityInfo, context.Context, []interface{})
	onLocalActivityComplete func(*ActivityInfo, Value, error)
	onLocalActivityCanceled func(*ActivityInfo)
	listenerMu              sync.Mutex
}

type delayedCallback struct {
	fn func()
	d  time.Duration
}

func newTestEnv(s *WorkflowTestSuite, m *mock.Mock) *testEnv {
	return &testEnv{
		mock:      m,
		registry:  newRegistry(s.registry),
		logger:    s.logger,
		scope:     s.scope,
		opts:      WorkerOptions{DataConverter: defaultConverter, BackgroundActivityContext: context.Background()},
		clock:     mockClock{now: time.Now()},
		callbacks: make(chan func(), 1024),
		timeout:   defaultTestTimeout,
		workerStp: make(chan struct{}),
		taskLists: map[string][]string{},
		async:     map[string]*testActivity{},
		tokens:    map[string]*testActivity{},
	}
}

func (e *testEnv) dc() DataConverter { return orDefault(e.opts.DataConverter) }

// post queues fn to run on the test's main goroutine.
func (e *testEnv) post(fn func()) { e.callbacks <- fn }

// MockCallWrapper configures a mocked call.
type MockCallWrapper struct {
	call    *mock.Call
	env     *testEnv
	delay   time.Duration
	delayFn func() time.Duration
}

func (e *testEnv) wrap(call *mock.Call) *MockCallWrapper {
	w := &MockCallWrapper{call: call, env: e}
	call.Run(func(mock.Arguments) { e.lastMatched = w })
	return w
}

// Once makes the mock match once.
func (c *MockCallWrapper) Once() *MockCallWrapper { c.call.Once(); return c }

// Twice makes the mock match twice.
func (c *MockCallWrapper) Twice() *MockCallWrapper { c.call.Twice(); return c }

// Times makes the mock match i times.
func (c *MockCallWrapper) Times(i int) *MockCallWrapper { c.call.Times(i); return c }

// After delays the mocked result by d of mock time.
func (c *MockCallWrapper) After(d time.Duration) *MockCallWrapper { c.delay = d; return c }

// AfterFn delays the mocked result by the duration fn returns.
func (c *MockCallWrapper) AfterFn(fn func() time.Duration) *MockCallWrapper { c.delayFn = fn; return c }

// Return sets the mocked results: either a function with the mocked
// function's signature, or the values it returns.
func (c *MockCallWrapper) Return(returnArguments ...interface{}) *MockCallWrapper {
	c.call.Return(returnArguments...)
	return c
}

func (c *MockCallWrapper) wait() time.Duration {
	if c == nil {
		return 0
	}
	if c.delayFn != nil {
		return c.delayFn()
	}
	return c.delay
}

func (e *testEnv) hasMock(name string) bool {
	for _, c := range e.mock.ExpectedCalls {
		if c.Method == name {
			return true
		}
	}
	return false
}

// callMock records a call and returns its results and configured delay.
// Activity mocks are called from activity goroutines, hence the lock.
func (e *testEnv) callMock(name string, args []interface{}) (mock.Arguments, time.Duration) {
	e.mockMu.Lock()
	defer e.mockMu.Unlock()
	e.lastMatched = nil
	rets := e.mock.MethodCalled(name, args...)
	return rets, e.lastMatched.wait()
}

// mockArgs prepends ctx when fn takes a context, as Cadence's mocks do.
func mockArgs(fn interface{}, ctx interface{}, args []interface{}) []interface{} {
	t := typeOf(fn)
	if t == nil || t.NumIn() == 0 {
		return args
	}
	if first := t.In(0); first == goContextType || first == workflowContextType {
		return append([]interface{}{ctx}, args...)
	}
	return args
}

// mockResult turns mock return values into (result, error) for fnType. A
// single function return value is called with args.
func (e *testEnv) mockResult(ctx interface{}, name string, fnType reflect.Type, rets mock.Arguments, args []interface{}) ([]byte, error) {
	if len(rets) == 1 {
		if fv := reflect.ValueOf(rets[0]); fv.Kind() == reflect.Func {
			in := make([]reflect.Value, 0, len(args)+1)
			if fv.Type().NumIn() == len(args)+1 && ctx != nil {
				in = append(in, reflect.ValueOf(ctx))
			}
			for _, a := range args {
				if a == nil {
					in = append(in, reflect.Zero(fv.Type().In(len(in))))
					continue
				}
				in = append(in, reflect.ValueOf(a))
			}
			result, err := callFn(fv, in)
			if err != nil || fv.Type().NumOut() == 1 {
				return nil, err
			}
			return encodeArgs(e.dc(), []interface{}{result})
		}
	}
	nOut := len(rets)
	if fnType != nil {
		nOut = fnType.NumOut()
	}
	switch {
	case nOut == 1 && len(rets) >= 1:
		err, _ := rets[0].(error)
		return nil, err
	case nOut == 2 && len(rets) >= 2:
		err, _ := rets[1].(error)
		if err != nil {
			return nil, err
		}
		return encodeArgs(e.dc(), []interface{}{rets[0]})
	default:
		panic(fmt.Sprintf("mock of %s must return a function or %d values, got %d", name, nOut, len(rets)))
	}
}

// roundTrip passes an error through its wire form, as a real execution would.
func (e *testEnv) roundTrip(err error) error {
	if err == nil {
		return nil
	}
	var (
		ce *CanceledError
		ae *api.WorkflowExecutionAlreadyStartedError
	)
	if errors.As(err, &ce) || errors.As(err, &ae) {
		return err
	}
	reason, details := errorDetails(err, e.dc())
	return errorFromDetails(reason, details, e.dc())
}

func (e *testEnv) workflowByID(id string) *testWorkflow {
	for _, w := range e.workflows {
		if w.info.WorkflowExecution.ID == id && !w.completed {
			return w
		}
	}
	return nil
}

// runDispatchers runs all workflows until none can make progress.
func (e *testEnv) runDispatchers() {
	for {
		e.dirty = false
		for _, w := range append([]*testWorkflow(nil), e.workflows...) {
			if w.completed {
				continue
			}
			if err := w.run.d.executeUntilAllBlocked(); err != nil {
				w.Complete(nil, err)
			}
			if w.completed {
				w.finish()
			}
		}
		if !e.dirty {
			return
		}
	}
}

// loop runs the root workflow to completion.
func (e *testEnv) loop() {
	for _, d := range e.delayed {
		fn := d.fn
		e.clock.add(d.d, func() { fn(); e.dirty = true })
	}
	e.delayed = nil
	for _, s := range e.pendingSg {
		s()
	}
	e.pendingSg = nil
	for {
		e.runDispatchers()
		if e.root.completed {
			return
		}
		select {
		case cb := <-e.callbacks:
			cb()
			continue
		default:
		}
		if e.running == 0 {
			if t := e.clock.popNext(); t != nil {
				if t.at.After(e.clock.now) {
					e.clock.now = t.at
				}
				t.fn()
				continue
			}
		}
		select {
		case cb := <-e.callbacks:
			cb()
		case <-time.After(e.timeout):
			panic(fmt.Sprintf("test timeout: %v, workflow stack: %v", e.timeout, e.root.run.d.stackTrace()))
		}
	}
}

// testWorkflow is one workflow (root or child) in a test environment.
type testWorkflow struct {
	env     *testEnv
	info    *WorkflowInfo
	run     *workflowRun
	parent  *testWorkflow
	handle  *testChild
	counter int64

	signalHandler func(string, []byte)
	cancelHandler func()
	queryHandler  func(string, []byte) ([]byte, error)

	completed bool
	finished  bool
	result    []byte
	err       error

	activities map[string]*testActivity
	timers     map[string]*testTimer
	children   map[string]*testChild
	versions   map[string]Version
	mutable    map[string][]byte
	las        map[string]*testActivity
}

func (e *testEnv) newWorkflow(info *WorkflowInfo, parent *testWorkflow) *testWorkflow {
	w := &testWorkflow{
		env:        e,
		info:       info,
		parent:     parent,
		activities: map[string]*testActivity{},
		timers:     map[string]*testTimer{},
		children:   map[string]*testChild{},
		versions:   map[string]Version{},
		mutable:    map[string][]byte{},
		las:        map[string]*testActivity{},
	}
	e.workflows = append(e.workflows, w)
	return w
}

func (w *testWorkflow) start(fn interface{}, input []byte) {
	w.run = newWorkflowRun(w, fn, input)
}

// finish runs once after the workflow completes.
func (w *testWorkflow) finish() {
	if w.finished {
		return
	}
	w.finished = true
	w.run.d.close()
	for _, a := range w.activities {
		if a.cancel != nil {
			a.cancel()
		}
	}
	for _, t := range w.timers {
		t.t.canceled = true
	}
	if w.handle != nil {
		w.handle.done(w.result, w.err)
	}
}

func (w *testWorkflow) Now() time.Time              { return w.env.clock.now }
func (w *testWorkflow) IsReplaying() bool           { return false }
func (w *testWorkflow) WorkflowInfo() *WorkflowInfo { return w.info }
func (w *testWorkflow) Registry() *registry         { return w.env.registry }
func (w *testWorkflow) DataConverter() DataConverter {
	return w.env.dc()
}
func (w *testWorkflow) Logger() *zap.Logger       { return w.env.logger }
func (w *testWorkflow) MetricsScope() tally.Scope { return w.env.scope }

func (w *testWorkflow) NextID() string {
	w.counter++
	return strconv.FormatInt(w.counter, 10)
}

func (w *testWorkflow) RegisterSignalHandler(h func(string, []byte)) { w.signalHandler = h }
func (w *testWorkflow) RegisterCancelHandler(h func())               { w.cancelHandler = h }
func (w *testWorkflow) RegisterQueryHandler(h func(string, []byte) ([]byte, error)) {
	w.queryHandler = h
}

func (w *testWorkflow) Complete(result []byte, err error) {
	if w.completed {
		return
	}
	w.completed = true
	w.result = result
	var (
		can *ContinueAsNewError
		pe  *PanicError
	)
	if errors.As(err, &can) || errors.As(err, &pe) {
		w.err = err
	} else {
		w.err = w.env.roundTrip(err)
	}
	w.env.dirty = true
}

// testActivity is a running activity or local activity.
type testActivity struct {
	id         string
	params     activityParams
	la         *localActivityParams
	cb         resultHandler
	info       ActivityInfo
	cancel     context.CancelFunc
	canceled   bool
	done       bool
	waitCancel bool
	heartbeat  []byte
	owner      *testWorkflow
	// early holds a CompleteActivity call that arrived before the activity
	// returned ErrResultPending.
	early func()
}

func (w *testWorkflow) activityInfo(id, name, taskList string, attempt int32) ActivityInfo {
	return ActivityInfo{
		TaskToken:          []byte(w.info.WorkflowExecution.RunID + "/" + id),
		WorkflowType:       &w.info.WorkflowType,
		WorkflowDomain:     w.info.Domain,
		WorkflowExecution:  w.info.WorkflowExecution,
		ActivityID:         id,
		ActivityType:       ActivityType{Name: name},
		TaskList:           taskList,
		ScheduledTimestamp: w.env.clock.now,
		StartedTimestamp:   w.env.clock.now,
		Attempt:            attempt,
	}
}

func (w *testWorkflow) ExecuteActivity(p activityParams, cb resultHandler) string {
	a := &testActivity{id: p.activityID, params: p, cb: cb, waitCancel: p.waitForCancellation, owner: w}
	w.activities[a.id] = a
	if lists, ok := w.env.taskLists[p.activityType]; ok && !contains(lists, p.taskList) {
		// Nobody polls this task list, so the activity never starts.
		w.env.clock.add(p.scheduleToStart, func() {
			w.finishActivity(a, nil, NewTimeoutError(api.TimeoutTypeScheduleToStart), false)
		})
		return a.id
	}
	w.startActivity(a, 0)
	return a.id
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func (w *testWorkflow) startActivity(a *testActivity, attempt int32) {
	e := w.env
	p := a.params
	a.info = w.activityInfo(a.id, p.activityType, p.taskList, attempt)
	a.info.HeartbeatTimeout = p.heartbeat
	a.info.Deadline = time.Now().Add(p.startToClose)
	if e.onActivityStarted != nil {
		e.onActivityStarted(&a.info, context.Background(), newEncodedValues(p.input, e.dc()))
	}
	if e.hasMock(p.activityType) {
		fn, _ := e.registry.getActivity(p.activityType)
		w.runActivity(a, func(ctx context.Context) ([]byte, time.Duration, error) {
			rets, delay := e.callMock(p.activityType, mockArgs(fn, ctx, p.args))
			result, err := e.mockResult(ctx, p.activityType, typeOf(fn), rets, p.args)
			return result, delay, err
		})
		return
	}
	fn, ok := e.registry.getActivity(p.activityType)
	if !ok {
		err := fmt.Errorf("unable to find activityType=%v. Supported types: %v", p.activityType, e.registry.activityNames())
		e.post(func() { w.finishActivity(a, nil, err, false) })
		return
	}
	w.runActivity(a, func(ctx context.Context) ([]byte, time.Duration, error) {
		result, err := runActivity(ctx, fn, p.input, e.dc())
		return result, 0, err
	})
}

func typeOf(fn interface{}) reflect.Type {
	if fn == nil {
		return nil
	}
	return reflect.TypeOf(fn)
}

// runActivity runs body in a goroutine and delivers its result on the main
// loop, after the delay body returns (mock time).
func (w *testWorkflow) runActivity(a *testActivity, body func(ctx context.Context) ([]byte, time.Duration, error)) {
	e := w.env
	ctx, cancel := context.WithDeadline(e.opts.BackgroundActivityContext, a.info.Deadline)
	a.cancel = cancel
	e.listenerMu.Lock()
	details := a.heartbeat
	e.listenerMu.Unlock()
	e.asyncMu.Lock()
	e.tokens[string(a.info.TaskToken)] = a
	e.asyncMu.Unlock()
	env := &activityEnv{
		info:             a.info,
		service:          &testHeartbeatService{env: e, act: a, info: a.info},
		dc:               e.dc(),
		logger:           e.logger,
		scope:            e.scope,
		heartbeatDetails: details,
		workerStop:       e.workerStp,
		isLocal:          a.la != nil,
		noThrottle:       true,
	}
	env.cancel = cancel
	ctx = context.WithValue(ctx, activityEnvKey, env)
	e.running++
	go func() {
		result, delay, err := body(ctx)
		ctxErr := ctx.Err()
		e.post(func() {
			e.running--
			if errors.Is(err, ErrActivityResultPending) {
				if a.early != nil {
					a.early()
					return
				}
				e.running++
				e.asyncMu.Lock()
				e.async[string(a.info.TaskToken)] = a
				e.asyncMu.Unlock()
				return
			}
			if err != nil && errors.Is(ctxErr, context.DeadlineExceeded) {
				err = NewTimeoutError(api.TimeoutTypeStartToClose)
			}
			deliver := func() { w.finishActivity(a, result, err, a.canceled) }
			if delay > 0 {
				e.clock.add(delay, deliver)
				return
			}
			deliver()
		})
	}()
}

func (w *testWorkflow) finishActivity(a *testActivity, result []byte, err error, canceled bool) {
	e := w.env
	if a.done {
		return
	}
	if err != nil && canceled && (errors.Is(err, context.Canceled) || IsCanceledError(err)) {
		err = ErrCanceled
	}
	fo := failureOptions(err)
	fatal := fo != nil && fo.FailureCategory == api.FailureCategoryFatal
	if a.la == nil && err != nil && !canceled && !fatal && a.params.retryPolicy != nil {
		reason, _ := errorDetails(err, e.dc())
		policy := a.params.retryPolicy
		if backoff, ok := retryBackoff(policy, a.info.Attempt, reason); ok {
			if fo != nil && fo.NextRetryIntervalSeconds > 0 {
				backoff = int64(fo.NextRetryIntervalSeconds) * int64(time.Second)
			}
			next := a.info.Attempt + 1
			e.clock.add(time.Duration(backoff), func() { w.startActivity(a, next) })
			return
		}
	}
	a.done = true
	if a.la != nil {
		if e.onLocalActivityComplete != nil {
			e.onLocalActivityComplete(&a.info, newEncodedValue(result, e.dc()), err)
		}
	} else if e.onActivityCompleted != nil {
		e.onActivityCompleted(&a.info, newEncodedValue(result, e.dc()), err)
	}
	a.cb(result, e.roundTrip(err))
	e.dirty = true
}

// retryBackoff mirrors the server's retry calculation, without expiration.
func retryBackoff(p *api.RetryPolicy, attempt int32, reason string) (int64, bool) {
	if p.MaximumAttempts > 0 && attempt+1 >= p.MaximumAttempts {
		return 0, false
	}
	if contains(p.NonRetriableErrorReasons, reason) {
		return 0, false
	}
	coeff := p.BackoffCoefficient
	if coeff < 1 {
		coeff = 2
	}
	interval := float64(p.InitialIntervalInSeconds)
	for range attempt {
		interval *= coeff
	}
	if p.MaximumIntervalInSeconds > 0 && interval > float64(p.MaximumIntervalInSeconds) {
		interval = float64(p.MaximumIntervalInSeconds)
	}
	return int64(interval * float64(time.Second)), true
}

func (w *testWorkflow) RequestCancelActivity(id string) {
	a := w.activities[id]
	if a == nil || a.done {
		return
	}
	a.canceled = true
	if a.cancel != nil {
		a.cancel()
	}
	if w.env.onActivityCanceled != nil {
		w.env.onActivityCanceled(&a.info)
	}
	if !a.waitCancel {
		a.done = true
		a.cb(nil, ErrCanceled)
		w.env.dirty = true
	}
}

func (w *testWorkflow) ExecuteLocalActivity(p localActivityParams, cb resultHandler) string {
	e := w.env
	pp := p
	a := &testActivity{id: p.activityID, la: &pp, cb: cb}
	a.info = w.activityInfo(p.activityID, p.activityType, w.info.TaskListName, 0)
	a.info.Deadline = time.Now().Add(p.options.ScheduleToCloseTimeout)
	w.las[a.id] = a
	if e.onLocalActivityStarted != nil {
		e.onLocalActivityStarted(&a.info, context.Background(), p.args)
	}
	if e.hasMock(p.activityType) {
		w.runActivity(a, func(ctx context.Context) ([]byte, time.Duration, error) {
			rets, delay := e.callMock(p.activityType, mockArgs(p.fn, ctx, p.args))
			result, err := e.mockResult(ctx, p.activityType, typeOf(p.fn), rets, p.args)
			return result, delay, err
		})
		return a.id
	}
	w.runActivity(a, func(ctx context.Context) ([]byte, time.Duration, error) {
		out := runLocalActivity(ctx, w.info, p, e.dc(), e.logger, e.scope, e.workerStp)
		return out.result, 0, out.err
	})
	return a.id
}

func (w *testWorkflow) RequestCancelLocalActivity(id string) {
	a := w.las[id]
	if a == nil || a.done {
		return
	}
	a.canceled = true
	if a.cancel != nil {
		a.cancel()
	}
	if w.env.onLocalActivityCanceled != nil {
		w.env.onLocalActivityCanceled(&a.info)
	}
	a.done = true
	a.cb(nil, ErrCanceled)
	w.env.dirty = true
}

type testTimer struct {
	t  *mockTimer
	cb resultHandler
}

func (w *testWorkflow) NewTimer(d time.Duration, cb resultHandler) string {
	id := w.NextID()
	e := w.env
	if e.onTimerScheduled != nil {
		e.onTimerScheduled(id, d)
	}
	t := e.clock.add(d, func() {
		if _, ok := w.timers[id]; !ok {
			return
		}
		delete(w.timers, id)
		if e.onTimerFired != nil {
			e.onTimerFired(id)
		}
		cb(nil, nil)
		e.dirty = true
	})
	w.timers[id] = &testTimer{t: t, cb: cb}
	return id
}

func (w *testWorkflow) RequestCancelTimer(id string) {
	tt, ok := w.timers[id]
	if !ok {
		return
	}
	tt.t.canceled = true
	delete(w.timers, id)
	if w.env.onTimerCanceled != nil {
		w.env.onTimerCanceled(id)
	}
	tt.cb(nil, ErrCanceled)
	w.env.dirty = true
}

// testChild is a child workflow of a test workflow.
type testChild struct {
	parent     *testWorkflow
	child      *testWorkflow
	params     childParams
	cb         resultHandler
	started    func(WorkflowExecution, error)
	waitCancel bool
	finished   bool
}

func (c *testChild) done(result []byte, err error) {
	if c.finished {
		return
	}
	c.finished = true
	e := c.parent.env
	if e.onChildCompleted != nil && c.child != nil {
		e.onChildCompleted(c.child.info, newEncodedValue(result, e.dc()), err)
	}
	c.cb(result, err)
	e.dirty = true
}

func (w *testWorkflow) ExecuteChildWorkflow(p childParams, cb resultHandler, started func(WorkflowExecution, error)) {
	e := w.env
	c := &testChild{parent: w, params: p, cb: cb, started: started, waitCancel: p.waitForCancellation}
	if existing := e.workflowByID(p.workflowID); existing != nil {
		err := &api.WorkflowExecutionAlreadyStartedError{Message: "Workflow execution already started"}
		started(WorkflowExecution{}, err)
		cb(nil, err)
		return
	}
	exec := WorkflowExecution{ID: p.workflowID, RunID: p.workflowID + "_run"}
	w.children[p.workflowID] = c
	if e.hasMock(p.workflowType) {
		started(exec, nil)
		fn, _ := e.registry.getWorkflow(p.workflowType)
		ctx := w.run.root
		rets, delay := e.callMock(p.workflowType, mockArgs(fn, ctx, p.args))
		e.clock.add(delay, func() {
			result, err := e.mockResult(ctx, p.workflowType, typeOf(fn), rets, p.args)
			c.done(result, e.roundTrip(err))
		})
		return
	}
	fn, ok := e.registry.getWorkflow(p.workflowType)
	if !ok {
		err := fmt.Errorf("unable to find workflow type: %v. Supported types: %v", p.workflowType, e.registry.workflowNames())
		started(WorkflowExecution{}, err)
		c.done(nil, err)
		return
	}
	parentExec := w.info.WorkflowExecution
	parentDomain := w.info.Domain
	info := &WorkflowInfo{
		WorkflowExecution:                   exec,
		WorkflowType:                        WorkflowType{Name: p.workflowType},
		TaskListName:                        p.taskList,
		ExecutionStartToCloseTimeoutSeconds: durationSeconds(p.execTimeout),
		TaskStartToCloseTimeoutSeconds:      durationSeconds(p.taskTimeout),
		Domain:                              p.domain,
		ParentWorkflowDomain:                &parentDomain,
		ParentWorkflowExecution:             &parentExec,
		Memo:                                p.memo,
		SearchAttributes:                    p.searchAttributes,
	}
	child := e.newWorkflow(info, w)
	child.handle = c
	c.child = child
	child.start(fn, p.input)
	started(exec, nil)
	if e.onChildStarted != nil {
		e.onChildStarted(info, child.run.root, newEncodedValues(p.input, e.dc()))
	}
	if p.execTimeout > 0 {
		e.clock.add(p.execTimeout, func() {
			if !child.completed {
				child.Complete(nil, NewTimeoutError(api.TimeoutTypeStartToClose))
				child.finish()
			}
		})
	}
	e.dirty = true
}

func (w *testWorkflow) RequestCancelChildWorkflow(_, workflowID string) {
	c := w.children[workflowID]
	if c == nil || c.finished {
		return
	}
	if c.child != nil {
		if w.env.onChildCanceled != nil {
			w.env.onChildCanceled(c.child.info)
		}
		if c.child.cancelHandler != nil {
			c.child.cancelHandler()
		}
	}
	if !c.waitCancel {
		c.finished = true
		c.cb(nil, ErrCanceled)
	}
	w.env.dirty = true
}

const (
	mockSignalExternal        = "SignalExternalWorkflow"
	mockRequestCancelExternal = "RequestCancelExternalWorkflow"
	mockGetVersion            = "GetVersion"
	mockUpsertSearchAttrs     = "UpsertSearchAttributes"
)

func (w *testWorkflow) RequestCancelExternalWorkflow(domain, workflowID, runID string, cb resultHandler) {
	e := w.env
	if e.hasMock(mockRequestCancelExternal) {
		rets, _ := e.callMock(mockRequestCancelExternal, []interface{}{domain, workflowID, runID})
		err, _ := rets.Get(0).(error)
		cb(nil, err)
		return
	}
	target := e.workflowByID(workflowID)
	if target == nil {
		cb(nil, &UnknownExternalWorkflowExecutionError{})
		return
	}
	if target.cancelHandler != nil {
		target.cancelHandler()
	}
	cb(nil, nil)
	e.dirty = true
}

func (w *testWorkflow) SignalExternalWorkflow(domain, workflowID, runID, signalName string, input []byte, arg interface{}, _ bool, cb resultHandler) {
	e := w.env
	if e.hasMock(mockSignalExternal) {
		rets, _ := e.callMock(mockSignalExternal, []interface{}{domain, workflowID, runID, signalName, arg})
		err, _ := rets.Get(0).(error)
		cb(nil, err)
		return
	}
	target := e.workflowByID(workflowID)
	if target == nil {
		cb(nil, &UnknownExternalWorkflowExecutionError{})
		return
	}
	target.signal(signalName, input)
	cb(nil, nil)
}

func (w *testWorkflow) signal(name string, input []byte) {
	if w.signalHandler != nil {
		w.signalHandler(name, input)
		w.env.dirty = true
	}
}

func (w *testWorkflow) SideEffect(f func() ([]byte, error), cb resultHandler) {
	cb(f())
}

func (w *testWorkflow) MutableSideEffect(id string, f func() interface{}, equals func(a, b interface{}) bool) Value {
	v := f()
	if existing, ok := w.mutable[id]; ok && equals != nil {
		old := newEncodedValue(existing, w.env.dc())
		if compareMutable(old, v, equals) {
			return old
		}
	}
	data, err := w.env.dc().ToData(v)
	if err != nil {
		panic(err)
	}
	w.mutable[id] = data
	return newEncodedValue(data, w.env.dc())
}

func (w *testWorkflow) GetVersion(changeID string, minSupported, maxSupported Version) Version {
	e := w.env
	if e.hasMock(mockGetVersion) {
		rets, _ := e.callMock(mockGetVersion, []interface{}{changeID, minSupported, maxSupported})
		v, ok := rets.Get(0).(Version)
		if !ok {
			panic("GetVersion mock must return a workflow.Version")
		}
		validateVersion(changeID, v, minSupported, maxSupported)
		return v
	}
	if v, ok := w.versions[changeID]; ok {
		validateVersion(changeID, v, minSupported, maxSupported)
		return v
	}
	w.versions[changeID] = maxSupported
	return maxSupported
}

func (w *testWorkflow) UpsertSearchAttributes(attrs map[string]interface{}) error {
	e := w.env
	if e.hasMock(mockUpsertSearchAttrs) {
		rets, _ := e.callMock(mockUpsertSearchAttrs, []interface{}{attrs})
		if err, _ := rets.Get(0).(error); err != nil {
			return err
		}
	}
	fields, err := encodeMap(defaultConverter, attrs)
	if err != nil {
		return err
	}
	if w.info.SearchAttributes == nil {
		w.info.SearchAttributes = &api.SearchAttributes{}
	}
	if w.info.SearchAttributes.IndexedFields == nil {
		w.info.SearchAttributes.IndexedFields = map[string][]byte{}
	}
	for k, v := range fields {
		w.info.SearchAttributes.IndexedFields[k] = v
	}
	return nil
}

// testHeartbeatService answers heartbeats of test activities.
type testHeartbeatService struct {
	api.Service
	env  *testEnv
	act  *testActivity
	info ActivityInfo
}

func (s *testHeartbeatService) RecordActivityTaskHeartbeat(_ context.Context, req *api.RecordActivityTaskHeartbeatRequest) (*api.RecordActivityTaskHeartbeatResponse, error) {
	e := s.env
	e.listenerMu.Lock()
	listener := e.onActivityHeartbeat
	e.listenerMu.Unlock()
	if listener != nil {
		info := s.info
		listener(&info, newEncodedValues(req.Details, e.dc()))
	}
	e.listenerMu.Lock()
	s.act.heartbeat = req.Details
	e.listenerMu.Unlock()
	return &api.RecordActivityTaskHeartbeatResponse{}, nil
}
