package sdk

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/larsla/takt/api"
)

// TestWorkflowEnvironment runs one workflow in memory. Create it with
// WorkflowTestSuite.NewTestWorkflowEnvironment.
type TestWorkflowEnvironment struct {
	mock.Mock
	impl *testEnv
}

// RegisterWorkflow registers a workflow with this environment.
func (e *TestWorkflowEnvironment) RegisterWorkflow(w interface{}) {
	e.RegisterWorkflowWithOptions(w, RegisterWorkflowOptions{})
}

// RegisterWorkflowWithOptions registers a workflow with options.
func (e *TestWorkflowEnvironment) RegisterWorkflowWithOptions(w interface{}, options RegisterWorkflowOptions) {
	options.DisableAlreadyRegisteredCheck = true
	e.impl.registry.RegisterWorkflowWithOptions(w, options)
}

// RegisterActivity registers an activity with this environment.
func (e *TestWorkflowEnvironment) RegisterActivity(a interface{}) {
	e.RegisterActivityWithOptions(a, RegisterActivityOptions{})
}

// RegisterActivityWithOptions registers an activity with options.
func (e *TestWorkflowEnvironment) RegisterActivityWithOptions(a interface{}, options RegisterActivityOptions) {
	options.DisableAlreadyRegisteredCheck = true
	e.impl.registry.RegisterActivityWithOptions(a, options)
}

// SetStartTime sets the mock clock's start time.
func (e *TestWorkflowEnvironment) SetStartTime(startTime time.Time) { e.impl.clock.now = startTime }

// Now returns the mock clock's time.
func (e *TestWorkflowEnvironment) Now() time.Time { return e.impl.clock.now }

// OnActivity mocks an activity (or local activity). activity is a function
// or a registered name; args are matched like testify arguments.
func (e *TestWorkflowEnvironment) OnActivity(activity interface{}, args ...interface{}) *MockCallWrapper {
	name := activity
	if _, isName := activity.(string); !isName {
		t := reflect.TypeOf(activity)
		if t == nil || t.Kind() != reflect.Func {
			panic("activity must be a function or a name")
		}
		if _, ok := e.impl.registry.getActivity(e.impl.registry.activityName(activity)); !ok {
			e.RegisterActivity(activity)
		}
		name = e.impl.registry.activityName(activity)
	}
	return e.impl.wrap(e.On(name.(string), args...))
}

// OnWorkflow mocks a child workflow.
func (e *TestWorkflowEnvironment) OnWorkflow(workflow interface{}, args ...interface{}) *MockCallWrapper {
	name := workflow
	if _, isName := workflow.(string); !isName {
		if _, ok := e.impl.registry.getWorkflow(e.impl.registry.workflowName(workflow)); !ok {
			e.RegisterWorkflow(workflow)
		}
		name = e.impl.registry.workflowName(workflow)
	}
	return e.impl.wrap(e.On(name.(string), args...))
}

// OnSignalExternalWorkflow mocks SignalExternalWorkflow. Return an error or nil.
func (e *TestWorkflowEnvironment) OnSignalExternalWorkflow(domainName, workflowID, runID, signalName, arg interface{}) *MockCallWrapper {
	return e.impl.wrap(e.On(mockSignalExternal, domainName, workflowID, runID, signalName, arg))
}

// OnRequestCancelExternalWorkflow mocks RequestCancelExternalWorkflow.
func (e *TestWorkflowEnvironment) OnRequestCancelExternalWorkflow(domainName, workflowID, runID interface{}) *MockCallWrapper {
	return e.impl.wrap(e.On(mockRequestCancelExternal, domainName, workflowID, runID))
}

// OnGetVersion mocks GetVersion. Return a workflow.Version.
func (e *TestWorkflowEnvironment) OnGetVersion(changeID string, minSupported, maxSupported Version) *MockCallWrapper {
	return e.impl.wrap(e.On(mockGetVersion, changeID, minSupported, maxSupported))
}

// OnUpsertSearchAttributes mocks UpsertSearchAttributes. Return an error or nil.
func (e *TestWorkflowEnvironment) OnUpsertSearchAttributes(attributes interface{}) *MockCallWrapper {
	return e.impl.wrap(e.On(mockUpsertSearchAttrs, attributes))
}

// ExecuteWorkflow runs the workflow until it completes. It panics if the
// workflow blocks for longer than the test timeout with nothing pending.
func (e *TestWorkflowEnvironment) ExecuteWorkflow(workflowFn interface{}, args ...interface{}) {
	impl := e.impl
	if impl.root != nil {
		panic("ExecuteWorkflow can only be called once per environment")
	}
	fn := workflowFn
	if name, isName := workflowFn.(string); isName {
		registered, ok := impl.registry.getWorkflow(name)
		if !ok {
			panic(fmt.Sprintf("workflow %s is not registered", name))
		}
		fn = registered
	} else if _, ok := impl.registry.getWorkflow(impl.registry.workflowName(workflowFn)); !ok {
		e.RegisterWorkflow(workflowFn)
	}
	if err := validateWorkflowFn(fn); err != nil {
		panic(err)
	}
	input, err := encodeArgs(impl.dc(), args)
	if err != nil {
		panic(err)
	}
	timeout := impl.wfTimeout
	if timeout == 0 {
		timeout = 10 * 365 * 24 * time.Hour
	}
	info := &WorkflowInfo{
		WorkflowExecution:                   WorkflowExecution{ID: defaultTestWorkflowID, RunID: defaultTestRunID},
		OriginalRunId:                       defaultTestRunID,
		WorkflowType:                        WorkflowType{Name: impl.registry.workflowName(workflowFn)},
		TaskListName:                        defaultTestTaskList,
		ExecutionStartToCloseTimeoutSeconds: durationSeconds(timeout),
		TaskStartToCloseTimeoutSeconds:      10,
		Domain:                              defaultTestDomain,
		lastCompletionResult:                impl.lastCompletion,
		Memo:                                impl.memo,
		SearchAttributes:                    impl.searchAttrs,
	}
	impl.root = impl.newWorkflow(info, nil)
	impl.root.header = impl.header
	impl.root.start(fn, input)
	if impl.wfTimeout > 0 {
		impl.clock.add(impl.wfTimeout, func() {
			if !impl.root.completed {
				impl.root.Complete(nil, NewTimeoutError(api.TimeoutTypeStartToClose))
			}
		})
	}
	impl.loop()
	for _, w := range impl.workflows {
		w.finish()
	}
}

// IsWorkflowCompleted reports whether the workflow finished.
func (e *TestWorkflowEnvironment) IsWorkflowCompleted() bool {
	return e.impl.root != nil && e.impl.root.completed
}

// GetWorkflowResult decodes the workflow's result.
func (e *TestWorkflowEnvironment) GetWorkflowResult(valuePtr interface{}) error {
	root := e.impl.root
	if root == nil || !root.completed {
		return errors.New("workflow is not completed")
	}
	if root.err != nil {
		return root.err
	}
	if root.result == nil || valuePtr == nil {
		return nil
	}
	return e.impl.dc().FromData(root.result, valuePtr)
}

// GetWorkflowError returns the workflow's error.
func (e *TestWorkflowEnvironment) GetWorkflowError() error {
	if e.impl.root == nil {
		return nil
	}
	return e.impl.root.err
}

// CompleteActivity completes an activity that returned ErrResultPending.
func (e *TestWorkflowEnvironment) CompleteActivity(taskToken []byte, result interface{}, err error) error {
	impl := e.impl
	impl.asyncMu.Lock()
	a, ok := impl.tokens[string(taskToken)]
	impl.asyncMu.Unlock()
	if !ok {
		return errors.New("activity not found")
	}
	var data []byte
	if err == nil && result != nil {
		var eerr error
		if data, eerr = encodeArgs(impl.dc(), []interface{}{result}); eerr != nil {
			return eerr
		}
	}
	impl.post(func() {
		finish := func() { a.owner.finishActivity(a, data, err, false) }
		impl.asyncMu.Lock()
		_, pending := impl.async[string(taskToken)]
		delete(impl.async, string(taskToken))
		impl.asyncMu.Unlock()
		switch {
		case pending:
			impl.running--
			finish()
		case !a.done:
			// The activity has not returned ErrResultPending yet.
			a.early = finish
		}
	})
	return nil
}

// CancelWorkflow requests cancellation of the workflow. Call it from a
// delayed callback.
func (e *TestWorkflowEnvironment) CancelWorkflow() {
	impl := e.impl
	cancel := func() {
		if impl.root.cancelHandler != nil {
			impl.root.cancelHandler()
			impl.dirty = true
		}
	}
	if impl.root == nil {
		impl.pendingSg = append(impl.pendingSg, cancel)
		return
	}
	cancel()
}

// SignalWorkflow sends a signal to the workflow.
func (e *TestWorkflowEnvironment) SignalWorkflow(name string, input interface{}) {
	impl := e.impl
	data, err := encodeArgs(impl.dc(), []interface{}{input})
	if err != nil {
		panic(err)
	}
	if impl.root == nil {
		impl.pendingSg = append(impl.pendingSg, func() { impl.root.signal(name, data) })
		return
	}
	impl.root.signal(name, data)
}

// SignalWorkflowByID sends a signal to a workflow (the root or a child) by ID.
func (e *TestWorkflowEnvironment) SignalWorkflowByID(workflowID, signalName string, input interface{}) error {
	impl := e.impl
	w := impl.workflowByID(workflowID)
	if w == nil {
		return &api.EntityNotExistsError{Message: "workflow " + workflowID + " not found"}
	}
	data, err := encodeArgs(impl.dc(), []interface{}{input})
	if err != nil {
		return err
	}
	w.signal(signalName, data)
	return nil
}

// QueryWorkflow queries the workflow.
func (e *TestWorkflowEnvironment) QueryWorkflow(queryType string, args ...interface{}) (Value, error) {
	root := e.impl.root
	if root == nil || root.queryHandler == nil {
		return nil, errors.New("workflow has not started")
	}
	input, err := encodeArgs(e.impl.dc(), args)
	if err != nil {
		return nil, err
	}
	result, err := root.queryHandler(queryType, input)
	if err != nil {
		return nil, err
	}
	return newEncodedValue(result, e.impl.dc()), nil
}

// RegisterDelayedCallback runs callback after d of mock time from the
// workflow start.
func (e *TestWorkflowEnvironment) RegisterDelayedCallback(callback func(), d time.Duration) {
	impl := e.impl
	if impl.root != nil {
		impl.clock.add(d, func() { callback(); impl.dirty = true })
		return
	}
	impl.delayed = append(impl.delayed, delayedCallback{fn: callback, d: d})
}

// SetLastCompletionResult sets what GetLastCompletionResult returns.
func (e *TestWorkflowEnvironment) SetLastCompletionResult(result interface{}) {
	data, err := encodeArgs(e.impl.dc(), []interface{}{result})
	if err != nil {
		panic(err)
	}
	e.impl.lastCompletion = data
}

// SetMemoOnStart sets the workflow's memo.
func (e *TestWorkflowEnvironment) SetMemoOnStart(memo map[string]interface{}) error {
	fields, err := encodeMap(e.impl.dc(), memo)
	if err != nil {
		return err
	}
	e.impl.memo = &api.Memo{Fields: fields}
	return nil
}

// SetSearchAttributesOnStart sets the workflow's search attributes.
func (e *TestWorkflowEnvironment) SetSearchAttributesOnStart(attrs map[string]interface{}) error {
	fields, err := encodeMap(defaultConverter, attrs)
	if err != nil {
		return err
	}
	e.impl.searchAttrs = &api.SearchAttributes{IndexedFields: fields}
	return nil
}

// SetActivityTaskList limits activities to a task list. Activities
// scheduled on another task list time out.
func (e *TestWorkflowEnvironment) SetActivityTaskList(tasklist string, activityFn ...interface{}) {
	for _, fn := range activityFn {
		name := e.impl.registry.activityName(fn)
		e.impl.taskLists[name] = append(e.impl.taskLists[name], tasklist)
	}
}

// SetWorkerOptions sets the worker options (DataConverter,
// BackgroundActivityContext and so on).
func (e *TestWorkflowEnvironment) SetWorkerOptions(options WorkerOptions) *TestWorkflowEnvironment {
	if options.DataConverter == nil {
		options.DataConverter = defaultConverter
	}
	if options.BackgroundActivityContext == nil {
		options.BackgroundActivityContext = context.Background()
	}
	e.impl.opts = options
	return e
}

// SetWorkerStopChannel sets the channel activities see as the worker stop channel.
func (e *TestWorkflowEnvironment) SetWorkerStopChannel(c chan struct{}) { e.impl.workerStp = c }

// SetTestTimeout sets how long the workflow may stay blocked, in real time,
// with nothing pending before the test panics.
func (e *TestWorkflowEnvironment) SetTestTimeout(idleTimeout time.Duration) *TestWorkflowEnvironment {
	e.impl.timeout = idleTimeout
	return e
}

// SetWorkflowTimeout sets the workflow's execution timeout in mock time.
func (e *TestWorkflowEnvironment) SetWorkflowTimeout(executionTimeout time.Duration) *TestWorkflowEnvironment {
	e.impl.wfTimeout = executionTimeout
	return e
}

func (e *TestWorkflowEnvironment) SetOnActivityStartedListener(listener func(activityInfo *ActivityInfo, ctx context.Context, args Values)) *TestWorkflowEnvironment {
	e.impl.onActivityStarted = listener
	return e
}

func (e *TestWorkflowEnvironment) SetOnActivityCompletedListener(listener func(activityInfo *ActivityInfo, result Value, err error)) *TestWorkflowEnvironment {
	e.impl.onActivityCompleted = listener
	return e
}

func (e *TestWorkflowEnvironment) SetOnActivityCanceledListener(listener func(activityInfo *ActivityInfo)) *TestWorkflowEnvironment {
	e.impl.onActivityCanceled = listener
	return e
}

func (e *TestWorkflowEnvironment) SetOnActivityHeartbeatListener(listener func(activityInfo *ActivityInfo, details Values)) *TestWorkflowEnvironment {
	e.impl.listenerMu.Lock()
	e.impl.onActivityHeartbeat = listener
	e.impl.listenerMu.Unlock()
	return e
}

func (e *TestWorkflowEnvironment) SetOnChildWorkflowStartedListener(listener func(workflowInfo *WorkflowInfo, ctx Context, args Values)) *TestWorkflowEnvironment {
	e.impl.onChildStarted = listener
	return e
}

func (e *TestWorkflowEnvironment) SetOnChildWorkflowCompletedListener(listener func(workflowInfo *WorkflowInfo, result Value, err error)) *TestWorkflowEnvironment {
	e.impl.onChildCompleted = listener
	return e
}

func (e *TestWorkflowEnvironment) SetOnChildWorkflowCanceledListener(listener func(workflowInfo *WorkflowInfo)) *TestWorkflowEnvironment {
	e.impl.onChildCanceled = listener
	return e
}

func (e *TestWorkflowEnvironment) SetOnTimerScheduledListener(listener func(timerID string, duration time.Duration)) *TestWorkflowEnvironment {
	e.impl.onTimerScheduled = listener
	return e
}

func (e *TestWorkflowEnvironment) SetOnTimerFiredListener(listener func(timerID string)) *TestWorkflowEnvironment {
	e.impl.onTimerFired = listener
	return e
}

func (e *TestWorkflowEnvironment) SetOnTimerCancelledListener(listener func(timerID string)) *TestWorkflowEnvironment {
	e.impl.onTimerCanceled = listener
	return e
}

func (e *TestWorkflowEnvironment) SetOnLocalActivityStartedListener(listener func(activityInfo *ActivityInfo, ctx context.Context, args []interface{})) *TestWorkflowEnvironment {
	e.impl.onLocalActivityStarted = listener
	return e
}

func (e *TestWorkflowEnvironment) SetOnLocalActivityCompletedListener(listener func(activityInfo *ActivityInfo, result Value, err error)) *TestWorkflowEnvironment {
	e.impl.onLocalActivityComplete = listener
	return e
}

func (e *TestWorkflowEnvironment) SetOnLocalActivityCanceledListener(listener func(activityInfo *ActivityInfo)) *TestWorkflowEnvironment {
	e.impl.onLocalActivityCanceled = listener
	return e
}

// TestActivityEnvironment runs activities directly. Create it with
// WorkflowTestSuite.NewTestActivityEnvironment.
type TestActivityEnvironment struct {
	impl *testEnv
}

// RegisterActivity registers an activity with this environment.
func (e *TestActivityEnvironment) RegisterActivity(a interface{}) {
	e.RegisterActivityWithOptions(a, RegisterActivityOptions{})
}

// RegisterActivityWithOptions registers an activity with options.
func (e *TestActivityEnvironment) RegisterActivityWithOptions(a interface{}, options RegisterActivityOptions) {
	options.DisableAlreadyRegisteredCheck = true
	e.impl.registry.RegisterActivityWithOptions(a, options)
}

func (e *TestActivityEnvironment) resolve(activityFn interface{}) (interface{}, string, error) {
	name := e.impl.registry.activityName(activityFn)
	if _, isName := activityFn.(string); !isName {
		return activityFn, name, validateActivityFn(reflect.TypeOf(activityFn))
	}
	fn, ok := e.impl.registry.getActivity(name)
	if !ok {
		return nil, name, fmt.Errorf("unable to find activityType=%v. Supported types: %v", name, e.impl.registry.activityNames())
	}
	return fn, name, nil
}

func (e *TestActivityEnvironment) execute(activityFn interface{}, isLocal bool, args []interface{}) (Value, error) {
	impl := e.impl
	fn, name, err := e.resolve(activityFn)
	if err != nil {
		return nil, err
	}
	if err := validateArgs(fn, args, skipContext(fn)); err != nil {
		return nil, err
	}
	info := ActivityInfo{
		TaskToken:          []byte("test-task-token"),
		WorkflowType:       &WorkflowType{Name: "default-test-workflow-type"},
		WorkflowDomain:     defaultTestDomain,
		WorkflowExecution:  WorkflowExecution{ID: defaultTestWorkflowID, RunID: defaultTestRunID},
		ActivityID:         "0",
		ActivityType:       ActivityType{Name: name},
		TaskList:           defaultTestTaskList,
		ScheduledTimestamp: time.Now(),
		StartedTimestamp:   time.Now(),
		Deadline:           time.Now().Add(impl.timeout),
	}
	ctx, cancel := context.WithDeadline(impl.opts.BackgroundActivityContext, info.Deadline)
	defer cancel()
	act := &testActivity{info: info}
	env := &activityEnv{
		info:             info,
		service:          &testHeartbeatService{env: impl, act: act, info: info},
		dc:               impl.dc(),
		logger:           impl.logger,
		scope:            impl.scope,
		heartbeatDetails: impl.heartbeat,
		workerStop:       impl.workerStp,
		isLocal:          isLocal,
		noThrottle:       true,
		cancel:           cancel,
	}
	ctx = context.WithValue(ctx, activityEnvKey, env)
	pctx, err := extractHeader(ctx, impl.props, impl.header)
	if err != nil {
		return nil, err
	}
	result, err := runActivityArgs(pctx, fn, args, impl.dc())
	if errors.Is(err, ErrActivityResultPending) {
		return nil, err
	}
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			err = NewTimeoutError(api.TimeoutTypeStartToClose)
		}
		return nil, impl.roundTrip(err)
	}
	return newEncodedValue(result, impl.dc()), nil
}

// ExecuteActivity runs an activity and returns its encoded result.
func (e *TestActivityEnvironment) ExecuteActivity(activityFn interface{}, args ...interface{}) (Value, error) {
	return e.execute(activityFn, false, args)
}

// ExecuteLocalActivity runs an activity as a local activity.
func (e *TestActivityEnvironment) ExecuteLocalActivity(activityFn interface{}, args ...interface{}) (Value, error) {
	return e.execute(activityFn, true, args)
}

// SetWorkerOptions sets the worker options.
func (e *TestActivityEnvironment) SetWorkerOptions(options WorkerOptions) *TestActivityEnvironment {
	if options.DataConverter == nil {
		options.DataConverter = defaultConverter
	}
	if options.BackgroundActivityContext == nil {
		options.BackgroundActivityContext = context.Background()
	}
	e.impl.opts = options
	return e
}

// SetWorkerStopChannel sets the channel activities see as the worker stop channel.
func (e *TestActivityEnvironment) SetWorkerStopChannel(c chan struct{}) { e.impl.workerStp = c }

// SetTestTimeout sets the activity's deadline.
func (e *TestActivityEnvironment) SetTestTimeout(idleTimeout time.Duration) *TestActivityEnvironment {
	e.impl.timeout = idleTimeout
	return e
}

// SetHeartbeatDetails sets what GetHeartbeatDetails returns to the activity.
func (e *TestActivityEnvironment) SetHeartbeatDetails(details interface{}) {
	data, err := encodeArgs(e.impl.dc(), []interface{}{details})
	if err != nil {
		panic(err)
	}
	e.impl.heartbeat = data
}

// SetOnActivityHeartbeatListener sets a listener for heartbeats.
func (e *TestActivityEnvironment) SetOnActivityHeartbeatListener(listener func(activityInfo *ActivityInfo, details Values)) *TestActivityEnvironment {
	e.impl.listenerMu.Lock()
	e.impl.onActivityHeartbeat = listener
	e.impl.listenerMu.Unlock()
	return e
}
