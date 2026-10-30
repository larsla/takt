package sdk

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/uber-go/tally"
	"go.uber.org/zap"

	"github.com/larsla/takt/api"
)

// WorkflowExecution identifies a workflow run.
type WorkflowExecution struct {
	ID    string
	RunID string
}

// WorkflowType names a workflow.
type WorkflowType struct {
	Name string
}

// WorkflowInfo describes the running workflow.
type WorkflowInfo struct {
	WorkflowExecution                   WorkflowExecution
	OriginalRunId                       string
	WorkflowType                        WorkflowType
	TaskListName                        string
	ExecutionStartToCloseTimeoutSeconds int32
	TaskStartToCloseTimeoutSeconds      int32
	Domain                              string
	Attempt                             int32
	lastCompletionResult                []byte
	CronSchedule                        *string
	ContinuedExecutionRunID             *string
	ParentWorkflowDomain                *string
	ParentWorkflowExecution             *WorkflowExecution
	Memo                                *api.Memo
	SearchAttributes                    *api.SearchAttributes
	BinaryChecksum                      *string
	DecisionStartedEventID              int64
	RetryPolicy                         *api.RetryPolicy
}

// Version is a workflow code version used with GetVersion.
type Version int

// DefaultVersion is the version of code that predates a GetVersion call.
const DefaultVersion Version = -1

type resultHandler func(result []byte, err error)

type activityParams struct {
	activityID          string
	activityType        string
	taskList            string
	input               []byte
	scheduleToClose     time.Duration
	scheduleToStart     time.Duration
	startToClose        time.Duration
	heartbeat           time.Duration
	retryPolicy         *api.RetryPolicy
	waitForCancellation bool
	header              *api.Header
	// args are the caller's arguments, for test mocks.
	args []interface{}
}

type localActivityParams struct {
	activityID   string
	activityType string
	fn           interface{}
	args         []interface{}
	options      LocalActivityOptions
	header       *api.Header
}

type childParams struct {
	domain              string
	workflowID          string
	workflowType        string
	taskList            string
	input               []byte
	execTimeout         time.Duration
	taskTimeout         time.Duration
	waitForCancellation bool
	reusePolicy         WorkflowIDReusePolicy
	retryPolicy         *api.RetryPolicy
	cronSchedule        string
	memo                *api.Memo
	searchAttributes    *api.SearchAttributes
	parentClosePolicy   ParentClosePolicy
	header              *api.Header
	// args are the caller's arguments, for test mocks.
	args []interface{}
}

// workflowEnvironment is what workflow code runs against. The replayer
// implements it for real executions and the test suite for unit tests.
type workflowEnvironment interface {
	Now() time.Time
	IsReplaying() bool
	WorkflowInfo() *WorkflowInfo
	Registry() *registry
	DataConverter() DataConverter
	Logger() *zap.Logger
	MetricsScope() tally.Scope
	NextID() string
	// ContextPropagators and StartHeader set up the root context.
	ContextPropagators() []ContextPropagator
	StartHeader() *api.Header
	Tracer() opentracing.Tracer

	ExecuteActivity(p activityParams, cb resultHandler) string
	RequestCancelActivity(activityID string)
	ExecuteLocalActivity(p localActivityParams, cb resultHandler) string
	RequestCancelLocalActivity(activityID string)
	NewTimer(d time.Duration, cb resultHandler) string
	RequestCancelTimer(timerID string)
	ExecuteChildWorkflow(p childParams, cb resultHandler, started func(WorkflowExecution, error))
	RequestCancelChildWorkflow(domain, workflowID string)
	RequestCancelExternalWorkflow(domain, workflowID, runID string, cb resultHandler)
	SignalExternalWorkflow(domain, workflowID, runID, signalName string, input []byte, arg interface{}, childOnly bool, cb resultHandler)
	SideEffect(f func() ([]byte, error), cb resultHandler)
	MutableSideEffect(id string, f func() interface{}, equals func(a, b interface{}) bool) Value
	GetVersion(changeID string, minSupported, maxSupported Version) Version
	UpsertSearchAttributes(attrs map[string]interface{}) error

	RegisterSignalHandler(func(name string, input []byte))
	RegisterCancelHandler(func())
	RegisterQueryHandler(func(queryType string, args []byte) ([]byte, error))
	Complete(result []byte, err error)
}

const workflowRunKey contextKey = "workflowRun"

func getEnv(ctx Context) workflowEnvironment {
	env, ok := ctx.Value(workflowEnvKey).(workflowEnvironment)
	if !ok {
		panic("not a workflow context")
	}
	return env
}

func getRun(ctx Context) *workflowRun {
	r, ok := ctx.Value(workflowRunKey).(*workflowRun)
	if !ok {
		panic("not a workflow context")
	}
	return r
}

// signalChannelSize matches Cadence's default for signal channels.
const signalChannelSize = 100000

// workflowRun is one execution of a workflow function under a dispatcher.
type workflowRun struct {
	env     workflowEnvironment
	d       *dispatcher
	root    Context
	cancel  CancelFunc
	signals map[string]*channelImpl
	queries map[string]interface{}
}

func newWorkflowRun(env workflowEnvironment, fn interface{}, input []byte) *workflowRun {
	r := &workflowRun{
		env:     env,
		d:       newDispatcher(),
		signals: map[string]*channelImpl{},
		queries: map[string]interface{}{},
	}
	ctx := WithValue(background, workflowEnvKey, env)
	ctx = WithValue(ctx, workflowRunKey, r)
	ctx = WithValue(ctx, dataConverterKey, env.DataConverter())
	r.root, r.cancel = WithCancel(ctx)
	env.RegisterSignalHandler(r.signal)
	env.RegisterCancelHandler(func() { r.cancel() })
	env.RegisterQueryHandler(r.query)
	r.d.newCoroutine(r.root, "root", func(ctx Context) {
		ctx, err := extractToWorkflow(ctx, env.ContextPropagators(), env.StartHeader())
		if err != nil {
			// A panic fails the decision task, as in Cadence.
			panic(err.Error())
		}
		if tracer := env.Tracer(); !env.IsReplaying() && !isNoopTracer(tracer) {
			info := env.WorkflowInfo()
			_, span := startSpan(context.Background(), tracer, spanNameExecuteWorkflow, opentracing.Tags{
				tagSpanWorkflowType: info.WorkflowType.Name,
				tagSpanWorkflowID:   info.WorkflowExecution.ID,
				tagSpanRunID:        info.WorkflowExecution.RunID,
			}, GetSpanContext(ctx))
			defer span.Finish()
			ctx = WithSpanContext(ctx, span.Context())
		}
		result, err := r.invoke(ctx, fn, input)
		env.Complete(result, err)
	})
	return r
}

func (r *workflowRun) invoke(ctx Context, fn interface{}, input []byte) ([]byte, error) {
	fv := reflect.ValueOf(fn)
	args, err := decodeArgs(r.env.DataConverter(), fv.Type(), input, 1)
	if err != nil {
		return nil, fmt.Errorf("unable to decode the workflow function input: %w", err)
	}
	result, err := callFn(fv, append([]reflect.Value{reflect.ValueOf(ctx)}, args...))
	if err != nil {
		return nil, err
	}
	if fv.Type().NumOut() == 1 {
		return nil, nil
	}
	return encodeArgs(r.env.DataConverter(), []interface{}{result})
}

func (r *workflowRun) signalChannel(name string) *channelImpl {
	ch, ok := r.signals[name]
	if !ok {
		ch = &channelImpl{name: name, size: signalChannelSize, dc: r.env.DataConverter()}
		r.signals[name] = ch
	}
	return ch
}

func (r *workflowRun) signal(name string, input []byte) {
	if input == nil {
		// A nil payload would be indistinguishable from "no value".
		input = []byte{}
	}
	if !r.signalChannel(name).SendAsync(input) {
		r.env.Logger().Error("signal channel is full, dropping signal", zap.String("signal", name))
	}
}

// QueryTypeStackTrace returns the stacks of all workflow goroutines.
const QueryTypeStackTrace = "__stack_trace"

func (r *workflowRun) query(queryType string, args []byte) ([]byte, error) {
	if queryType == QueryTypeStackTrace {
		return encodeArgs(r.env.DataConverter(), []interface{}{r.d.stackTrace()})
	}
	h, ok := r.queries[queryType]
	if !ok {
		known := []string{QueryTypeStackTrace}
		for k := range r.queries {
			known = append(known, k)
		}
		return nil, fmt.Errorf("unknown queryType %v. KnownQueryTypes=%v", queryType, known)
	}
	hv := reflect.ValueOf(h)
	in, err := decodeArgs(r.env.DataConverter(), hv.Type(), args, 0)
	if err != nil {
		return nil, fmt.Errorf("unable to decode the query arguments: %w", err)
	}
	result, err := func() (res interface{}, err error) {
		defer func() {
			if p := recover(); p != nil {
				err = newPanicError(p, stack())
			}
		}()
		return callFn(hv, in)
	}()
	if err != nil {
		return nil, err
	}
	return encodeArgs(r.env.DataConverter(), []interface{}{result})
}

// Go starts a workflow goroutine.
func Go(ctx Context, f func(ctx Context)) { GoNamed(ctx, "", f) }

// GoNamed starts a workflow goroutine with a name shown in stack traces.
func GoNamed(ctx Context, name string, f func(ctx Context)) {
	getRun(ctx).d.newCoroutine(ctx, name, f)
}

// Now returns the workflow's deterministic current time.
func Now(ctx Context) time.Time { return getEnv(ctx).Now() }

// IsReplaying reports whether the workflow is replaying history.
func IsReplaying(ctx Context) bool { return getEnv(ctx).IsReplaying() }

// GetInfo returns information about the workflow.
func GetInfo(ctx Context) *WorkflowInfo { return getEnv(ctx).WorkflowInfo() }

// GetLogger returns a logger that stays quiet during replay.
func GetLogger(ctx Context) *zap.Logger { return getEnv(ctx).Logger() }

// GetMetricsScope returns a metrics scope that is a no-op during replay.
func GetMetricsScope(ctx Context) tally.Scope {
	env := getEnv(ctx)
	if env.IsReplaying() {
		return tally.NoopScope
	}
	return env.MetricsScope()
}

func newFutureFor(ctx Context) *futureImpl { return newFuture(contextDataConverter(ctx)) }

func skipContext(fn interface{}) int {
	t := reflect.TypeOf(fn)
	if t != nil && t.Kind() == reflect.Func && t.NumIn() > 0 && t.In(0) == goContextType {
		return 1
	}
	return 0
}

// ExecuteActivity schedules an activity and returns a future for its result.
func ExecuteActivity(ctx Context, activity interface{}, args ...interface{}) Future {
	env := getEnv(ctx)
	f := newFutureFor(ctx)
	p, err := activityParamsFrom(ctx, env, activity, args)
	if err != nil {
		f.Set(nil, err)
		return f
	}
	if ctx.Err() != nil {
		f.Set(nil, ErrCanceled)
		return f
	}
	id := env.ExecuteActivity(p, func(result []byte, err error) {
		if !f.ready {
			f.Set(bytesOrNil(result), err)
		}
	})
	onCancel(ctx, func() { env.RequestCancelActivity(id) })
	return f
}

func bytesOrNil(b []byte) interface{} {
	if b == nil {
		return nil
	}
	return b
}

func activityParamsFrom(ctx Context, env workflowEnvironment, activity interface{}, args []interface{}) (activityParams, error) {
	if _, ok := ctx.Value(activityOptionsKey).(*ActivityOptions); !ok {
		return activityParams{}, errors.New("missing activity parameters through context, check ActivityOptions")
	}
	o := activityOptions(ctx)
	if _, isName := activity.(string); !isName {
		if err := validateActivityFn(reflect.TypeOf(activity)); err != nil {
			return activityParams{}, err
		}
		if err := validateArgs(activity, args, skipContext(activity)); err != nil {
			return activityParams{}, err
		}
	}
	if o.ScheduleToStartTimeout <= 0 {
		return activityParams{}, errors.New("missing or negative ScheduleToStartTimeoutSeconds")
	}
	if o.StartToCloseTimeout <= 0 {
		return activityParams{}, errors.New("missing or negative StartToCloseTimeoutSeconds")
	}
	if o.ScheduleToCloseTimeout < 0 || o.HeartbeatTimeout < 0 {
		return activityParams{}, errors.New("negative activity timeout")
	}
	if err := o.RetryPolicy.validate(); err != nil {
		return activityParams{}, err
	}
	s2c := o.ScheduleToCloseTimeout
	if s2c == 0 {
		s2c = o.ScheduleToStartTimeout + o.StartToCloseTimeout
	}
	input, err := encodeArgs(contextDataConverter(ctx), args)
	if err != nil {
		return activityParams{}, err
	}
	tl := o.TaskList
	if tl == "" {
		tl = env.WorkflowInfo().TaskListName
	}
	header, err := workflowHeader(ctx)
	if err != nil {
		return activityParams{}, err
	}
	id := o.ActivityID
	if id == "" {
		id = env.NextID()
	}
	return activityParams{
		header:              header,
		activityID:          id,
		activityType:        env.Registry().activityName(activity),
		taskList:            tl,
		input:               input,
		scheduleToClose:     s2c,
		scheduleToStart:     o.ScheduleToStartTimeout,
		startToClose:        o.StartToCloseTimeout,
		heartbeat:           o.HeartbeatTimeout,
		retryPolicy:         o.RetryPolicy.toAPI(),
		waitForCancellation: o.WaitForCancellation,
		args:                args,
	}, nil
}

// ExecuteLocalActivity runs an activity inside the workflow worker and
// records its result in history.
func ExecuteLocalActivity(ctx Context, activity interface{}, args ...interface{}) Future {
	env := getEnv(ctx)
	f := newFutureFor(ctx)
	o := localActivityOptions(ctx)
	if o.ScheduleToCloseTimeout <= 0 {
		f.Set(nil, errors.New("missing or negative ScheduleToCloseTimeoutSeconds"))
		return f
	}
	if err := o.RetryPolicy.validate(); err != nil {
		f.Set(nil, err)
		return f
	}
	fn := activity
	if name, ok := activity.(string); ok {
		registered, found := env.Registry().getActivity(name)
		if !found {
			f.Set(nil, fmt.Errorf("local activity %s is not registered", name))
			return f
		}
		fn = registered
	}
	if err := validateActivityFn(reflect.TypeOf(fn)); err != nil {
		f.Set(nil, err)
		return f
	}
	if err := validateArgs(fn, args, skipContext(fn)); err != nil {
		f.Set(nil, err)
		return f
	}
	if ctx.Err() != nil {
		f.Set(nil, ErrCanceled)
		return f
	}
	header, err := workflowHeader(ctx)
	if err != nil {
		f.Set(nil, err)
		return f
	}
	id := env.ExecuteLocalActivity(localActivityParams{
		activityID:   env.NextID(),
		activityType: env.Registry().activityName(activity),
		fn:           fn,
		args:         args,
		options:      o,
		header:       header,
	}, func(result []byte, err error) {
		if !f.ready {
			f.Set(bytesOrNil(result), err)
		}
	})
	onCancel(ctx, func() { env.RequestCancelLocalActivity(id) })
	return f
}

// ChildWorkflowFuture is the future of a child workflow.
type ChildWorkflowFuture interface {
	Future
	// GetChildWorkflowExecution resolves to the child's WorkflowExecution once it starts.
	GetChildWorkflowExecution() Future
	// SignalChildWorkflow signals the child.
	SignalChildWorkflow(ctx Context, signalName string, data interface{}) Future
}

type childFuture struct {
	*futureImpl
	execution *futureImpl
	domain    string
	id        string
}

func (c *childFuture) GetChildWorkflowExecution() Future { return c.execution }

func (c *childFuture) SignalChildWorkflow(ctx Context, signalName string, data interface{}) Future {
	f := newFutureFor(ctx)
	Go(ctx, func(ctx Context) {
		var exec WorkflowExecution
		if err := c.execution.Get(ctx, &exec); err != nil {
			f.Set(nil, err)
			return
		}
		f.Chain(signalExternal(ctx, c.domain, exec.ID, exec.RunID, signalName, data, true))
	})
	return f
}

// ExecuteChildWorkflow starts a child workflow.
func ExecuteChildWorkflow(ctx Context, childWorkflow interface{}, args ...interface{}) ChildWorkflowFuture {
	env := getEnv(ctx)
	dc := contextDataConverter(ctx)
	cf := &childFuture{futureImpl: newFuture(dc), execution: newFuture(dc)}
	fail := func(err error) ChildWorkflowFuture {
		cf.execution.Set(nil, err)
		cf.Set(nil, err)
		return cf
	}
	o := childOptions(ctx)
	if _, isName := childWorkflow.(string); !isName {
		if err := validateWorkflowFn(childWorkflow); err != nil {
			return fail(err)
		}
		if err := validateArgs(childWorkflow, args, 1); err != nil {
			return fail(err)
		}
	}
	if o.ExecutionStartToCloseTimeout <= 0 {
		return fail(errors.New("missing or negative ExecutionStartToCloseTimeout"))
	}
	if err := o.RetryPolicy.validate(); err != nil {
		return fail(err)
	}
	input, err := encodeArgs(dc, args)
	if err != nil {
		return fail(err)
	}
	memo, err := encodeMap(dc, o.Memo)
	if err != nil {
		return fail(err)
	}
	sa, err := encodeMap(defaultConverter, o.SearchAttributes)
	if err != nil {
		return fail(err)
	}
	header, err := workflowHeader(ctx)
	if err != nil {
		return fail(err)
	}
	info := env.WorkflowInfo()
	p := childParams{
		header:              header,
		domain:              o.Domain,
		workflowID:          o.WorkflowID,
		workflowType:        env.Registry().workflowName(childWorkflow),
		taskList:            o.TaskList,
		input:               input,
		execTimeout:         o.ExecutionStartToCloseTimeout,
		taskTimeout:         o.TaskStartToCloseTimeout,
		waitForCancellation: o.WaitForCancellation,
		reusePolicy:         o.WorkflowIDReusePolicy,
		retryPolicy:         o.RetryPolicy.toAPI(),
		cronSchedule:        o.CronSchedule,
		parentClosePolicy:   o.ParentClosePolicy,
		args:                args,
	}
	if memo != nil {
		p.memo = &api.Memo{Fields: memo}
	}
	if sa != nil {
		p.searchAttributes = &api.SearchAttributes{IndexedFields: sa}
	}
	if p.domain == "" {
		p.domain = info.Domain
	}
	if p.taskList == "" {
		p.taskList = info.TaskListName
	}
	if p.taskTimeout == 0 {
		p.taskTimeout = 10 * time.Second
	}
	if p.workflowID == "" {
		p.workflowID = info.WorkflowExecution.RunID + "_" + env.NextID()
	}
	cf.domain, cf.id = p.domain, p.workflowID
	if ctx.Err() != nil {
		return fail(ErrCanceled)
	}
	env.ExecuteChildWorkflow(p, func(result []byte, err error) {
		if !cf.ready {
			cf.Set(bytesOrNil(result), err)
		}
	}, func(exec WorkflowExecution, err error) {
		if !cf.execution.ready {
			cf.execution.Set(exec, err)
		}
	})
	onCancel(ctx, func() { env.RequestCancelChildWorkflow(p.domain, p.workflowID) })
	return cf
}

func encodeMap(dc DataConverter, m map[string]interface{}) (map[string][]byte, error) {
	if len(m) == 0 {
		return nil, nil
	}
	out := make(map[string][]byte, len(m))
	for k, v := range m {
		b, err := orDefault(dc).ToData(v)
		if err != nil {
			return nil, fmt.Errorf("encode %s: %w", k, err)
		}
		out[k] = b
	}
	return out, nil
}

// NewTimer returns a future that is ready after d.
func NewTimer(ctx Context, d time.Duration) Future {
	f := newFutureFor(ctx)
	if d < 0 {
		f.Set(nil, fmt.Errorf("negative duration provided %v", d))
		return f
	}
	if d == 0 {
		f.Set(true, nil)
		return f
	}
	if ctx.Err() != nil {
		f.Set(nil, ErrCanceled)
		return f
	}
	env := getEnv(ctx)
	id := env.NewTimer(d, func(_ []byte, err error) {
		if !f.ready {
			f.Set(nil, err)
		}
	})
	onCancel(ctx, func() { env.RequestCancelTimer(id) })
	return f
}

// Sleep pauses the workflow for d. It returns ErrCanceled if ctx is canceled.
func Sleep(ctx Context, d time.Duration) error {
	return NewTimer(ctx, d).Get(ctx, nil)
}

// RequestCancelExternalWorkflow asks another workflow to cancel.
func RequestCancelExternalWorkflow(ctx Context, workflowID, runID string) Future {
	env := getEnv(ctx)
	f := newFutureFor(ctx)
	if workflowID == "" {
		f.Set(nil, errors.New("invalid workflowID"))
		return f
	}
	env.RequestCancelExternalWorkflow(env.WorkflowInfo().Domain, workflowID, runID, func(_ []byte, err error) {
		if !f.ready {
			f.Set(nil, err)
		}
	})
	return f
}

// SignalExternalWorkflow sends a signal to another workflow.
func SignalExternalWorkflow(ctx Context, workflowID, runID, signalName string, arg interface{}) Future {
	return signalExternal(ctx, getEnv(ctx).WorkflowInfo().Domain, workflowID, runID, signalName, arg, false)
}

func signalExternal(ctx Context, domain, workflowID, runID, signalName string, arg interface{}, childOnly bool) Future {
	env := getEnv(ctx)
	f := newFutureFor(ctx)
	if workflowID == "" {
		f.Set(nil, errors.New("invalid workflowID"))
		return f
	}
	input, err := encodeArgs(contextDataConverter(ctx), []interface{}{arg})
	if err != nil {
		f.Set(nil, err)
		return f
	}
	env.SignalExternalWorkflow(domain, workflowID, runID, signalName, input, arg, childOnly, func(_ []byte, err error) {
		if !f.ready {
			f.Set(nil, err)
		}
	})
	return f
}

// GetSignalChannel returns the channel signals named signalName arrive on.
func GetSignalChannel(ctx Context, signalName string) Channel {
	return getRun(ctx).signalChannel(signalName)
}

// SideEffect runs f once and records its result so replays return the same value.
func SideEffect(ctx Context, f func(ctx Context) interface{}) Value {
	env := getEnv(ctx)
	dc := contextDataConverter(ctx)
	future := newFuture(dc)
	env.SideEffect(func() ([]byte, error) {
		return encodeArgs(dc, []interface{}{f(ctx)})
	}, func(result []byte, err error) {
		future.Set(result, err)
	})
	var data []byte
	if err := future.Get(ctx, &data); err != nil {
		panic(err)
	}
	return newEncodedValue(data, dc)
}

// MutableSideEffect records f's result only when it changes.
func MutableSideEffect(ctx Context, id string, f func(ctx Context) interface{}, equals func(a, b interface{}) bool) Value {
	return getEnv(ctx).MutableSideEffect(id, func() interface{} { return f(ctx) }, equals)
}

// GetVersion returns the code version to use for changeID.
func GetVersion(ctx Context, changeID string, minSupported, maxSupported Version) Version {
	return getEnv(ctx).GetVersion(changeID, minSupported, maxSupported)
}

func validateVersion(changeID string, version, minSupported, maxSupported Version) {
	if version < minSupported {
		panic(fmt.Sprintf("Workflow code removed support of version %v. for \"%v\" changeID. The oldest supported version is %v",
			version, changeID, minSupported))
	}
	if version > maxSupported {
		panic(fmt.Sprintf("Workflow code is too old to support version %v for \"%v\" changeID. The maximum supported version is %v",
			version, changeID, maxSupported))
	}
}

// SetQueryHandler registers a handler for queryType. The handler takes the
// query arguments and returns (result, error).
func SetQueryHandler(ctx Context, queryType string, handler interface{}) error {
	if queryType == QueryTypeStackTrace {
		return errors.New("queryType __stack_trace is reserved")
	}
	t := reflect.TypeOf(handler)
	if t == nil || t.Kind() != reflect.Func {
		return fmt.Errorf("query handler must be function but was %T", handler)
	}
	if t.NumOut() != 2 || t.Out(1) != errorType {
		return errors.New("query handler must return 2 values (serializable result and error)")
	}
	getRun(ctx).queries[queryType] = handler
	return nil
}

// HasLastCompletionResult reports whether a previous cron run left a result.
func HasLastCompletionResult(ctx Context) bool {
	return len(getEnv(ctx).WorkflowInfo().lastCompletionResult) > 0
}

// GetLastCompletionResult decodes the result of the previous cron run.
func GetLastCompletionResult(ctx Context, d ...interface{}) error {
	info := getEnv(ctx).WorkflowInfo()
	if len(info.lastCompletionResult) == 0 {
		return ErrNoData
	}
	return contextDataConverter(ctx).FromData(info.lastCompletionResult, d...)
}

// UpsertSearchAttributes adds or updates search attributes of the workflow.
func UpsertSearchAttributes(ctx Context, attributes map[string]interface{}) error {
	if len(attributes) == 0 {
		return errors.New("search attributes is empty")
	}
	return getEnv(ctx).UpsertSearchAttributes(attributes)
}

// NewContinueAsNewError returns an error that, returned from a workflow,
// closes the run and starts a new one with the given function and args.
func NewContinueAsNewError(ctx Context, wfn interface{}, args ...interface{}) *ContinueAsNewError {
	env := getEnv(ctx)
	if _, isName := wfn.(string); !isName {
		if err := validateWorkflowFn(wfn); err != nil {
			panic(err)
		}
		if err := validateArgs(wfn, args, 1); err != nil {
			panic(err)
		}
	}
	input, err := encodeArgs(contextDataConverter(ctx), args)
	if err != nil {
		panic(err)
	}
	header, err := workflowHeader(ctx)
	if err != nil {
		panic(err)
	}
	info := env.WorkflowInfo()
	o := childOptions(ctx)
	p := &continueAsNewParams{
		header:       header,
		workflowType: env.Registry().workflowName(wfn),
		input:        input,
		taskList:     info.TaskListName,
		execTimeout:  info.ExecutionStartToCloseTimeoutSeconds,
		taskTimeout:  info.TaskStartToCloseTimeoutSeconds,
	}
	if o.TaskList != "" {
		p.taskList = o.TaskList
	}
	if o.ExecutionStartToCloseTimeout > 0 {
		p.execTimeout = durationSeconds(o.ExecutionStartToCloseTimeout)
	}
	if o.TaskStartToCloseTimeout > 0 {
		p.taskTimeout = durationSeconds(o.TaskStartToCloseTimeout)
	}
	return &ContinueAsNewError{params: p}
}

// Await blocks until condition returns true or ctx is canceled.
func Await(ctx Context, condition func() bool) error {
	st := getState(ctx)
	for {
		if ctx.Err() != nil {
			st.unblocked()
			return ctx.Err()
		}
		if condition() {
			st.unblocked()
			return nil
		}
		st.yield("Await")
	}
}

// AwaitWithTimeout is Await with a timeout. ok is false if it timed out.
func AwaitWithTimeout(ctx Context, timeout time.Duration, condition func() bool) (ok bool, err error) {
	tctx, cancel := WithCancel(ctx)
	defer cancel()
	timer := NewTimer(tctx, timeout)
	st := getState(ctx)
	for {
		if ctx.Err() != nil {
			st.unblocked()
			return false, ctx.Err()
		}
		if condition() {
			st.unblocked()
			return true, nil
		}
		if timer.IsReady() {
			st.unblocked()
			return false, nil
		}
		st.yield("AwaitWithTimeout")
	}
}

// activity context plumbing shared with the activity package.
type activityEnvKeyType struct{}

var activityEnvKey = activityEnvKeyType{}

func getActivityEnv(ctx context.Context) *activityEnv {
	env, ok := ctx.Value(activityEnvKey).(*activityEnv)
	if !ok {
		panic("getActivityEnv: not an activity context")
	}
	return env
}
