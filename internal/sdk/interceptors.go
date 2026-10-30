package sdk

import (
	"time"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

// WorkflowInterceptorFactory creates one link of a workflow's interceptor
// chain. The interceptor it returns must pass every call on to next.
type WorkflowInterceptorFactory interface {
	NewInterceptor(info *WorkflowInfo, next WorkflowInterceptor) WorkflowInterceptor
}

// WorkflowInterceptor intercepts the workflow function and the workflow
// API calls it makes. It runs as workflow code, so it must be
// deterministic; use IsReplaying to skip side effects during replay.
// Embed WorkflowInterceptorBase to forward the calls you don't intercept.
type WorkflowInterceptor interface {
	// ExecuteWorkflow calls the workflow function and returns its results,
	// the error last.
	ExecuteWorkflow(ctx Context, workflowType string, args ...interface{}) []interface{}

	ExecuteActivity(ctx Context, activityType string, args ...interface{}) Future
	ExecuteLocalActivity(ctx Context, activityType string, args ...interface{}) Future
	ExecuteChildWorkflow(ctx Context, childWorkflowType string, args ...interface{}) ChildWorkflowFuture
	GetWorkflowInfo(ctx Context) *WorkflowInfo
	GetLogger(ctx Context) *zap.Logger
	GetMetricsScope(ctx Context) tally.Scope
	Now(ctx Context) time.Time
	NewTimer(ctx Context, d time.Duration) Future
	Sleep(ctx Context, d time.Duration) (err error)
	RequestCancelExternalWorkflow(ctx Context, workflowID, runID string) Future
	SignalExternalWorkflow(ctx Context, workflowID, runID, signalName string, arg interface{}) Future
	UpsertSearchAttributes(ctx Context, attributes map[string]interface{}) error
	GetSignalChannel(ctx Context, signalName string) Channel
	SideEffect(ctx Context, f func(ctx Context) interface{}) Value
	MutableSideEffect(ctx Context, id string, f func(ctx Context) interface{}, equals func(a, b interface{}) bool) Value
	GetVersion(ctx Context, changeID string, minSupported, maxSupported Version, opts ...GetVersionOption) Version
	SetQueryHandler(ctx Context, queryType string, handler interface{}) error
	IsReplaying(ctx Context) bool
	HasLastCompletionResult(ctx Context) bool
	GetLastCompletionResult(ctx Context, d ...interface{}) error
}

var _ WorkflowInterceptor = (*WorkflowInterceptorBase)(nil)

// WorkflowInterceptorBase forwards every call to Next.
type WorkflowInterceptorBase struct {
	Next WorkflowInterceptor
}

func (t *WorkflowInterceptorBase) ExecuteWorkflow(ctx Context, workflowType string, args ...interface{}) []interface{} {
	return t.Next.ExecuteWorkflow(ctx, workflowType, args...)
}

func (t *WorkflowInterceptorBase) ExecuteActivity(ctx Context, activityType string, args ...interface{}) Future {
	return t.Next.ExecuteActivity(ctx, activityType, args...)
}

func (t *WorkflowInterceptorBase) ExecuteLocalActivity(ctx Context, activityType string, args ...interface{}) Future {
	return t.Next.ExecuteLocalActivity(ctx, activityType, args...)
}

func (t *WorkflowInterceptorBase) ExecuteChildWorkflow(ctx Context, childWorkflowType string, args ...interface{}) ChildWorkflowFuture {
	return t.Next.ExecuteChildWorkflow(ctx, childWorkflowType, args...)
}

func (t *WorkflowInterceptorBase) GetWorkflowInfo(ctx Context) *WorkflowInfo {
	return t.Next.GetWorkflowInfo(ctx)
}

func (t *WorkflowInterceptorBase) GetLogger(ctx Context) *zap.Logger { return t.Next.GetLogger(ctx) }

func (t *WorkflowInterceptorBase) GetMetricsScope(ctx Context) tally.Scope {
	return t.Next.GetMetricsScope(ctx)
}

func (t *WorkflowInterceptorBase) Now(ctx Context) time.Time { return t.Next.Now(ctx) }

func (t *WorkflowInterceptorBase) NewTimer(ctx Context, d time.Duration) Future {
	return t.Next.NewTimer(ctx, d)
}

func (t *WorkflowInterceptorBase) Sleep(ctx Context, d time.Duration) (err error) {
	return t.Next.Sleep(ctx, d)
}

func (t *WorkflowInterceptorBase) RequestCancelExternalWorkflow(ctx Context, workflowID, runID string) Future {
	return t.Next.RequestCancelExternalWorkflow(ctx, workflowID, runID)
}

func (t *WorkflowInterceptorBase) SignalExternalWorkflow(ctx Context, workflowID, runID, signalName string, arg interface{}) Future {
	return t.Next.SignalExternalWorkflow(ctx, workflowID, runID, signalName, arg)
}

func (t *WorkflowInterceptorBase) UpsertSearchAttributes(ctx Context, attributes map[string]interface{}) error {
	return t.Next.UpsertSearchAttributes(ctx, attributes)
}

func (t *WorkflowInterceptorBase) GetSignalChannel(ctx Context, signalName string) Channel {
	return t.Next.GetSignalChannel(ctx, signalName)
}

func (t *WorkflowInterceptorBase) SideEffect(ctx Context, f func(ctx Context) interface{}) Value {
	return t.Next.SideEffect(ctx, f)
}

func (t *WorkflowInterceptorBase) MutableSideEffect(ctx Context, id string, f func(ctx Context) interface{}, equals func(a, b interface{}) bool) Value {
	return t.Next.MutableSideEffect(ctx, id, f, equals)
}

func (t *WorkflowInterceptorBase) GetVersion(ctx Context, changeID string, minSupported, maxSupported Version, opts ...GetVersionOption) Version {
	return t.Next.GetVersion(ctx, changeID, minSupported, maxSupported, opts...)
}

func (t *WorkflowInterceptorBase) SetQueryHandler(ctx Context, queryType string, handler interface{}) error {
	return t.Next.SetQueryHandler(ctx, queryType, handler)
}

func (t *WorkflowInterceptorBase) IsReplaying(ctx Context) bool { return t.Next.IsReplaying(ctx) }

func (t *WorkflowInterceptorBase) HasLastCompletionResult(ctx Context) bool {
	return t.Next.HasLastCompletionResult(ctx)
}

func (t *WorkflowInterceptorBase) GetLastCompletionResult(ctx Context, d ...interface{}) error {
	return t.Next.GetLastCompletionResult(ctx, d...)
}

const (
	interceptorKey contextKey = "workflowInterceptor"
	// activityFnKey and childFnKey carry the function a workflow passed to
	// ExecuteActivity, ExecuteLocalActivity or ExecuteChildWorkflow down
	// the chain, which only passes the type name.
	activityFnKey contextKey = "activityFn"
	childFnKey    contextKey = "childWorkflowFn"
)

// newInterceptorChain builds a workflow's chain; the first factory's
// interceptor is called first.
func newInterceptorChain(info *WorkflowInfo, factories []WorkflowInterceptorFactory, tail WorkflowInterceptor) WorkflowInterceptor {
	head := tail
	for i := len(factories) - 1; i >= 0; i-- {
		head = factories[i].NewInterceptor(info, head)
	}
	return head
}

func interceptor(ctx Context) WorkflowInterceptor {
	if i, ok := ctx.Value(interceptorKey).(WorkflowInterceptor); ok {
		return i
	}
	return envInterceptor{}
}

// fnFor returns the function stored under key when it still has the type
// name the chain passed, and the name otherwise.
func fnFor(ctx Context, key contextKey, name string, activity bool) interface{} {
	fn := ctx.Value(key)
	if fn == nil {
		return name
	}
	reg := getEnv(ctx).Registry()
	if activity && reg.activityName(fn) == name || !activity && reg.workflowName(fn) == name {
		return fn
	}
	return name
}

// envInterceptor is the end of every chain: it does the work.
type envInterceptor struct {
	// invoke calls the workflow function.
	invoke func(ctx Context, args []interface{}) []interface{}
}

func (i envInterceptor) ExecuteWorkflow(ctx Context, _ string, args ...interface{}) []interface{} {
	return i.invoke(ctx, args)
}

func (envInterceptor) ExecuteActivity(ctx Context, activityType string, args ...interface{}) Future {
	return executeActivity(ctx, fnFor(ctx, activityFnKey, activityType, true), args...)
}

func (envInterceptor) ExecuteLocalActivity(ctx Context, activityType string, args ...interface{}) Future {
	return executeLocalActivity(ctx, fnFor(ctx, activityFnKey, activityType, true), args...)
}

func (envInterceptor) ExecuteChildWorkflow(ctx Context, childWorkflowType string, args ...interface{}) ChildWorkflowFuture {
	return executeChildWorkflow(ctx, fnFor(ctx, childFnKey, childWorkflowType, false), args...)
}

func (envInterceptor) GetWorkflowInfo(ctx Context) *WorkflowInfo      { return getInfo(ctx) }
func (envInterceptor) GetLogger(ctx Context) *zap.Logger              { return getLogger(ctx) }
func (envInterceptor) GetMetricsScope(ctx Context) tally.Scope        { return getMetricsScope(ctx) }
func (envInterceptor) Now(ctx Context) time.Time                      { return workflowNow(ctx) }
func (envInterceptor) NewTimer(ctx Context, d time.Duration) Future   { return newTimer(ctx, d) }
func (envInterceptor) Sleep(ctx Context, d time.Duration) (err error) { return sleep(ctx, d) }

func (envInterceptor) RequestCancelExternalWorkflow(ctx Context, workflowID, runID string) Future {
	return requestCancelExternalWorkflow(ctx, workflowID, runID)
}

func (envInterceptor) SignalExternalWorkflow(ctx Context, workflowID, runID, signalName string, arg interface{}) Future {
	return signalExternalWorkflow(ctx, workflowID, runID, signalName, arg)
}

func (envInterceptor) UpsertSearchAttributes(ctx Context, attributes map[string]interface{}) error {
	return upsertSearchAttributes(ctx, attributes)
}

func (envInterceptor) GetSignalChannel(ctx Context, signalName string) Channel {
	return getSignalChannel(ctx, signalName)
}

func (envInterceptor) SideEffect(ctx Context, f func(ctx Context) interface{}) Value {
	return sideEffect(ctx, f)
}

func (envInterceptor) MutableSideEffect(ctx Context, id string, f func(ctx Context) interface{}, equals func(a, b interface{}) bool) Value {
	return mutableSideEffect(ctx, id, f, equals)
}

func (envInterceptor) GetVersion(ctx Context, changeID string, minSupported, maxSupported Version, opts ...GetVersionOption) Version {
	var cfg getVersionConfig
	for _, o := range opts {
		o.apply(&cfg)
	}
	return getVersion(ctx, changeID, minSupported, maxSupported, cfg)
}

func (envInterceptor) SetQueryHandler(ctx Context, queryType string, handler interface{}) error {
	return setQueryHandler(ctx, queryType, handler)
}

func (envInterceptor) IsReplaying(ctx Context) bool             { return isReplaying(ctx) }
func (envInterceptor) HasLastCompletionResult(ctx Context) bool { return hasLastCompletionResult(ctx) }

func (envInterceptor) GetLastCompletionResult(ctx Context, d ...interface{}) error {
	return getLastCompletionResult(ctx, d...)
}

// The workflow API. Each call goes through the workflow's interceptor
// chain.

// ExecuteActivity schedules an activity and returns a future for its result.
func ExecuteActivity(ctx Context, activity interface{}, args ...interface{}) Future {
	name := getEnv(ctx).Registry().activityName(activity)
	return interceptor(ctx).ExecuteActivity(WithValue(ctx, activityFnKey, activity), name, args...)
}

// ExecuteLocalActivity runs an activity inside the workflow worker and
// records its result in history.
func ExecuteLocalActivity(ctx Context, activity interface{}, args ...interface{}) Future {
	name := getEnv(ctx).Registry().activityName(activity)
	return interceptor(ctx).ExecuteLocalActivity(WithValue(ctx, activityFnKey, activity), name, args...)
}

// ExecuteChildWorkflow starts a child workflow.
func ExecuteChildWorkflow(ctx Context, childWorkflow interface{}, args ...interface{}) ChildWorkflowFuture {
	name := getEnv(ctx).Registry().workflowName(childWorkflow)
	return interceptor(ctx).ExecuteChildWorkflow(WithValue(ctx, childFnKey, childWorkflow), name, args...)
}

// GetInfo returns information about the workflow.
func GetInfo(ctx Context) *WorkflowInfo { return interceptor(ctx).GetWorkflowInfo(ctx) }

// GetLogger returns a logger that stays quiet during replay.
func GetLogger(ctx Context) *zap.Logger { return interceptor(ctx).GetLogger(ctx) }

// GetMetricsScope returns a metrics scope that is a no-op during replay.
func GetMetricsScope(ctx Context) tally.Scope { return interceptor(ctx).GetMetricsScope(ctx) }

// Now returns the workflow's deterministic current time.
func Now(ctx Context) time.Time { return interceptor(ctx).Now(ctx) }

// NewTimer returns a future that is ready after d.
func NewTimer(ctx Context, d time.Duration) Future { return interceptor(ctx).NewTimer(ctx, d) }

// Sleep pauses the workflow for d. It returns ErrCanceled if ctx is canceled.
func Sleep(ctx Context, d time.Duration) error { return interceptor(ctx).Sleep(ctx, d) }

// RequestCancelExternalWorkflow asks another workflow to cancel.
func RequestCancelExternalWorkflow(ctx Context, workflowID, runID string) Future {
	return interceptor(ctx).RequestCancelExternalWorkflow(ctx, workflowID, runID)
}

// SignalExternalWorkflow sends a signal to another workflow.
func SignalExternalWorkflow(ctx Context, workflowID, runID, signalName string, arg interface{}) Future {
	return interceptor(ctx).SignalExternalWorkflow(ctx, workflowID, runID, signalName, arg)
}

// UpsertSearchAttributes adds or updates search attributes of the workflow.
func UpsertSearchAttributes(ctx Context, attributes map[string]interface{}) error {
	return interceptor(ctx).UpsertSearchAttributes(ctx, attributes)
}

// GetSignalChannel returns the channel signals named signalName arrive on.
func GetSignalChannel(ctx Context, signalName string) Channel {
	return interceptor(ctx).GetSignalChannel(ctx, signalName)
}

// SideEffect runs f once and records its result so replays return the same value.
func SideEffect(ctx Context, f func(ctx Context) interface{}) Value {
	return interceptor(ctx).SideEffect(ctx, f)
}

// MutableSideEffect records f's result only when it changes.
func MutableSideEffect(ctx Context, id string, f func(ctx Context) interface{}, equals func(a, b interface{}) bool) Value {
	return interceptor(ctx).MutableSideEffect(ctx, id, f, equals)
}

// GetVersion returns the code version to use for changeID.
func GetVersion(ctx Context, changeID string, minSupported, maxSupported Version, opts ...GetVersionOption) Version {
	return interceptor(ctx).GetVersion(ctx, changeID, minSupported, maxSupported, opts...)
}

// SetQueryHandler registers a handler for queryType. The handler takes the
// query arguments and returns (result, error).
func SetQueryHandler(ctx Context, queryType string, handler interface{}) error {
	return interceptor(ctx).SetQueryHandler(ctx, queryType, handler)
}

// IsReplaying reports whether the workflow is replaying history.
func IsReplaying(ctx Context) bool { return interceptor(ctx).IsReplaying(ctx) }

// HasLastCompletionResult reports whether a previous cron run left a result.
func HasLastCompletionResult(ctx Context) bool { return interceptor(ctx).HasLastCompletionResult(ctx) }

// GetLastCompletionResult decodes the result of the previous cron run.
func GetLastCompletionResult(ctx Context, d ...interface{}) error {
	return interceptor(ctx).GetLastCompletionResult(ctx, d...)
}
