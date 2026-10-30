// Package workflow mirrors go.uber.org/cadence/workflow: the API used inside
// workflow functions. Workflow code must be deterministic: use this
// package's Go, Channel, Selector, Now and Sleep instead of the standard
// library equivalents.
package workflow

import (
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/uber-go/tally"
	"go.uber.org/zap"

	"github.com/larsla/takt/api"
	"github.com/larsla/takt/internal/sdk"
)

type (
	// Context is the workflow counterpart of context.Context.
	Context = sdk.Context
	// CancelFunc cancels a Context.
	CancelFunc = sdk.CancelFunc
	// Channel is a workflow-safe channel.
	Channel = sdk.Channel
	// Selector waits on several channels and futures.
	Selector = sdk.Selector
	// Future is the result of an asynchronous operation.
	Future = sdk.Future
	// Settable completes a Future.
	Settable = sdk.Settable
	// ChildWorkflowFuture is the future of a child workflow.
	ChildWorkflowFuture = sdk.ChildWorkflowFuture
	// Info describes the running workflow.
	Info = sdk.WorkflowInfo
	// Execution identifies a workflow run.
	Execution = sdk.WorkflowExecution
	// Type names a workflow.
	Type = sdk.WorkflowType
	// ActivityOptions configures ExecuteActivity.
	ActivityOptions = sdk.ActivityOptions
	// LocalActivityOptions configures ExecuteLocalActivity.
	LocalActivityOptions = sdk.LocalActivityOptions
	// ChildWorkflowOptions configures ExecuteChildWorkflow.
	ChildWorkflowOptions = sdk.ChildWorkflowOptions
	// RegisterOptions configures workflow registration.
	RegisterOptions = sdk.RegisterWorkflowOptions
	// Version is a workflow code version used with GetVersion.
	Version = sdk.Version
	// ContinueAsNewError ends a run and starts a new one.
	ContinueAsNewError = sdk.ContinueAsNewError
	// GenericError is returned by an activity or child workflow that failed
	// with an error other than a CustomError.
	GenericError = sdk.GenericError
	// TimeoutError is returned when an activity or child workflow timed out.
	TimeoutError = sdk.TimeoutError
	// TerminatedError is returned when a workflow was terminated.
	TerminatedError = sdk.TerminatedError
	// PanicError holds a panic in workflow or activity code.
	PanicError = sdk.PanicError
	// UnknownExternalWorkflowExecutionError is returned when signaling or
	// canceling a workflow that does not exist.
	UnknownExternalWorkflowExecutionError = sdk.UnknownExternalWorkflowExecutionError
	// RetryPolicy says how to retry failed activities and workflows.
	RetryPolicy = sdk.RetryPolicy
	// ContextPropagator carries context values across workflow and activity
	// boundaries in headers. Set it in worker.Options and client.Options.
	ContextPropagator = sdk.ContextPropagator
	// HeaderReader reads header fields.
	HeaderReader = sdk.HeaderReader
	// HeaderWriter writes header fields.
	HeaderWriter = sdk.HeaderWriter
	// ParentClosePolicy says what happens to a child when its parent closes.
	ParentClosePolicy = sdk.ParentClosePolicy
	// WorkflowIDReusePolicy says when a workflow ID may be reused.
	WorkflowIDReusePolicy = sdk.WorkflowIDReusePolicy
)

const (
	// DefaultVersion is the version of code that predates a GetVersion call.
	DefaultVersion = sdk.DefaultVersion

	ParentClosePolicyTerminate     = sdk.ParentClosePolicyTerminate
	ParentClosePolicyRequestCancel = sdk.ParentClosePolicyRequestCancel
	ParentClosePolicyAbandon       = sdk.ParentClosePolicyAbandon

	WorkflowIDReusePolicyAllowDuplicateFailedOnly = sdk.WorkflowIDReusePolicyAllowDuplicateFailedOnly
	WorkflowIDReusePolicyAllowDuplicate           = sdk.WorkflowIDReusePolicyAllowDuplicate
	WorkflowIDReusePolicyRejectDuplicate          = sdk.WorkflowIDReusePolicyRejectDuplicate
	WorkflowIDReusePolicyTerminateIfRunning       = sdk.WorkflowIDReusePolicyTerminateIfRunning
)

var (
	// ErrCanceled is the error of a canceled workflow context.
	ErrCanceled = sdk.ErrCanceled
	// ErrDeadlineExceeded is the error of a context past its deadline.
	ErrDeadlineExceeded = sdk.ErrDeadlineExceeded
)

// GetSpanContext returns the tracing span context of a workflow context,
// or nil.
func GetSpanContext(ctx Context) opentracing.SpanContext { return sdk.GetSpanContext(ctx) }

// WithSpanContext returns a workflow context with a tracing span context,
// which activities and child workflows started from it continue.
func WithSpanContext(ctx Context, spanContext opentracing.SpanContext) Context {
	return sdk.WithSpanContext(ctx, spanContext)
}

// NewTimeoutError creates a TimeoutError, for unit tests of workflows.
func NewTimeoutError(timeoutType api.TimeoutType, details ...interface{}) *TimeoutError {
	return sdk.NewTimeoutError(timeoutType, details...)
}

// NewHeartbeatTimeoutError creates a heartbeat TimeoutError, for unit tests
// of workflows.
func NewHeartbeatTimeoutError(details ...interface{}) *TimeoutError {
	return sdk.NewHeartbeatTimeoutError(details...)
}

// Register registers a workflow function with all workers.
func Register(workflowFunc interface{}) { sdk.RegisterWorkflow(workflowFunc) }

// RegisterWithOptions registers a workflow function with options.
func RegisterWithOptions(workflowFunc interface{}, opts RegisterOptions) {
	sdk.RegisterWorkflowWithOptions(workflowFunc, opts)
}

// ExecuteActivity schedules an activity and returns a future for its result.
func ExecuteActivity(ctx Context, activity interface{}, args ...interface{}) Future {
	return sdk.ExecuteActivity(ctx, activity, args...)
}

// ExecuteLocalActivity runs an activity inside the workflow worker.
func ExecuteLocalActivity(ctx Context, activity interface{}, args ...interface{}) Future {
	return sdk.ExecuteLocalActivity(ctx, activity, args...)
}

// ExecuteChildWorkflow starts a child workflow.
func ExecuteChildWorkflow(ctx Context, childWorkflow interface{}, args ...interface{}) ChildWorkflowFuture {
	return sdk.ExecuteChildWorkflow(ctx, childWorkflow, args...)
}

// GetInfo returns information about the running workflow.
func GetInfo(ctx Context) *Info { return sdk.GetInfo(ctx) }

// GetLogger returns a logger that is quiet during replay.
func GetLogger(ctx Context) *zap.Logger { return sdk.GetLogger(ctx) }

// GetMetricsScope returns a metrics scope that is a no-op during replay.
func GetMetricsScope(ctx Context) tally.Scope { return sdk.GetMetricsScope(ctx) }

// RequestCancelExternalWorkflow asks another workflow to cancel.
func RequestCancelExternalWorkflow(ctx Context, workflowID, runID string) Future {
	return sdk.RequestCancelExternalWorkflow(ctx, workflowID, runID)
}

// SignalExternalWorkflow sends a signal to another workflow.
func SignalExternalWorkflow(ctx Context, workflowID, runID, signalName string, arg interface{}) Future {
	return sdk.SignalExternalWorkflow(ctx, workflowID, runID, signalName, arg)
}

// GetSignalChannel returns the channel signals named signalName arrive on.
func GetSignalChannel(ctx Context, signalName string) Channel {
	return sdk.GetSignalChannel(ctx, signalName)
}

// SideEffect runs f once and records the result for replays.
func SideEffect(ctx Context, f func(ctx Context) interface{}) sdk.Value {
	return sdk.SideEffect(ctx, f)
}

// MutableSideEffect records f's result only when it changes.
func MutableSideEffect(ctx Context, id string, f func(ctx Context) interface{}, equals func(a, b interface{}) bool) sdk.Value {
	return sdk.MutableSideEffect(ctx, id, f, equals)
}

// GetVersion returns the code version to use for changeID.
func GetVersion(ctx Context, changeID string, minSupported, maxSupported Version) Version {
	return sdk.GetVersion(ctx, changeID, minSupported, maxSupported)
}

// SetQueryHandler registers a query handler.
func SetQueryHandler(ctx Context, queryType string, handler interface{}) error {
	return sdk.SetQueryHandler(ctx, queryType, handler)
}

// IsReplaying reports whether the workflow is replaying history.
func IsReplaying(ctx Context) bool { return sdk.IsReplaying(ctx) }

// HasLastCompletionResult reports whether a previous cron run left a result.
func HasLastCompletionResult(ctx Context) bool { return sdk.HasLastCompletionResult(ctx) }

// GetLastCompletionResult decodes the result of the previous cron run.
func GetLastCompletionResult(ctx Context, d ...interface{}) error {
	return sdk.GetLastCompletionResult(ctx, d...)
}

// UpsertSearchAttributes adds or updates search attributes.
func UpsertSearchAttributes(ctx Context, attributes map[string]interface{}) error {
	return sdk.UpsertSearchAttributes(ctx, attributes)
}

// NewContinueAsNewError returns an error that continues the workflow as a
// new run with the given function and arguments.
func NewContinueAsNewError(ctx Context, wfn interface{}, args ...interface{}) *ContinueAsNewError {
	return sdk.NewContinueAsNewError(ctx, wfn, args...)
}

// Go starts a workflow goroutine.
func Go(ctx Context, f func(ctx Context)) { sdk.Go(ctx, f) }

// GoNamed starts a named workflow goroutine.
func GoNamed(ctx Context, name string, f func(ctx Context)) { sdk.GoNamed(ctx, name, f) }

// NewChannel creates an unbuffered channel.
func NewChannel(ctx Context) Channel { return sdk.NewChannel(ctx) }

// NewNamedChannel creates a named unbuffered channel.
func NewNamedChannel(ctx Context, name string) Channel { return sdk.NewNamedChannel(ctx, name) }

// NewBufferedChannel creates a buffered channel.
func NewBufferedChannel(ctx Context, size int) Channel { return sdk.NewBufferedChannel(ctx, size) }

// NewNamedBufferedChannel creates a named buffered channel.
func NewNamedBufferedChannel(ctx Context, name string, size int) Channel {
	return sdk.NewNamedBufferedChannel(ctx, name, size)
}

// NewSelector creates a Selector.
func NewSelector(ctx Context) Selector { return sdk.NewSelector(ctx) }

// NewNamedSelector creates a named Selector.
func NewNamedSelector(ctx Context, name string) Selector { return sdk.NewNamedSelector(ctx, name) }

// NewFuture creates a Future and its Settable.
func NewFuture(ctx Context) (Future, Settable) { return sdk.NewFuture(ctx) }

// Now returns the workflow's deterministic current time.
func Now(ctx Context) time.Time { return sdk.Now(ctx) }

// NewTimer returns a future that is ready after d.
func NewTimer(ctx Context, d time.Duration) Future { return sdk.NewTimer(ctx, d) }

// Sleep pauses the workflow for d.
func Sleep(ctx Context, d time.Duration) error { return sdk.Sleep(ctx, d) }

// Await blocks until condition returns true or ctx is canceled.
func Await(ctx Context, condition func() bool) error { return sdk.Await(ctx, condition) }

// AwaitWithTimeout is Await with a timeout.
func AwaitWithTimeout(ctx Context, timeout time.Duration, condition func() bool) (bool, error) {
	return sdk.AwaitWithTimeout(ctx, timeout, condition)
}

// WithValue returns a copy of parent carrying key and val.
func WithValue(parent Context, key interface{}, val interface{}) Context {
	return sdk.WithValue(parent, key, val)
}

// WithCancel returns a cancelable copy of parent.
func WithCancel(parent Context) (Context, CancelFunc) { return sdk.WithCancel(parent) }

// NewDisconnectedContext returns a context that is not canceled with parent.
func NewDisconnectedContext(parent Context) (Context, CancelFunc) {
	return sdk.NewDisconnectedContext(parent)
}

// WithActivityOptions sets the options for activities.
func WithActivityOptions(ctx Context, options ActivityOptions) Context {
	return sdk.WithActivityOptions(ctx, options)
}

// WithLocalActivityOptions sets the options for local activities.
func WithLocalActivityOptions(ctx Context, options LocalActivityOptions) Context {
	return sdk.WithLocalActivityOptions(ctx, options)
}

// WithChildOptions sets the options for child workflows.
func WithChildOptions(ctx Context, options ChildWorkflowOptions) Context {
	return sdk.WithChildOptions(ctx, options)
}

// WithTaskList sets the activity task list.
func WithTaskList(ctx Context, name string) Context { return sdk.WithTaskList(ctx, name) }

// WithScheduleToCloseTimeout sets the activity schedule-to-close timeout.
func WithScheduleToCloseTimeout(ctx Context, d time.Duration) Context {
	return sdk.WithScheduleToCloseTimeout(ctx, d)
}

// WithScheduleToStartTimeout sets the activity schedule-to-start timeout.
func WithScheduleToStartTimeout(ctx Context, d time.Duration) Context {
	return sdk.WithScheduleToStartTimeout(ctx, d)
}

// WithStartToCloseTimeout sets the activity start-to-close timeout.
func WithStartToCloseTimeout(ctx Context, d time.Duration) Context {
	return sdk.WithStartToCloseTimeout(ctx, d)
}

// WithHeartbeatTimeout sets the activity heartbeat timeout.
func WithHeartbeatTimeout(ctx Context, d time.Duration) Context {
	return sdk.WithHeartbeatTimeout(ctx, d)
}

// WithWaitForCancellation sets whether canceled activities are waited for.
func WithWaitForCancellation(ctx Context, wait bool) Context {
	return sdk.WithWaitForCancellation(ctx, wait)
}

// WithRetryPolicy sets the activity retry policy.
func WithRetryPolicy(ctx Context, retryPolicy sdk.RetryPolicy) Context {
	return sdk.WithRetryPolicy(ctx, retryPolicy)
}

// WithWorkflowDomain sets the child workflow domain.
func WithWorkflowDomain(ctx Context, name string) Context { return sdk.WithWorkflowDomain(ctx, name) }

// WithWorkflowTaskList sets the child workflow task list.
func WithWorkflowTaskList(ctx Context, name string) Context {
	return sdk.WithWorkflowTaskList(ctx, name)
}

// WithWorkflowID sets the child workflow ID.
func WithWorkflowID(ctx Context, workflowID string) Context {
	return sdk.WithWorkflowID(ctx, workflowID)
}

// WithExecutionStartToCloseTimeout sets the child workflow timeout.
func WithExecutionStartToCloseTimeout(ctx Context, d time.Duration) Context {
	return sdk.WithExecutionStartToCloseTimeout(ctx, d)
}

// WithWorkflowTaskStartToCloseTimeout sets the child decision task timeout.
func WithWorkflowTaskStartToCloseTimeout(ctx Context, d time.Duration) Context {
	return sdk.WithWorkflowTaskStartToCloseTimeout(ctx, d)
}

// WithDataConverter sets the payload converter.
func WithDataConverter(ctx Context, dc sdk.DataConverter) Context {
	return sdk.WithDataConverter(ctx, dc)
}
