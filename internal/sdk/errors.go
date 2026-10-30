package sdk

import (
	"errors"
	"fmt"
	"math"
	"runtime/debug"
	"strings"
	"time"

	"github.com/larsla/takt/api"
)

// Failure reasons used on the wire, the same strings Cadence uses.
const (
	errReasonPanic    = "cadenceInternal:Panic"
	errReasonGeneric  = "cadenceInternal:Generic"
	errReasonCanceled = "cadenceInternal:Canceled"
	errReasonTimeout  = "cadenceInternal:Timeout"
)

// CustomError carries an application-defined reason and optional details.
type CustomError struct {
	reason          string
	details         Values
	failureCategory api.FailureCategory
	nextRetryDelay  time.Duration
}

// CustomErrorOption is passed among the details of NewCustomError to set
// an option instead of a detail.
type CustomErrorOption func(*CustomError)

// WithFailureCategory sets the failure category of an activity error.
// FailureCategoryFatal stops the server from retrying the activity.
func WithFailureCategory(c api.FailureCategory) CustomErrorOption {
	return func(e *CustomError) { e.failureCategory = c }
}

// WithNextRetryDelay sets the delay before the next attempt of the activity
// that returns the error, in place of the retry policy's backoff. Whether
// there is a next attempt is still up to the retry policy.
func WithNextRetryDelay(d time.Duration) CustomErrorOption {
	return func(e *CustomError) { e.nextRetryDelay = d }
}

// NewCustomError creates an error with a reason and details. The reason must
// not start with "cadenceInternal:". CustomErrorOption values among the
// details are applied to the error instead.
func NewCustomError(reason string, details ...interface{}) *CustomError {
	if strings.HasPrefix(reason, "cadenceInternal:") {
		panic("'cadenceInternal:' is reserved prefix, please use different reason")
	}
	if len(details) == 1 {
		if v, ok := details[0].(*encodedValues); ok {
			return &CustomError{reason: reason, details: v}
		}
	}
	e := &CustomError{reason: reason}
	values := make(ErrorDetailsValues, 0, len(details))
	for _, d := range details {
		if o, ok := d.(CustomErrorOption); ok {
			o(e)
			continue
		}
		values = append(values, d)
	}
	if details != nil {
		e.details = values
	} else {
		e.details = ErrorDetailsValues(nil)
	}
	return e
}

// GetFailureCategory returns the category set with WithFailureCategory,
// FailureCategoryStandard by default.
func (e *CustomError) GetFailureCategory() api.FailureCategory {
	if e.failureCategory == "" {
		return api.FailureCategoryStandard
	}
	return e.failureCategory
}

// GetNextRetryDelay returns the delay set with WithNextRetryDelay.
func (e *CustomError) GetNextRetryDelay() time.Duration { return e.nextRetryDelay }

func (e *CustomError) Error() string { return e.reason }

// Reason returns the reason of the error.
func (e *CustomError) Reason() string { return e.reason }

// HasDetails reports whether the error has details.
func (e *CustomError) HasDetails() bool { return e.details != nil && e.details.HasValues() }

// Details decodes the details into the given pointers.
func (e *CustomError) Details(d ...interface{}) error {
	if !e.HasDetails() {
		return ErrNoData
	}
	return e.details.Get(d...)
}

// CanceledError is returned when an operation was canceled.
type CanceledError struct {
	details Values
}

// NewCanceledError creates a CanceledError with optional details.
func NewCanceledError(details ...interface{}) *CanceledError {
	return &CanceledError{details: ErrorDetailsValues(details)}
}

func (e *CanceledError) Error() string { return "CanceledError" }

func (e *CanceledError) HasDetails() bool { return e.details != nil && e.details.HasValues() }

func (e *CanceledError) Details(d ...interface{}) error {
	if !e.HasDetails() {
		return ErrNoData
	}
	return e.details.Get(d...)
}

// TimeoutError is returned when an activity or child workflow timed out.
type TimeoutError struct {
	timeoutType api.TimeoutType
	details     Values
}

// NewTimeoutError creates a TimeoutError.
func NewTimeoutError(timeoutType api.TimeoutType, details ...interface{}) *TimeoutError {
	return &TimeoutError{timeoutType: timeoutType, details: ErrorDetailsValues(details)}
}

// NewHeartbeatTimeoutError creates a heartbeat TimeoutError.
func NewHeartbeatTimeoutError(details ...interface{}) *TimeoutError {
	return NewTimeoutError(api.TimeoutTypeHeartbeat, details...)
}

func (e *TimeoutError) Error() string { return fmt.Sprintf("TimeoutType: %v", e.timeoutType) }

// TimeoutType returns which timeout fired.
func (e *TimeoutError) TimeoutType() api.TimeoutType { return e.timeoutType }

func (e *TimeoutError) HasDetails() bool { return e.details != nil && e.details.HasValues() }

func (e *TimeoutError) Details(d ...interface{}) error {
	if !e.HasDetails() {
		return ErrNoData
	}
	return e.details.Get(d...)
}

// GenericError wraps a plain error that crossed a process boundary.
type GenericError struct {
	err string
}

func (e *GenericError) Error() string { return e.err }

// TerminatedError is returned when a child workflow was terminated.
type TerminatedError struct{}

func (e *TerminatedError) Error() string { return "Terminated" }

// PanicError holds a recovered panic and its stack trace.
type PanicError struct {
	value      interface{}
	stackTrace string
}

func newPanicError(value interface{}, stackTrace string) *PanicError {
	return &PanicError{value: value, stackTrace: stackTrace}
}

func (e *PanicError) Error() string { return fmt.Sprintf("%v", e.value) }

// StackTrace returns where the panic happened.
func (e *PanicError) StackTrace() string { return e.stackTrace }

// UnknownExternalWorkflowExecutionError is returned when signaling or
// canceling a workflow that does not exist.
type UnknownExternalWorkflowExecutionError struct{}

func (e *UnknownExternalWorkflowExecutionError) Error() string {
	return "UnknownExternalWorkflowExecution"
}

// ContinueAsNewError ends a workflow run and starts a new one.
type ContinueAsNewError struct {
	params *continueAsNewParams
}

type continueAsNewParams struct {
	workflowType string
	input        []byte
	args         []interface{}
	cronSchedule string
	taskList     string
	execTimeout  int32
	taskTimeout  int32
	header       *api.Header
}

func (e *ContinueAsNewError) Error() string { return "ContinueAsNew" }

// WorkflowType returns the workflow type of the new run.
func (e *ContinueAsNewError) WorkflowType() *WorkflowType {
	return &WorkflowType{Name: e.params.workflowType}
}

// Args returns the arguments of the new run.
func (e *ContinueAsNewError) Args() []interface{} { return e.params.args }

// Input returns the encoded arguments of the new run.
func (e *ContinueAsNewError) Input() []byte { return e.params.input }

// Header returns the header of the new run.
func (e *ContinueAsNewError) Header() *api.Header { return e.params.header }

// WorkflowIDReusePolicy returns the reuse policy of the new run, which
// takt doesn't change on continue-as-new.
func (e *ContinueAsNewError) WorkflowIDReusePolicy() WorkflowIDReusePolicy {
	return WorkflowIDReusePolicyAllowDuplicateFailedOnly
}

// IsCanceledError reports whether err is a CanceledError.
func IsCanceledError(err error) bool {
	var ce *CanceledError
	return errors.As(err, &ce)
}

// IsCustomError reports whether err is a CustomError.
func IsCustomError(err error) bool {
	var e *CustomError
	return errors.As(err, &e)
}

// IsGenericError reports whether err is a GenericError.
func IsGenericError(err error) bool {
	var e *GenericError
	return errors.As(err, &e)
}

// IsTimeoutError reports whether err is a TimeoutError.
func IsTimeoutError(err error) bool {
	var e *TimeoutError
	return errors.As(err, &e)
}

// IsTerminatedError reports whether err is a TerminatedError.
func IsTerminatedError(err error) bool {
	var e *TerminatedError
	return errors.As(err, &e)
}

// IsPanicError reports whether err is a PanicError.
func IsPanicError(err error) bool {
	var e *PanicError
	return errors.As(err, &e)
}

// IsWorkflowExecutionAlreadyStartedError reports whether err says that a
// workflow with the same ID is already running.
func IsWorkflowExecutionAlreadyStartedError(err error) bool {
	var e *api.WorkflowExecutionAlreadyStartedError
	return errors.As(err, &e)
}

// failureOptions returns the retry options an activity error carries.
func failureOptions(err error) *api.FailureOptions {
	var custom *CustomError
	if !errors.As(err, &custom) || (custom.failureCategory == "" && custom.nextRetryDelay <= 0) {
		return nil
	}
	return &api.FailureOptions{
		FailureCategory:          custom.failureCategory,
		NextRetryIntervalSeconds: int32(min(math.Ceil(custom.nextRetryDelay.Seconds()), math.MaxInt32)),
	}
}

var (
	// ErrCanceled is the error of a canceled workflow context.
	ErrCanceled = NewCanceledError()
	// ErrDeadlineExceeded is the error of a workflow context past its deadline.
	ErrDeadlineExceeded = NewTimeoutError(api.TimeoutTypeScheduleToClose)
	// ErrActivityResultPending is returned by an activity that will be
	// completed later through the client.
	ErrActivityResultPending = errors.New("not error: do not autocomplete, using Client.CompleteActivity() to complete")
)

// errorDetails turns an error into the reason and details sent to the server.
func errorDetails(err error, dc DataConverter) (string, []byte) {
	var (
		custom   *CustomError
		canceled *CanceledError
		panicErr *PanicError
		timeout  *TimeoutError
	)
	switch {
	case errors.As(err, &custom):
		return custom.reason, encodeDetails(custom.details, dc)
	case errors.As(err, &canceled):
		return errReasonCanceled, encodeDetails(canceled.details, dc)
	case errors.As(err, &panicErr):
		data, _ := encodeArgs(dc, []interface{}{panicErr.Error(), panicErr.StackTrace()})
		return errReasonPanic, data
	case errors.As(err, &timeout):
		return fmt.Sprintf("%v %v", errReasonTimeout, timeout.timeoutType), encodeDetails(timeout.details, dc)
	default:
		return errReasonGeneric, []byte(err.Error())
	}
}

func encodeDetails(v Values, dc DataConverter) []byte {
	switch d := v.(type) {
	case ErrorDetailsValues:
		data, _ := encodeArgs(dc, d)
		return data
	case *encodedValues:
		return d.data
	}
	return nil
}

// errorFromDetails rebuilds an error from a reason and details.
func errorFromDetails(reason string, details []byte, dc DataConverter) error {
	switch {
	case reason == errReasonGeneric:
		return &GenericError{err: string(details)}
	case reason == errReasonCanceled:
		return &CanceledError{details: newEncodedValues(details, dc)}
	case reason == errReasonPanic:
		var msg, stack string
		_ = orDefault(dc).FromData(details, &msg, &stack)
		return newPanicError(msg, stack)
	case strings.HasPrefix(reason, errReasonTimeout):
		tt := api.TimeoutType(strings.TrimSpace(strings.TrimPrefix(reason, errReasonTimeout)))
		return &TimeoutError{timeoutType: tt, details: newEncodedValues(details, dc)}
	default:
		return &CustomError{reason: reason, details: newEncodedValues(details, dc)}
	}
}

func stack() string { return string(debug.Stack()) }
