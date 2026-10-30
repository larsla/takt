package sdk

import (
	"errors"
	"math"
	"time"

	"github.com/larsla/takt/api"
)

// RetryPolicy says how to retry failed activities and workflows.
type RetryPolicy struct {
	// InitialInterval is the backoff before the first retry. Required.
	InitialInterval time.Duration
	// BackoffCoefficient multiplies the interval after each retry. Must be
	// at least 1; 0 means 2.
	BackoffCoefficient float64
	// MaximumInterval caps the backoff. 0 means no cap.
	MaximumInterval time.Duration
	// ExpirationInterval caps the total time spent retrying. 0 means no cap
	// when MaximumAttempts is set.
	ExpirationInterval time.Duration
	// MaximumAttempts caps the number of attempts, including the first.
	MaximumAttempts int32
	// NonRetriableErrorReasons lists failure reasons that are not retried.
	NonRetriableErrorReasons []string
}

func durationSeconds(d time.Duration) int32 {
	if d <= 0 {
		return 0
	}
	s := math.Ceil(d.Seconds())
	if s > math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(s)
}

func (p *RetryPolicy) validate() error {
	if p == nil {
		return nil
	}
	if p.InitialInterval <= 0 {
		return errors.New("retry policy: InitialInterval must be greater than 0")
	}
	if p.BackoffCoefficient != 0 && p.BackoffCoefficient < 1 {
		return errors.New("retry policy: BackoffCoefficient must be at least 1")
	}
	if p.MaximumInterval != 0 && p.MaximumInterval < p.InitialInterval {
		return errors.New("retry policy: MaximumInterval must not be less than InitialInterval")
	}
	if p.MaximumAttempts < 0 {
		return errors.New("retry policy: MaximumAttempts must not be negative")
	}
	if p.ExpirationInterval < 0 {
		return errors.New("retry policy: ExpirationInterval must not be negative")
	}
	if p.MaximumAttempts == 0 && p.ExpirationInterval == 0 {
		return errors.New("retry policy: one of MaximumAttempts or ExpirationInterval must be set")
	}
	return nil
}

func (p *RetryPolicy) toAPI() *api.RetryPolicy {
	if p == nil {
		return nil
	}
	coeff := p.BackoffCoefficient
	if coeff == 0 {
		coeff = 2
	}
	return &api.RetryPolicy{
		InitialIntervalInSeconds:    durationSeconds(p.InitialInterval),
		BackoffCoefficient:          coeff,
		MaximumIntervalInSeconds:    durationSeconds(p.MaximumInterval),
		MaximumAttempts:             p.MaximumAttempts,
		NonRetriableErrorReasons:    p.NonRetriableErrorReasons,
		ExpirationIntervalInSeconds: durationSeconds(p.ExpirationInterval),
	}
}

// ActivityOptions configures ExecuteActivity.
type ActivityOptions struct {
	TaskList               string
	ScheduleToCloseTimeout time.Duration
	ScheduleToStartTimeout time.Duration
	StartToCloseTimeout    time.Duration
	HeartbeatTimeout       time.Duration
	// WaitForCancellation makes a canceled activity's future wait for the
	// activity to actually stop instead of failing right away.
	WaitForCancellation bool
	// ActivityID defaults to a sequence number unique within the workflow.
	ActivityID  string
	RetryPolicy *RetryPolicy
}

// LocalActivityOptions configures ExecuteLocalActivity.
type LocalActivityOptions struct {
	ScheduleToCloseTimeout time.Duration
	RetryPolicy            *RetryPolicy
}

// ChildWorkflowOptions configures ExecuteChildWorkflow.
type ChildWorkflowOptions struct {
	Domain                       string
	WorkflowID                   string
	TaskList                     string
	ExecutionStartToCloseTimeout time.Duration
	TaskStartToCloseTimeout      time.Duration
	WaitForCancellation          bool
	WorkflowIDReusePolicy        WorkflowIDReusePolicy
	RetryPolicy                  *RetryPolicy
	CronSchedule                 string
	Memo                         map[string]interface{}
	SearchAttributes             map[string]interface{}
	ParentClosePolicy            ParentClosePolicy
	// Bugports is accepted for compatibility with Cadence and not used.
	Bugports Bugports
}

// Bugports turns on old Cadence client behavior. takt accepts it and
// ignores it.
type Bugports struct {
	StartChildWorkflowsOnCanceledContext bool
}

// WorkflowIDReusePolicy says when a workflow ID may be reused.
type WorkflowIDReusePolicy int

const (
	WorkflowIDReusePolicyAllowDuplicateFailedOnly WorkflowIDReusePolicy = iota
	WorkflowIDReusePolicyAllowDuplicate
	WorkflowIDReusePolicyRejectDuplicate
	WorkflowIDReusePolicyTerminateIfRunning
)

func (p WorkflowIDReusePolicy) toAPI() api.WorkflowIDReusePolicy {
	switch p {
	case WorkflowIDReusePolicyAllowDuplicate:
		return api.WorkflowIDReusePolicyAllowDuplicate
	case WorkflowIDReusePolicyRejectDuplicate:
		return api.WorkflowIDReusePolicyRejectDuplicate
	case WorkflowIDReusePolicyTerminateIfRunning:
		return api.WorkflowIDReusePolicyTerminateIfRunning
	default:
		return api.WorkflowIDReusePolicyAllowDuplicateFailedOnly
	}
}

// ParentClosePolicy says what happens to a child when its parent closes.
type ParentClosePolicy int

const (
	ParentClosePolicyTerminate ParentClosePolicy = iota
	ParentClosePolicyRequestCancel
	ParentClosePolicyAbandon
)

func (p ParentClosePolicy) toAPI() api.ParentClosePolicy {
	switch p {
	case ParentClosePolicyRequestCancel:
		return api.ParentClosePolicyRequestCancel
	case ParentClosePolicyAbandon:
		return api.ParentClosePolicyAbandon
	default:
		return api.ParentClosePolicyTerminate
	}
}

const (
	activityOptionsKey      contextKey = "activityOptions"
	localActivityOptionsKey contextKey = "localActivityOptions"
	childOptionsKey         contextKey = "childWorkflowOptions"
	dataConverterKey        contextKey = "dataConverter"
	workflowEnvKey          contextKey = "workflowEnv"
)

func activityOptions(ctx Context) ActivityOptions {
	if o, ok := ctx.Value(activityOptionsKey).(*ActivityOptions); ok {
		return *o
	}
	return ActivityOptions{}
}

func withActivityOptions(ctx Context, f func(*ActivityOptions)) Context {
	o := activityOptions(ctx)
	f(&o)
	return WithValue(ctx, activityOptionsKey, &o)
}

// WithActivityOptions sets the options for activities started with ctx.
func WithActivityOptions(ctx Context, options ActivityOptions) Context {
	return withActivityOptions(ctx, func(o *ActivityOptions) { *o = options })
}

func WithTaskList(ctx Context, name string) Context {
	return withActivityOptions(ctx, func(o *ActivityOptions) { o.TaskList = name })
}

func WithScheduleToCloseTimeout(ctx Context, d time.Duration) Context {
	return withActivityOptions(ctx, func(o *ActivityOptions) { o.ScheduleToCloseTimeout = d })
}

func WithScheduleToStartTimeout(ctx Context, d time.Duration) Context {
	return withActivityOptions(ctx, func(o *ActivityOptions) { o.ScheduleToStartTimeout = d })
}

func WithStartToCloseTimeout(ctx Context, d time.Duration) Context {
	return withActivityOptions(ctx, func(o *ActivityOptions) { o.StartToCloseTimeout = d })
}

func WithHeartbeatTimeout(ctx Context, d time.Duration) Context {
	return withActivityOptions(ctx, func(o *ActivityOptions) { o.HeartbeatTimeout = d })
}

func WithWaitForCancellation(ctx Context, wait bool) Context {
	return withActivityOptions(ctx, func(o *ActivityOptions) { o.WaitForCancellation = wait })
}

func WithRetryPolicy(ctx Context, p RetryPolicy) Context {
	return withActivityOptions(ctx, func(o *ActivityOptions) { o.RetryPolicy = &p })
}

// WithLocalActivityOptions sets the options for local activities.
func WithLocalActivityOptions(ctx Context, options LocalActivityOptions) Context {
	o := options
	return WithValue(ctx, localActivityOptionsKey, &o)
}

func localActivityOptions(ctx Context) LocalActivityOptions {
	if o, ok := ctx.Value(localActivityOptionsKey).(*LocalActivityOptions); ok {
		return *o
	}
	return LocalActivityOptions{}
}

func childOptions(ctx Context) ChildWorkflowOptions {
	if o, ok := ctx.Value(childOptionsKey).(*ChildWorkflowOptions); ok {
		return *o
	}
	return ChildWorkflowOptions{}
}

func withChildOptions(ctx Context, f func(*ChildWorkflowOptions)) Context {
	o := childOptions(ctx)
	f(&o)
	return WithValue(ctx, childOptionsKey, &o)
}

// WithChildOptions sets the options for child workflows started with ctx.
func WithChildOptions(ctx Context, options ChildWorkflowOptions) Context {
	return withChildOptions(ctx, func(o *ChildWorkflowOptions) { *o = options })
}

func WithWorkflowDomain(ctx Context, name string) Context {
	return withChildOptions(ctx, func(o *ChildWorkflowOptions) { o.Domain = name })
}

func WithWorkflowTaskList(ctx Context, name string) Context {
	return withChildOptions(ctx, func(o *ChildWorkflowOptions) { o.TaskList = name })
}

func WithWorkflowID(ctx Context, id string) Context {
	return withChildOptions(ctx, func(o *ChildWorkflowOptions) { o.WorkflowID = id })
}

func WithExecutionStartToCloseTimeout(ctx Context, d time.Duration) Context {
	return withChildOptions(ctx, func(o *ChildWorkflowOptions) { o.ExecutionStartToCloseTimeout = d })
}

func WithWorkflowTaskStartToCloseTimeout(ctx Context, d time.Duration) Context {
	return withChildOptions(ctx, func(o *ChildWorkflowOptions) { o.TaskStartToCloseTimeout = d })
}

// WithChildWorkflowOptions is an alias of WithChildOptions.
func WithChildWorkflowOptions(ctx Context, options ChildWorkflowOptions) Context {
	return WithChildOptions(ctx, options)
}

// WithDataConverter sets the converter for payloads created with ctx.
func WithDataConverter(ctx Context, dc DataConverter) Context {
	if dc == nil {
		panic("data converter is nil")
	}
	return WithValue(ctx, dataConverterKey, dc)
}

func contextDataConverter(ctx Context) DataConverter {
	if ctx == nil {
		return defaultConverter
	}
	if dc, ok := ctx.Value(dataConverterKey).(DataConverter); ok {
		return dc
	}
	return defaultConverter
}
