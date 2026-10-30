package sdk

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/uber-go/tally"

	"github.com/larsla/takt/api"
)

// ClientOptions configures a Client.
type ClientOptions struct {
	MetricsScope  tally.Scope
	Identity      string
	DataConverter DataConverter
	// FeatureFlags and IsolationGroup are accepted for compatibility and
	// ignored.
	FeatureFlags   FeatureFlags
	IsolationGroup string
}

// StartWorkflowOptions configures a workflow start.
type StartWorkflowOptions struct {
	// ID defaults to a random UUID.
	ID                              string
	TaskList                        string
	ExecutionStartToCloseTimeout    time.Duration
	DecisionTaskStartToCloseTimeout time.Duration
	WorkflowIDReusePolicy           WorkflowIDReusePolicy
	RetryPolicy                     *RetryPolicy
	CronSchedule                    string
	Memo                            map[string]interface{}
	SearchAttributes                map[string]interface{}
	DelayStart                      time.Duration
	// JitterStart adds a random delay below it to the start of the first
	// run and of each cron run.
	JitterStart time.Duration
	// FirstRunAt starts the first run at this time, in place of DelayStart
	// and JitterStart.
	FirstRunAt time.Time
	// CronOverlapPolicy says what happens to cron fire times that pass
	// while a run is open: CronOverlapPolicySkipped (the default) or
	// CronOverlapPolicyBufferone.
	CronOverlapPolicy api.CronOverlapPolicy
	// RequestID makes starts with the same ID idempotent. It defaults to a
	// random UUID.
	RequestID string
}

// WorkflowRun is a started workflow.
type WorkflowRun interface {
	GetID() string
	GetRunID() string
	// Get waits for the workflow to finish and decodes its result. It follows
	// continue-as-new to the last run.
	Get(ctx context.Context, valuePtr interface{}) error
}

// HistoryEventIterator iterates over a workflow history.
type HistoryEventIterator interface {
	HasNext() bool
	Next() (*api.HistoryEvent, error)
}

// QueryWorkflowWithOptionsRequest is the request of QueryWorkflowWithOptions.
type QueryWorkflowWithOptionsRequest struct {
	WorkflowID           string
	RunID                string
	QueryType            string
	Args                 []interface{}
	QueryRejectCondition *api.QueryRejectCondition
}

// QueryWorkflowWithOptionsResponse is the response of QueryWorkflowWithOptions.
type QueryWorkflowWithOptionsResponse struct {
	QueryResult   Value
	QueryRejected *api.QueryRejected
}

// Client starts and manages workflows.
type Client interface {
	StartWorkflow(ctx context.Context, options StartWorkflowOptions, workflow interface{}, args ...interface{}) (*WorkflowExecution, error)
	ExecuteWorkflow(ctx context.Context, options StartWorkflowOptions, workflow interface{}, args ...interface{}) (WorkflowRun, error)
	GetWorkflow(ctx context.Context, workflowID, runID string) WorkflowRun
	SignalWorkflow(ctx context.Context, workflowID, runID, signalName string, arg interface{}) error
	SignalWithStartWorkflow(ctx context.Context, workflowID, signalName string, signalArg interface{},
		options StartWorkflowOptions, workflow interface{}, workflowArgs ...interface{}) (*WorkflowExecution, error)
	CancelWorkflow(ctx context.Context, workflowID, runID string, opts ...Option) error
	TerminateWorkflow(ctx context.Context, workflowID, runID, reason string, details []byte) error
	GetWorkflowHistory(ctx context.Context, workflowID, runID string, isLongPoll bool, filterType api.HistoryEventFilterType) HistoryEventIterator
	CompleteActivity(ctx context.Context, taskToken []byte, result interface{}, err error) error
	CompleteActivityByID(ctx context.Context, domain, workflowID, runID, activityID string, result interface{}, err error) error
	RecordActivityHeartbeat(ctx context.Context, taskToken []byte, details ...interface{}) error
	RecordActivityHeartbeatByID(ctx context.Context, domain, workflowID, runID, activityID string, details ...interface{}) error
	ListClosedWorkflow(ctx context.Context, request *api.ListClosedWorkflowExecutionsRequest) (*api.ListClosedWorkflowExecutionsResponse, error)
	ListOpenWorkflow(ctx context.Context, request *api.ListOpenWorkflowExecutionsRequest) (*api.ListOpenWorkflowExecutionsResponse, error)
	ListWorkflow(ctx context.Context, request *api.ListWorkflowExecutionsRequest) (*api.ListWorkflowExecutionsResponse, error)
	ScanWorkflow(ctx context.Context, request *api.ListWorkflowExecutionsRequest) (*api.ListWorkflowExecutionsResponse, error)
	CountWorkflow(ctx context.Context, request *api.CountWorkflowExecutionsRequest) (*api.CountWorkflowExecutionsResponse, error)
	QueryWorkflow(ctx context.Context, workflowID, runID, queryType string, args ...interface{}) (Value, error)
	QueryWorkflowWithOptions(ctx context.Context, request *QueryWorkflowWithOptionsRequest) (*QueryWorkflowWithOptionsResponse, error)
	DescribeWorkflowExecution(ctx context.Context, workflowID, runID string) (*api.DescribeWorkflowExecutionResponse, error)
	DescribeTaskList(ctx context.Context, taskList string, taskListType api.TaskListType) (*api.DescribeTaskListResponse, error)
}

type workflowClient struct {
	service  api.Service
	domain   string
	dc       DataConverter
	identity string
	scope    tally.Scope
}

// NewClient creates a client for a domain.
func NewClient(service api.Service, domain string, options *ClientOptions) Client {
	var o ClientOptions
	if options != nil {
		o = *options
	}
	if o.Identity == "" {
		host, _ := os.Hostname()
		o.Identity = fmt.Sprintf("%d@%s", os.Getpid(), host)
	}
	if o.MetricsScope == nil {
		o.MetricsScope = tally.NoopScope
	}
	scope := tagged(o.MetricsScope, tagDomain, domain)
	return &workflowClient{service: withServiceMetrics(service, scope), domain: domain, dc: orDefault(o.DataConverter), identity: o.Identity, scope: scope}
}

func (c *workflowClient) startRequest(options StartWorkflowOptions, workflow interface{}, args []interface{}) (*api.StartWorkflowExecutionRequest, error) {
	if options.TaskList == "" {
		return nil, errors.New("missing TaskList")
	}
	if options.ExecutionStartToCloseTimeout <= 0 {
		return nil, errors.New("missing or invalid ExecutionStartToCloseTimeout")
	}
	if options.DecisionTaskStartToCloseTimeout < 0 {
		return nil, errors.New("negative DecisionTaskStartToCloseTimeout provided")
	}
	if options.DelayStart < 0 {
		return nil, errors.New("negative DelayStart provided")
	}
	if options.JitterStart < 0 {
		return nil, errors.New("negative JitterStart provided")
	}
	if err := options.RetryPolicy.validate(); err != nil {
		return nil, err
	}
	if _, isName := workflow.(string); !isName {
		if err := validateWorkflowFn(workflow); err != nil {
			return nil, err
		}
		if err := validateArgs(workflow, args, 1); err != nil {
			return nil, err
		}
	}
	input, err := encodeArgs(c.dc, args)
	if err != nil {
		return nil, err
	}
	memo, err := encodeMap(c.dc, options.Memo)
	if err != nil {
		return nil, err
	}
	sa, err := encodeMap(defaultConverter, options.SearchAttributes)
	if err != nil {
		return nil, err
	}
	id := options.ID
	if id == "" {
		id = uuid.NewString()
	}
	taskTimeout := options.DecisionTaskStartToCloseTimeout
	if taskTimeout == 0 {
		taskTimeout = 10 * time.Second
	}
	req := &api.StartWorkflowExecutionRequest{
		Domain:                              c.domain,
		WorkflowID:                          id,
		WorkflowType:                        &api.WorkflowType{Name: globalRegistry.workflowName(workflow)},
		TaskList:                            &api.TaskList{Name: options.TaskList},
		Input:                               input,
		ExecutionStartToCloseTimeoutSeconds: durationSeconds(options.ExecutionStartToCloseTimeout),
		TaskStartToCloseTimeoutSeconds:      durationSeconds(taskTimeout),
		Identity:                            c.identity,
		RequestID:                           options.RequestID,
		WorkflowIDReusePolicy:               options.WorkflowIDReusePolicy.toAPI(),
		RetryPolicy:                         options.RetryPolicy.toAPI(),
		CronSchedule:                        options.CronSchedule,
		DelayStartSeconds:                   durationSeconds(options.DelayStart),
		JitterStartSeconds:                  durationSeconds(options.JitterStart),
		CronOverlapPolicy:                   options.CronOverlapPolicy,
	}
	if req.RequestID == "" {
		req.RequestID = uuid.NewString()
	}
	if !options.FirstRunAt.IsZero() {
		req.FirstRunAtTimestamp = options.FirstRunAt.UnixNano()
	}
	if memo != nil {
		req.Memo = &api.Memo{Fields: memo}
	}
	if sa != nil {
		req.SearchAttributes = &api.SearchAttributes{IndexedFields: sa}
	}
	return req, nil
}

// StartWorkflow starts a workflow and returns its execution.
func (c *workflowClient) StartWorkflow(ctx context.Context, options StartWorkflowOptions, workflow interface{}, args ...interface{}) (*WorkflowExecution, error) {
	req, err := c.startRequest(options, workflow, args)
	if err != nil {
		return nil, err
	}
	resp, err := c.service.StartWorkflowExecution(ctx, req)
	if err != nil {
		return nil, err
	}
	return &WorkflowExecution{ID: req.WorkflowID, RunID: resp.RunID}, nil
}

// ExecuteWorkflow starts a workflow and returns a WorkflowRun to wait on.
// If the workflow is already running with the same ID, it returns that run.
func (c *workflowClient) ExecuteWorkflow(ctx context.Context, options StartWorkflowOptions, workflow interface{}, args ...interface{}) (WorkflowRun, error) {
	exec, err := c.StartWorkflow(ctx, options, workflow, args...)
	if err != nil {
		var already *api.WorkflowExecutionAlreadyStartedError
		if !errors.As(err, &already) || options.ID == "" {
			return nil, err
		}
		exec = &WorkflowExecution{ID: options.ID, RunID: already.RunID}
	}
	return c.GetWorkflow(ctx, exec.ID, exec.RunID), nil
}

// GetWorkflow returns a WorkflowRun for an existing workflow.
func (c *workflowClient) GetWorkflow(_ context.Context, workflowID, runID string) WorkflowRun {
	return &workflowRunImpl{client: c, id: workflowID, runID: runID}
}

type workflowRunImpl struct {
	client *workflowClient
	id     string
	runID  string
}

func (r *workflowRunImpl) GetID() string    { return r.id }
func (r *workflowRunImpl) GetRunID() string { return r.runID }

func (r *workflowRunImpl) Get(ctx context.Context, valuePtr interface{}) error {
	runID := r.runID
	continued := false
	for {
		ev, err := r.client.closeEvent(ctx, r.id, runID, continued)
		if err != nil {
			return err
		}
		switch ev.EventType {
		case api.EventTypeWorkflowExecutionCompleted:
			result := ev.WorkflowExecutionCompletedEventAttributes.Result
			if valuePtr == nil || result == nil {
				return nil
			}
			return r.client.dc.FromData(result, valuePtr)
		case api.EventTypeWorkflowExecutionFailed:
			a := ev.WorkflowExecutionFailedEventAttributes
			return errorFromDetails(a.Reason, a.Details, r.client.dc)
		case api.EventTypeWorkflowExecutionCanceled:
			return &CanceledError{details: newEncodedValues(ev.WorkflowExecutionCanceledEventAttributes.Details, r.client.dc)}
		case api.EventTypeWorkflowExecutionTerminated:
			return &TerminatedError{}
		case api.EventTypeWorkflowExecutionTimedOut:
			return &TimeoutError{timeoutType: ev.WorkflowExecutionTimedOutEventAttributes.TimeoutType}
		case api.EventTypeWorkflowExecutionContinuedAsNew:
			runID = ev.WorkflowExecutionContinuedAsNewEventAttributes.NewExecutionRunID
			continued = true
		default:
			return fmt.Errorf("unexpected close event %s", ev.EventType)
		}
	}
}

// closeEvent long-polls until the run closes. A continued run may take a
// moment to appear, so not-found is retried for a while when following one.
func (c *workflowClient) closeEvent(ctx context.Context, workflowID, runID string, continued bool) (*api.HistoryEvent, error) {
	notFoundUntil := time.Now().Add(10 * time.Second)
	for {
		resp, err := c.service.GetWorkflowExecutionHistory(ctx, &api.GetWorkflowExecutionHistoryRequest{
			Domain:                 c.domain,
			Execution:              &api.WorkflowExecution{WorkflowID: workflowID, RunID: runID},
			WaitForNewEvent:        true,
			HistoryEventFilterType: api.HistoryEventFilterTypeCloseEvent,
		})
		var nf *api.EntityNotExistsError
		if continued && errors.As(err, &nf) && time.Now().Before(notFoundUntil) {
			select {
			case <-time.After(100 * time.Millisecond):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if err != nil {
			return nil, err
		}
		if len(resp.History.Events) > 0 {
			return resp.History.Events[len(resp.History.Events)-1], nil
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
}

// SignalWorkflow sends a signal. An empty runID targets the current run.
func (c *workflowClient) SignalWorkflow(ctx context.Context, workflowID, runID, signalName string, arg interface{}) error {
	input, err := encodeArgs(c.dc, []interface{}{arg})
	if err != nil {
		return err
	}
	return c.service.SignalWorkflowExecution(ctx, &api.SignalWorkflowExecutionRequest{
		Domain:            c.domain,
		WorkflowExecution: &api.WorkflowExecution{WorkflowID: workflowID, RunID: runID},
		SignalName:        signalName,
		Input:             input,
		Identity:          c.identity,
		RequestID:         uuid.NewString(),
	})
}

// SignalWithStartWorkflow signals a workflow, starting it first if needed.
func (c *workflowClient) SignalWithStartWorkflow(ctx context.Context, workflowID, signalName string, signalArg interface{},
	options StartWorkflowOptions, workflow interface{}, workflowArgs ...interface{}) (*WorkflowExecution, error) {
	options.ID = workflowID
	req, err := c.startRequest(options, workflow, workflowArgs)
	if err != nil {
		return nil, err
	}
	sigInput, err := encodeArgs(c.dc, []interface{}{signalArg})
	if err != nil {
		return nil, err
	}
	resp, err := c.service.SignalWithStartWorkflowExecution(ctx, &api.SignalWithStartWorkflowExecutionRequest{
		Domain:                              req.Domain,
		WorkflowID:                          req.WorkflowID,
		WorkflowType:                        req.WorkflowType,
		TaskList:                            req.TaskList,
		Input:                               req.Input,
		ExecutionStartToCloseTimeoutSeconds: req.ExecutionStartToCloseTimeoutSeconds,
		TaskStartToCloseTimeoutSeconds:      req.TaskStartToCloseTimeoutSeconds,
		Identity:                            req.Identity,
		RequestID:                           req.RequestID,
		WorkflowIDReusePolicy:               req.WorkflowIDReusePolicy,
		SignalName:                          signalName,
		SignalInput:                         sigInput,
		RetryPolicy:                         req.RetryPolicy,
		CronSchedule:                        req.CronSchedule,
		Memo:                                req.Memo,
		SearchAttributes:                    req.SearchAttributes,
		Header:                              req.Header,
		DelayStartSeconds:                   req.DelayStartSeconds,
		JitterStartSeconds:                  req.JitterStartSeconds,
		FirstRunAtTimestamp:                 req.FirstRunAtTimestamp,
		CronOverlapPolicy:                   req.CronOverlapPolicy,
	})
	if err != nil {
		return nil, err
	}
	return &WorkflowExecution{ID: workflowID, RunID: resp.RunID}, nil
}

// Option is a functional option of a Client method, such as
// WithCancelReason for CancelWorkflow.
type Option interface{ private() }

// CancelReason is the Option WithCancelReason returns.
type CancelReason string

func (CancelReason) private() {}

// WithCancelReason sets the reason CancelWorkflow records in the
// workflow's cancel-requested event.
func WithCancelReason(reason string) Option { return CancelReason(reason) }

// CancelWorkflow requests cancellation of a workflow.
func (c *workflowClient) CancelWorkflow(ctx context.Context, workflowID, runID string, opts ...Option) error {
	req := &api.RequestCancelWorkflowExecutionRequest{
		Domain:            c.domain,
		WorkflowExecution: &api.WorkflowExecution{WorkflowID: workflowID, RunID: runID},
		Identity:          c.identity,
		RequestID:         uuid.NewString(),
	}
	for _, o := range opts {
		if r, ok := o.(CancelReason); ok {
			req.Cause = string(r)
		}
	}
	return c.service.RequestCancelWorkflowExecution(ctx, req)
}

// TerminateWorkflow terminates a workflow right away.
func (c *workflowClient) TerminateWorkflow(ctx context.Context, workflowID, runID, reason string, details []byte) error {
	return c.service.TerminateWorkflowExecution(ctx, &api.TerminateWorkflowExecutionRequest{
		Domain:            c.domain,
		WorkflowExecution: &api.WorkflowExecution{WorkflowID: workflowID, RunID: runID},
		Reason:            reason,
		Details:           details,
		Identity:          c.identity,
	})
}

// GetWorkflowHistory iterates over a workflow's history. With isLongPoll it
// waits for new events until the workflow closes.
func (c *workflowClient) GetWorkflowHistory(ctx context.Context, workflowID, runID string, isLongPoll bool, filterType api.HistoryEventFilterType) HistoryEventIterator {
	return &historyIterator{ctx: ctx, c: c, exec: &api.WorkflowExecution{WorkflowID: workflowID, RunID: runID}, longPoll: isLongPoll, filter: filterType}
}

type historyIterator struct {
	ctx      context.Context
	c        *workflowClient
	exec     *api.WorkflowExecution
	longPoll bool
	filter   api.HistoryEventFilterType
	events   []*api.HistoryEvent
	token    []byte
	started  bool
	err      error
}

func (it *historyIterator) fill() {
	for len(it.events) == 0 && it.err == nil && (!it.started || len(it.token) > 0) {
		it.started = true
		resp, err := it.c.service.GetWorkflowExecutionHistory(it.ctx, &api.GetWorkflowExecutionHistoryRequest{
			Domain: it.c.domain, Execution: it.exec, NextPageToken: it.token,
			WaitForNewEvent: it.longPoll, HistoryEventFilterType: it.filter,
		})
		if err != nil {
			it.err = err
			return
		}
		it.events = resp.History.Events
		it.token = resp.NextPageToken
	}
}

func (it *historyIterator) HasNext() bool {
	it.fill()
	return len(it.events) > 0 || it.err != nil
}

func (it *historyIterator) Next() (*api.HistoryEvent, error) {
	it.fill()
	if it.err != nil {
		err := it.err
		it.err = nil
		it.token = nil
		return nil, err
	}
	if len(it.events) == 0 {
		return nil, errors.New("no more events")
	}
	ev := it.events[0]
	it.events = it.events[1:]
	return ev, nil
}

// CompleteActivity reports the result of an activity that returned
// ErrResultPending. A CanceledError reports cancellation.
func (c *workflowClient) CompleteActivity(ctx context.Context, taskToken []byte, result interface{}, err error) error {
	if len(taskToken) == 0 {
		return errors.New("invalid task token provided")
	}
	if err == nil {
		data, eerr := c.encodeResult(result)
		if eerr != nil {
			return eerr
		}
		return c.service.RespondActivityTaskCompleted(ctx, &api.RespondActivityTaskCompletedRequest{TaskToken: taskToken, Result: data, Identity: c.identity})
	}
	var ce *CanceledError
	if errors.As(err, &ce) {
		return c.service.RespondActivityTaskCanceled(ctx, &api.RespondActivityTaskCanceledRequest{
			TaskToken: taskToken, Details: encodeDetails(ce.details, c.dc), Identity: c.identity,
		})
	}
	reason, details := errorDetails(err, c.dc)
	return c.service.RespondActivityTaskFailed(ctx, &api.RespondActivityTaskFailedRequest{
		TaskToken: taskToken, Reason: reason, Details: details, Identity: c.identity, FailureOptions: failureOptions(err),
	})
}

// CompleteActivityByID is CompleteActivity addressed by activity ID.
func (c *workflowClient) CompleteActivityByID(ctx context.Context, domain, workflowID, runID, activityID string, result interface{}, err error) error {
	if activityID == "" || workflowID == "" || domain == "" {
		return errors.New("empty activity or workflow id or domainName")
	}
	if err == nil {
		data, eerr := c.encodeResult(result)
		if eerr != nil {
			return eerr
		}
		return c.service.RespondActivityTaskCompletedByID(ctx, &api.RespondActivityTaskCompletedByIDRequest{
			Domain: domain, WorkflowID: workflowID, RunID: runID, ActivityID: activityID, Result: data, Identity: c.identity,
		})
	}
	var ce *CanceledError
	if errors.As(err, &ce) {
		return c.service.RespondActivityTaskCanceledByID(ctx, &api.RespondActivityTaskCanceledByIDRequest{
			Domain: domain, WorkflowID: workflowID, RunID: runID, ActivityID: activityID,
			Details: encodeDetails(ce.details, c.dc), Identity: c.identity,
		})
	}
	reason, details := errorDetails(err, c.dc)
	return c.service.RespondActivityTaskFailedByID(ctx, &api.RespondActivityTaskFailedByIDRequest{
		Domain: domain, WorkflowID: workflowID, RunID: runID, ActivityID: activityID, Reason: reason, Details: details, Identity: c.identity,
		FailureOptions: failureOptions(err),
	})
}

func (c *workflowClient) encodeResult(result interface{}) ([]byte, error) {
	if result == nil {
		return nil, nil
	}
	return encodeArgs(c.dc, []interface{}{result})
}

// RecordActivityHeartbeat records a heartbeat for an activity.
func (c *workflowClient) RecordActivityHeartbeat(ctx context.Context, taskToken []byte, details ...interface{}) error {
	data, err := encodeArgs(c.dc, details)
	if err != nil {
		return err
	}
	resp, err := c.service.RecordActivityTaskHeartbeat(ctx, &api.RecordActivityTaskHeartbeatRequest{TaskToken: taskToken, Details: data, Identity: c.identity})
	if err == nil && resp.CancelRequested {
		return NewCanceledError()
	}
	return err
}

// RecordActivityHeartbeatByID records a heartbeat addressed by activity ID.
func (c *workflowClient) RecordActivityHeartbeatByID(ctx context.Context, domain, workflowID, runID, activityID string, details ...interface{}) error {
	data, err := encodeArgs(c.dc, details)
	if err != nil {
		return err
	}
	resp, err := c.service.RecordActivityTaskHeartbeatByID(ctx, &api.RecordActivityTaskHeartbeatByIDRequest{
		Domain: domain, WorkflowID: workflowID, RunID: runID, ActivityID: activityID, Details: data, Identity: c.identity,
	})
	if err == nil && resp.CancelRequested {
		return NewCanceledError()
	}
	return err
}

// ListClosedWorkflow lists closed workflows of the client's domain.
func (c *workflowClient) ListClosedWorkflow(ctx context.Context, request *api.ListClosedWorkflowExecutionsRequest) (*api.ListClosedWorkflowExecutionsResponse, error) {
	if request.Domain == "" {
		request.Domain = c.domain
	}
	return c.service.ListClosedWorkflowExecutions(ctx, request)
}

// ListOpenWorkflow lists open workflows of the client's domain.
func (c *workflowClient) ListOpenWorkflow(ctx context.Context, request *api.ListOpenWorkflowExecutionsRequest) (*api.ListOpenWorkflowExecutionsResponse, error) {
	if request.Domain == "" {
		request.Domain = c.domain
	}
	return c.service.ListOpenWorkflowExecutions(ctx, request)
}

// ListWorkflow lists workflows matching a query.
func (c *workflowClient) ListWorkflow(ctx context.Context, request *api.ListWorkflowExecutionsRequest) (*api.ListWorkflowExecutionsResponse, error) {
	if request.Domain == "" {
		request.Domain = c.domain
	}
	return c.service.ListWorkflowExecutions(ctx, request)
}

// ScanWorkflow is ListWorkflow without ordering guarantees.
func (c *workflowClient) ScanWorkflow(ctx context.Context, request *api.ListWorkflowExecutionsRequest) (*api.ListWorkflowExecutionsResponse, error) {
	return c.ListWorkflow(ctx, request)
}

// CountWorkflow counts workflows matching a query.
func (c *workflowClient) CountWorkflow(ctx context.Context, request *api.CountWorkflowExecutionsRequest) (*api.CountWorkflowExecutionsResponse, error) {
	if request.Domain == "" {
		request.Domain = c.domain
	}
	return c.service.CountWorkflowExecutions(ctx, request)
}

// QueryWorkflow queries a workflow.
func (c *workflowClient) QueryWorkflow(ctx context.Context, workflowID, runID, queryType string, args ...interface{}) (Value, error) {
	resp, err := c.QueryWorkflowWithOptions(ctx, &QueryWorkflowWithOptionsRequest{WorkflowID: workflowID, RunID: runID, QueryType: queryType, Args: args})
	if err != nil {
		return nil, err
	}
	return resp.QueryResult, nil
}

// QueryWorkflowWithOptions queries a workflow with a reject condition.
func (c *workflowClient) QueryWorkflowWithOptions(ctx context.Context, request *QueryWorkflowWithOptionsRequest) (*QueryWorkflowWithOptionsResponse, error) {
	input, err := encodeArgs(c.dc, request.Args)
	if err != nil {
		return nil, err
	}
	req := &api.QueryWorkflowRequest{
		Domain:    c.domain,
		Execution: &api.WorkflowExecution{WorkflowID: request.WorkflowID, RunID: request.RunID},
		Query:     &api.WorkflowQuery{QueryType: request.QueryType, QueryArgs: input},
	}
	if request.QueryRejectCondition != nil {
		req.QueryRejectCondition = *request.QueryRejectCondition
	}
	resp, err := c.service.QueryWorkflow(ctx, req)
	if err != nil {
		return nil, err
	}
	return &QueryWorkflowWithOptionsResponse{
		QueryResult:   newEncodedValue(resp.QueryResult, c.dc),
		QueryRejected: resp.QueryRejected,
	}, nil
}

// DescribeWorkflowExecution describes a run.
func (c *workflowClient) DescribeWorkflowExecution(ctx context.Context, workflowID, runID string) (*api.DescribeWorkflowExecutionResponse, error) {
	return c.service.DescribeWorkflowExecution(ctx, &api.DescribeWorkflowExecutionRequest{
		Domain: c.domain, Execution: &api.WorkflowExecution{WorkflowID: workflowID, RunID: runID},
	})
}

// DescribeTaskList describes the pollers of a task list.
func (c *workflowClient) DescribeTaskList(ctx context.Context, taskList string, taskListType api.TaskListType) (*api.DescribeTaskListResponse, error) {
	return c.service.DescribeTaskList(ctx, &api.DescribeTaskListRequest{
		Domain: c.domain, TaskList: &api.TaskList{Name: taskList}, TaskListType: taskListType, IncludeTaskListStatus: true,
	})
}

// DomainClient manages domains.
type DomainClient interface {
	Register(ctx context.Context, request *api.RegisterDomainRequest) error
	Describe(ctx context.Context, name string) (*api.DescribeDomainResponse, error)
	Update(ctx context.Context, request *api.UpdateDomainRequest) error
}

type domainClient struct {
	service api.Service
}

// NewDomainClient creates a DomainClient.
func NewDomainClient(service api.Service, options *ClientOptions) DomainClient {
	if options != nil && options.MetricsScope != nil {
		service = withServiceMetrics(service, options.MetricsScope)
	}
	return &domainClient{service: service}
}

// Register registers a domain.
func (d *domainClient) Register(ctx context.Context, request *api.RegisterDomainRequest) error {
	return d.service.RegisterDomain(ctx, request)
}

// Describe describes a domain.
func (d *domainClient) Describe(ctx context.Context, name string) (*api.DescribeDomainResponse, error) {
	return d.service.DescribeDomain(ctx, &api.DescribeDomainRequest{Name: name})
}

// Update updates a domain.
func (d *domainClient) Update(ctx context.Context, request *api.UpdateDomainRequest) error {
	_, err := d.service.UpdateDomain(ctx, request)
	return err
}
