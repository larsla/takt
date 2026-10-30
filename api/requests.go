package api

// Domains

type RegisterDomainRequest struct {
	Name                                   string            `json:"name"`
	Description                            string            `json:"description,omitempty"`
	OwnerEmail                             string            `json:"ownerEmail,omitempty"`
	WorkflowExecutionRetentionPeriodInDays int32             `json:"workflowExecutionRetentionPeriodInDays,omitempty"`
	Data                                   map[string]string `json:"data,omitempty"`
	SecurityToken                          string            `json:"securityToken,omitempty"`
}

type DescribeDomainRequest struct {
	Name string `json:"name"`
}

type DomainInfo struct {
	Name        string            `json:"name"`
	Status      DomainStatus      `json:"status,omitempty"`
	Description string            `json:"description,omitempty"`
	OwnerEmail  string            `json:"ownerEmail,omitempty"`
	Data        map[string]string `json:"data,omitempty"`
	UUID        string            `json:"uuid,omitempty"`
}

type DomainConfiguration struct {
	WorkflowExecutionRetentionPeriodInDays int32 `json:"workflowExecutionRetentionPeriodInDays,omitempty"`
}

type DescribeDomainResponse struct {
	DomainInfo    *DomainInfo          `json:"domainInfo,omitempty"`
	Configuration *DomainConfiguration `json:"configuration,omitempty"`
}

type UpdateDomainInfo struct {
	Description *string           `json:"description,omitempty"`
	OwnerEmail  *string           `json:"ownerEmail,omitempty"`
	Data        map[string]string `json:"data,omitempty"`
}

type UpdateDomainRequest struct {
	Name          string               `json:"name"`
	UpdatedInfo   *UpdateDomainInfo    `json:"updatedInfo,omitempty"`
	Configuration *DomainConfiguration `json:"configuration,omitempty"`
}

type UpdateDomainResponse = DescribeDomainResponse

type ListDomainsRequest struct {
	PageSize      int32  `json:"pageSize,omitempty"`
	NextPageToken []byte `json:"nextPageToken,omitempty"`
}

type ListDomainsResponse struct {
	Domains       []*DescribeDomainResponse `json:"domains"`
	NextPageToken []byte                    `json:"nextPageToken,omitempty"`
}

// Workflow lifecycle

type StartWorkflowExecutionRequest struct {
	Domain                              string                `json:"domain"`
	WorkflowID                          string                `json:"workflowId"`
	WorkflowType                        *WorkflowType         `json:"workflowType,omitempty"`
	TaskList                            *TaskList             `json:"taskList,omitempty"`
	Input                               []byte                `json:"input,omitempty"`
	ExecutionStartToCloseTimeoutSeconds int32                 `json:"executionStartToCloseTimeoutSeconds,omitempty"`
	TaskStartToCloseTimeoutSeconds      int32                 `json:"taskStartToCloseTimeoutSeconds,omitempty"`
	Identity                            string                `json:"identity,omitempty"`
	RequestID                           string                `json:"requestId,omitempty"`
	WorkflowIDReusePolicy               WorkflowIDReusePolicy `json:"workflowIdReusePolicy,omitempty"`
	RetryPolicy                         *RetryPolicy          `json:"retryPolicy,omitempty"`
	CronSchedule                        string                `json:"cronSchedule,omitempty"`
	Memo                                *Memo                 `json:"memo,omitempty"`
	SearchAttributes                    *SearchAttributes     `json:"searchAttributes,omitempty"`
	Header                              *Header               `json:"header,omitempty"`
	DelayStartSeconds                   int32                 `json:"delayStartSeconds,omitempty"`
	JitterStartSeconds                  int32                 `json:"jitterStartSeconds,omitempty"`
	FirstRunAtTimestamp                 int64                 `json:"firstRunAtTimestamp,omitempty"`
	CronOverlapPolicy                   CronOverlapPolicy     `json:"cronOverlapPolicy,omitempty"`
}

type StartWorkflowExecutionResponse struct {
	RunID string `json:"runId"`
}

type SignalWorkflowExecutionRequest struct {
	Domain            string             `json:"domain"`
	WorkflowExecution *WorkflowExecution `json:"workflowExecution,omitempty"`
	SignalName        string             `json:"signalName"`
	Input             []byte             `json:"input,omitempty"`
	Identity          string             `json:"identity,omitempty"`
	RequestID         string             `json:"requestId,omitempty"`
	Control           []byte             `json:"control,omitempty"`
}

type SignalWithStartWorkflowExecutionRequest struct {
	Domain                              string                `json:"domain"`
	WorkflowID                          string                `json:"workflowId"`
	WorkflowType                        *WorkflowType         `json:"workflowType,omitempty"`
	TaskList                            *TaskList             `json:"taskList,omitempty"`
	Input                               []byte                `json:"input,omitempty"`
	ExecutionStartToCloseTimeoutSeconds int32                 `json:"executionStartToCloseTimeoutSeconds,omitempty"`
	TaskStartToCloseTimeoutSeconds      int32                 `json:"taskStartToCloseTimeoutSeconds,omitempty"`
	Identity                            string                `json:"identity,omitempty"`
	RequestID                           string                `json:"requestId,omitempty"`
	WorkflowIDReusePolicy               WorkflowIDReusePolicy `json:"workflowIdReusePolicy,omitempty"`
	SignalName                          string                `json:"signalName"`
	SignalInput                         []byte                `json:"signalInput,omitempty"`
	Control                             []byte                `json:"control,omitempty"`
	RetryPolicy                         *RetryPolicy          `json:"retryPolicy,omitempty"`
	CronSchedule                        string                `json:"cronSchedule,omitempty"`
	Memo                                *Memo                 `json:"memo,omitempty"`
	SearchAttributes                    *SearchAttributes     `json:"searchAttributes,omitempty"`
	Header                              *Header               `json:"header,omitempty"`
	DelayStartSeconds                   int32                 `json:"delayStartSeconds,omitempty"`
	JitterStartSeconds                  int32                 `json:"jitterStartSeconds,omitempty"`
	FirstRunAtTimestamp                 int64                 `json:"firstRunAtTimestamp,omitempty"`
	CronOverlapPolicy                   CronOverlapPolicy     `json:"cronOverlapPolicy,omitempty"`
}

type RequestCancelWorkflowExecutionRequest struct {
	Domain            string             `json:"domain"`
	WorkflowExecution *WorkflowExecution `json:"workflowExecution,omitempty"`
	Identity          string             `json:"identity,omitempty"`
	RequestID         string             `json:"requestId,omitempty"`
	Cause             string             `json:"cause,omitempty"`
}

type TerminateWorkflowExecutionRequest struct {
	Domain            string             `json:"domain"`
	WorkflowExecution *WorkflowExecution `json:"workflowExecution,omitempty"`
	Reason            string             `json:"reason,omitempty"`
	Details           []byte             `json:"details,omitempty"`
	Identity          string             `json:"identity,omitempty"`
}

type GetWorkflowExecutionHistoryRequest struct {
	Domain                 string                 `json:"domain"`
	Execution              *WorkflowExecution     `json:"execution,omitempty"`
	MaximumPageSize        int32                  `json:"maximumPageSize,omitempty"`
	NextPageToken          []byte                 `json:"nextPageToken,omitempty"`
	WaitForNewEvent        bool                   `json:"waitForNewEvent,omitempty"`
	HistoryEventFilterType HistoryEventFilterType `json:"HistoryEventFilterType,omitempty"`
}

type GetWorkflowExecutionHistoryResponse struct {
	History       *History `json:"history,omitempty"`
	NextPageToken []byte   `json:"nextPageToken,omitempty"`
}

type DescribeWorkflowExecutionRequest struct {
	Domain    string             `json:"domain"`
	Execution *WorkflowExecution `json:"execution,omitempty"`
}

type WorkflowExecutionConfiguration struct {
	TaskList                            *TaskList `json:"taskList,omitempty"`
	ExecutionStartToCloseTimeoutSeconds int32     `json:"executionStartToCloseTimeoutSeconds,omitempty"`
	TaskStartToCloseTimeoutSeconds      int32     `json:"taskStartToCloseTimeoutSeconds,omitempty"`
}

type WorkflowExecutionInfo struct {
	Execution        *WorkflowExecution            `json:"execution,omitempty"`
	Type             *WorkflowType                 `json:"type,omitempty"`
	StartTime        int64                         `json:"startTime,omitempty"`
	CloseTime        int64                         `json:"closeTime,omitempty"`
	CloseStatus      *WorkflowExecutionCloseStatus `json:"closeStatus,omitempty"`
	HistoryLength    int64                         `json:"historyLength,omitempty"`
	ParentDomain     string                        `json:"parentDomain,omitempty"`
	ParentExecution  *WorkflowExecution            `json:"parentExecution,omitempty"`
	ExecutionTime    int64                         `json:"executionTime,omitempty"`
	Memo             *Memo                         `json:"memo,omitempty"`
	SearchAttributes *SearchAttributes             `json:"searchAttributes,omitempty"`
	TaskList         string                        `json:"taskList,omitempty"`
	CronSchedule     string                        `json:"cronSchedule,omitempty"`
}

type PendingActivityInfo struct {
	ActivityID             string               `json:"activityID"`
	ActivityType           *ActivityType        `json:"activityType,omitempty"`
	State                  PendingActivityState `json:"state,omitempty"`
	HeartbeatDetails       []byte               `json:"heartbeatDetails,omitempty"`
	LastHeartbeatTimestamp int64                `json:"lastHeartbeatTimestamp,omitempty"`
	LastStartedTimestamp   int64                `json:"lastStartedTimestamp,omitempty"`
	Attempt                int32                `json:"attempt,omitempty"`
	MaximumAttempts        int32                `json:"maximumAttempts,omitempty"`
	ScheduledTimestamp     int64                `json:"scheduledTimestamp,omitempty"`
	ExpirationTimestamp    int64                `json:"expirationTimestamp,omitempty"`
	LastFailureReason      string               `json:"lastFailureReason,omitempty"`
	LastFailureDetails     []byte               `json:"lastFailureDetails,omitempty"`
	LastWorkerIdentity     string               `json:"lastWorkerIdentity,omitempty"`
}

type PendingChildExecutionInfo struct {
	Domain            string            `json:"domain,omitempty"`
	WorkflowID        string            `json:"workflowID"`
	RunID             string            `json:"runID,omitempty"`
	WorkflowTypeName  string            `json:"workflowTypName,omitempty"`
	InitiatedID       int64             `json:"initiatedID"`
	ParentClosePolicy ParentClosePolicy `json:"parentClosePolicy,omitempty"`
}

type PendingDecisionInfo struct {
	State                      PendingDecisionState `json:"state,omitempty"`
	ScheduledTimestamp         int64                `json:"scheduledTimestamp,omitempty"`
	StartedTimestamp           int64                `json:"startedTimestamp,omitempty"`
	Attempt                    int64                `json:"attempt,omitempty"`
	OriginalScheduledTimestamp int64                `json:"originalScheduledTimestamp,omitempty"`
}

type DescribeWorkflowExecutionResponse struct {
	ExecutionConfiguration *WorkflowExecutionConfiguration `json:"executionConfiguration,omitempty"`
	WorkflowExecutionInfo  *WorkflowExecutionInfo          `json:"workflowExecutionInfo,omitempty"`
	PendingActivities      []*PendingActivityInfo          `json:"pendingActivities,omitempty"`
	PendingChildren        []*PendingChildExecutionInfo    `json:"pendingChildren,omitempty"`
	PendingDecision        *PendingDecisionInfo            `json:"pendingDecision,omitempty"`
}

// Queries

type WorkflowQuery struct {
	QueryType string `json:"queryType"`
	QueryArgs []byte `json:"queryArgs,omitempty"`
}

type QueryWorkflowRequest struct {
	Domain               string               `json:"domain"`
	Execution            *WorkflowExecution   `json:"execution,omitempty"`
	Query                *WorkflowQuery       `json:"query,omitempty"`
	QueryRejectCondition QueryRejectCondition `json:"queryRejectCondition,omitempty"`
}

type QueryRejected struct {
	CloseStatus *WorkflowExecutionCloseStatus `json:"closeStatus,omitempty"`
}

type QueryWorkflowResponse struct {
	QueryResult   []byte         `json:"queryResult,omitempty"`
	QueryRejected *QueryRejected `json:"queryRejected,omitempty"`
}

// WorkerQueryRequest is what the server sends to a worker on the task list's
// query subject. The worker answers with a WorkerQueryResponse.
type WorkerQueryRequest struct {
	Domain       string             `json:"domain"`
	Execution    *WorkflowExecution `json:"execution"`
	WorkflowType *WorkflowType      `json:"workflowType"`
	Query        *WorkflowQuery     `json:"query"`
}

type WorkerQueryResponse struct {
	Result       []byte `json:"result,omitempty"`
	ErrorMessage string `json:"errorMessage,omitempty"`
}

// Decision tasks

type PollForDecisionTaskRequest struct {
	Domain         string    `json:"domain"`
	TaskList       *TaskList `json:"taskList,omitempty"`
	Identity       string    `json:"identity,omitempty"`
	BinaryChecksum string    `json:"binaryChecksum,omitempty"`
}

type PollForDecisionTaskResponse struct {
	TaskToken                 []byte             `json:"taskToken,omitempty"`
	WorkflowExecution         *WorkflowExecution `json:"workflowExecution,omitempty"`
	WorkflowType              *WorkflowType      `json:"workflowType,omitempty"`
	PreviousStartedEventID    int64              `json:"previousStartedEventId,omitempty"`
	StartedEventID            int64              `json:"startedEventId,omitempty"`
	Attempt                   int64              `json:"attempt,omitempty"`
	BacklogCountHint          int64              `json:"backlogCountHint,omitempty"`
	History                   *History           `json:"history,omitempty"`
	NextPageToken             []byte             `json:"nextPageToken,omitempty"`
	WorkflowExecutionTaskList *TaskList          `json:"WorkflowExecutionTaskList,omitempty"`
	ScheduledTimestamp        int64              `json:"scheduledTimestamp,omitempty"`
	StartedTimestamp          int64              `json:"startedTimestamp,omitempty"`
}

type StickyExecutionAttributes struct {
	WorkerTaskList                *TaskList `json:"workerTaskList,omitempty"`
	ScheduleToStartTimeoutSeconds int32     `json:"scheduleToStartTimeoutSeconds,omitempty"`
}

type RespondDecisionTaskCompletedRequest struct {
	TaskToken                  []byte                     `json:"taskToken"`
	Decisions                  []*Decision                `json:"decisions,omitempty"`
	ExecutionContext           []byte                     `json:"executionContext,omitempty"`
	Identity                   string                     `json:"identity,omitempty"`
	StickyAttributes           *StickyExecutionAttributes `json:"stickyAttributes,omitempty"`
	ReturnNewDecisionTask      bool                       `json:"returnNewDecisionTask,omitempty"`
	ForceCreateNewDecisionTask bool                       `json:"forceCreateNewDecisionTask,omitempty"`
	BinaryChecksum             string                     `json:"binaryChecksum,omitempty"`
}

type RespondDecisionTaskCompletedResponse struct {
	DecisionTask *PollForDecisionTaskResponse `json:"decisionTask,omitempty"`
}

type RespondDecisionTaskFailedRequest struct {
	TaskToken      []byte                  `json:"taskToken"`
	Cause          DecisionTaskFailedCause `json:"cause,omitempty"`
	Details        []byte                  `json:"details,omitempty"`
	Identity       string                  `json:"identity,omitempty"`
	BinaryChecksum string                  `json:"binaryChecksum,omitempty"`
}

// Activity tasks

type PollForActivityTaskRequest struct {
	Domain   string    `json:"domain"`
	TaskList *TaskList `json:"taskList,omitempty"`
	Identity string    `json:"identity,omitempty"`
}

type PollForActivityTaskResponse struct {
	TaskToken                       []byte             `json:"taskToken,omitempty"`
	WorkflowExecution               *WorkflowExecution `json:"workflowExecution,omitempty"`
	ActivityID                      string             `json:"activityId,omitempty"`
	ActivityType                    *ActivityType      `json:"activityType,omitempty"`
	Input                           []byte             `json:"input,omitempty"`
	ScheduledTimestamp              int64              `json:"scheduledTimestamp,omitempty"`
	ScheduleToCloseTimeoutSeconds   int32              `json:"scheduleToCloseTimeoutSeconds,omitempty"`
	StartedTimestamp                int64              `json:"startedTimestamp,omitempty"`
	StartToCloseTimeoutSeconds      int32              `json:"startToCloseTimeoutSeconds,omitempty"`
	HeartbeatTimeoutSeconds         int32              `json:"heartbeatTimeoutSeconds,omitempty"`
	Attempt                         int32              `json:"attempt,omitempty"`
	ScheduledTimestampOfThisAttempt int64              `json:"scheduledTimestampOfThisAttempt,omitempty"`
	HeartbeatDetails                []byte             `json:"heartbeatDetails,omitempty"`
	WorkflowType                    *WorkflowType      `json:"workflowType,omitempty"`
	WorkflowDomain                  string             `json:"workflowDomain,omitempty"`
	Header                          *Header            `json:"header,omitempty"`
}

type RecordActivityTaskHeartbeatRequest struct {
	TaskToken []byte `json:"taskToken"`
	Details   []byte `json:"details,omitempty"`
	Identity  string `json:"identity,omitempty"`
}

type RecordActivityTaskHeartbeatByIDRequest struct {
	Domain     string `json:"domain"`
	WorkflowID string `json:"workflowID"`
	RunID      string `json:"runID,omitempty"`
	ActivityID string `json:"activityID"`
	Details    []byte `json:"details,omitempty"`
	Identity   string `json:"identity,omitempty"`
}

type RecordActivityTaskHeartbeatResponse struct {
	CancelRequested bool `json:"cancelRequested"`
}

type RespondActivityTaskCompletedRequest struct {
	TaskToken []byte `json:"taskToken"`
	Result    []byte `json:"result,omitempty"`
	Identity  string `json:"identity,omitempty"`
}

type RespondActivityTaskCompletedByIDRequest struct {
	Domain     string `json:"domain"`
	WorkflowID string `json:"workflowID"`
	RunID      string `json:"runID,omitempty"`
	ActivityID string `json:"activityID"`
	Result     []byte `json:"result,omitempty"`
	Identity   string `json:"identity,omitempty"`
}

type RespondActivityTaskFailedRequest struct {
	TaskToken      []byte          `json:"taskToken"`
	Reason         string          `json:"reason,omitempty"`
	Details        []byte          `json:"details,omitempty"`
	Identity       string          `json:"identity,omitempty"`
	FailureOptions *FailureOptions `json:"failureOptions,omitempty"`
}

// FailureOptions change how the server retries a failed activity.
type FailureOptions struct {
	FailureCategory FailureCategory `json:"failureCategory,omitempty"`
	// NextRetryIntervalSeconds replaces the retry policy's backoff before
	// the next attempt when it is above zero.
	NextRetryIntervalSeconds int32 `json:"nextRetryIntervalSeconds,omitempty"`
}

type RespondActivityTaskFailedByIDRequest struct {
	Domain         string          `json:"domain"`
	WorkflowID     string          `json:"workflowID"`
	RunID          string          `json:"runID,omitempty"`
	ActivityID     string          `json:"activityID"`
	Reason         string          `json:"reason,omitempty"`
	Details        []byte          `json:"details,omitempty"`
	Identity       string          `json:"identity,omitempty"`
	FailureOptions *FailureOptions `json:"failureOptions,omitempty"`
}

type RespondActivityTaskCanceledRequest struct {
	TaskToken []byte `json:"taskToken"`
	Details   []byte `json:"details,omitempty"`
	Identity  string `json:"identity,omitempty"`
}

type RespondActivityTaskCanceledByIDRequest struct {
	Domain     string `json:"domain"`
	WorkflowID string `json:"workflowID"`
	RunID      string `json:"runID,omitempty"`
	ActivityID string `json:"activityID"`
	Details    []byte `json:"details,omitempty"`
	Identity   string `json:"identity,omitempty"`
}

// Server-internal task start calls. Workers pull a TaskStub from JetStream
// and exchange it for the full task with one of these.

type RecordDecisionTaskStartedRequest struct {
	Stub      TaskStub `json:"stub"`
	Identity  string   `json:"identity,omitempty"`
	RequestID string   `json:"requestId"`
}

type RecordActivityTaskStartedRequest struct {
	Stub      TaskStub `json:"stub"`
	Identity  string   `json:"identity,omitempty"`
	RequestID string   `json:"requestId"`
}

type RecordPollerRequest struct {
	Domain       string       `json:"domain"`
	TaskList     string       `json:"taskList"`
	TaskListType TaskListType `json:"taskListType"`
	Identity     string       `json:"identity"`
}

// Visibility

type StartTimeFilter struct {
	EarliestTime int64 `json:"earliestTime,omitempty"`
	LatestTime   int64 `json:"latestTime,omitempty"`
}

type WorkflowExecutionFilter struct {
	WorkflowID string `json:"workflowId,omitempty"`
	RunID      string `json:"runId,omitempty"`
}

type WorkflowTypeFilter struct {
	Name string `json:"name,omitempty"`
}

type ListOpenWorkflowExecutionsRequest struct {
	Domain          string                   `json:"domain"`
	MaximumPageSize int32                    `json:"maximumPageSize,omitempty"`
	NextPageToken   []byte                   `json:"nextPageToken,omitempty"`
	StartTimeFilter *StartTimeFilter         `json:"StartTimeFilter,omitempty"`
	ExecutionFilter *WorkflowExecutionFilter `json:"executionFilter,omitempty"`
	TypeFilter      *WorkflowTypeFilter      `json:"typeFilter,omitempty"`
}

type ListClosedWorkflowExecutionsRequest struct {
	Domain          string                        `json:"domain"`
	MaximumPageSize int32                         `json:"maximumPageSize,omitempty"`
	NextPageToken   []byte                        `json:"nextPageToken,omitempty"`
	StartTimeFilter *StartTimeFilter              `json:"StartTimeFilter,omitempty"`
	ExecutionFilter *WorkflowExecutionFilter      `json:"executionFilter,omitempty"`
	TypeFilter      *WorkflowTypeFilter           `json:"typeFilter,omitempty"`
	StatusFilter    *WorkflowExecutionCloseStatus `json:"statusFilter,omitempty"`
}

type ListWorkflowExecutionsRequest struct {
	Domain        string `json:"domain"`
	PageSize      int32  `json:"pageSize,omitempty"`
	NextPageToken []byte `json:"nextPageToken,omitempty"`
	Query         string `json:"query,omitempty"`
}

type ListWorkflowExecutionsResponse struct {
	Executions    []*WorkflowExecutionInfo `json:"executions"`
	NextPageToken []byte                   `json:"nextPageToken,omitempty"`
}

type ListOpenWorkflowExecutionsResponse = ListWorkflowExecutionsResponse
type ListClosedWorkflowExecutionsResponse = ListWorkflowExecutionsResponse

type CountWorkflowExecutionsRequest struct {
	Domain string `json:"domain"`
	Query  string `json:"query,omitempty"`
}

type CountWorkflowExecutionsResponse struct {
	Count int64 `json:"count"`
}

// Task lists

type DescribeTaskListRequest struct {
	Domain                string       `json:"domain"`
	TaskList              *TaskList    `json:"taskList,omitempty"`
	TaskListType          TaskListType `json:"taskListType,omitempty"`
	IncludeTaskListStatus bool         `json:"includeTaskListStatus,omitempty"`
}

type PollerInfo struct {
	LastAccessTime int64   `json:"lastAccessTime,omitempty"`
	Identity       string  `json:"identity,omitempty"`
	RatePerSecond  float64 `json:"ratePerSecond,omitempty"`
}

type TaskListStatus struct {
	BacklogCountHint int64 `json:"backlogCountHint,omitempty"`
}

type DescribeTaskListResponse struct {
	Pollers        []*PollerInfo   `json:"pollers"`
	TaskListStatus *TaskListStatus `json:"taskListStatus,omitempty"`
}

type ListTaskListsRequest struct {
	Domain string `json:"domain"`
}

type TaskListSummary struct {
	Name         string       `json:"name"`
	TaskListType TaskListType `json:"taskListType"`
	Backlog      int64        `json:"backlog"`
	Pollers      int          `json:"pollers"`
}

type ListTaskListsResponse struct {
	TaskLists []*TaskListSummary `json:"taskLists"`
}
