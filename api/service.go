package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
)

// Service is the workflow service the SDK talks to. It mirrors Cadence's
// WorkflowService. The NATS implementation lives in the root takt package.
type Service interface {
	RegisterDomain(ctx context.Context, req *RegisterDomainRequest) error
	DescribeDomain(ctx context.Context, req *DescribeDomainRequest) (*DescribeDomainResponse, error)
	UpdateDomain(ctx context.Context, req *UpdateDomainRequest) (*UpdateDomainResponse, error)
	ListDomains(ctx context.Context, req *ListDomainsRequest) (*ListDomainsResponse, error)

	StartWorkflowExecution(ctx context.Context, req *StartWorkflowExecutionRequest) (*StartWorkflowExecutionResponse, error)
	SignalWorkflowExecution(ctx context.Context, req *SignalWorkflowExecutionRequest) error
	SignalWithStartWorkflowExecution(ctx context.Context, req *SignalWithStartWorkflowExecutionRequest) (*StartWorkflowExecutionResponse, error)
	RequestCancelWorkflowExecution(ctx context.Context, req *RequestCancelWorkflowExecutionRequest) error
	TerminateWorkflowExecution(ctx context.Context, req *TerminateWorkflowExecutionRequest) error
	GetWorkflowExecutionHistory(ctx context.Context, req *GetWorkflowExecutionHistoryRequest) (*GetWorkflowExecutionHistoryResponse, error)
	DescribeWorkflowExecution(ctx context.Context, req *DescribeWorkflowExecutionRequest) (*DescribeWorkflowExecutionResponse, error)
	QueryWorkflow(ctx context.Context, req *QueryWorkflowRequest) (*QueryWorkflowResponse, error)

	PollForDecisionTask(ctx context.Context, req *PollForDecisionTaskRequest) (*PollForDecisionTaskResponse, error)
	RespondDecisionTaskCompleted(ctx context.Context, req *RespondDecisionTaskCompletedRequest) (*RespondDecisionTaskCompletedResponse, error)
	RespondDecisionTaskFailed(ctx context.Context, req *RespondDecisionTaskFailedRequest) error

	PollForActivityTask(ctx context.Context, req *PollForActivityTaskRequest) (*PollForActivityTaskResponse, error)
	RecordActivityTaskHeartbeat(ctx context.Context, req *RecordActivityTaskHeartbeatRequest) (*RecordActivityTaskHeartbeatResponse, error)
	RecordActivityTaskHeartbeatByID(ctx context.Context, req *RecordActivityTaskHeartbeatByIDRequest) (*RecordActivityTaskHeartbeatResponse, error)
	RespondActivityTaskCompleted(ctx context.Context, req *RespondActivityTaskCompletedRequest) error
	RespondActivityTaskCompletedByID(ctx context.Context, req *RespondActivityTaskCompletedByIDRequest) error
	RespondActivityTaskFailed(ctx context.Context, req *RespondActivityTaskFailedRequest) error
	RespondActivityTaskFailedByID(ctx context.Context, req *RespondActivityTaskFailedByIDRequest) error
	RespondActivityTaskCanceled(ctx context.Context, req *RespondActivityTaskCanceledRequest) error
	RespondActivityTaskCanceledByID(ctx context.Context, req *RespondActivityTaskCanceledByIDRequest) error

	ListOpenWorkflowExecutions(ctx context.Context, req *ListOpenWorkflowExecutionsRequest) (*ListOpenWorkflowExecutionsResponse, error)
	ListClosedWorkflowExecutions(ctx context.Context, req *ListClosedWorkflowExecutionsRequest) (*ListClosedWorkflowExecutionsResponse, error)
	ListWorkflowExecutions(ctx context.Context, req *ListWorkflowExecutionsRequest) (*ListWorkflowExecutionsResponse, error)
	CountWorkflowExecutions(ctx context.Context, req *CountWorkflowExecutionsRequest) (*CountWorkflowExecutionsResponse, error)
	DescribeTaskList(ctx context.Context, req *DescribeTaskListRequest) (*DescribeTaskListResponse, error)
	ListTaskLists(ctx context.Context, req *ListTaskListsRequest) (*ListTaskListsResponse, error)
	ResetStickyTaskList(ctx context.Context, req *ResetStickyTaskListRequest) (*ResetStickyTaskListResponse, error)
}

// API method names, used as the last token of takt.api.<method>.
const (
	MethodRegisterDomain                   = "RegisterDomain"
	MethodDescribeDomain                   = "DescribeDomain"
	MethodUpdateDomain                     = "UpdateDomain"
	MethodListDomains                      = "ListDomains"
	MethodStartWorkflowExecution           = "StartWorkflowExecution"
	MethodSignalWorkflowExecution          = "SignalWorkflowExecution"
	MethodSignalWithStartWorkflowExecution = "SignalWithStartWorkflowExecution"
	MethodRequestCancelWorkflowExecution   = "RequestCancelWorkflowExecution"
	MethodTerminateWorkflowExecution       = "TerminateWorkflowExecution"
	MethodGetWorkflowExecutionHistory      = "GetWorkflowExecutionHistory"
	MethodDescribeWorkflowExecution        = "DescribeWorkflowExecution"
	MethodQueryWorkflow                    = "QueryWorkflow"
	MethodRecordDecisionTaskStarted        = "RecordDecisionTaskStarted"
	MethodRespondDecisionTaskCompleted     = "RespondDecisionTaskCompleted"
	MethodRespondDecisionTaskFailed        = "RespondDecisionTaskFailed"
	MethodRecordActivityTaskStarted        = "RecordActivityTaskStarted"
	MethodRecordActivityTaskHeartbeat      = "RecordActivityTaskHeartbeat"
	MethodRecordActivityTaskHeartbeatByID  = "RecordActivityTaskHeartbeatByID"
	MethodRespondActivityTaskCompleted     = "RespondActivityTaskCompleted"
	MethodRespondActivityTaskCompletedByID = "RespondActivityTaskCompletedByID"
	MethodRespondActivityTaskFailed        = "RespondActivityTaskFailed"
	MethodRespondActivityTaskFailedByID    = "RespondActivityTaskFailedByID"
	MethodRespondActivityTaskCanceled      = "RespondActivityTaskCanceled"
	MethodRespondActivityTaskCanceledByID  = "RespondActivityTaskCanceledByID"
	MethodListOpenWorkflowExecutions       = "ListOpenWorkflowExecutions"
	MethodListClosedWorkflowExecutions     = "ListClosedWorkflowExecutions"
	MethodListWorkflowExecutions           = "ListWorkflowExecutions"
	MethodCountWorkflowExecutions          = "CountWorkflowExecutions"
	MethodDescribeTaskList                 = "DescribeTaskList"
	MethodListTaskLists                    = "ListTaskLists"
	MethodRecordPoller                     = "RecordPoller"
	MethodResetStickyTaskList              = "ResetStickyTaskList"
)

// Subject layout.
const (
	APISubjectPrefix = "takt.api."
	TaskStream       = "TAKT_TASKS"
	HistoryStream    = "TAKT_HISTORY"
	TimerStream      = "TAKT_TIMERS"
	TransferStream   = "TAKT_TRANSFER"
)

// Token makes s safe to use as one NATS subject token or KV key segment.
func Token(s string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(s))
}

// FromToken reverses Token.
func FromToken(t string) (string, error) {
	b, err := base64.RawURLEncoding.DecodeString(t)
	return string(b), err
}

func APISubject(method string) string { return APISubjectPrefix + method }

func kindToken(t TaskListType) string {
	if t == TaskListTypeActivity {
		return "activity"
	}
	return "decision"
}

// TaskSubject is where stubs for a task list are published.
func TaskSubject(domain, taskList string, t TaskListType) string {
	return fmt.Sprintf("takt.task.%s.%s.%s", Token(domain), Token(taskList), kindToken(t))
}

// StickyTaskListPrefix starts the names of workers' sticky task lists.
const StickyTaskListPrefix = "__sticky:"

// IsStickyTaskList reports whether name is a worker's sticky task list.
func IsStickyTaskList(name string) bool { return strings.HasPrefix(name, StickyTaskListPrefix) }

// TaskConsumerName is the durable consumer that serves one task list.
func TaskConsumerName(domain, taskList string, t TaskListType) string {
	return fmt.Sprintf("tl_%s_%s_%s", Token(domain), Token(taskList), kindToken(t))
}

// HistorySubject holds the event batches of one run.
func HistorySubject(domain, workflowID, runID string) string {
	return fmt.Sprintf("takt.hist.%s.%s.%s", Token(domain), Token(workflowID), runID)
}

// QuerySubject is where workers on a task list answer queries.
func QuerySubject(domain, taskList string) string {
	return fmt.Sprintf("takt.query.%s.%s", Token(domain), Token(taskList))
}

// TaskStub is the small message put on a task list. A worker exchanges it
// for the full task through RecordDecisionTaskStarted or
// RecordActivityTaskStarted; stale stubs are rejected there.
type TaskStub struct {
	Domain     string       `json:"domain"`
	WorkflowID string       `json:"workflowId"`
	RunID      string       `json:"runId"`
	ScheduleID int64        `json:"scheduleId"`
	Attempt    int64        `json:"attempt"`
	Type       TaskListType `json:"type"`
}

// TaskToken identifies a started task. Clients treat it as opaque bytes.
type TaskToken struct {
	Domain     string `json:"d"`
	WorkflowID string `json:"w"`
	RunID      string `json:"r"`
	ScheduleID int64  `json:"s"`
	Attempt    int64  `json:"a"`
	ActivityID string `json:"act,omitempty"`
}

func (t TaskToken) Encode() []byte {
	b, _ := json.Marshal(t)
	return b
}

func DecodeTaskToken(b []byte) (TaskToken, error) {
	var t TaskToken
	if err := json.Unmarshal(b, &t); err != nil {
		return t, &BadRequestError{Message: "invalid task token"}
	}
	if t.Domain == "" || t.WorkflowID == "" || t.RunID == "" {
		return t, &BadRequestError{Message: "invalid task token"}
	}
	return t, nil
}
