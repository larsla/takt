package engine

import (
	"encoding/json"

	"github.com/larsla/takt/api"
)

// State is the mutable state of one workflow run, stored in the
// takt_executions bucket. It is the source of truth; the history stream is
// kept in step with it through Pending (see Engine.finalize).
type State struct {
	Domain     string `json:"domain"`
	WorkflowID string `json:"workflowId"`
	RunID      string `json:"runId"`

	// Start holds the attributes of WorkflowExecutionStarted. Retries,
	// cron and continue-as-new build the next run from it.
	Start *api.WorkflowExecutionStartedEventAttributes `json:"start"`

	StartTime      int64  `json:"startTime"`
	ExecutionTime  int64  `json:"executionTime"`
	CloseTime      int64  `json:"closeTime,omitempty"`
	StartRequestID string `json:"startRequestId,omitempty"`

	// CloseStatus is empty while the run is open.
	CloseStatus  api.WorkflowExecutionCloseStatus `json:"closeStatus,omitempty"`
	CloseEventID int64                            `json:"closeEventId,omitempty"`
	// NewRunID and Continue are set when the run continued as new.
	NewRunID string                                                `json:"newRunId,omitempty"`
	Continue *api.ContinueAsNewWorkflowExecutionDecisionAttributes `json:"continue,omitempty"`

	NextEventID int64 `json:"nextEventId"`
	// HistorySeq is the stream sequence of the last appended batch for this
	// run, used as the expected-last-subject-sequence of the next append.
	HistorySeq uint64 `json:"historySeq"`
	// Pending is a batch already committed to state but maybe not yet
	// appended to the history stream.
	Pending []*api.HistoryEvent `json:"pending,omitempty"`
	// Outbox holds messages to publish once Pending is appended.
	Outbox []*OutboxItem `json:"outbox,omitempty"`

	Decision        *DecisionInfo `json:"decision,omitempty"`
	DecisionAttempt int64         `json:"decisionAttempt,omitempty"`
	// LastDecisionStartedID is the started event of the last completed decision.
	LastDecisionStartedID int64 `json:"lastDecisionStartedId,omitempty"`
	// StickyTaskList is the task list of the worker that has the run
	// cached, set by its last completed decision task. Decision tasks go
	// there first and move back to the run's task list after
	// StickyTimeout seconds.
	StickyTaskList string `json:"stickyTaskList,omitempty"`
	StickyTimeout  int32  `json:"stickyTimeout,omitempty"`
	// Buffered events arrived while a decision was running. They get IDs when
	// the decision finishes.
	Buffered []*api.HistoryEvent `json:"buffered,omitempty"`

	Activities  map[int64]*ActivityInfo `json:"activities,omitempty"`
	Timers      map[string]*TimerInfo   `json:"timers,omitempty"`
	Children    map[int64]*ChildInfo    `json:"children,omitempty"`
	Signals     map[int64]*ExternalInfo `json:"signals,omitempty"`
	CancelsReqs map[int64]*ExternalInfo `json:"cancels,omitempty"`

	SignalRequestIDs map[string]bool `json:"signalRequestIds,omitempty"`
	CancelRequested  bool            `json:"cancelRequested,omitempty"`
	CancelRequestID  string          `json:"cancelRequestId,omitempty"`

	SearchAttributes *api.SearchAttributes `json:"searchAttributes,omitempty"`
}

func (s *State) Running() bool { return s.CloseStatus == "" }

func (s *State) Execution() *api.WorkflowExecution {
	return &api.WorkflowExecution{WorkflowID: s.WorkflowID, RunID: s.RunID}
}

func (s *State) TaskList() string { return s.Start.TaskList.Name }

func (s *State) WorkflowType() string { return s.Start.WorkflowType.Name }

func (s *State) activityByID(id string) *ActivityInfo {
	for _, ai := range s.Activities {
		if ai.ActivityID == id {
			return ai
		}
	}
	return nil
}

type DecisionInfo struct {
	ScheduleID    int64  `json:"scheduleId"`
	StartedID     int64  `json:"startedId,omitempty"`
	Attempt       int64  `json:"attempt"`
	ScheduledTime int64  `json:"scheduledTime"`
	StartedTime   int64  `json:"startedTime,omitempty"`
	RequestID     string `json:"requestId,omitempty"`
	TaskList      string `json:"taskList"`
	Timeout       int32  `json:"timeout"`
	// Sticky is set while the task waits on the run's sticky task list.
	Sticky bool `json:"sticky,omitempty"`
}

type ActivityInfo struct {
	ScheduleID      int64            `json:"scheduleId"`
	ActivityID      string           `json:"activityId"`
	ActivityType    string           `json:"activityType"`
	TaskList        string           `json:"taskList"`
	Input           []byte           `json:"input,omitempty"`
	Header          *api.Header      `json:"header,omitempty"`
	RetryPolicy     *api.RetryPolicy `json:"retryPolicy,omitempty"`
	ScheduleToClose int32            `json:"s2c"`
	ScheduleToStart int32            `json:"s2s"`
	StartToClose    int32            `json:"st2c"`
	Heartbeat       int32            `json:"hb,omitempty"`

	ScheduledTime        int64 `json:"scheduledTime"`
	AttemptScheduledTime int64 `json:"attemptScheduledTime"`
	ExpirationTime       int64 `json:"expirationTime,omitempty"`
	Attempt              int32 `json:"attempt"`

	Started          bool   `json:"started,omitempty"`
	StartedTime      int64  `json:"startedTime,omitempty"`
	StartedIdentity  string `json:"startedIdentity,omitempty"`
	StartedRequestID string `json:"startedRequestId,omitempty"`

	LastHeartbeat    int64  `json:"lastHeartbeat,omitempty"`
	HeartbeatDetails []byte `json:"heartbeatDetails,omitempty"`

	CancelRequested      bool  `json:"cancelRequested,omitempty"`
	CancelRequestEventID int64 `json:"cancelRequestEventId,omitempty"`

	LastFailureReason  string `json:"lastFailureReason,omitempty"`
	LastFailureDetails []byte `json:"lastFailureDetails,omitempty"`
}

type TimerInfo struct {
	TimerID        string `json:"timerId"`
	StartedEventID int64  `json:"startedEventId"`
	FireTime       int64  `json:"fireTime"`
}

type ChildInfo struct {
	InitiatedID  int64  `json:"initiatedId"`
	Domain       string `json:"domain"`
	WorkflowID   string `json:"workflowId"`
	RunID        string `json:"runId,omitempty"`
	WorkflowType string `json:"workflowType"`
	// StartedID is 0 until the child starts, and bufferedStartedID while
	// the started event sits in Buffered.
	StartedID         int64                 `json:"startedId,omitempty"`
	ParentClosePolicy api.ParentClosePolicy `json:"parentClosePolicy,omitempty"`
	// Attrs is what the start transfer needs to create the child.
	Attrs *api.StartChildWorkflowExecutionInitiatedEventAttributes `json:"attrs,omitempty"`
}

// bufferedStartedID marks a reference to a started event that is still
// buffered and has no ID yet.
const bufferedStartedID = -1

type ExternalInfo struct {
	InitiatedID int64                  `json:"initiatedId"`
	Domain      string                 `json:"domain"`
	Execution   *api.WorkflowExecution `json:"execution"`
	SignalName  string                 `json:"signalName,omitempty"`
	Input       []byte                 `json:"input,omitempty"`
	Control     []byte                 `json:"control,omitempty"`
	ChildOnly   bool                   `json:"childOnly,omitempty"`
}

// OutboxKind says where an outbox item is published.
type OutboxKind string

const (
	OutboxTask     OutboxKind = "task"     // task stub on a task list
	OutboxTimer    OutboxKind = "timer"    // scheduled timer
	OutboxTransfer OutboxKind = "transfer" // cross-workflow work
	OutboxVisible  OutboxKind = "visible"  // visibility record update
)

type OutboxItem struct {
	Kind     OutboxKind    `json:"kind"`
	Stub     *api.TaskStub `json:"stub,omitempty"`
	TaskList string        `json:"taskList,omitempty"`
	// TTL, in nanoseconds, removes a sticky task stub that nobody took, so
	// a gone worker's sticky task list empties itself.
	TTL      int64      `json:"ttl,omitempty"`
	Timer    *TimerTask `json:"timer,omitempty"`
	Transfer *Transfer  `json:"transfer,omitempty"`
}

// TimerKind says what a fired timer means.
type TimerKind string

const (
	TimerUser            TimerKind = "user"
	TimerActivityTimeout TimerKind = "activityTimeout"
	TimerActivityRetry   TimerKind = "activityRetry"
	TimerDecisionTimeout TimerKind = "decisionTimeout"
	TimerDecisionBackoff TimerKind = "decisionBackoff"
	TimerWorkflowTimeout TimerKind = "workflowTimeout"
	TimerWorkflowBackoff TimerKind = "workflowBackoff"
	// TimerStickyTimeout moves a decision task that no worker took from the
	// run's sticky task list back to its task list.
	TimerStickyTimeout TimerKind = "stickyTimeout"
)

type TimerTask struct {
	Domain      string          `json:"domain"`
	WorkflowID  string          `json:"workflowId"`
	RunID       string          `json:"runId"`
	Kind        TimerKind       `json:"kind"`
	EventID     int64           `json:"eventId,omitempty"`
	Attempt     int64           `json:"attempt,omitempty"`
	TimeoutType api.TimeoutType `json:"timeoutType,omitempty"`
	FireAt      int64           `json:"fireAt"`
	// Key makes the timer subject unique within the run.
	Key string `json:"key"`
}

// TransferKind names cross-workflow work.
type TransferKind string

const (
	TransferStartChild     TransferKind = "startChild"
	TransferSignalExternal TransferKind = "signalExternal"
	TransferCancelExternal TransferKind = "cancelExternal"
	TransferClosed         TransferKind = "closed"
)

type Transfer struct {
	Kind        TransferKind `json:"kind"`
	Domain      string       `json:"domain"`
	WorkflowID  string       `json:"workflowId"`
	RunID       string       `json:"runId"`
	InitiatedID int64        `json:"initiatedId,omitempty"`
}

// Current points at the latest run of a workflow ID.
type Current struct {
	RunID          string `json:"runId"`
	StartRequestID string `json:"startRequestId,omitempty"`
}

func encodeState(s *State) []byte {
	b, err := json.Marshal(s)
	if err != nil {
		// State only holds JSON-safe types.
		panic(err)
	}
	return b
}

func decodeState(b []byte) (*State, error) {
	s := &State{}
	if err := json.Unmarshal(b, s); err != nil {
		return nil, err
	}
	return s, nil
}

// runKey and currentKey are keys in takt_executions.
func runKey(domain, workflowID, runID string) string {
	return api.Token(domain) + "." + api.Token(workflowID) + "." + runID
}

func currentKey(domain, workflowID string) string {
	return api.Token(domain) + "." + api.Token(workflowID) + ".current"
}
