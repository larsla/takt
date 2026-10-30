package api

type WorkflowExecution struct {
	WorkflowID string `json:"workflowId"`
	RunID      string `json:"runId,omitempty"`
}

type WorkflowType struct {
	Name string `json:"name"`
}

type ActivityType struct {
	Name string `json:"name"`
}

type TaskList struct {
	Name string       `json:"name"`
	Kind TaskListKind `json:"kind,omitempty"`
}

type RetryPolicy struct {
	InitialIntervalInSeconds    int32    `json:"initialIntervalInSeconds,omitempty"`
	BackoffCoefficient          float64  `json:"backoffCoefficient,omitempty"`
	MaximumIntervalInSeconds    int32    `json:"maximumIntervalInSeconds,omitempty"`
	MaximumAttempts             int32    `json:"maximumAttempts,omitempty"`
	NonRetriableErrorReasons    []string `json:"nonRetriableErrorReasons,omitempty"`
	ExpirationIntervalInSeconds int32    `json:"expirationIntervalInSeconds,omitempty"`
}

type Header struct {
	Fields map[string][]byte `json:"fields,omitempty"`
}

type Memo struct {
	Fields map[string][]byte `json:"fields,omitempty"`
}

type SearchAttributes struct {
	IndexedFields map[string][]byte `json:"indexedFields,omitempty"`
}

type History struct {
	Events []*HistoryEvent `json:"events"`
}

// HistoryEvent is one entry in a workflow history. Exactly one attributes
// field is set, matching EventType.
type HistoryEvent struct {
	EventID   int64     `json:"eventId"`
	Timestamp int64     `json:"timestamp"`
	EventType EventType `json:"eventType"`
	Version   int64     `json:"version,omitempty"`
	TaskID    int64     `json:"taskId,omitempty"`

	WorkflowExecutionStartedEventAttributes                        *WorkflowExecutionStartedEventAttributes                        `json:"workflowExecutionStartedEventAttributes,omitempty"`
	WorkflowExecutionCompletedEventAttributes                      *WorkflowExecutionCompletedEventAttributes                      `json:"workflowExecutionCompletedEventAttributes,omitempty"`
	WorkflowExecutionFailedEventAttributes                         *WorkflowExecutionFailedEventAttributes                         `json:"workflowExecutionFailedEventAttributes,omitempty"`
	WorkflowExecutionTimedOutEventAttributes                       *WorkflowExecutionTimedOutEventAttributes                       `json:"workflowExecutionTimedOutEventAttributes,omitempty"`
	DecisionTaskScheduledEventAttributes                           *DecisionTaskScheduledEventAttributes                           `json:"decisionTaskScheduledEventAttributes,omitempty"`
	DecisionTaskStartedEventAttributes                             *DecisionTaskStartedEventAttributes                             `json:"decisionTaskStartedEventAttributes,omitempty"`
	DecisionTaskCompletedEventAttributes                           *DecisionTaskCompletedEventAttributes                           `json:"decisionTaskCompletedEventAttributes,omitempty"`
	DecisionTaskTimedOutEventAttributes                            *DecisionTaskTimedOutEventAttributes                            `json:"decisionTaskTimedOutEventAttributes,omitempty"`
	DecisionTaskFailedEventAttributes                              *DecisionTaskFailedEventAttributes                              `json:"decisionTaskFailedEventAttributes,omitempty"`
	ActivityTaskScheduledEventAttributes                           *ActivityTaskScheduledEventAttributes                           `json:"activityTaskScheduledEventAttributes,omitempty"`
	ActivityTaskStartedEventAttributes                             *ActivityTaskStartedEventAttributes                             `json:"activityTaskStartedEventAttributes,omitempty"`
	ActivityTaskCompletedEventAttributes                           *ActivityTaskCompletedEventAttributes                           `json:"activityTaskCompletedEventAttributes,omitempty"`
	ActivityTaskFailedEventAttributes                              *ActivityTaskFailedEventAttributes                              `json:"activityTaskFailedEventAttributes,omitempty"`
	ActivityTaskTimedOutEventAttributes                            *ActivityTaskTimedOutEventAttributes                            `json:"activityTaskTimedOutEventAttributes,omitempty"`
	ActivityTaskCancelRequestedEventAttributes                     *ActivityTaskCancelRequestedEventAttributes                     `json:"activityTaskCancelRequestedEventAttributes,omitempty"`
	RequestCancelActivityTaskFailedEventAttributes                 *RequestCancelActivityTaskFailedEventAttributes                 `json:"requestCancelActivityTaskFailedEventAttributes,omitempty"`
	ActivityTaskCanceledEventAttributes                            *ActivityTaskCanceledEventAttributes                            `json:"activityTaskCanceledEventAttributes,omitempty"`
	TimerStartedEventAttributes                                    *TimerStartedEventAttributes                                    `json:"timerStartedEventAttributes,omitempty"`
	TimerFiredEventAttributes                                      *TimerFiredEventAttributes                                      `json:"timerFiredEventAttributes,omitempty"`
	CancelTimerFailedEventAttributes                               *CancelTimerFailedEventAttributes                               `json:"cancelTimerFailedEventAttributes,omitempty"`
	TimerCanceledEventAttributes                                   *TimerCanceledEventAttributes                                   `json:"timerCanceledEventAttributes,omitempty"`
	WorkflowExecutionCancelRequestedEventAttributes                *WorkflowExecutionCancelRequestedEventAttributes                `json:"workflowExecutionCancelRequestedEventAttributes,omitempty"`
	WorkflowExecutionCanceledEventAttributes                       *WorkflowExecutionCanceledEventAttributes                       `json:"workflowExecutionCanceledEventAttributes,omitempty"`
	RequestCancelExternalWorkflowExecutionInitiatedEventAttributes *RequestCancelExternalWorkflowExecutionInitiatedEventAttributes `json:"requestCancelExternalWorkflowExecutionInitiatedEventAttributes,omitempty"`
	RequestCancelExternalWorkflowExecutionFailedEventAttributes    *RequestCancelExternalWorkflowExecutionFailedEventAttributes    `json:"requestCancelExternalWorkflowExecutionFailedEventAttributes,omitempty"`
	ExternalWorkflowExecutionCancelRequestedEventAttributes        *ExternalWorkflowExecutionCancelRequestedEventAttributes        `json:"externalWorkflowExecutionCancelRequestedEventAttributes,omitempty"`
	MarkerRecordedEventAttributes                                  *MarkerRecordedEventAttributes                                  `json:"markerRecordedEventAttributes,omitempty"`
	WorkflowExecutionSignaledEventAttributes                       *WorkflowExecutionSignaledEventAttributes                       `json:"workflowExecutionSignaledEventAttributes,omitempty"`
	WorkflowExecutionTerminatedEventAttributes                     *WorkflowExecutionTerminatedEventAttributes                     `json:"workflowExecutionTerminatedEventAttributes,omitempty"`
	WorkflowExecutionContinuedAsNewEventAttributes                 *WorkflowExecutionContinuedAsNewEventAttributes                 `json:"workflowExecutionContinuedAsNewEventAttributes,omitempty"`
	StartChildWorkflowExecutionInitiatedEventAttributes            *StartChildWorkflowExecutionInitiatedEventAttributes            `json:"startChildWorkflowExecutionInitiatedEventAttributes,omitempty"`
	StartChildWorkflowExecutionFailedEventAttributes               *StartChildWorkflowExecutionFailedEventAttributes               `json:"startChildWorkflowExecutionFailedEventAttributes,omitempty"`
	ChildWorkflowExecutionStartedEventAttributes                   *ChildWorkflowExecutionStartedEventAttributes                   `json:"childWorkflowExecutionStartedEventAttributes,omitempty"`
	ChildWorkflowExecutionCompletedEventAttributes                 *ChildWorkflowExecutionCompletedEventAttributes                 `json:"childWorkflowExecutionCompletedEventAttributes,omitempty"`
	ChildWorkflowExecutionFailedEventAttributes                    *ChildWorkflowExecutionFailedEventAttributes                    `json:"childWorkflowExecutionFailedEventAttributes,omitempty"`
	ChildWorkflowExecutionCanceledEventAttributes                  *ChildWorkflowExecutionCanceledEventAttributes                  `json:"childWorkflowExecutionCanceledEventAttributes,omitempty"`
	ChildWorkflowExecutionTimedOutEventAttributes                  *ChildWorkflowExecutionTimedOutEventAttributes                  `json:"childWorkflowExecutionTimedOutEventAttributes,omitempty"`
	ChildWorkflowExecutionTerminatedEventAttributes                *ChildWorkflowExecutionTerminatedEventAttributes                `json:"childWorkflowExecutionTerminatedEventAttributes,omitempty"`
	SignalExternalWorkflowExecutionInitiatedEventAttributes        *SignalExternalWorkflowExecutionInitiatedEventAttributes        `json:"signalExternalWorkflowExecutionInitiatedEventAttributes,omitempty"`
	SignalExternalWorkflowExecutionFailedEventAttributes           *SignalExternalWorkflowExecutionFailedEventAttributes           `json:"signalExternalWorkflowExecutionFailedEventAttributes,omitempty"`
	ExternalWorkflowExecutionSignaledEventAttributes               *ExternalWorkflowExecutionSignaledEventAttributes               `json:"externalWorkflowExecutionSignaledEventAttributes,omitempty"`
	UpsertWorkflowSearchAttributesEventAttributes                  *UpsertWorkflowSearchAttributesEventAttributes                  `json:"upsertWorkflowSearchAttributesEventAttributes,omitempty"`
}

type WorkflowExecutionStartedEventAttributes struct {
	WorkflowType                        *WorkflowType          `json:"workflowType,omitempty"`
	ParentWorkflowDomain                string                 `json:"parentWorkflowDomain,omitempty"`
	ParentWorkflowExecution             *WorkflowExecution     `json:"parentWorkflowExecution,omitempty"`
	ParentInitiatedEventID              int64                  `json:"parentInitiatedEventId,omitempty"`
	TaskList                            *TaskList              `json:"taskList,omitempty"`
	Input                               []byte                 `json:"input,omitempty"`
	ExecutionStartToCloseTimeoutSeconds int32                  `json:"executionStartToCloseTimeoutSeconds,omitempty"`
	TaskStartToCloseTimeoutSeconds      int32                  `json:"taskStartToCloseTimeoutSeconds,omitempty"`
	ContinuedExecutionRunID             string                 `json:"continuedExecutionRunId,omitempty"`
	Initiator                           ContinueAsNewInitiator `json:"initiator,omitempty"`
	ContinuedFailureReason              string                 `json:"continuedFailureReason,omitempty"`
	ContinuedFailureDetails             []byte                 `json:"continuedFailureDetails,omitempty"`
	LastCompletionResult                []byte                 `json:"lastCompletionResult,omitempty"`
	OriginalExecutionRunID              string                 `json:"originalExecutionRunId,omitempty"`
	Identity                            string                 `json:"identity,omitempty"`
	FirstExecutionRunID                 string                 `json:"firstExecutionRunId,omitempty"`
	RetryPolicy                         *RetryPolicy           `json:"retryPolicy,omitempty"`
	Attempt                             int32                  `json:"attempt,omitempty"`
	ExpirationTimestamp                 int64                  `json:"expirationTimestamp,omitempty"`
	CronSchedule                        string                 `json:"cronSchedule,omitempty"`
	FirstDecisionTaskBackoffSeconds     int32                  `json:"firstDecisionTaskBackoffSeconds,omitempty"`
	CronOverlapPolicy                   CronOverlapPolicy      `json:"cronOverlapPolicy,omitempty"`
	JitterStartSeconds                  int32                  `json:"jitterStartSeconds,omitempty"`
	Memo                                *Memo                  `json:"memo,omitempty"`
	SearchAttributes                    *SearchAttributes      `json:"searchAttributes,omitempty"`
	Header                              *Header                `json:"header,omitempty"`
	RequestID                           string                 `json:"requestId,omitempty"`
}

type WorkflowExecutionCompletedEventAttributes struct {
	Result                       []byte `json:"result,omitempty"`
	DecisionTaskCompletedEventID int64  `json:"decisionTaskCompletedEventId,omitempty"`
}

type WorkflowExecutionFailedEventAttributes struct {
	Reason                       string `json:"reason,omitempty"`
	Details                      []byte `json:"details,omitempty"`
	DecisionTaskCompletedEventID int64  `json:"decisionTaskCompletedEventId,omitempty"`
}

type WorkflowExecutionTimedOutEventAttributes struct {
	TimeoutType TimeoutType `json:"timeoutType,omitempty"`
}

type DecisionTaskScheduledEventAttributes struct {
	TaskList                   *TaskList `json:"taskList,omitempty"`
	StartToCloseTimeoutSeconds int32     `json:"startToCloseTimeoutSeconds,omitempty"`
	Attempt                    int64     `json:"attempt,omitempty"`
}

type DecisionTaskStartedEventAttributes struct {
	ScheduledEventID int64  `json:"scheduledEventId"`
	Identity         string `json:"identity,omitempty"`
	RequestID        string `json:"requestId,omitempty"`
}

type DecisionTaskCompletedEventAttributes struct {
	ExecutionContext []byte `json:"executionContext,omitempty"`
	ScheduledEventID int64  `json:"scheduledEventId"`
	StartedEventID   int64  `json:"startedEventId"`
	Identity         string `json:"identity,omitempty"`
	BinaryChecksum   string `json:"binaryChecksum,omitempty"`
}

type DecisionTaskTimedOutEventAttributes struct {
	ScheduledEventID int64       `json:"scheduledEventId"`
	StartedEventID   int64       `json:"startedEventId"`
	TimeoutType      TimeoutType `json:"timeoutType,omitempty"`
}

type DecisionTaskFailedEventAttributes struct {
	ScheduledEventID int64                   `json:"scheduledEventId"`
	StartedEventID   int64                   `json:"startedEventId"`
	Cause            DecisionTaskFailedCause `json:"cause,omitempty"`
	Details          []byte                  `json:"details,omitempty"`
	Identity         string                  `json:"identity,omitempty"`
	Reason           string                  `json:"reason,omitempty"`
	BinaryChecksum   string                  `json:"binaryChecksum,omitempty"`
}

type ActivityTaskScheduledEventAttributes struct {
	ActivityID                    string        `json:"activityId"`
	ActivityType                  *ActivityType `json:"activityType,omitempty"`
	Domain                        string        `json:"domain,omitempty"`
	TaskList                      *TaskList     `json:"taskList,omitempty"`
	Input                         []byte        `json:"input,omitempty"`
	ScheduleToCloseTimeoutSeconds int32         `json:"scheduleToCloseTimeoutSeconds,omitempty"`
	ScheduleToStartTimeoutSeconds int32         `json:"scheduleToStartTimeoutSeconds,omitempty"`
	StartToCloseTimeoutSeconds    int32         `json:"startToCloseTimeoutSeconds,omitempty"`
	HeartbeatTimeoutSeconds       int32         `json:"heartbeatTimeoutSeconds,omitempty"`
	DecisionTaskCompletedEventID  int64         `json:"decisionTaskCompletedEventId,omitempty"`
	RetryPolicy                   *RetryPolicy  `json:"retryPolicy,omitempty"`
	Header                        *Header       `json:"header,omitempty"`
}

type ActivityTaskStartedEventAttributes struct {
	ScheduledEventID   int64  `json:"scheduledEventId"`
	Identity           string `json:"identity,omitempty"`
	RequestID          string `json:"requestId,omitempty"`
	Attempt            int32  `json:"attempt,omitempty"`
	LastFailureReason  string `json:"lastFailureReason,omitempty"`
	LastFailureDetails []byte `json:"lastFailureDetails,omitempty"`
}

type ActivityTaskCompletedEventAttributes struct {
	Result           []byte `json:"result,omitempty"`
	ScheduledEventID int64  `json:"scheduledEventId"`
	StartedEventID   int64  `json:"startedEventId"`
	Identity         string `json:"identity,omitempty"`
}

type ActivityTaskFailedEventAttributes struct {
	Reason           string `json:"reason,omitempty"`
	Details          []byte `json:"details,omitempty"`
	ScheduledEventID int64  `json:"scheduledEventId"`
	StartedEventID   int64  `json:"startedEventId"`
	Identity         string `json:"identity,omitempty"`
}

type ActivityTaskTimedOutEventAttributes struct {
	Details            []byte      `json:"details,omitempty"`
	ScheduledEventID   int64       `json:"scheduledEventId"`
	StartedEventID     int64       `json:"startedEventId"`
	TimeoutType        TimeoutType `json:"timeoutType,omitempty"`
	LastFailureReason  string      `json:"lastFailureReason,omitempty"`
	LastFailureDetails []byte      `json:"lastFailureDetails,omitempty"`
}

type ActivityTaskCancelRequestedEventAttributes struct {
	ActivityID                   string `json:"activityId"`
	DecisionTaskCompletedEventID int64  `json:"decisionTaskCompletedEventId,omitempty"`
}

type RequestCancelActivityTaskFailedEventAttributes struct {
	ActivityID                   string `json:"activityId"`
	Cause                        string `json:"cause,omitempty"`
	DecisionTaskCompletedEventID int64  `json:"decisionTaskCompletedEventId,omitempty"`
}

type ActivityTaskCanceledEventAttributes struct {
	Details                      []byte `json:"details,omitempty"`
	LatestCancelRequestedEventID int64  `json:"latestCancelRequestedEventId,omitempty"`
	ScheduledEventID             int64  `json:"scheduledEventId"`
	StartedEventID               int64  `json:"startedEventId"`
	Identity                     string `json:"identity,omitempty"`
}

type TimerStartedEventAttributes struct {
	TimerID                      string `json:"timerId"`
	StartToFireTimeoutSeconds    int64  `json:"startToFireTimeoutSeconds"`
	DecisionTaskCompletedEventID int64  `json:"decisionTaskCompletedEventId,omitempty"`
}

type TimerFiredEventAttributes struct {
	TimerID        string `json:"timerId"`
	StartedEventID int64  `json:"startedEventId"`
}

type TimerCanceledEventAttributes struct {
	TimerID                      string `json:"timerId"`
	StartedEventID               int64  `json:"startedEventId"`
	DecisionTaskCompletedEventID int64  `json:"decisionTaskCompletedEventId,omitempty"`
	Identity                     string `json:"identity,omitempty"`
}

type CancelTimerFailedEventAttributes struct {
	TimerID                      string `json:"timerId"`
	Cause                        string `json:"cause,omitempty"`
	DecisionTaskCompletedEventID int64  `json:"decisionTaskCompletedEventId,omitempty"`
	Identity                     string `json:"identity,omitempty"`
}

type WorkflowExecutionCancelRequestedEventAttributes struct {
	Cause                     string             `json:"cause,omitempty"`
	ExternalInitiatedEventID  int64              `json:"externalInitiatedEventId,omitempty"`
	ExternalWorkflowExecution *WorkflowExecution `json:"externalWorkflowExecution,omitempty"`
	Identity                  string             `json:"identity,omitempty"`
}

type WorkflowExecutionCanceledEventAttributes struct {
	DecisionTaskCompletedEventID int64  `json:"decisionTaskCompletedEventId,omitempty"`
	Details                      []byte `json:"details,omitempty"`
}

type MarkerRecordedEventAttributes struct {
	MarkerName                   string  `json:"markerName"`
	Details                      []byte  `json:"details,omitempty"`
	DecisionTaskCompletedEventID int64   `json:"decisionTaskCompletedEventId,omitempty"`
	Header                       *Header `json:"header,omitempty"`
}

type WorkflowExecutionSignaledEventAttributes struct {
	SignalName string `json:"signalName"`
	Input      []byte `json:"input,omitempty"`
	Identity   string `json:"identity,omitempty"`
}

type WorkflowExecutionTerminatedEventAttributes struct {
	Reason   string `json:"reason,omitempty"`
	Details  []byte `json:"details,omitempty"`
	Identity string `json:"identity,omitempty"`
}

type WorkflowExecutionContinuedAsNewEventAttributes struct {
	NewExecutionRunID                   string                 `json:"newExecutionRunId"`
	WorkflowType                        *WorkflowType          `json:"workflowType,omitempty"`
	TaskList                            *TaskList              `json:"taskList,omitempty"`
	Input                               []byte                 `json:"input,omitempty"`
	ExecutionStartToCloseTimeoutSeconds int32                  `json:"executionStartToCloseTimeoutSeconds,omitempty"`
	TaskStartToCloseTimeoutSeconds      int32                  `json:"taskStartToCloseTimeoutSeconds,omitempty"`
	DecisionTaskCompletedEventID        int64                  `json:"decisionTaskCompletedEventId,omitempty"`
	BackoffStartIntervalInSeconds       int32                  `json:"backoffStartIntervalInSeconds,omitempty"`
	Initiator                           ContinueAsNewInitiator `json:"initiator,omitempty"`
	FailureReason                       string                 `json:"failureReason,omitempty"`
	FailureDetails                      []byte                 `json:"failureDetails,omitempty"`
	LastCompletionResult                []byte                 `json:"lastCompletionResult,omitempty"`
	Header                              *Header                `json:"header,omitempty"`
	Memo                                *Memo                  `json:"memo,omitempty"`
	SearchAttributes                    *SearchAttributes      `json:"searchAttributes,omitempty"`
}

type StartChildWorkflowExecutionInitiatedEventAttributes struct {
	Domain                              string                `json:"domain,omitempty"`
	WorkflowID                          string                `json:"workflowId"`
	WorkflowType                        *WorkflowType         `json:"workflowType,omitempty"`
	TaskList                            *TaskList             `json:"taskList,omitempty"`
	Input                               []byte                `json:"input,omitempty"`
	ExecutionStartToCloseTimeoutSeconds int32                 `json:"executionStartToCloseTimeoutSeconds,omitempty"`
	TaskStartToCloseTimeoutSeconds      int32                 `json:"taskStartToCloseTimeoutSeconds,omitempty"`
	ParentClosePolicy                   ParentClosePolicy     `json:"parentClosePolicy,omitempty"`
	Control                             []byte                `json:"control,omitempty"`
	DecisionTaskCompletedEventID        int64                 `json:"decisionTaskCompletedEventId,omitempty"`
	WorkflowIDReusePolicy               WorkflowIDReusePolicy `json:"workflowIdReusePolicy,omitempty"`
	RetryPolicy                         *RetryPolicy          `json:"retryPolicy,omitempty"`
	CronSchedule                        string                `json:"cronSchedule,omitempty"`
	Header                              *Header               `json:"header,omitempty"`
	Memo                                *Memo                 `json:"memo,omitempty"`
	SearchAttributes                    *SearchAttributes     `json:"searchAttributes,omitempty"`
}

type StartChildWorkflowExecutionFailedEventAttributes struct {
	Domain                       string                            `json:"domain,omitempty"`
	WorkflowID                   string                            `json:"workflowId"`
	WorkflowType                 *WorkflowType                     `json:"workflowType,omitempty"`
	Cause                        ChildWorkflowExecutionFailedCause `json:"cause,omitempty"`
	Control                      []byte                            `json:"control,omitempty"`
	InitiatedEventID             int64                             `json:"initiatedEventId"`
	DecisionTaskCompletedEventID int64                             `json:"decisionTaskCompletedEventId,omitempty"`
}

type ChildWorkflowExecutionStartedEventAttributes struct {
	Domain            string             `json:"domain,omitempty"`
	InitiatedEventID  int64              `json:"initiatedEventId"`
	WorkflowExecution *WorkflowExecution `json:"workflowExecution,omitempty"`
	WorkflowType      *WorkflowType      `json:"workflowType,omitempty"`
	Header            *Header            `json:"header,omitempty"`
}

type ChildWorkflowExecutionCompletedEventAttributes struct {
	Result            []byte             `json:"result,omitempty"`
	Domain            string             `json:"domain,omitempty"`
	WorkflowExecution *WorkflowExecution `json:"workflowExecution,omitempty"`
	WorkflowType      *WorkflowType      `json:"workflowType,omitempty"`
	InitiatedEventID  int64              `json:"initiatedEventId"`
	StartedEventID    int64              `json:"startedEventId"`
}

type ChildWorkflowExecutionFailedEventAttributes struct {
	Reason            string             `json:"reason,omitempty"`
	Details           []byte             `json:"details,omitempty"`
	Domain            string             `json:"domain,omitempty"`
	WorkflowExecution *WorkflowExecution `json:"workflowExecution,omitempty"`
	WorkflowType      *WorkflowType      `json:"workflowType,omitempty"`
	InitiatedEventID  int64              `json:"initiatedEventId"`
	StartedEventID    int64              `json:"startedEventId"`
}

type ChildWorkflowExecutionCanceledEventAttributes struct {
	Details           []byte             `json:"details,omitempty"`
	Domain            string             `json:"domain,omitempty"`
	WorkflowExecution *WorkflowExecution `json:"workflowExecution,omitempty"`
	WorkflowType      *WorkflowType      `json:"workflowType,omitempty"`
	InitiatedEventID  int64              `json:"initiatedEventId"`
	StartedEventID    int64              `json:"startedEventId"`
}

type ChildWorkflowExecutionTimedOutEventAttributes struct {
	TimeoutType       TimeoutType        `json:"timeoutType,omitempty"`
	Domain            string             `json:"domain,omitempty"`
	WorkflowExecution *WorkflowExecution `json:"workflowExecution,omitempty"`
	WorkflowType      *WorkflowType      `json:"workflowType,omitempty"`
	InitiatedEventID  int64              `json:"initiatedEventId"`
	StartedEventID    int64              `json:"startedEventId"`
}

type ChildWorkflowExecutionTerminatedEventAttributes struct {
	Domain            string             `json:"domain,omitempty"`
	WorkflowExecution *WorkflowExecution `json:"workflowExecution,omitempty"`
	WorkflowType      *WorkflowType      `json:"workflowType,omitempty"`
	InitiatedEventID  int64              `json:"initiatedEventId"`
	StartedEventID    int64              `json:"startedEventId"`
}

type SignalExternalWorkflowExecutionInitiatedEventAttributes struct {
	DecisionTaskCompletedEventID int64              `json:"decisionTaskCompletedEventId,omitempty"`
	Domain                       string             `json:"domain,omitempty"`
	WorkflowExecution            *WorkflowExecution `json:"workflowExecution,omitempty"`
	SignalName                   string             `json:"signalName"`
	Input                        []byte             `json:"input,omitempty"`
	Control                      []byte             `json:"control,omitempty"`
	ChildWorkflowOnly            bool               `json:"childWorkflowOnly,omitempty"`
}

type SignalExternalWorkflowExecutionFailedEventAttributes struct {
	Cause                        ExternalFailedCause `json:"cause,omitempty"`
	DecisionTaskCompletedEventID int64               `json:"decisionTaskCompletedEventId,omitempty"`
	Domain                       string              `json:"domain,omitempty"`
	WorkflowExecution            *WorkflowExecution  `json:"workflowExecution,omitempty"`
	InitiatedEventID             int64               `json:"initiatedEventId"`
	Control                      []byte              `json:"control,omitempty"`
}

type ExternalWorkflowExecutionSignaledEventAttributes struct {
	InitiatedEventID  int64              `json:"initiatedEventId"`
	Domain            string             `json:"domain,omitempty"`
	WorkflowExecution *WorkflowExecution `json:"workflowExecution,omitempty"`
	Control           []byte             `json:"control,omitempty"`
}

type RequestCancelExternalWorkflowExecutionInitiatedEventAttributes struct {
	DecisionTaskCompletedEventID int64              `json:"decisionTaskCompletedEventId,omitempty"`
	Domain                       string             `json:"domain,omitempty"`
	WorkflowExecution            *WorkflowExecution `json:"workflowExecution,omitempty"`
	Control                      []byte             `json:"control,omitempty"`
	ChildWorkflowOnly            bool               `json:"childWorkflowOnly,omitempty"`
}

type RequestCancelExternalWorkflowExecutionFailedEventAttributes struct {
	Cause                        ExternalFailedCause `json:"cause,omitempty"`
	DecisionTaskCompletedEventID int64               `json:"decisionTaskCompletedEventId,omitempty"`
	Domain                       string              `json:"domain,omitempty"`
	WorkflowExecution            *WorkflowExecution  `json:"workflowExecution,omitempty"`
	InitiatedEventID             int64               `json:"initiatedEventId"`
	Control                      []byte              `json:"control,omitempty"`
}

type ExternalWorkflowExecutionCancelRequestedEventAttributes struct {
	InitiatedEventID  int64              `json:"initiatedEventId"`
	Domain            string             `json:"domain,omitempty"`
	WorkflowExecution *WorkflowExecution `json:"workflowExecution,omitempty"`
}

type UpsertWorkflowSearchAttributesEventAttributes struct {
	DecisionTaskCompletedEventID int64             `json:"decisionTaskCompletedEventId,omitempty"`
	SearchAttributes             *SearchAttributes `json:"searchAttributes,omitempty"`
}
