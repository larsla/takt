package api

// Decision is one command returned by a decision task. Exactly one
// attributes field is set, matching DecisionType.
type Decision struct {
	DecisionType DecisionType `json:"decisionType"`

	ScheduleActivityTaskDecisionAttributes                   *ScheduleActivityTaskDecisionAttributes                   `json:"scheduleActivityTaskDecisionAttributes,omitempty"`
	StartTimerDecisionAttributes                             *StartTimerDecisionAttributes                             `json:"startTimerDecisionAttributes,omitempty"`
	CompleteWorkflowExecutionDecisionAttributes              *CompleteWorkflowExecutionDecisionAttributes              `json:"completeWorkflowExecutionDecisionAttributes,omitempty"`
	FailWorkflowExecutionDecisionAttributes                  *FailWorkflowExecutionDecisionAttributes                  `json:"failWorkflowExecutionDecisionAttributes,omitempty"`
	RequestCancelActivityTaskDecisionAttributes              *RequestCancelActivityTaskDecisionAttributes              `json:"requestCancelActivityTaskDecisionAttributes,omitempty"`
	CancelTimerDecisionAttributes                            *CancelTimerDecisionAttributes                            `json:"cancelTimerDecisionAttributes,omitempty"`
	CancelWorkflowExecutionDecisionAttributes                *CancelWorkflowExecutionDecisionAttributes                `json:"cancelWorkflowExecutionDecisionAttributes,omitempty"`
	RequestCancelExternalWorkflowExecutionDecisionAttributes *RequestCancelExternalWorkflowExecutionDecisionAttributes `json:"requestCancelExternalWorkflowExecutionDecisionAttributes,omitempty"`
	RecordMarkerDecisionAttributes                           *RecordMarkerDecisionAttributes                           `json:"recordMarkerDecisionAttributes,omitempty"`
	ContinueAsNewWorkflowExecutionDecisionAttributes         *ContinueAsNewWorkflowExecutionDecisionAttributes         `json:"continueAsNewWorkflowExecutionDecisionAttributes,omitempty"`
	StartChildWorkflowExecutionDecisionAttributes            *StartChildWorkflowExecutionDecisionAttributes            `json:"startChildWorkflowExecutionDecisionAttributes,omitempty"`
	SignalExternalWorkflowExecutionDecisionAttributes        *SignalExternalWorkflowExecutionDecisionAttributes        `json:"signalExternalWorkflowExecutionDecisionAttributes,omitempty"`
	UpsertWorkflowSearchAttributesDecisionAttributes         *UpsertWorkflowSearchAttributesDecisionAttributes         `json:"upsertWorkflowSearchAttributesDecisionAttributes,omitempty"`
}

type ScheduleActivityTaskDecisionAttributes struct {
	ActivityID                    string        `json:"activityId"`
	ActivityType                  *ActivityType `json:"activityType,omitempty"`
	Domain                        string        `json:"domain,omitempty"`
	TaskList                      *TaskList     `json:"taskList,omitempty"`
	Input                         []byte        `json:"input,omitempty"`
	ScheduleToCloseTimeoutSeconds int32         `json:"scheduleToCloseTimeoutSeconds,omitempty"`
	ScheduleToStartTimeoutSeconds int32         `json:"scheduleToStartTimeoutSeconds,omitempty"`
	StartToCloseTimeoutSeconds    int32         `json:"startToCloseTimeoutSeconds,omitempty"`
	HeartbeatTimeoutSeconds       int32         `json:"heartbeatTimeoutSeconds,omitempty"`
	RetryPolicy                   *RetryPolicy  `json:"retryPolicy,omitempty"`
	Header                        *Header       `json:"header,omitempty"`
	RequestLocalDispatch          bool          `json:"requestLocalDispatch,omitempty"`
}

type StartTimerDecisionAttributes struct {
	TimerID                   string `json:"timerId"`
	StartToFireTimeoutSeconds int64  `json:"startToFireTimeoutSeconds"`
}

type CompleteWorkflowExecutionDecisionAttributes struct {
	Result []byte `json:"result,omitempty"`
}

type FailWorkflowExecutionDecisionAttributes struct {
	Reason  string `json:"reason,omitempty"`
	Details []byte `json:"details,omitempty"`
}

type RequestCancelActivityTaskDecisionAttributes struct {
	ActivityID string `json:"activityId"`
}

type CancelTimerDecisionAttributes struct {
	TimerID string `json:"timerId"`
}

type CancelWorkflowExecutionDecisionAttributes struct {
	Details []byte `json:"details,omitempty"`
}

type RequestCancelExternalWorkflowExecutionDecisionAttributes struct {
	Domain            string `json:"domain,omitempty"`
	WorkflowID        string `json:"workflowId"`
	RunID             string `json:"runId,omitempty"`
	Control           []byte `json:"control,omitempty"`
	ChildWorkflowOnly bool   `json:"childWorkflowOnly,omitempty"`
}

type RecordMarkerDecisionAttributes struct {
	MarkerName string  `json:"markerName"`
	Details    []byte  `json:"details,omitempty"`
	Header     *Header `json:"header,omitempty"`
}

type ContinueAsNewWorkflowExecutionDecisionAttributes struct {
	WorkflowType                        *WorkflowType          `json:"workflowType,omitempty"`
	TaskList                            *TaskList              `json:"taskList,omitempty"`
	Input                               []byte                 `json:"input,omitempty"`
	ExecutionStartToCloseTimeoutSeconds int32                  `json:"executionStartToCloseTimeoutSeconds,omitempty"`
	TaskStartToCloseTimeoutSeconds      int32                  `json:"taskStartToCloseTimeoutSeconds,omitempty"`
	BackoffStartIntervalInSeconds       int32                  `json:"backoffStartIntervalInSeconds,omitempty"`
	RetryPolicy                         *RetryPolicy           `json:"retryPolicy,omitempty"`
	Initiator                           ContinueAsNewInitiator `json:"initiator,omitempty"`
	FailureReason                       string                 `json:"failureReason,omitempty"`
	FailureDetails                      []byte                 `json:"failureDetails,omitempty"`
	LastCompletionResult                []byte                 `json:"lastCompletionResult,omitempty"`
	CronSchedule                        string                 `json:"cronSchedule,omitempty"`
	Header                              *Header                `json:"header,omitempty"`
	Memo                                *Memo                  `json:"memo,omitempty"`
	SearchAttributes                    *SearchAttributes      `json:"searchAttributes,omitempty"`
	CronOverlapPolicy                   CronOverlapPolicy      `json:"cronOverlapPolicy,omitempty"`
	JitterStartSeconds                  int32                  `json:"jitterStartSeconds,omitempty"`
}

type StartChildWorkflowExecutionDecisionAttributes struct {
	Domain                              string                `json:"domain,omitempty"`
	WorkflowID                          string                `json:"workflowId"`
	WorkflowType                        *WorkflowType         `json:"workflowType,omitempty"`
	TaskList                            *TaskList             `json:"taskList,omitempty"`
	Input                               []byte                `json:"input,omitempty"`
	ExecutionStartToCloseTimeoutSeconds int32                 `json:"executionStartToCloseTimeoutSeconds,omitempty"`
	TaskStartToCloseTimeoutSeconds      int32                 `json:"taskStartToCloseTimeoutSeconds,omitempty"`
	ParentClosePolicy                   ParentClosePolicy     `json:"parentClosePolicy,omitempty"`
	Control                             []byte                `json:"control,omitempty"`
	WorkflowIDReusePolicy               WorkflowIDReusePolicy `json:"workflowIdReusePolicy,omitempty"`
	RetryPolicy                         *RetryPolicy          `json:"retryPolicy,omitempty"`
	CronSchedule                        string                `json:"cronSchedule,omitempty"`
	Header                              *Header               `json:"header,omitempty"`
	Memo                                *Memo                 `json:"memo,omitempty"`
	SearchAttributes                    *SearchAttributes     `json:"searchAttributes,omitempty"`
}

type SignalExternalWorkflowExecutionDecisionAttributes struct {
	Domain            string             `json:"domain,omitempty"`
	Execution         *WorkflowExecution `json:"execution,omitempty"`
	SignalName        string             `json:"signalName"`
	Input             []byte             `json:"input,omitempty"`
	Control           []byte             `json:"control,omitempty"`
	ChildWorkflowOnly bool               `json:"childWorkflowOnly,omitempty"`
}

type UpsertWorkflowSearchAttributesDecisionAttributes struct {
	SearchAttributes *SearchAttributes `json:"searchAttributes,omitempty"`
}
