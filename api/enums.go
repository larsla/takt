// Package api holds the wire types shared by the takt server and SDK. Names
// and JSON field names follow Cadence's shared types so histories exported
// from Cadence decode here too.
package api

// EventType identifies a history event.
type EventType string

const (
	EventTypeWorkflowExecutionStarted                        EventType = "WorkflowExecutionStarted"
	EventTypeWorkflowExecutionCompleted                      EventType = "WorkflowExecutionCompleted"
	EventTypeWorkflowExecutionFailed                         EventType = "WorkflowExecutionFailed"
	EventTypeWorkflowExecutionTimedOut                       EventType = "WorkflowExecutionTimedOut"
	EventTypeDecisionTaskScheduled                           EventType = "DecisionTaskScheduled"
	EventTypeDecisionTaskStarted                             EventType = "DecisionTaskStarted"
	EventTypeDecisionTaskCompleted                           EventType = "DecisionTaskCompleted"
	EventTypeDecisionTaskTimedOut                            EventType = "DecisionTaskTimedOut"
	EventTypeDecisionTaskFailed                              EventType = "DecisionTaskFailed"
	EventTypeActivityTaskScheduled                           EventType = "ActivityTaskScheduled"
	EventTypeActivityTaskStarted                             EventType = "ActivityTaskStarted"
	EventTypeActivityTaskCompleted                           EventType = "ActivityTaskCompleted"
	EventTypeActivityTaskFailed                              EventType = "ActivityTaskFailed"
	EventTypeActivityTaskTimedOut                            EventType = "ActivityTaskTimedOut"
	EventTypeActivityTaskCancelRequested                     EventType = "ActivityTaskCancelRequested"
	EventTypeRequestCancelActivityTaskFailed                 EventType = "RequestCancelActivityTaskFailed"
	EventTypeActivityTaskCanceled                            EventType = "ActivityTaskCanceled"
	EventTypeTimerStarted                                    EventType = "TimerStarted"
	EventTypeTimerFired                                      EventType = "TimerFired"
	EventTypeCancelTimerFailed                               EventType = "CancelTimerFailed"
	EventTypeTimerCanceled                                   EventType = "TimerCanceled"
	EventTypeWorkflowExecutionCancelRequested                EventType = "WorkflowExecutionCancelRequested"
	EventTypeWorkflowExecutionCanceled                       EventType = "WorkflowExecutionCanceled"
	EventTypeRequestCancelExternalWorkflowExecutionInitiated EventType = "RequestCancelExternalWorkflowExecutionInitiated"
	EventTypeRequestCancelExternalWorkflowExecutionFailed    EventType = "RequestCancelExternalWorkflowExecutionFailed"
	EventTypeExternalWorkflowExecutionCancelRequested        EventType = "ExternalWorkflowExecutionCancelRequested"
	EventTypeMarkerRecorded                                  EventType = "MarkerRecorded"
	EventTypeWorkflowExecutionSignaled                       EventType = "WorkflowExecutionSignaled"
	EventTypeWorkflowExecutionTerminated                     EventType = "WorkflowExecutionTerminated"
	EventTypeWorkflowExecutionContinuedAsNew                 EventType = "WorkflowExecutionContinuedAsNew"
	EventTypeStartChildWorkflowExecutionInitiated            EventType = "StartChildWorkflowExecutionInitiated"
	EventTypeStartChildWorkflowExecutionFailed               EventType = "StartChildWorkflowExecutionFailed"
	EventTypeChildWorkflowExecutionStarted                   EventType = "ChildWorkflowExecutionStarted"
	EventTypeChildWorkflowExecutionCompleted                 EventType = "ChildWorkflowExecutionCompleted"
	EventTypeChildWorkflowExecutionFailed                    EventType = "ChildWorkflowExecutionFailed"
	EventTypeChildWorkflowExecutionCanceled                  EventType = "ChildWorkflowExecutionCanceled"
	EventTypeChildWorkflowExecutionTimedOut                  EventType = "ChildWorkflowExecutionTimedOut"
	EventTypeChildWorkflowExecutionTerminated                EventType = "ChildWorkflowExecutionTerminated"
	EventTypeSignalExternalWorkflowExecutionInitiated        EventType = "SignalExternalWorkflowExecutionInitiated"
	EventTypeSignalExternalWorkflowExecutionFailed           EventType = "SignalExternalWorkflowExecutionFailed"
	EventTypeExternalWorkflowExecutionSignaled               EventType = "ExternalWorkflowExecutionSignaled"
	EventTypeUpsertWorkflowSearchAttributes                  EventType = "UpsertWorkflowSearchAttributes"
)

// DecisionType identifies a decision returned by a decision task.
type DecisionType string

const (
	DecisionTypeScheduleActivityTask                   DecisionType = "ScheduleActivityTask"
	DecisionTypeRequestCancelActivityTask              DecisionType = "RequestCancelActivityTask"
	DecisionTypeStartTimer                             DecisionType = "StartTimer"
	DecisionTypeCompleteWorkflowExecution              DecisionType = "CompleteWorkflowExecution"
	DecisionTypeFailWorkflowExecution                  DecisionType = "FailWorkflowExecution"
	DecisionTypeCancelTimer                            DecisionType = "CancelTimer"
	DecisionTypeCancelWorkflowExecution                DecisionType = "CancelWorkflowExecution"
	DecisionTypeRequestCancelExternalWorkflowExecution DecisionType = "RequestCancelExternalWorkflowExecution"
	DecisionTypeRecordMarker                           DecisionType = "RecordMarker"
	DecisionTypeContinueAsNewWorkflowExecution         DecisionType = "ContinueAsNewWorkflowExecution"
	DecisionTypeStartChildWorkflowExecution            DecisionType = "StartChildWorkflowExecution"
	DecisionTypeSignalExternalWorkflowExecution        DecisionType = "SignalExternalWorkflowExecution"
	DecisionTypeUpsertWorkflowSearchAttributes         DecisionType = "UpsertWorkflowSearchAttributes"
)

type TaskListKind string

const (
	TaskListKindNormal TaskListKind = "NORMAL"
	TaskListKindSticky TaskListKind = "STICKY"
)

type TaskListType string

const (
	TaskListTypeDecision TaskListType = "Decision"
	TaskListTypeActivity TaskListType = "Activity"
)

type TimeoutType string

const (
	TimeoutTypeStartToClose    TimeoutType = "START_TO_CLOSE"
	TimeoutTypeScheduleToStart TimeoutType = "SCHEDULE_TO_START"
	TimeoutTypeScheduleToClose TimeoutType = "SCHEDULE_TO_CLOSE"
	TimeoutTypeHeartbeat       TimeoutType = "HEARTBEAT"
)

// QueryConsistencyLevel is accepted for compatibility: takt has one cluster
// and answers every query from the run's current state.
type QueryConsistencyLevel string

const (
	QueryConsistencyLevelEventual QueryConsistencyLevel = "EVENTUAL"
	QueryConsistencyLevelStrong   QueryConsistencyLevel = "STRONG"
)

// CronOverlapPolicy says what happens to a cron fire time that passes
// while the previous run is still open.
type CronOverlapPolicy string

const (
	// CronOverlapPolicySkipped skips fire times missed while a run was open.
	// It is the default.
	CronOverlapPolicySkipped CronOverlapPolicy = "SKIPPED"
	// CronOverlapPolicyBufferone starts the next run as soon as the
	// previous one closes if a fire time passed meanwhile.
	CronOverlapPolicyBufferone CronOverlapPolicy = "BUFFER_ONE"
)

// FailureCategory classifies an activity failure apart from its reason.
type FailureCategory string

const (
	FailureCategoryPoll     FailureCategory = "POLL"
	FailureCategoryStandard FailureCategory = "STANDARD"
	// FailureCategoryFatal failures are not retried, whatever the retry
	// policy says.
	FailureCategoryFatal FailureCategory = "FATAL"
)

type WorkflowIDReusePolicy string

const (
	WorkflowIDReusePolicyAllowDuplicateFailedOnly WorkflowIDReusePolicy = "AllowDuplicateFailedOnly"
	WorkflowIDReusePolicyAllowDuplicate           WorkflowIDReusePolicy = "AllowDuplicate"
	WorkflowIDReusePolicyRejectDuplicate          WorkflowIDReusePolicy = "RejectDuplicate"
	WorkflowIDReusePolicyTerminateIfRunning       WorkflowIDReusePolicy = "TerminateIfRunning"
)

type ParentClosePolicy string

const (
	ParentClosePolicyAbandon       ParentClosePolicy = "ABANDON"
	ParentClosePolicyRequestCancel ParentClosePolicy = "REQUEST_CANCEL"
	ParentClosePolicyTerminate     ParentClosePolicy = "TERMINATE"
)

type ContinueAsNewInitiator string

const (
	ContinueAsNewInitiatorDecider      ContinueAsNewInitiator = "Decider"
	ContinueAsNewInitiatorRetryPolicy  ContinueAsNewInitiator = "RetryPolicy"
	ContinueAsNewInitiatorCronSchedule ContinueAsNewInitiator = "CronSchedule"
)

// WorkflowExecutionCloseStatus is the final status of a closed run.
type WorkflowExecutionCloseStatus string

const (
	CloseStatusCompleted      WorkflowExecutionCloseStatus = "COMPLETED"
	CloseStatusFailed         WorkflowExecutionCloseStatus = "FAILED"
	CloseStatusCanceled       WorkflowExecutionCloseStatus = "CANCELED"
	CloseStatusTerminated     WorkflowExecutionCloseStatus = "TERMINATED"
	CloseStatusContinuedAsNew WorkflowExecutionCloseStatus = "CONTINUED_AS_NEW"
	CloseStatusTimedOut       WorkflowExecutionCloseStatus = "TIMED_OUT"
)

type DecisionTaskFailedCause string

const (
	DecisionTaskFailedCauseUnhandledDecision                   DecisionTaskFailedCause = "UNHANDLED_DECISION"
	DecisionTaskFailedCauseBadScheduleActivityAttributes       DecisionTaskFailedCause = "BAD_SCHEDULE_ACTIVITY_ATTRIBUTES"
	DecisionTaskFailedCauseBadRequestCancelActivityAttributes  DecisionTaskFailedCause = "BAD_REQUEST_CANCEL_ACTIVITY_ATTRIBUTES"
	DecisionTaskFailedCauseBadStartTimerAttributes             DecisionTaskFailedCause = "BAD_START_TIMER_ATTRIBUTES"
	DecisionTaskFailedCauseBadCancelTimerAttributes            DecisionTaskFailedCause = "BAD_CANCEL_TIMER_ATTRIBUTES"
	DecisionTaskFailedCauseBadRecordMarkerAttributes           DecisionTaskFailedCause = "BAD_RECORD_MARKER_ATTRIBUTES"
	DecisionTaskFailedCauseBadCompleteWorkflowAttributes       DecisionTaskFailedCause = "BAD_COMPLETE_WORKFLOW_EXECUTION_ATTRIBUTES"
	DecisionTaskFailedCauseBadFailWorkflowAttributes           DecisionTaskFailedCause = "BAD_FAIL_WORKFLOW_EXECUTION_ATTRIBUTES"
	DecisionTaskFailedCauseBadCancelWorkflowAttributes         DecisionTaskFailedCause = "BAD_CANCEL_WORKFLOW_EXECUTION_ATTRIBUTES"
	DecisionTaskFailedCauseBadRequestCancelExternalAttributes  DecisionTaskFailedCause = "BAD_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_ATTRIBUTES"
	DecisionTaskFailedCauseBadContinueAsNewAttributes          DecisionTaskFailedCause = "BAD_CONTINUE_AS_NEW_ATTRIBUTES"
	DecisionTaskFailedCauseStartTimerDuplicateID               DecisionTaskFailedCause = "START_TIMER_DUPLICATE_ID"
	DecisionTaskFailedCauseResetStickyTasklist                 DecisionTaskFailedCause = "RESET_STICKY_TASKLIST"
	DecisionTaskFailedCauseWorkflowWorkerUnhandledFailure      DecisionTaskFailedCause = "WORKFLOW_WORKER_UNHANDLED_FAILURE"
	DecisionTaskFailedCauseBadSignalWorkflowExecutionAttribute DecisionTaskFailedCause = "BAD_SIGNAL_WORKFLOW_EXECUTION_ATTRIBUTES"
	DecisionTaskFailedCauseBadStartChildExecutionAttributes    DecisionTaskFailedCause = "BAD_START_CHILD_EXECUTION_ATTRIBUTES"
	DecisionTaskFailedCauseForceCloseDecision                  DecisionTaskFailedCause = "FORCE_CLOSE_DECISION"
	DecisionTaskFailedCauseFailoverCloseDecision               DecisionTaskFailedCause = "FAILOVER_CLOSE_DECISION"
	DecisionTaskFailedCauseBadSearchAttributes                 DecisionTaskFailedCause = "BAD_SEARCH_ATTRIBUTES"
	DecisionTaskFailedCauseBadBinary                           DecisionTaskFailedCause = "BAD_BINARY"
)

type ChildWorkflowExecutionFailedCause string

const ChildWorkflowExecutionFailedCauseWorkflowAlreadyRunning ChildWorkflowExecutionFailedCause = "WORKFLOW_ALREADY_RUNNING"

// ExternalFailedCause covers signal and cancel requests to other workflows.
type ExternalFailedCause string

const ExternalFailedCauseUnknownExternalWorkflowExecution ExternalFailedCause = "UNKNOWN_EXTERNAL_WORKFLOW_EXECUTION"

type PendingActivityState string

const (
	PendingActivityStateScheduled       PendingActivityState = "SCHEDULED"
	PendingActivityStateStarted         PendingActivityState = "STARTED"
	PendingActivityStateCancelRequested PendingActivityState = "CANCEL_REQUESTED"
)

type PendingDecisionState string

const (
	PendingDecisionStateScheduled PendingDecisionState = "SCHEDULED"
	PendingDecisionStateStarted   PendingDecisionState = "STARTED"
)

type HistoryEventFilterType string

const (
	HistoryEventFilterTypeAllEvent   HistoryEventFilterType = "ALL_EVENT"
	HistoryEventFilterTypeCloseEvent HistoryEventFilterType = "CLOSE_EVENT"
)

type QueryRejectCondition string

const (
	QueryRejectConditionNotOpen             QueryRejectCondition = "NOT_OPEN"
	QueryRejectConditionNotCompletedCleanly QueryRejectCondition = "NOT_COMPLETED_CLEANLY"
)

type DomainStatus string

const (
	DomainStatusRegistered DomainStatus = "REGISTERED"
	DomainStatusDeprecated DomainStatus = "DEPRECATED"
)
