package engine

import (
	"time"

	"github.com/uber-go/tally"

	"github.com/larsla/takt/api"
)

// Metric names follow the Cadence server's history and queue processor
// metrics (common/metrics/defs.go), with a "takt_" prefix.
const (
	MetricWorkflowStarted        = "takt_workflow_started"
	MetricWorkflowSuccess        = "takt_workflow_success"
	MetricWorkflowFailed         = "takt_workflow_failed"
	MetricWorkflowTimeout        = "takt_workflow_timeout"
	MetricWorkflowCancel         = "takt_workflow_cancel"
	MetricWorkflowTerminate      = "takt_workflow_terminate"
	MetricWorkflowContinuedAsNew = "takt_workflow_continued_as_new"
	MetricHistoryCount           = "takt_history_count"
	MetricDecisionTimeout        = "takt_decision_timeout"
	MetricActivityTimeout        = "takt_activity_timeout"
	MetricTaskRequests           = "takt_task_requests"
	MetricTaskErrors             = "takt_task_errors"
	MetricTaskLatency            = "takt_task_latency"
)

// Cadence's operation names for timer and transfer queue tasks.
var timerOperations = map[TimerKind]string{
	TimerUser:            "TimerActiveTaskUserTimer",
	TimerActivityTimeout: "TimerActiveTaskActivityTimeout",
	TimerActivityRetry:   "TimerActiveTaskActivityRetryTimer",
	TimerDecisionTimeout: "TimerActiveTaskDecisionTimeout",
	TimerDecisionBackoff: "TimerActiveTaskDecisionBackoffTimer",
	TimerWorkflowTimeout: "TimerActiveTaskWorkflowTimeout",
	TimerWorkflowBackoff: "TimerActiveTaskWorkflowBackoffTimer",
}

var transferOperations = map[TransferKind]string{
	TransferStartChild:     "TransferActiveTaskStartChildExecution",
	TransferSignalExternal: "TransferActiveTaskSignalExecution",
	TransferCancelExternal: "TransferActiveTaskCancelExecution",
	TransferClosed:         "TransferActiveTaskCloseExecution",
}

var closeMetrics = map[api.EventType]string{
	api.EventTypeWorkflowExecutionCompleted:      MetricWorkflowSuccess,
	api.EventTypeWorkflowExecutionFailed:         MetricWorkflowFailed,
	api.EventTypeWorkflowExecutionTimedOut:       MetricWorkflowTimeout,
	api.EventTypeWorkflowExecutionCanceled:       MetricWorkflowCancel,
	api.EventTypeWorkflowExecutionTerminated:     MetricWorkflowTerminate,
	api.EventTypeWorkflowExecutionContinuedAsNew: MetricWorkflowContinuedAsNew,
}

// historyCountBuckets covers event counts of a closed run.
var historyCountBuckets = tally.MustMakeExponentialValueBuckets(8, 2, 14)

// emitEvents records metrics for events that were just committed.
func (e *Engine) emitEvents(s *State, events []*api.HistoryEvent) {
	if e.scope == tally.NoopScope || len(events) == 0 {
		return
	}
	scope := e.scope.Tagged(map[string]string{"domain": s.Domain, "workflowtype": s.WorkflowType()})
	for _, ev := range events {
		switch ev.EventType {
		case api.EventTypeWorkflowExecutionStarted:
			scope.Counter(MetricWorkflowStarted).Inc(1)
		case api.EventTypeDecisionTaskTimedOut:
			scope.Counter(MetricDecisionTimeout).Inc(1)
		case api.EventTypeActivityTaskTimedOut:
			scope.Tagged(map[string]string{"timeout_type": string(ev.ActivityTaskTimedOutEventAttributes.TimeoutType)}).
				Counter(MetricActivityTimeout).Inc(1)
		}
		if name, ok := closeMetrics[ev.EventType]; ok {
			scope.Counter(name).Inc(1)
			scope.Histogram(MetricHistoryCount, historyCountBuckets).RecordValue(float64(ev.EventID))
		}
	}
}

// queueTask records one timer or transfer task.
func (e *Engine) queueTask(operation, domain string, start time.Time, err error) {
	scope := e.scope.Tagged(map[string]string{"operation": operation, "domain": domain})
	scope.Counter(MetricTaskRequests).Inc(1)
	scope.Timer(MetricTaskLatency).Record(time.Since(start))
	if err != nil {
		scope.Counter(MetricTaskErrors).Inc(1)
	}
}
