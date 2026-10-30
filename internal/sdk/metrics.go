package sdk

import (
	"errors"
	"sync"
	"time"

	"github.com/uber-go/tally"

	"github.com/larsla/takt/api"
)

// Metric names and tags follow the ones go.uber.org/cadence emits
// (internal/common/metrics/constants.go), with "takt-" in place of the
// "cadence-" prefix.
const (
	metricsPrefix = "takt-"

	MetricWorkflowCompleted         = metricsPrefix + "workflow-completed"
	MetricWorkflowCanceled          = metricsPrefix + "workflow-canceled"
	MetricWorkflowFailed            = metricsPrefix + "workflow-failed"
	MetricWorkflowContinueAsNew     = metricsPrefix + "workflow-continue-as-new"
	MetricWorkflowEndToEndLatency   = metricsPrefix + "workflow-endtoend-latency"
	MetricWorkflowGetHistory        = metricsPrefix + "workflow-get-history-total"
	MetricWorkflowGetHistoryFailed  = metricsPrefix + "workflow-get-history-failed"
	MetricWorkflowGetHistorySucceed = metricsPrefix + "workflow-get-history-succeed"
	MetricWorkflowGetHistoryLatency = metricsPrefix + "workflow-get-history-latency"

	MetricDecisionPoll                  = metricsPrefix + "decision-poll-total"
	MetricDecisionPollFailed            = metricsPrefix + "decision-poll-failed"
	MetricDecisionPollNoTask            = metricsPrefix + "decision-poll-no-task"
	MetricDecisionPollSucceed           = metricsPrefix + "decision-poll-succeed"
	MetricDecisionPollLatency           = metricsPrefix + "decision-poll-latency"
	MetricDecisionScheduledToStart      = metricsPrefix + "decision-scheduled-to-start-latency"
	MetricDecisionExecutionFailed       = metricsPrefix + "decision-execution-failed"
	MetricDecisionExecutionLatency      = metricsPrefix + "decision-execution-latency"
	MetricDecisionResponseFailed        = metricsPrefix + "decision-response-failed"
	MetricDecisionResponseLatency       = metricsPrefix + "decision-response-latency"
	MetricDecisionTaskPanic             = metricsPrefix + "decision-task-panic"
	MetricDecisionTaskCompleted         = metricsPrefix + "decision-task-completed"
	MetricNonDeterministicError         = metricsPrefix + "non-deterministic-error"
	MetricActivityPoll                  = metricsPrefix + "activity-poll-total"
	MetricActivityPollFailed            = metricsPrefix + "activity-poll-failed"
	MetricActivityPollNoTask            = metricsPrefix + "activity-poll-no-task"
	MetricActivityPollSucceed           = metricsPrefix + "activity-poll-succeed"
	MetricActivityPollLatency           = metricsPrefix + "activity-poll-latency"
	MetricActivityScheduledToStart      = metricsPrefix + "activity-scheduled-to-start-latency"
	MetricActivityExecutionFailed       = metricsPrefix + "activity-execution-failed"
	MetricActivityExecutionLatency      = metricsPrefix + "activity-execution-latency"
	MetricActivityResponseFailed        = metricsPrefix + "activity-response-failed"
	MetricActivityResponseLatency       = metricsPrefix + "activity-response-latency"
	MetricActivityEndToEndLatency       = metricsPrefix + "activity-endtoend-latency"
	MetricActivityTaskPanic             = metricsPrefix + "activity-task-panic"
	MetricActivityTaskCompleted         = metricsPrefix + "activity-task-completed"
	MetricActivityTaskFailed            = metricsPrefix + "activity-task-failed"
	MetricActivityTaskCanceled          = metricsPrefix + "activity-task-canceled"
	MetricLocalActivityTotal            = metricsPrefix + "local-activity-total"
	MetricLocalActivityTimeout          = metricsPrefix + "local-activity-timeout"
	MetricLocalActivityCanceled         = metricsPrefix + "local-activity-canceled"
	MetricLocalActivityFailed           = metricsPrefix + "local-activity-failed"
	MetricLocalActivityPanic            = metricsPrefix + "local-activity-panic"
	MetricLocalActivityExecutionLatency = metricsPrefix + "local-activity-execution-latency"

	MetricWorkerStart = metricsPrefix + "worker-start"
	MetricPollerStart = metricsPrefix + "poller-start"

	MetricStickyCacheHit   = metricsPrefix + "sticky-cache-hit"
	MetricStickyCacheMiss  = metricsPrefix + "sticky-cache-miss"
	MetricStickyCacheEvict = metricsPrefix + "sticky-cache-evict"
	MetricStickyCacheSize  = metricsPrefix + "sticky-cache-size"

	MetricRequest        = metricsPrefix + "request"
	MetricError          = metricsPrefix + "error"
	MetricLatency        = metricsPrefix + "latency"
	MetricInvalidRequest = metricsPrefix + "invalid-request"

	tagDomain       = "domain"
	tagTaskList     = "tasklist"
	tagWorkerType   = "workertype"
	tagWorkflowType = "workflowtype"
	tagActivityType = "activitytype"

	workerTypeDecision      = "DecisionWorker"
	workerTypeActivity      = "ActivityWorker"
	workerTypeLocalActivity = "LocalActivityWorker"
)

func tagged(scope tally.Scope, kv ...string) tally.Scope {
	tags := make(map[string]string, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		tags[kv[i]] = kv[i+1]
	}
	return scope.Tagged(tags)
}

// metricsService wraps a service and records per-operation request
// metrics in a sub scope named takt-<Operation>, as Cadence's client does
// with cadence-<Operation>.
type metricsService struct {
	api.Service
	scope tally.Scope
	mu    sync.Mutex
	ops   map[string]tally.Scope
}

func withServiceMetrics(service api.Service, scope tally.Scope) api.Service {
	if scope == nil || scope == tally.NoopScope {
		return service
	}
	if _, ok := service.(*metricsService); ok {
		return service
	}
	return &metricsService{Service: service, scope: scope, ops: map[string]tally.Scope{}}
}

type opMetrics struct {
	scope tally.Scope
	start time.Time
}

func (s *metricsService) op(name string) opMetrics {
	s.mu.Lock()
	scope, ok := s.ops[name]
	if !ok {
		scope = s.scope.SubScope(metricsPrefix + name)
		s.ops[name] = scope
	}
	s.mu.Unlock()
	scope.Counter(MetricRequest).Inc(1)
	return opMetrics{scope: scope, start: time.Now()}
}

func (o opMetrics) done(err error) {
	o.scope.Timer(MetricLatency).Record(time.Since(o.start))
	switch {
	case err == nil:
	case isClientError(err):
		o.scope.Counter(MetricInvalidRequest).Inc(1)
	default:
		o.scope.Counter(MetricError).Inc(1)
	}
}

// isClientError reports the errors Cadence counts as invalid requests.
func isClientError(err error) bool {
	var (
		badRequest *api.BadRequestError
		notExists  *api.EntityNotExistsError
		started    *api.WorkflowExecutionAlreadyStartedError
		domain     *api.DomainAlreadyExistsError
		query      *api.QueryFailedError
		canceled   *api.CancellationAlreadyRequestedError
	)
	return errors.As(err, &badRequest) || errors.As(err, &notExists) || errors.As(err, &started) ||
		errors.As(err, &domain) || errors.As(err, &query) || errors.As(err, &canceled)
}

// pollMetrics records one poll of a task list.
func pollMetrics(scope tally.Scope, total, failed, noTask, succeed, latency string, start time.Time, gotTask bool, err error) {
	scope.Counter(total).Inc(1)
	scope.Timer(latency).Record(time.Since(start))
	switch {
	case err != nil:
		scope.Counter(failed).Inc(1)
	case !gotTask:
		scope.Counter(noTask).Inc(1)
	default:
		scope.Counter(succeed).Inc(1)
	}
}

// workflowCloseMetrics records how the decisions of a task closed the
// workflow, if they did.
func workflowCloseMetrics(scope tally.Scope, decisions []*api.Decision, started time.Time) {
	for _, d := range decisions {
		var name string
		switch d.DecisionType {
		case api.DecisionTypeCompleteWorkflowExecution:
			name = MetricWorkflowCompleted
		case api.DecisionTypeFailWorkflowExecution:
			name = MetricWorkflowFailed
		case api.DecisionTypeCancelWorkflowExecution:
			name = MetricWorkflowCanceled
		case api.DecisionTypeContinueAsNewWorkflowExecution:
			name = MetricWorkflowContinueAsNew
		default:
			continue
		}
		scope.Counter(name).Inc(1)
		if !started.IsZero() {
			scope.Timer(MetricWorkflowEndToEndLatency).Record(time.Since(started))
		}
		return
	}
}

// workflowStartTime is when the run started, from its first event.
func workflowStartTime(events []*api.HistoryEvent) time.Time {
	if len(events) == 0 || events[0].WorkflowExecutionStartedEventAttributes == nil {
		return time.Time{}
	}
	return time.Unix(0, events[0].Timestamp)
}
