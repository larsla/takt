package server

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/uber-go/tally"

	"github.com/larsla/takt/api"
)

// Request metric names follow the Cadence frontend's (common/metrics/defs.go),
// with "takt_" in place of the "cadence_" prefix.
const (
	MetricRequests       = "takt_requests"
	MetricFailures       = "takt_errors"
	MetricLatency        = "takt_latency"
	metricErrorPrefix    = "takt_errors_"
	operationUnknownCode = "unknown"
)

// errorMetrics maps error codes to Cadence's per-error counters. Errors
// not listed count as takt_errors.
var errorMetrics = map[string]string{
	"BadRequestError":                        metricErrorPrefix + "bad_request",
	"EntityNotExistsError":                   metricErrorPrefix + "entity_not_exists",
	"WorkflowExecutionAlreadyStartedError":   metricErrorPrefix + "execution_already_started",
	"DomainAlreadyExistsError":               metricErrorPrefix + "domain_already_exists",
	"CancellationAlreadyRequestedError":      metricErrorPrefix + "cancellation_already_requested",
	"QueryFailedError":                       metricErrorPrefix + "query_failed",
	"ServiceBusyError":                       metricErrorPrefix + "service_busy",
	"LimitExceededError":                     metricErrorPrefix + "limit_exceeded",
	"WorkflowExecutionAlreadyCompletedError": metricErrorPrefix + "workflow_execution_already_completed",
}

type requestMetrics struct {
	scope tally.Scope
	start time.Time
}

// request starts the metrics of one API call on subject.
func (s *Server) request(subject string) requestMetrics {
	op := strings.TrimPrefix(subject, api.APISubjectPrefix)
	if op == "" || op == subject {
		op = operationUnknownCode
	}
	scope := s.scope.Tagged(map[string]string{"operation": op})
	scope.Counter(MetricRequests).Inc(1)
	return requestMetrics{scope: scope, start: time.Now()}
}

func (m requestMetrics) done(err error) {
	m.scope.Timer(MetricLatency).Record(time.Since(m.start))
	if err == nil {
		return
	}
	if errors.Is(err, context.DeadlineExceeded) {
		m.scope.Counter(metricErrorPrefix + "context_timeout").Inc(1)
		return
	}
	code, _ := api.EncodeError(err)
	if name, ok := errorMetrics[code]; ok {
		m.scope.Counter(name).Inc(1)
		return
	}
	m.scope.Counter(MetricFailures).Inc(1)
}
