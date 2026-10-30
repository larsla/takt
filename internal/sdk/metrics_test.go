package sdk

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/larsla/takt/internal/metrics"
	"github.com/larsla/takt/internal/servertest"
)

func metricsWorkflow(ctx Context, name string) (string, error) {
	ctx = WithActivityOptions(ctx, shortActivity)
	ctx = WithLocalActivityOptions(ctx, LocalActivityOptions{ScheduleToCloseTimeout: 10 * time.Second})
	var a, b string
	if err := ExecuteActivity(ctx, greet, name).Get(ctx, &a); err != nil {
		return "", err
	}
	if err := ExecuteLocalActivity(ctx, greet, name).Get(ctx, &b); err != nil {
		return "", err
	}
	return a + b, nil
}

// Server and worker report into one Prometheus registry, and the scrape
// has the names and labels the docs list.
func TestMetrics(t *testing.T) {
	t.Parallel()
	prom := metrics.NewPrometheus(slog.New(slog.NewTextHandler(io.Discard, nil)))
	stack := servertest.New(t, servertest.Options{Metrics: prom.Scope})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	c := NewClient(stack.Client, servertest.Domain, &ClientOptions{MetricsScope: prom.Scope})
	w := NewWorker(stack.Client, servertest.Domain, testTaskList, WorkerOptions{MetricsScope: prom.Scope})
	w.RegisterWorkflow(metricsWorkflow)
	w.RegisterActivity(greet)
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	run, err := c.ExecuteWorkflow(ctx, StartWorkflowOptions{
		ID: "metrics", TaskList: testTaskList, ExecutionStartToCloseTimeout: time.Minute,
	}, metricsWorkflow, "m")
	if err != nil {
		t.Fatal(err)
	}
	if err := run.Get(ctx, nil); err != nil {
		t.Fatal(err)
	}
	w.Stop()
	if err := prom.Close(); err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	prom.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		// server
		`takt_requests{operation="StartWorkflowExecution"}`,
		`takt_latency_bucket{operation="RespondDecisionTaskCompleted"`,
		`takt_workflow_started{domain="test-domain",workflowtype="github.com/larsla/takt/internal/sdk.metricsWorkflow"} 1`,
		`takt_workflow_success{domain="test-domain",workflowtype="github.com/larsla/takt/internal/sdk.metricsWorkflow"} 1`,
		`takt_history_count_count{domain="test-domain",workflowtype="github.com/larsla/takt/internal/sdk.metricsWorkflow"} 1`,
		`takt_task_requests{domain="test-domain",operation="TransferActiveTaskCloseExecution"}`,
		// worker
		`takt_worker_start{domain="test-domain",tasklist="` + testTaskList + `",workertype="DecisionWorker"} 1`,
		`takt_decision_poll_succeed{`,
		`takt_decision_execution_latency_bucket{`,
		`takt_workflow_completed{domain="test-domain",tasklist="` + testTaskList + `",workertype="DecisionWorker",workflowtype="github.com/larsla/takt/internal/sdk.metricsWorkflow"} 1`,
		`takt_workflow_endtoend_latency_count{`,
		`takt_activity_task_completed{activitytype="github.com/larsla/takt/internal/sdk.greet",`,
		`takt_activity_scheduled_to_start_latency_count{`,
		`takt_local_activity_total{activitytype="github.com/larsla/takt/internal/sdk.greet",`,
		`takt_sticky_cache_size{`,
		`takt_PollForActivityTask_takt_request{domain="test-domain",tasklist="` + testTaskList + `"}`,
		`takt_StartWorkflowExecution_takt_request{domain="test-domain"} 1`,
		`go_goroutines`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("scrape is missing %s", want)
		}
	}
	if t.Failed() {
		t.Log(body)
	}
}
