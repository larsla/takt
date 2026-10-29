package server_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/larsla/takt/api"
	"github.com/larsla/takt/internal/servertest"
	"github.com/larsla/takt/internal/transport"
)

const (
	domain   = servertest.Domain
	taskList = "tl"
)

type harness struct {
	t   *testing.T
	env *servertest.Env
	c   *transport.Client
	ctx context.Context
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	env := servertest.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	return &harness{t: t, env: env, c: env.Client, ctx: ctx}
}

func (h *harness) startReq(id string) *api.StartWorkflowExecutionRequest {
	return &api.StartWorkflowExecutionRequest{
		Domain:                              domain,
		WorkflowID:                          id,
		WorkflowType:                        &api.WorkflowType{Name: "wf"},
		TaskList:                            &api.TaskList{Name: taskList},
		Input:                               []byte(`"in"`),
		ExecutionStartToCloseTimeoutSeconds: 60,
		TaskStartToCloseTimeoutSeconds:      10,
		Identity:                            "test",
		RequestID:                           id + "-req",
	}
}

func (h *harness) start(id string) string {
	h.t.Helper()
	resp, err := h.c.StartWorkflowExecution(h.ctx, h.startReq(id))
	if err != nil {
		h.t.Fatalf("start %s: %v", id, err)
	}
	return resp.RunID
}

// pollDecision polls until it gets a decision task.
func (h *harness) pollDecision() *api.PollForDecisionTaskResponse {
	h.t.Helper()
	return h.pollDecisionOn(taskList)
}

func (h *harness) pollDecisionOn(tl string) *api.PollForDecisionTaskResponse {
	h.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := h.c.PollForDecisionTask(h.ctx, &api.PollForDecisionTaskRequest{
			Domain: domain, TaskList: &api.TaskList{Name: tl}, Identity: "decider",
		})
		if err != nil {
			h.t.Fatalf("poll decision: %v", err)
		}
		if len(resp.TaskToken) > 0 {
			return resp
		}
	}
	h.t.Fatal("no decision task")
	return nil
}

func (h *harness) pollActivity() *api.PollForActivityTaskResponse {
	h.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := h.c.PollForActivityTask(h.ctx, &api.PollForActivityTaskRequest{
			Domain: domain, TaskList: &api.TaskList{Name: taskList}, Identity: "worker",
		})
		if err != nil {
			h.t.Fatalf("poll activity: %v", err)
		}
		if len(resp.TaskToken) > 0 {
			return resp
		}
	}
	h.t.Fatal("no activity task")
	return nil
}

func (h *harness) respond(task *api.PollForDecisionTaskResponse, ds ...*api.Decision) {
	h.t.Helper()
	_, err := h.c.RespondDecisionTaskCompleted(h.ctx, &api.RespondDecisionTaskCompletedRequest{
		TaskToken: task.TaskToken, Decisions: ds, Identity: "decider",
	})
	if err != nil {
		h.t.Fatalf("respond decision: %v", err)
	}
}

func (h *harness) history(id, runID string) []*api.HistoryEvent {
	h.t.Helper()
	var events []*api.HistoryEvent
	var token []byte
	for {
		resp, err := h.c.GetWorkflowExecutionHistory(h.ctx, &api.GetWorkflowExecutionHistoryRequest{
			Domain: domain, Execution: &api.WorkflowExecution{WorkflowID: id, RunID: runID}, NextPageToken: token,
		})
		if err != nil {
			h.t.Fatalf("history: %v", err)
		}
		events = append(events, resp.History.Events...)
		if len(resp.NextPageToken) == 0 {
			return events
		}
		token = resp.NextPageToken
	}
}

// waitClosed long-polls for the close event.
func (h *harness) waitClosed(id, runID string) *api.HistoryEvent {
	h.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := h.c.GetWorkflowExecutionHistory(h.ctx, &api.GetWorkflowExecutionHistoryRequest{
			Domain:                 domain,
			Execution:              &api.WorkflowExecution{WorkflowID: id, RunID: runID},
			WaitForNewEvent:        true,
			HistoryEventFilterType: api.HistoryEventFilterTypeCloseEvent,
		})
		if err != nil {
			h.t.Fatalf("wait closed: %v", err)
		}
		if len(resp.History.Events) == 1 {
			return resp.History.Events[0]
		}
	}
	h.t.Fatal("workflow did not close")
	return nil
}

func (h *harness) describe(id, runID string) *api.DescribeWorkflowExecutionResponse {
	h.t.Helper()
	resp, err := h.c.DescribeWorkflowExecution(h.ctx, &api.DescribeWorkflowExecutionRequest{
		Domain: domain, Execution: &api.WorkflowExecution{WorkflowID: id, RunID: runID},
	})
	if err != nil {
		h.t.Fatalf("describe: %v", err)
	}
	return resp
}

func types(events []*api.HistoryEvent) []api.EventType {
	out := make([]api.EventType, len(events))
	for i, e := range events {
		out[i] = e.EventType
	}
	return out
}

func requireTypes(t *testing.T, events []*api.HistoryEvent, want ...api.EventType) {
	t.Helper()
	got := types(events)
	if !slices.Equal(got, want) {
		t.Fatalf("event types:\n got  %v\n want %v", got, want)
	}
	for i, e := range events {
		if e.EventID != int64(i+1) {
			t.Fatalf("event %d has ID %d", i, e.EventID)
		}
	}
}

func hasType(events []*api.HistoryEvent, et api.EventType) *api.HistoryEvent {
	for _, e := range events {
		if e.EventType == et {
			return e
		}
	}
	return nil
}

func scheduleActivity(id string, retry *api.RetryPolicy) *api.Decision {
	return &api.Decision{
		DecisionType: api.DecisionTypeScheduleActivityTask,
		ScheduleActivityTaskDecisionAttributes: &api.ScheduleActivityTaskDecisionAttributes{
			ActivityID:                    id,
			ActivityType:                  &api.ActivityType{Name: "act"},
			TaskList:                      &api.TaskList{Name: taskList},
			Input:                         []byte(`"act-in"`),
			ScheduleToCloseTimeoutSeconds: 30,
			RetryPolicy:                   retry,
		},
	}
}

func completeWorkflow(result string) *api.Decision {
	return &api.Decision{
		DecisionType: api.DecisionTypeCompleteWorkflowExecution,
		CompleteWorkflowExecutionDecisionAttributes: &api.CompleteWorkflowExecutionDecisionAttributes{Result: []byte(result)},
	}
}

func startTimer(id string, secs int64) *api.Decision {
	return &api.Decision{
		DecisionType:                 api.DecisionTypeStartTimer,
		StartTimerDecisionAttributes: &api.StartTimerDecisionAttributes{TimerID: id, StartToFireTimeoutSeconds: secs},
	}
}

func (h *harness) signal(id, name string) {
	h.t.Helper()
	err := h.c.SignalWorkflowExecution(h.ctx, &api.SignalWorkflowExecutionRequest{
		Domain: domain, WorkflowExecution: &api.WorkflowExecution{WorkflowID: id}, SignalName: name, Input: []byte(`"sig"`),
	})
	if err != nil {
		h.t.Fatalf("signal: %v", err)
	}
}
