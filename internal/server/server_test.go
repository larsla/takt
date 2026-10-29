package server_test

import (
	"testing"

	"github.com/larsla/takt/api"
)

func TestActivityRoundTrip(t *testing.T) {
	h := newHarness(t)
	runID := h.start("wf-basic")

	d := h.pollDecision()
	requireTypes(t, d.History.Events,
		api.EventTypeWorkflowExecutionStarted, api.EventTypeDecisionTaskScheduled, api.EventTypeDecisionTaskStarted)
	if d.WorkflowType.Name != "wf" || d.WorkflowExecution.RunID != runID || d.StartedEventID != 3 {
		t.Fatalf("unexpected decision task: %+v", d)
	}
	h.respond(d, scheduleActivity("a1", nil))

	a := h.pollActivity()
	if a.ActivityID != "a1" || string(a.Input) != `"act-in"` || a.ActivityType.Name != "act" {
		t.Fatalf("unexpected activity task: %+v", a)
	}
	desc := h.describe("wf-basic", "")
	if len(desc.PendingActivities) != 1 || desc.PendingActivities[0].State != api.PendingActivityStateStarted {
		t.Fatalf("pending activities: %+v", desc.PendingActivities)
	}
	if err := h.c.RespondActivityTaskCompleted(h.ctx, &api.RespondActivityTaskCompletedRequest{
		TaskToken: a.TaskToken, Result: []byte(`"done"`), Identity: "worker",
	}); err != nil {
		t.Fatal(err)
	}

	d = h.pollDecision()
	if d.PreviousStartedEventID != 3 {
		t.Fatalf("previous started = %d", d.PreviousStartedEventID)
	}
	h.respond(d, completeWorkflow(`"result"`))

	closeEv := h.waitClosed("wf-basic", runID)
	if closeEv.EventType != api.EventTypeWorkflowExecutionCompleted ||
		string(closeEv.WorkflowExecutionCompletedEventAttributes.Result) != `"result"` {
		t.Fatalf("close event: %+v", closeEv)
	}
	requireTypes(t, h.history("wf-basic", runID),
		api.EventTypeWorkflowExecutionStarted,
		api.EventTypeDecisionTaskScheduled,
		api.EventTypeDecisionTaskStarted,
		api.EventTypeDecisionTaskCompleted,
		api.EventTypeActivityTaskScheduled,
		api.EventTypeActivityTaskStarted,
		api.EventTypeActivityTaskCompleted,
		api.EventTypeDecisionTaskScheduled,
		api.EventTypeDecisionTaskStarted,
		api.EventTypeDecisionTaskCompleted,
		api.EventTypeWorkflowExecutionCompleted,
	)
	desc = h.describe("wf-basic", runID)
	if cs := desc.WorkflowExecutionInfo.CloseStatus; cs == nil || *cs != api.CloseStatusCompleted {
		t.Fatalf("close status %v", cs)
	}
	if desc.WorkflowExecutionInfo.HistoryLength != 11 {
		t.Fatalf("history length %d", desc.WorkflowExecutionInfo.HistoryLength)
	}
}
