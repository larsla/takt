package server_test

import (
	"errors"
	"testing"
	"time"

	"github.com/larsla/takt/api"
)

func TestTimerFires(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	runID := h.start("wf-timer")
	h.respond(h.pollDecision(), startTimer("t1", 1))
	began := time.Now()
	d := h.pollDecision()
	if time.Since(began) < 900*time.Millisecond {
		t.Fatalf("timer fired after %v", time.Since(began))
	}
	fired := hasType(d.History.Events, api.EventTypeTimerFired)
	if fired == nil || fired.TimerFiredEventAttributes.TimerID != "t1" || fired.TimerFiredEventAttributes.StartedEventID != 5 {
		t.Fatalf("timer fired event: %+v", fired)
	}
	h.respond(d, completeWorkflow(`1`))
	h.waitClosed("wf-timer", runID)
}

func TestCancelTimer(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.start("wf-cancel-timer")
	h.respond(h.pollDecision(), startTimer("t1", 30))
	h.signal("wf-cancel-timer", "wake")
	d := h.pollDecision()
	h.respond(d,
		&api.Decision{DecisionType: api.DecisionTypeCancelTimer, CancelTimerDecisionAttributes: &api.CancelTimerDecisionAttributes{TimerID: "t1"}},
		&api.Decision{DecisionType: api.DecisionTypeCancelTimer, CancelTimerDecisionAttributes: &api.CancelTimerDecisionAttributes{TimerID: "nope"}},
	)
	d = h.pollDecision()
	if hasType(d.History.Events, api.EventTypeTimerCanceled) == nil || hasType(d.History.Events, api.EventTypeCancelTimerFailed) == nil {
		t.Fatalf("history: %v", types(d.History.Events))
	}
}

func TestSignalBufferedDuringDecision(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	runID := h.start("wf-buffer")
	d := h.pollDecision()
	h.signal("wf-buffer", "s1")
	h.respond(d, scheduleActivity("a1", nil))

	d = h.pollDecision()
	requireTypes(t, d.History.Events,
		api.EventTypeWorkflowExecutionStarted,
		api.EventTypeDecisionTaskScheduled,
		api.EventTypeDecisionTaskStarted,
		api.EventTypeDecisionTaskCompleted,
		api.EventTypeActivityTaskScheduled,
		api.EventTypeWorkflowExecutionSignaled,
		api.EventTypeDecisionTaskScheduled,
		api.EventTypeDecisionTaskStarted,
	)

	// Complete the activity while this decision runs: both its events are
	// buffered and the started reference is fixed when they are flushed.
	a := h.pollActivity()
	if err := h.c.RespondActivityTaskCompleted(h.ctx, &api.RespondActivityTaskCompletedRequest{TaskToken: a.TaskToken}); err != nil {
		t.Fatal(err)
	}
	h.respond(d)
	d = h.pollDecision()
	completed := hasType(d.History.Events, api.EventTypeActivityTaskCompleted)
	started := hasType(d.History.Events, api.EventTypeActivityTaskStarted)
	if completed == nil || started == nil || completed.ActivityTaskCompletedEventAttributes.StartedEventID != started.EventID {
		t.Fatalf("activity events: started %+v completed %+v", started, completed)
	}
	h.respond(d, completeWorkflow(`null`))
	h.waitClosed("wf-buffer", runID)
}

func TestCloseWithUnhandledSignal(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	runID := h.start("wf-unhandled")
	d := h.pollDecision()
	h.signal("wf-unhandled", "late")
	h.respond(d, completeWorkflow(`"too early"`))

	d = h.pollDecision()
	failed := hasType(d.History.Events, api.EventTypeDecisionTaskFailed)
	if failed == nil || failed.DecisionTaskFailedEventAttributes.Cause != api.DecisionTaskFailedCauseUnhandledDecision {
		t.Fatalf("expected unhandled decision failure, got %v", types(d.History.Events))
	}
	if hasType(d.History.Events, api.EventTypeWorkflowExecutionSignaled) == nil {
		t.Fatal("signal missing")
	}
	if d.Attempt != 1 {
		t.Fatalf("attempt %d", d.Attempt)
	}
	h.respond(d, completeWorkflow(`"ok"`))
	if ev := h.waitClosed("wf-unhandled", runID); ev.EventType != api.EventTypeWorkflowExecutionCompleted {
		t.Fatalf("close: %v", ev.EventType)
	}
}

func TestDecisionTimeoutAndFailure(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	req := h.startReq("wf-dto")
	req.TaskStartToCloseTimeoutSeconds = 1
	if _, err := h.c.StartWorkflowExecution(h.ctx, req); err != nil {
		t.Fatal(err)
	}
	first := h.pollDecision()
	// Don't answer; the decision times out and is rescheduled.
	d := h.pollDecision()
	if d.Attempt != 1 || hasType(d.History.Events, api.EventTypeDecisionTaskTimedOut) == nil {
		t.Fatalf("attempt %d, history %v", d.Attempt, types(d.History.Events))
	}
	// The stale token is rejected.
	_, err := h.c.RespondDecisionTaskCompleted(h.ctx, &api.RespondDecisionTaskCompletedRequest{TaskToken: first.TaskToken})
	var nf *api.EntityNotExistsError
	if !errors.As(err, &nf) {
		t.Fatalf("stale token: %v", err)
	}
	if err := h.c.RespondDecisionTaskFailed(h.ctx, &api.RespondDecisionTaskFailedRequest{
		TaskToken: d.TaskToken, Cause: api.DecisionTaskFailedCauseWorkflowWorkerUnhandledFailure, Details: []byte("panic"),
	}); err != nil {
		t.Fatal(err)
	}
	d = h.pollDecision()
	if d.Attempt != 2 {
		t.Fatalf("attempt %d", d.Attempt)
	}
	h.respond(d, completeWorkflow(`0`))
}

func TestInvalidDecisionFailsTask(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.start("wf-invalid")
	h.respond(h.pollDecision(), startTimer("dup", 10), startTimer("dup", 10))
	d := h.pollDecision()
	failed := hasType(d.History.Events, api.EventTypeDecisionTaskFailed)
	if failed == nil || failed.DecisionTaskFailedEventAttributes.Cause != api.DecisionTaskFailedCauseStartTimerDuplicateID {
		t.Fatalf("history %v", types(d.History.Events))
	}
	if hasType(d.History.Events, api.EventTypeTimerStarted) != nil {
		t.Fatal("no decision of a failed task may apply")
	}
	bad := []*api.Decision{
		{DecisionType: api.DecisionTypeScheduleActivityTask, ScheduleActivityTaskDecisionAttributes: &api.ScheduleActivityTaskDecisionAttributes{ActivityID: "x", ActivityType: &api.ActivityType{Name: "a"}}},
		{DecisionType: "Bogus"},
		nil,
	}
	for _, dec := range bad {
		h.respond(d, dec)
		d = h.pollDecision()
		if hasType(d.History.Events[len(d.History.Events)-3:], api.EventTypeDecisionTaskFailed) == nil {
			t.Fatalf("decision %+v was accepted: %v", dec, types(d.History.Events))
		}
	}
}

func TestMarkerAndSearchAttributes(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	runID := h.start("wf-marker")
	h.respond(h.pollDecision(),
		&api.Decision{DecisionType: api.DecisionTypeRecordMarker, RecordMarkerDecisionAttributes: &api.RecordMarkerDecisionAttributes{MarkerName: "SideEffect", Details: []byte("1")}},
		&api.Decision{DecisionType: api.DecisionTypeUpsertWorkflowSearchAttributes, UpsertWorkflowSearchAttributesDecisionAttributes: &api.UpsertWorkflowSearchAttributesDecisionAttributes{
			SearchAttributes: &api.SearchAttributes{IndexedFields: map[string][]byte{"CustomKeywordField": []byte(`"v"`)}},
		}},
		completeWorkflow(`null`),
	)
	h.waitClosed("wf-marker", runID)
	events := h.history("wf-marker", runID)
	if hasType(events, api.EventTypeMarkerRecorded) == nil || hasType(events, api.EventTypeUpsertWorkflowSearchAttributes) == nil {
		t.Fatalf("history %v", types(events))
	}
	info := h.describe("wf-marker", runID).WorkflowExecutionInfo
	if string(info.SearchAttributes.IndexedFields["CustomKeywordField"]) != `"v"` {
		t.Fatalf("search attributes %+v", info.SearchAttributes)
	}
}

func TestFailWorkflow(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	runID := h.start("wf-fail")
	h.respond(h.pollDecision(), &api.Decision{
		DecisionType:                            api.DecisionTypeFailWorkflowExecution,
		FailWorkflowExecutionDecisionAttributes: &api.FailWorkflowExecutionDecisionAttributes{Reason: "bad", Details: []byte("d")},
	})
	ev := h.waitClosed("wf-fail", runID)
	if ev.EventType != api.EventTypeWorkflowExecutionFailed || ev.WorkflowExecutionFailedEventAttributes.Reason != "bad" {
		t.Fatalf("close %+v", ev)
	}
}
