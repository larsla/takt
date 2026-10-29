package server_test

import (
	"errors"
	"testing"
	"time"

	"github.com/larsla/takt/api"
)

func TestActivityRetry(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	runID := h.start("wf-retry")
	h.respond(h.pollDecision(), scheduleActivity("a1", &api.RetryPolicy{
		InitialIntervalInSeconds: 1, BackoffCoefficient: 2, MaximumAttempts: 3,
	}))

	a := h.pollActivity()
	if err := h.c.RespondActivityTaskFailed(h.ctx, &api.RespondActivityTaskFailedRequest{TaskToken: a.TaskToken, Reason: "boom"}); err != nil {
		t.Fatal(err)
	}
	began := time.Now()
	a = h.pollActivity()
	if a.Attempt != 1 || time.Since(began) < 900*time.Millisecond {
		t.Fatalf("attempt %d after %v", a.Attempt, time.Since(began))
	}
	if err := h.c.RespondActivityTaskCompleted(h.ctx, &api.RespondActivityTaskCompletedRequest{TaskToken: a.TaskToken, Result: []byte("1")}); err != nil {
		t.Fatal(err)
	}
	d := h.pollDecision()
	started := hasType(d.History.Events, api.EventTypeActivityTaskStarted)
	if started == nil || started.ActivityTaskStartedEventAttributes.Attempt != 1 ||
		started.ActivityTaskStartedEventAttributes.LastFailureReason != "boom" {
		t.Fatalf("started event %+v", started)
	}
	if hasType(d.History.Events, api.EventTypeActivityTaskFailed) != nil {
		t.Fatal("retried failures must not reach history")
	}
	h.respond(d, completeWorkflow(`null`))
	h.waitClosed("wf-retry", runID)
}

func TestActivityRetryExhaustedAndNonRetriable(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.start("wf-retry-x")
	h.respond(h.pollDecision(),
		scheduleActivity("a1", &api.RetryPolicy{InitialIntervalInSeconds: 1, MaximumAttempts: 1}),
	)
	a := h.pollActivity()
	if err := h.c.RespondActivityTaskFailed(h.ctx, &api.RespondActivityTaskFailedRequest{TaskToken: a.TaskToken, Reason: "boom", Details: []byte("d")}); err != nil {
		t.Fatal(err)
	}
	d := h.pollDecision()
	failed := hasType(d.History.Events, api.EventTypeActivityTaskFailed)
	if failed == nil || failed.ActivityTaskFailedEventAttributes.Reason != "boom" {
		t.Fatalf("history %v", types(d.History.Events))
	}

	h.respond(d, scheduleActivity("a2", &api.RetryPolicy{InitialIntervalInSeconds: 1, NonRetriableErrorReasons: []string{"fatal"}}))
	a = h.pollActivity()
	if err := h.c.RespondActivityTaskFailed(h.ctx, &api.RespondActivityTaskFailedRequest{TaskToken: a.TaskToken, Reason: "fatal"}); err != nil {
		t.Fatal(err)
	}
	d = h.pollDecision()
	if n := countType(d.History.Events, api.EventTypeActivityTaskFailed); n != 2 {
		t.Fatalf("want 2 failures, got %d", n)
	}
}

func countType(events []*api.HistoryEvent, et api.EventType) int {
	n := 0
	for _, e := range events {
		if e.EventType == et {
			n++
		}
	}
	return n
}

func TestActivityScheduleToStartTimeout(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.start("wf-s2s")
	dec := scheduleActivity("a1", nil)
	a := dec.ScheduleActivityTaskDecisionAttributes
	a.TaskList = &api.TaskList{Name: "nobody-listens"}
	a.ScheduleToCloseTimeoutSeconds = 0
	a.ScheduleToStartTimeoutSeconds = 1
	a.StartToCloseTimeoutSeconds = 5
	h.respond(h.pollDecision(), dec)
	d := h.pollDecision()
	to := hasType(d.History.Events, api.EventTypeActivityTaskTimedOut)
	if to == nil || to.ActivityTaskTimedOutEventAttributes.TimeoutType != api.TimeoutTypeScheduleToStart ||
		to.ActivityTaskTimedOutEventAttributes.StartedEventID != 0 {
		t.Fatalf("history %v (%+v)", types(d.History.Events), to)
	}
	sched := hasType(d.History.Events, api.EventTypeActivityTaskScheduled).ActivityTaskScheduledEventAttributes
	if sched.ScheduleToCloseTimeoutSeconds != 6 {
		t.Fatalf("derived schedule-to-close %d", sched.ScheduleToCloseTimeoutSeconds)
	}
}

func TestActivityHeartbeatTimeout(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.start("wf-hb")
	dec := scheduleActivity("a1", nil)
	dec.ScheduleActivityTaskDecisionAttributes.HeartbeatTimeoutSeconds = 1
	h.respond(h.pollDecision(), dec)
	a := h.pollActivity()
	if a.HeartbeatTimeoutSeconds != 1 {
		t.Fatalf("heartbeat timeout %d", a.HeartbeatTimeoutSeconds)
	}
	for range 2 {
		time.Sleep(600 * time.Millisecond)
		resp, err := h.c.RecordActivityTaskHeartbeat(h.ctx, &api.RecordActivityTaskHeartbeatRequest{TaskToken: a.TaskToken, Details: []byte(`"p"`)})
		if err != nil || resp.CancelRequested {
			t.Fatalf("heartbeat: %v %+v", err, resp)
		}
	}
	// Heartbeats kept it alive past the first deadline; now stop.
	d := h.pollDecision()
	to := hasType(d.History.Events, api.EventTypeActivityTaskTimedOut)
	if to == nil || to.ActivityTaskTimedOutEventAttributes.TimeoutType != api.TimeoutTypeHeartbeat ||
		string(to.ActivityTaskTimedOutEventAttributes.Details) != `"p"` {
		t.Fatalf("history %v (%+v)", types(d.History.Events), to)
	}
	err := h.c.RespondActivityTaskCompleted(h.ctx, &api.RespondActivityTaskCompletedRequest{TaskToken: a.TaskToken})
	var nf *api.EntityNotExistsError
	if !errors.As(err, &nf) {
		t.Fatalf("completing a timed out activity: %v", err)
	}
}

func TestActivityStartToCloseRetriesThenTimesOut(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.start("wf-st2c")
	dec := scheduleActivity("a1", &api.RetryPolicy{InitialIntervalInSeconds: 1, MaximumAttempts: 2})
	dec.ScheduleActivityTaskDecisionAttributes.StartToCloseTimeoutSeconds = 1
	h.respond(h.pollDecision(), dec)
	if a := h.pollActivity(); a.Attempt != 0 {
		t.Fatalf("attempt %d", a.Attempt)
	}
	if a := h.pollActivity(); a.Attempt != 1 {
		t.Fatalf("attempt %d", a.Attempt)
	}
	d := h.pollDecision()
	to := hasType(d.History.Events, api.EventTypeActivityTaskTimedOut)
	if to == nil || to.ActivityTaskTimedOutEventAttributes.TimeoutType != api.TimeoutTypeStartToClose ||
		to.ActivityTaskTimedOutEventAttributes.LastFailureReason != "cadenceInternal:Timeout START_TO_CLOSE" {
		t.Fatalf("history %v (%+v)", types(d.History.Events), to)
	}
}

func TestActivityCancel(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.start("wf-act-cancel")
	unstarted := scheduleActivity("idle", nil)
	unstarted.ScheduleActivityTaskDecisionAttributes.TaskList = &api.TaskList{Name: "nobody"}
	h.respond(h.pollDecision(), scheduleActivity("busy", nil), unstarted)
	a := h.pollActivity()

	h.signal("wf-act-cancel", "cancel")
	cancel := func(id string) *api.Decision {
		return &api.Decision{
			DecisionType: api.DecisionTypeRequestCancelActivityTask,
			RequestCancelActivityTaskDecisionAttributes: &api.RequestCancelActivityTaskDecisionAttributes{ActivityID: id},
		}
	}
	h.respond(h.pollDecision(), cancel("busy"), cancel("idle"), cancel("unknown"))

	d := h.pollDecision()
	if n := countType(d.History.Events, api.EventTypeActivityTaskCancelRequested); n != 2 {
		t.Fatalf("cancel requested events: %d (%v)", n, types(d.History.Events))
	}
	if hasType(d.History.Events, api.EventTypeRequestCancelActivityTaskFailed) == nil {
		t.Fatal("unknown activity cancel should fail")
	}
	if n := countType(d.History.Events, api.EventTypeActivityTaskCanceled); n != 1 {
		t.Fatalf("only the unstarted activity is canceled right away, got %d", n)
	}
	desc := h.describe("wf-act-cancel", "")
	if len(desc.PendingActivities) != 1 || desc.PendingActivities[0].State != api.PendingActivityStateCancelRequested {
		t.Fatalf("pending %+v", desc.PendingActivities)
	}
	h.respond(d)

	hb, err := h.c.RecordActivityTaskHeartbeat(h.ctx, &api.RecordActivityTaskHeartbeatRequest{TaskToken: a.TaskToken})
	if err != nil || !hb.CancelRequested {
		t.Fatalf("heartbeat %v %+v", err, hb)
	}
	if err := h.c.RespondActivityTaskCanceled(h.ctx, &api.RespondActivityTaskCanceledRequest{TaskToken: a.TaskToken, Details: []byte("bye")}); err != nil {
		t.Fatal(err)
	}
	d = h.pollDecision()
	if n := countType(d.History.Events, api.EventTypeActivityTaskCanceled); n != 2 {
		t.Fatalf("history %v", types(d.History.Events))
	}
}

func TestActivityByID(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	runID := h.start("wf-byid")
	h.respond(h.pollDecision(), scheduleActivity("a1", nil), scheduleActivity("a2", nil), scheduleActivity("a3", nil))
	for range 3 {
		h.pollActivity()
	}
	if _, err := h.c.RecordActivityTaskHeartbeatByID(h.ctx, &api.RecordActivityTaskHeartbeatByIDRequest{
		Domain: domain, WorkflowID: "wf-byid", ActivityID: "a1", Details: []byte("x"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := h.c.RespondActivityTaskCompletedByID(h.ctx, &api.RespondActivityTaskCompletedByIDRequest{
		Domain: domain, WorkflowID: "wf-byid", RunID: runID, ActivityID: "a1", Result: []byte("1"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := h.c.RespondActivityTaskFailedByID(h.ctx, &api.RespondActivityTaskFailedByIDRequest{
		Domain: domain, WorkflowID: "wf-byid", ActivityID: "a2", Reason: "r",
	}); err != nil {
		t.Fatal(err)
	}
	if err := h.c.RespondActivityTaskCanceledByID(h.ctx, &api.RespondActivityTaskCanceledByIDRequest{
		Domain: domain, WorkflowID: "wf-byid", ActivityID: "a3",
	}); err != nil {
		t.Fatal(err)
	}
	err := h.c.RespondActivityTaskCompletedByID(h.ctx, &api.RespondActivityTaskCompletedByIDRequest{
		Domain: domain, WorkflowID: "wf-byid", ActivityID: "a1",
	})
	var nf *api.EntityNotExistsError
	if !errors.As(err, &nf) {
		t.Fatalf("second completion: %v", err)
	}
	events := h.history("wf-byid", runID)
	for _, et := range []api.EventType{api.EventTypeActivityTaskCompleted, api.EventTypeActivityTaskFailed, api.EventTypeActivityTaskCanceled} {
		if hasType(events, et) == nil {
			t.Errorf("missing %s", et)
		}
	}
}
