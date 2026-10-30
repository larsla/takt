package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/larsla/takt/api"
)

var errActivityNotFound = &api.EntityNotExistsError{Message: "activity task not found"}

type timeouts struct{ s2c, s2s, st2c, hb int32 }

// activityTimeouts fills in missing activity timeouts the way Cadence does
// and caps them at the workflow timeout.
func activityTimeouts(a *api.ScheduleActivityTaskDecisionAttributes, wfTimeout int32) (timeouts, error) {
	t := timeouts{a.ScheduleToCloseTimeoutSeconds, a.ScheduleToStartTimeoutSeconds, a.StartToCloseTimeoutSeconds, a.HeartbeatTimeoutSeconds}
	if t.s2c < 0 || t.s2s < 0 || t.st2c < 0 || t.hb < 0 {
		return t, errors.New("activity timeouts must not be negative")
	}
	switch {
	case t.s2c > 0:
		if t.s2s == 0 {
			t.s2s = t.s2c
		}
		if t.st2c == 0 {
			t.st2c = t.s2c
		}
	case t.s2s > 0 && t.st2c > 0:
		t.s2c = t.s2s + t.st2c
	default:
		return t, errors.New("a valid ScheduleToCloseTimeout is not set, or ScheduleToStart and StartToClose timeouts are not both set")
	}
	if wfTimeout > 0 {
		t.s2c = min(t.s2c, wfTimeout)
		t.s2s = min(t.s2s, wfTimeout)
		t.st2c = min(t.st2c, wfTimeout)
	}
	if t.hb > t.st2c {
		t.hb = t.st2c
	}
	return t, nil
}

func (t *tx) scheduleActivity(a *api.ScheduleActivityTaskDecisionAttributes, completedID int64) {
	s := t.s
	to, _ := activityTimeouts(a, s.Start.ExecutionStartToCloseTimeoutSeconds)
	tl := s.TaskList()
	if a.TaskList != nil && a.TaskList.Name != "" {
		tl = a.TaskList.Name
	}
	policy := normalizeRetryPolicy(a.RetryPolicy)
	ev := t.event(api.EventTypeActivityTaskScheduled)
	ev.ActivityTaskScheduledEventAttributes = &api.ActivityTaskScheduledEventAttributes{
		ActivityID:                    a.ActivityID,
		ActivityType:                  a.ActivityType,
		Domain:                        s.Domain,
		TaskList:                      &api.TaskList{Name: tl},
		Input:                         a.Input,
		ScheduleToCloseTimeoutSeconds: to.s2c,
		ScheduleToStartTimeoutSeconds: to.s2s,
		StartToCloseTimeoutSeconds:    to.st2c,
		HeartbeatTimeoutSeconds:       to.hb,
		DecisionTaskCompletedEventID:  completedID,
		RetryPolicy:                   policy,
		Header:                        a.Header,
	}
	t.add(ev)
	ai := &ActivityInfo{
		ScheduleID:           ev.EventID,
		ActivityID:           a.ActivityID,
		ActivityType:         a.ActivityType.Name,
		TaskList:             tl,
		Input:                a.Input,
		Header:               a.Header,
		RetryPolicy:          policy,
		ScheduleToClose:      to.s2c,
		ScheduleToStart:      to.s2s,
		StartToClose:         to.st2c,
		Heartbeat:            to.hb,
		ScheduledTime:        t.now,
		AttemptScheduledTime: t.now,
	}
	if policy != nil && policy.ExpirationIntervalInSeconds > 0 {
		ai.ExpirationTime = t.now + seconds(policy.ExpirationIntervalInSeconds)
	}
	if s.Activities == nil {
		s.Activities = map[int64]*ActivityInfo{}
	}
	s.Activities[ai.ScheduleID] = ai
	t.dispatchActivity(ai)
}

// dispatchActivity puts the current attempt on its task list and arms the
// timeouts that run from scheduling.
func (t *tx) dispatchActivity(ai *ActivityInfo) {
	at := int64(ai.Attempt)
	t.task(api.TaskListTypeActivity, ai.TaskList, ai.ScheduleID, at)
	t.timer(TimerActivityTimeout, ai.ScheduleID, at, api.TimeoutTypeScheduleToStart,
		ai.AttemptScheduledTime+seconds(ai.ScheduleToStart), fmt.Sprintf("a-%d-%d-s2s", ai.ScheduleID, at))
	t.timer(TimerActivityTimeout, ai.ScheduleID, at, api.TimeoutTypeScheduleToClose,
		ai.AttemptScheduledTime+seconds(ai.ScheduleToClose), fmt.Sprintf("a-%d-%d-s2c", ai.ScheduleID, at))
}

func (t *tx) requestCancelActivity(activityID string, completedID int64) bool {
	ai := t.s.activityByID(activityID)
	if ai == nil {
		ev := t.event(api.EventTypeRequestCancelActivityTaskFailed)
		ev.RequestCancelActivityTaskFailedEventAttributes = &api.RequestCancelActivityTaskFailedEventAttributes{
			ActivityID: activityID, Cause: "ACTIVITY_ID_UNKNOWN", DecisionTaskCompletedEventID: completedID,
		}
		t.add(ev)
		return true
	}
	ev := t.event(api.EventTypeActivityTaskCancelRequested)
	ev.ActivityTaskCancelRequestedEventAttributes = &api.ActivityTaskCancelRequestedEventAttributes{
		ActivityID: activityID, DecisionTaskCompletedEventID: completedID,
	}
	t.add(ev)
	if ai.Started {
		// The worker learns about it from its next heartbeat.
		ai.CancelRequested = true
		ai.CancelRequestEventID = ev.EventID
		return false
	}
	// Written after all events of this decision task so they stay together.
	canceled := t.event(api.EventTypeActivityTaskCanceled)
	canceled.ActivityTaskCanceledEventAttributes = &api.ActivityTaskCanceledEventAttributes{
		ScheduledEventID:             ai.ScheduleID,
		LatestCancelRequestedEventID: ev.EventID,
	}
	t.afterDecisions = append(t.afterDecisions, canceled)
	delete(t.s.Activities, ai.ScheduleID)
	return true
}

func (e *Engine) RecordActivityTaskStarted(ctx context.Context, req *api.RecordActivityTaskStartedRequest) (*api.PollForActivityTaskResponse, error) {
	stub := req.Stub
	var ai ActivityInfo
	s, err := e.update(ctx, stub.Domain, stub.WorkflowID, stub.RunID, func(t *tx) error {
		cur := t.s.Activities[stub.ScheduleID]
		if !t.s.Running() || cur == nil || int64(cur.Attempt) != stub.Attempt {
			return errActivityNotFound
		}
		if cur.Started {
			if cur.StartedRequestID == req.RequestID {
				ai = *cur
				return nil
			}
			return &api.EntityNotExistsError{Message: "activity task already started"}
		}
		cur.Started = true
		cur.StartedTime = t.now
		cur.StartedIdentity = req.Identity
		cur.StartedRequestID = req.RequestID
		t.changed = true
		at := int64(cur.Attempt)
		t.timer(TimerActivityTimeout, cur.ScheduleID, at, api.TimeoutTypeStartToClose,
			t.now+seconds(cur.StartToClose), fmt.Sprintf("a-%d-%d-st2c", cur.ScheduleID, at))
		if cur.Heartbeat > 0 {
			t.heartbeatTimer(cur, t.now+seconds(cur.Heartbeat))
		}
		ai = *cur
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &api.PollForActivityTaskResponse{
		TaskToken: api.TaskToken{
			Domain:     s.Domain,
			WorkflowID: s.WorkflowID,
			RunID:      s.RunID,
			ScheduleID: ai.ScheduleID,
			Attempt:    int64(ai.Attempt),
			ActivityID: ai.ActivityID,
		}.Encode(),
		WorkflowExecution:               s.Execution(),
		ActivityID:                      ai.ActivityID,
		ActivityType:                    &api.ActivityType{Name: ai.ActivityType},
		Input:                           ai.Input,
		ScheduledTimestamp:              ai.ScheduledTime,
		ScheduleToCloseTimeoutSeconds:   ai.ScheduleToClose,
		StartedTimestamp:                ai.StartedTime,
		StartToCloseTimeoutSeconds:      ai.StartToClose,
		HeartbeatTimeoutSeconds:         ai.Heartbeat,
		Attempt:                         ai.Attempt,
		ScheduledTimestampOfThisAttempt: ai.AttemptScheduledTime,
		HeartbeatDetails:                ai.HeartbeatDetails,
		WorkflowType:                    s.Start.WorkflowType,
		WorkflowDomain:                  s.Domain,
		Header:                          ai.Header,
	}, nil
}

func (t *tx) heartbeatTimer(ai *ActivityInfo, deadline int64) {
	at := int64(ai.Attempt)
	t.timer(TimerActivityTimeout, ai.ScheduleID, at, api.TimeoutTypeHeartbeat, deadline,
		fmt.Sprintf("a-%d-%d-hb-%d", ai.ScheduleID, at, deadline))
}

// activityRef identifies a started activity, by token or by ID.
type activityRef struct {
	domain, workflowID, runID string
	scheduleID                int64
	attempt                   int64
	activityID                string
}

func refFromToken(b []byte) (activityRef, error) {
	tok, err := api.DecodeTaskToken(b)
	if err != nil {
		return activityRef{}, err
	}
	return activityRef{tok.Domain, tok.WorkflowID, tok.RunID, tok.ScheduleID, tok.Attempt, ""}, nil
}

func (e *Engine) refFromID(ctx context.Context, domain, workflowID, runID, activityID string) (activityRef, error) {
	if workflowID == "" || activityID == "" {
		return activityRef{}, badRequest("workflow ID and activity ID are required")
	}
	runID, err := e.resolveRunID(ctx, domain, &api.WorkflowExecution{WorkflowID: workflowID, RunID: runID})
	if err != nil {
		return activityRef{}, err
	}
	return activityRef{domain, workflowID, runID, 0, 0, activityID}, nil
}

// updateActivity runs fn on the started activity ref points at.
func (e *Engine) updateActivity(ctx context.Context, ref activityRef, fn func(t *tx, ai *ActivityInfo) error) error {
	_, err := e.update(ctx, ref.domain, ref.workflowID, ref.runID, func(t *tx) error {
		if !t.s.Running() {
			return errWorkflowClosed
		}
		var ai *ActivityInfo
		if ref.activityID != "" {
			ai = t.s.activityByID(ref.activityID)
		} else {
			ai = t.s.Activities[ref.scheduleID]
			if ai != nil && int64(ai.Attempt) != ref.attempt {
				ai = nil
			}
		}
		if ai == nil || !ai.Started {
			return errActivityNotFound
		}
		return fn(t, ai)
	})
	return err
}

func (e *Engine) heartbeat(ctx context.Context, ref activityRef, details []byte) (*api.RecordActivityTaskHeartbeatResponse, error) {
	resp := &api.RecordActivityTaskHeartbeatResponse{}
	err := e.updateActivity(ctx, ref, func(t *tx, ai *ActivityInfo) error {
		ai.LastHeartbeat = t.now
		ai.HeartbeatDetails = details
		t.changed = true
		resp.CancelRequested = ai.CancelRequested
		return nil
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (e *Engine) RecordActivityTaskHeartbeat(ctx context.Context, req *api.RecordActivityTaskHeartbeatRequest) (*api.RecordActivityTaskHeartbeatResponse, error) {
	ref, err := refFromToken(req.TaskToken)
	if err != nil {
		return nil, err
	}
	return e.heartbeat(ctx, ref, req.Details)
}

func (e *Engine) RecordActivityTaskHeartbeatByID(ctx context.Context, req *api.RecordActivityTaskHeartbeatByIDRequest) (*api.RecordActivityTaskHeartbeatResponse, error) {
	ref, err := e.refFromID(ctx, req.Domain, req.WorkflowID, req.RunID, req.ActivityID)
	if err != nil {
		return nil, err
	}
	return e.heartbeat(ctx, ref, req.Details)
}

func (e *Engine) completeActivity(ctx context.Context, ref activityRef, result []byte, identity string) error {
	return e.updateActivity(ctx, ref, func(t *tx, ai *ActivityInfo) error {
		t.closeActivity(ai, func(startedID int64) *api.HistoryEvent {
			ev := t.event(api.EventTypeActivityTaskCompleted)
			ev.ActivityTaskCompletedEventAttributes = &api.ActivityTaskCompletedEventAttributes{
				Result: result, ScheduledEventID: ai.ScheduleID, StartedEventID: startedID, Identity: identity,
			}
			return ev
		})
		return nil
	})
}

func (e *Engine) RespondActivityTaskCompleted(ctx context.Context, req *api.RespondActivityTaskCompletedRequest) error {
	ref, err := refFromToken(req.TaskToken)
	if err != nil {
		return err
	}
	return e.completeActivity(ctx, ref, req.Result, req.Identity)
}

func (e *Engine) RespondActivityTaskCompletedByID(ctx context.Context, req *api.RespondActivityTaskCompletedByIDRequest) error {
	ref, err := e.refFromID(ctx, req.Domain, req.WorkflowID, req.RunID, req.ActivityID)
	if err != nil {
		return err
	}
	return e.completeActivity(ctx, ref, req.Result, req.Identity)
}

func (e *Engine) failActivity(ctx context.Context, ref activityRef, reason string, details []byte, identity string, opts *api.FailureOptions) error {
	return e.updateActivity(ctx, ref, func(t *tx, ai *ActivityInfo) error {
		retry := opts == nil || opts.FailureCategory != api.FailureCategoryFatal
		var delay int64
		if opts != nil && opts.NextRetryIntervalSeconds > 0 {
			delay = int64(opts.NextRetryIntervalSeconds) * int64(time.Second)
		}
		if retry && t.retryActivityAfter(ai, reason, details, delay) {
			return nil
		}
		t.closeActivity(ai, func(startedID int64) *api.HistoryEvent {
			ev := t.event(api.EventTypeActivityTaskFailed)
			ev.ActivityTaskFailedEventAttributes = &api.ActivityTaskFailedEventAttributes{
				Reason: reason, Details: details, ScheduledEventID: ai.ScheduleID, StartedEventID: startedID, Identity: identity,
			}
			return ev
		})
		return nil
	})
}

func (e *Engine) RespondActivityTaskFailed(ctx context.Context, req *api.RespondActivityTaskFailedRequest) error {
	ref, err := refFromToken(req.TaskToken)
	if err != nil {
		return err
	}
	return e.failActivity(ctx, ref, req.Reason, req.Details, req.Identity, req.FailureOptions)
}

func (e *Engine) RespondActivityTaskFailedByID(ctx context.Context, req *api.RespondActivityTaskFailedByIDRequest) error {
	ref, err := e.refFromID(ctx, req.Domain, req.WorkflowID, req.RunID, req.ActivityID)
	if err != nil {
		return err
	}
	return e.failActivity(ctx, ref, req.Reason, req.Details, req.Identity, req.FailureOptions)
}

func (e *Engine) cancelActivity(ctx context.Context, ref activityRef, details []byte, identity string) error {
	return e.updateActivity(ctx, ref, func(t *tx, ai *ActivityInfo) error {
		t.closeActivity(ai, func(startedID int64) *api.HistoryEvent {
			ev := t.event(api.EventTypeActivityTaskCanceled)
			ev.ActivityTaskCanceledEventAttributes = &api.ActivityTaskCanceledEventAttributes{
				Details:                      details,
				LatestCancelRequestedEventID: ai.CancelRequestEventID,
				ScheduledEventID:             ai.ScheduleID,
				StartedEventID:               startedID,
				Identity:                     identity,
			}
			return ev
		})
		return nil
	})
}

func (e *Engine) RespondActivityTaskCanceled(ctx context.Context, req *api.RespondActivityTaskCanceledRequest) error {
	ref, err := refFromToken(req.TaskToken)
	if err != nil {
		return err
	}
	return e.cancelActivity(ctx, ref, req.Details, req.Identity)
}

func (e *Engine) RespondActivityTaskCanceledByID(ctx context.Context, req *api.RespondActivityTaskCanceledByIDRequest) error {
	ref, err := e.refFromID(ctx, req.Domain, req.WorkflowID, req.RunID, req.ActivityID)
	if err != nil {
		return err
	}
	return e.cancelActivity(ctx, ref, req.Details, req.Identity)
}

// retryActivity schedules another attempt if the retry policy allows it.
func (t *tx) retryActivity(ai *ActivityInfo, reason string, details []byte) bool {
	return t.retryActivityAfter(ai, reason, details, 0)
}

// retryActivityAfter schedules the next attempt of an activity if its retry
// policy allows one. A delay above zero replaces the policy's backoff; the
// policy's expiration still applies.
func (t *tx) retryActivityAfter(ai *ActivityInfo, reason string, details []byte, delay int64) bool {
	if ai.RetryPolicy == nil {
		return false
	}
	expiration := ai.ExpirationTime
	if delay > 0 {
		if expiration > 0 && t.now+delay > expiration {
			return false
		}
		expiration = 0
	}
	backoff, ok := nextBackoff(ai.RetryPolicy, ai.Attempt, reason, t.now, expiration)
	if !ok {
		return false
	}
	if delay > 0 {
		backoff = delay
	}
	ai.Attempt++
	ai.Started = false
	ai.StartedTime = 0
	ai.StartedIdentity = ""
	ai.StartedRequestID = ""
	ai.LastHeartbeat = 0
	ai.LastFailureReason = reason
	ai.LastFailureDetails = details
	ai.AttemptScheduledTime = t.now + backoff
	t.changed = true
	if backoff <= 0 {
		t.dispatchActivity(ai)
	} else {
		t.timer(TimerActivityRetry, ai.ScheduleID, int64(ai.Attempt), "", ai.AttemptScheduledTime,
			fmt.Sprintf("ar-%d-%d", ai.ScheduleID, ai.Attempt))
	}
	return true
}

// timeoutReason is the failure reason retry policies see for timeouts.
func timeoutReason(tt api.TimeoutType) string { return "cadenceInternal:Timeout " + string(tt) }

func (t *tx) activityTimeout(tt *TimerTask) {
	ai := t.s.Activities[tt.EventID]
	if ai == nil || int64(ai.Attempt) != tt.Attempt {
		return
	}
	switch tt.TimeoutType {
	case api.TimeoutTypeScheduleToStart:
		if ai.Started {
			return
		}
	case api.TimeoutTypeStartToClose:
		if !ai.Started {
			return
		}
	case api.TimeoutTypeHeartbeat:
		if !ai.Started {
			return
		}
		deadline := max(ai.StartedTime, ai.LastHeartbeat) + seconds(ai.Heartbeat)
		if deadline > t.now {
			t.heartbeatTimer(ai, deadline)
			return
		}
	}
	if tt.TimeoutType == api.TimeoutTypeStartToClose || tt.TimeoutType == api.TimeoutTypeHeartbeat {
		if t.retryActivity(ai, timeoutReason(tt.TimeoutType), ai.HeartbeatDetails) {
			return
		}
	}
	t.closeActivity(ai, func(startedID int64) *api.HistoryEvent {
		ev := t.event(api.EventTypeActivityTaskTimedOut)
		ev.ActivityTaskTimedOutEventAttributes = &api.ActivityTaskTimedOutEventAttributes{
			Details:            ai.HeartbeatDetails,
			ScheduledEventID:   ai.ScheduleID,
			StartedEventID:     startedID,
			TimeoutType:        tt.TimeoutType,
			LastFailureReason:  ai.LastFailureReason,
			LastFailureDetails: ai.LastFailureDetails,
		}
		return ev
	})
}
