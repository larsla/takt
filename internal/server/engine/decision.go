package engine

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/larsla/takt/api"
)

var errDecisionNotFound = &api.EntityNotExistsError{Message: "decision task not found"}

// RecordDecisionTaskStarted turns a task stub into a running decision task.
func (e *Engine) RecordDecisionTaskStarted(ctx context.Context, req *api.RecordDecisionTaskStartedRequest) (*api.PollForDecisionTaskResponse, error) {
	stub := req.Stub
	s, err := e.update(ctx, stub.Domain, stub.WorkflowID, stub.RunID, func(t *tx) error {
		d := t.s.Decision
		if !t.s.Running() || d == nil || d.ScheduleID != stub.ScheduleID || d.Attempt != stub.Attempt {
			return errDecisionNotFound
		}
		if d.StartedID != 0 {
			if d.RequestID == req.RequestID {
				return nil
			}
			return &api.EntityNotExistsError{Message: "decision task already started"}
		}
		ev := t.event(api.EventTypeDecisionTaskStarted)
		ev.DecisionTaskStartedEventAttributes = &api.DecisionTaskStartedEventAttributes{
			ScheduledEventID: d.ScheduleID,
			Identity:         req.Identity,
			RequestID:        req.RequestID,
		}
		t.add(ev)
		d.StartedID = ev.EventID
		d.StartedTime = t.now
		d.RequestID = req.RequestID
		t.timer(TimerDecisionTimeout, d.ScheduleID, d.Attempt, api.TimeoutTypeStartToClose,
			t.now+seconds(d.Timeout), fmt.Sprintf("d-%d-%d", d.ScheduleID, d.Attempt))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return e.decisionTask(ctx, s)
}

func (e *Engine) decisionTask(ctx context.Context, s *State) (*api.PollForDecisionTaskResponse, error) {
	d := s.Decision
	events, token, err := e.readHistory(ctx, s, 0, e.pageSize)
	if err != nil {
		return nil, err
	}
	return &api.PollForDecisionTaskResponse{
		TaskToken: api.TaskToken{
			Domain:     s.Domain,
			WorkflowID: s.WorkflowID,
			RunID:      s.RunID,
			ScheduleID: d.ScheduleID,
			Attempt:    d.Attempt,
		}.Encode(),
		WorkflowExecution:         s.Execution(),
		WorkflowType:              s.Start.WorkflowType,
		PreviousStartedEventID:    s.LastDecisionStartedID,
		StartedEventID:            d.StartedID,
		Attempt:                   d.Attempt,
		History:                   &api.History{Events: events},
		NextPageToken:             token,
		WorkflowExecutionTaskList: &api.TaskList{Name: s.TaskList()},
		ScheduledTimestamp:        d.ScheduledTime,
		StartedTimestamp:          d.StartedTime,
	}, nil
}

// runningDecision checks that tok names the decision task in flight.
func runningDecision(s *State, tok api.TaskToken) (*DecisionInfo, error) {
	d := s.Decision
	if !s.Running() || d == nil || d.ScheduleID != tok.ScheduleID || d.Attempt != tok.Attempt || d.StartedID == 0 {
		return nil, errDecisionNotFound
	}
	return d, nil
}

func (e *Engine) RespondDecisionTaskCompleted(ctx context.Context, req *api.RespondDecisionTaskCompletedRequest) (*api.RespondDecisionTaskCompletedResponse, error) {
	tok, err := api.DecodeTaskToken(req.TaskToken)
	if err != nil {
		return nil, err
	}
	_, err = e.update(ctx, tok.Domain, tok.WorkflowID, tok.RunID, func(t *tx) error {
		d, err := runningDecision(t.s, tok)
		if err != nil {
			return err
		}
		if hasCloseDecision(req.Decisions) && len(t.s.Buffered) > 0 {
			// New events arrived that the workflow hasn't seen yet.
			t.failDecision(d, api.DecisionTaskFailedCauseUnhandledDecision, nil, req.Identity)
			return nil
		}
		if cause, msg := t.validateDecisions(req.Decisions); cause != "" {
			t.failDecision(d, cause, []byte(msg), req.Identity)
			return nil
		}
		t.completeDecision(d, req)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &api.RespondDecisionTaskCompletedResponse{}, nil
}

func (t *tx) completeDecision(d *DecisionInfo, req *api.RespondDecisionTaskCompletedRequest) {
	s := t.s
	ev := t.event(api.EventTypeDecisionTaskCompleted)
	ev.DecisionTaskCompletedEventAttributes = &api.DecisionTaskCompletedEventAttributes{
		ExecutionContext: req.ExecutionContext,
		ScheduledEventID: d.ScheduleID,
		StartedEventID:   d.StartedID,
		Identity:         req.Identity,
		BinaryChecksum:   req.BinaryChecksum,
	}
	t.add(ev)
	s.Decision = nil
	s.DecisionAttempt = 0
	s.LastDecisionStartedID = d.StartedID

	need := req.ForceCreateNewDecisionTask
	for _, dec := range req.Decisions {
		if !s.Running() {
			break
		}
		if t.applyDecision(dec, ev.EventID, req.Identity) {
			need = true
		}
	}
	if !s.Running() {
		return
	}
	if t.flushBuffered() > 0 {
		need = true
	}
	if need {
		t.scheduleDecision()
	}
}

func (e *Engine) RespondDecisionTaskFailed(ctx context.Context, req *api.RespondDecisionTaskFailedRequest) error {
	tok, err := api.DecodeTaskToken(req.TaskToken)
	if err != nil {
		return err
	}
	_, err = e.update(ctx, tok.Domain, tok.WorkflowID, tok.RunID, func(t *tx) error {
		d, err := runningDecision(t.s, tok)
		if err != nil {
			return err
		}
		cause := req.Cause
		if cause == "" {
			cause = api.DecisionTaskFailedCauseWorkflowWorkerUnhandledFailure
		}
		t.failDecision(d, cause, req.Details, req.Identity)
		return nil
	})
	return err
}

func (t *tx) failDecision(d *DecisionInfo, cause api.DecisionTaskFailedCause, details []byte, identity string) {
	ev := t.event(api.EventTypeDecisionTaskFailed)
	ev.DecisionTaskFailedEventAttributes = &api.DecisionTaskFailedEventAttributes{
		ScheduledEventID: d.ScheduleID,
		StartedEventID:   d.StartedID,
		Cause:            cause,
		Details:          details,
		Identity:         identity,
	}
	t.add(ev)
	t.s.Decision = nil
	t.flushBuffered()
	t.retryDecision()
}

func (t *tx) timeoutDecision(d *DecisionInfo) {
	ev := t.event(api.EventTypeDecisionTaskTimedOut)
	ev.DecisionTaskTimedOutEventAttributes = &api.DecisionTaskTimedOutEventAttributes{
		ScheduledEventID: d.ScheduleID,
		StartedEventID:   d.StartedID,
		TimeoutType:      api.TimeoutTypeStartToClose,
	}
	t.add(ev)
	t.s.Decision = nil
	t.flushBuffered()
	t.retryDecision()
}

func isCloseDecision(dt api.DecisionType) bool {
	switch dt {
	case api.DecisionTypeCompleteWorkflowExecution, api.DecisionTypeFailWorkflowExecution,
		api.DecisionTypeCancelWorkflowExecution, api.DecisionTypeContinueAsNewWorkflowExecution:
		return true
	}
	return false
}

func hasCloseDecision(ds []*api.Decision) bool {
	for _, d := range ds {
		if d != nil && isCloseDecision(d.DecisionType) {
			return true
		}
	}
	return false
}

// validateDecisions checks decisions against each other and the state. It
// returns the failure cause and a message, or "" if all are valid.
func (t *tx) validateDecisions(ds []*api.Decision) (api.DecisionTaskFailedCause, string) {
	s := t.s
	activityIDs := map[string]bool{}
	for _, ai := range s.Activities {
		activityIDs[ai.ActivityID] = true
	}
	timerIDs := map[string]bool{}
	for id := range s.Timers {
		timerIDs[id] = true
	}
	for i, d := range ds {
		if d == nil {
			return api.DecisionTaskFailedCauseUnhandledDecision, fmt.Sprintf("decision %d is nil", i)
		}
		switch d.DecisionType {
		case api.DecisionTypeScheduleActivityTask:
			a := d.ScheduleActivityTaskDecisionAttributes
			c := api.DecisionTaskFailedCauseBadScheduleActivityAttributes
			if a == nil || a.ActivityID == "" || a.ActivityType == nil || a.ActivityType.Name == "" {
				return c, "activity ID and type are required"
			}
			if activityIDs[a.ActivityID] {
				return c, "duplicate activity ID " + a.ActivityID
			}
			if a.Domain != "" && a.Domain != s.Domain {
				return c, "activities must run in the workflow's domain"
			}
			if _, err := activityTimeouts(a, s.Start.ExecutionStartToCloseTimeoutSeconds); err != nil {
				return c, err.Error()
			}
			activityIDs[a.ActivityID] = true
		case api.DecisionTypeRequestCancelActivityTask:
			if a := d.RequestCancelActivityTaskDecisionAttributes; a == nil || a.ActivityID == "" {
				return api.DecisionTaskFailedCauseBadRequestCancelActivityAttributes, "activity ID is required"
			}
		case api.DecisionTypeStartTimer:
			a := d.StartTimerDecisionAttributes
			if a == nil || a.TimerID == "" || a.StartToFireTimeoutSeconds <= 0 {
				return api.DecisionTaskFailedCauseBadStartTimerAttributes, "timer ID and a positive timeout are required"
			}
			if timerIDs[a.TimerID] {
				return api.DecisionTaskFailedCauseStartTimerDuplicateID, "duplicate timer ID " + a.TimerID
			}
			timerIDs[a.TimerID] = true
		case api.DecisionTypeCancelTimer:
			if a := d.CancelTimerDecisionAttributes; a == nil || a.TimerID == "" {
				return api.DecisionTaskFailedCauseBadCancelTimerAttributes, "timer ID is required"
			}
		case api.DecisionTypeCompleteWorkflowExecution:
			if d.CompleteWorkflowExecutionDecisionAttributes == nil {
				return api.DecisionTaskFailedCauseBadCompleteWorkflowAttributes, "attributes are required"
			}
		case api.DecisionTypeFailWorkflowExecution:
			if d.FailWorkflowExecutionDecisionAttributes == nil {
				return api.DecisionTaskFailedCauseBadFailWorkflowAttributes, "attributes are required"
			}
		case api.DecisionTypeCancelWorkflowExecution:
			if d.CancelWorkflowExecutionDecisionAttributes == nil {
				return api.DecisionTaskFailedCauseBadCancelWorkflowAttributes, "attributes are required"
			}
		case api.DecisionTypeContinueAsNewWorkflowExecution:
			a := d.ContinueAsNewWorkflowExecutionDecisionAttributes
			if a == nil {
				return api.DecisionTaskFailedCauseBadContinueAsNewAttributes, "attributes are required"
			}
			if err := validateCron(a.CronSchedule); err != nil {
				return api.DecisionTaskFailedCauseBadContinueAsNewAttributes, err.Error()
			}
		case api.DecisionTypeRecordMarker:
			if a := d.RecordMarkerDecisionAttributes; a == nil || a.MarkerName == "" {
				return api.DecisionTaskFailedCauseBadRecordMarkerAttributes, "marker name is required"
			}
		case api.DecisionTypeStartChildWorkflowExecution:
			a := d.StartChildWorkflowExecutionDecisionAttributes
			if a == nil || a.WorkflowID == "" || a.WorkflowType == nil || a.WorkflowType.Name == "" {
				return api.DecisionTaskFailedCauseBadStartChildExecutionAttributes, "workflow ID and type are required"
			}
			if err := validateCron(a.CronSchedule); err != nil {
				return api.DecisionTaskFailedCauseBadStartChildExecutionAttributes, err.Error()
			}
		case api.DecisionTypeSignalExternalWorkflowExecution:
			a := d.SignalExternalWorkflowExecutionDecisionAttributes
			if a == nil || a.Execution == nil || a.Execution.WorkflowID == "" || a.SignalName == "" {
				return api.DecisionTaskFailedCauseBadSignalWorkflowExecutionAttribute, "workflow ID and signal name are required"
			}
		case api.DecisionTypeRequestCancelExternalWorkflowExecution:
			if a := d.RequestCancelExternalWorkflowExecutionDecisionAttributes; a == nil || a.WorkflowID == "" {
				return api.DecisionTaskFailedCauseBadRequestCancelExternalAttributes, "workflow ID is required"
			}
		case api.DecisionTypeUpsertWorkflowSearchAttributes:
			if a := d.UpsertWorkflowSearchAttributesDecisionAttributes; a == nil || a.SearchAttributes == nil {
				return api.DecisionTaskFailedCauseBadSearchAttributes, "search attributes are required"
			}
		default:
			return api.DecisionTaskFailedCauseUnhandledDecision, "unknown decision type " + string(d.DecisionType)
		}
	}
	return "", ""
}

// applyDecision turns one validated decision into events. It reports
// whether the workflow needs another decision task to see the result.
func (t *tx) applyDecision(d *api.Decision, completedID int64, identity string) bool {
	s := t.s
	switch d.DecisionType {
	case api.DecisionTypeScheduleActivityTask:
		t.scheduleActivity(d.ScheduleActivityTaskDecisionAttributes, completedID)

	case api.DecisionTypeRequestCancelActivityTask:
		return t.requestCancelActivity(d.RequestCancelActivityTaskDecisionAttributes.ActivityID, completedID)

	case api.DecisionTypeStartTimer:
		a := d.StartTimerDecisionAttributes
		ev := t.event(api.EventTypeTimerStarted)
		ev.TimerStartedEventAttributes = &api.TimerStartedEventAttributes{
			TimerID:                      a.TimerID,
			StartToFireTimeoutSeconds:    a.StartToFireTimeoutSeconds,
			DecisionTaskCompletedEventID: completedID,
		}
		t.add(ev)
		fire := t.now + a.StartToFireTimeoutSeconds*int64(1e9)
		if s.Timers == nil {
			s.Timers = map[string]*TimerInfo{}
		}
		s.Timers[a.TimerID] = &TimerInfo{TimerID: a.TimerID, StartedEventID: ev.EventID, FireTime: fire}
		t.timer(TimerUser, ev.EventID, 0, "", fire, fmt.Sprintf("u-%d", ev.EventID))

	case api.DecisionTypeCancelTimer:
		id := d.CancelTimerDecisionAttributes.TimerID
		ti := s.Timers[id]
		if ti == nil {
			ev := t.event(api.EventTypeCancelTimerFailed)
			ev.CancelTimerFailedEventAttributes = &api.CancelTimerFailedEventAttributes{
				TimerID: id, Cause: "TIMER_ID_UNKNOWN", DecisionTaskCompletedEventID: completedID, Identity: identity,
			}
			t.add(ev)
			return true
		}
		ev := t.event(api.EventTypeTimerCanceled)
		ev.TimerCanceledEventAttributes = &api.TimerCanceledEventAttributes{
			TimerID: id, StartedEventID: ti.StartedEventID, DecisionTaskCompletedEventID: completedID, Identity: identity,
		}
		t.add(ev)
		delete(s.Timers, id)

	case api.DecisionTypeCompleteWorkflowExecution:
		t.completeWorkflow(d.CompleteWorkflowExecutionDecisionAttributes.Result, completedID)

	case api.DecisionTypeFailWorkflowExecution:
		a := d.FailWorkflowExecutionDecisionAttributes
		t.failWorkflow(a.Reason, a.Details, completedID)

	case api.DecisionTypeCancelWorkflowExecution:
		ev := t.event(api.EventTypeWorkflowExecutionCanceled)
		ev.WorkflowExecutionCanceledEventAttributes = &api.WorkflowExecutionCanceledEventAttributes{
			DecisionTaskCompletedEventID: completedID,
			Details:                      d.CancelWorkflowExecutionDecisionAttributes.Details,
		}
		t.add(ev)
		t.closeRun(api.CloseStatusCanceled, ev.EventID)

	case api.DecisionTypeContinueAsNewWorkflowExecution:
		a := *d.ContinueAsNewWorkflowExecutionDecisionAttributes
		a.Initiator = api.ContinueAsNewInitiatorDecider
		t.continueAsNew(&a, completedID)

	case api.DecisionTypeRecordMarker:
		a := d.RecordMarkerDecisionAttributes
		ev := t.event(api.EventTypeMarkerRecorded)
		ev.MarkerRecordedEventAttributes = &api.MarkerRecordedEventAttributes{
			MarkerName: a.MarkerName, Details: a.Details, Header: a.Header, DecisionTaskCompletedEventID: completedID,
		}
		t.add(ev)

	case api.DecisionTypeStartChildWorkflowExecution:
		t.initiateChild(d.StartChildWorkflowExecutionDecisionAttributes, completedID)

	case api.DecisionTypeSignalExternalWorkflowExecution:
		a := d.SignalExternalWorkflowExecutionDecisionAttributes
		domain := a.Domain
		if domain == "" {
			domain = s.Domain
		}
		ev := t.event(api.EventTypeSignalExternalWorkflowExecutionInitiated)
		ev.SignalExternalWorkflowExecutionInitiatedEventAttributes = &api.SignalExternalWorkflowExecutionInitiatedEventAttributes{
			DecisionTaskCompletedEventID: completedID,
			Domain:                       domain,
			WorkflowExecution:            a.Execution,
			SignalName:                   a.SignalName,
			Input:                        a.Input,
			Control:                      a.Control,
			ChildWorkflowOnly:            a.ChildWorkflowOnly,
		}
		t.add(ev)
		if s.Signals == nil {
			s.Signals = map[int64]*ExternalInfo{}
		}
		s.Signals[ev.EventID] = &ExternalInfo{
			InitiatedID: ev.EventID, Domain: domain, Execution: a.Execution, SignalName: a.SignalName,
			Input: a.Input, Control: a.Control, ChildOnly: a.ChildWorkflowOnly,
		}
		t.transfer(TransferSignalExternal, ev.EventID)

	case api.DecisionTypeRequestCancelExternalWorkflowExecution:
		a := d.RequestCancelExternalWorkflowExecutionDecisionAttributes
		domain := a.Domain
		if domain == "" {
			domain = s.Domain
		}
		exec := &api.WorkflowExecution{WorkflowID: a.WorkflowID, RunID: a.RunID}
		ev := t.event(api.EventTypeRequestCancelExternalWorkflowExecutionInitiated)
		ev.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes = &api.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
			DecisionTaskCompletedEventID: completedID,
			Domain:                       domain,
			WorkflowExecution:            exec,
			Control:                      a.Control,
			ChildWorkflowOnly:            a.ChildWorkflowOnly,
		}
		t.add(ev)
		if s.CancelsReqs == nil {
			s.CancelsReqs = map[int64]*ExternalInfo{}
		}
		s.CancelsReqs[ev.EventID] = &ExternalInfo{
			InitiatedID: ev.EventID, Domain: domain, Execution: exec, Control: a.Control, ChildOnly: a.ChildWorkflowOnly,
		}
		t.transfer(TransferCancelExternal, ev.EventID)

	case api.DecisionTypeUpsertWorkflowSearchAttributes:
		sa := d.UpsertWorkflowSearchAttributesDecisionAttributes.SearchAttributes
		ev := t.event(api.EventTypeUpsertWorkflowSearchAttributes)
		ev.UpsertWorkflowSearchAttributesEventAttributes = &api.UpsertWorkflowSearchAttributesEventAttributes{
			DecisionTaskCompletedEventID: completedID, SearchAttributes: sa,
		}
		t.add(ev)
		if s.SearchAttributes == nil {
			s.SearchAttributes = &api.SearchAttributes{}
		}
		if s.SearchAttributes.IndexedFields == nil {
			s.SearchAttributes.IndexedFields = map[string][]byte{}
		}
		for k, v := range sa.IndexedFields {
			s.SearchAttributes.IndexedFields[k] = v
		}
		t.visible = true
	}
	return false
}

func (t *tx) initiateChild(a *api.StartChildWorkflowExecutionDecisionAttributes, completedID int64) {
	s := t.s
	attrs := &api.StartChildWorkflowExecutionInitiatedEventAttributes{
		Domain:                              a.Domain,
		WorkflowID:                          a.WorkflowID,
		WorkflowType:                        a.WorkflowType,
		TaskList:                            a.TaskList,
		Input:                               a.Input,
		ExecutionStartToCloseTimeoutSeconds: a.ExecutionStartToCloseTimeoutSeconds,
		TaskStartToCloseTimeoutSeconds:      a.TaskStartToCloseTimeoutSeconds,
		ParentClosePolicy:                   a.ParentClosePolicy,
		Control:                             a.Control,
		DecisionTaskCompletedEventID:        completedID,
		WorkflowIDReusePolicy:               a.WorkflowIDReusePolicy,
		RetryPolicy:                         a.RetryPolicy,
		CronSchedule:                        a.CronSchedule,
		Header:                              a.Header,
		Memo:                                a.Memo,
		SearchAttributes:                    a.SearchAttributes,
	}
	if attrs.Domain == "" {
		attrs.Domain = s.Domain
	}
	if attrs.TaskList == nil || attrs.TaskList.Name == "" {
		attrs.TaskList = &api.TaskList{Name: s.TaskList()}
	}
	if attrs.ExecutionStartToCloseTimeoutSeconds <= 0 {
		attrs.ExecutionStartToCloseTimeoutSeconds = s.Start.ExecutionStartToCloseTimeoutSeconds
	}
	if attrs.TaskStartToCloseTimeoutSeconds <= 0 {
		attrs.TaskStartToCloseTimeoutSeconds = s.Start.TaskStartToCloseTimeoutSeconds
	}
	if attrs.ParentClosePolicy == "" {
		attrs.ParentClosePolicy = api.ParentClosePolicyTerminate
	}
	ev := t.event(api.EventTypeStartChildWorkflowExecutionInitiated)
	ev.StartChildWorkflowExecutionInitiatedEventAttributes = attrs
	t.add(ev)
	if s.Children == nil {
		s.Children = map[int64]*ChildInfo{}
	}
	s.Children[ev.EventID] = &ChildInfo{
		InitiatedID:       ev.EventID,
		Domain:            attrs.Domain,
		WorkflowID:        attrs.WorkflowID,
		WorkflowType:      attrs.WorkflowType.Name,
		ParentClosePolicy: attrs.ParentClosePolicy,
		Attrs:             attrs,
	}
	t.transfer(TransferStartChild, ev.EventID)
}

// completeWorkflow closes the run as completed, or continues it if it has a
// cron schedule.
func (t *tx) completeWorkflow(result []byte, completedID int64) {
	if cron := t.s.Start.CronSchedule; cron != "" {
		t.continueAsNew(&api.ContinueAsNewWorkflowExecutionDecisionAttributes{
			Initiator:                     api.ContinueAsNewInitiatorCronSchedule,
			BackoffStartIntervalInSeconds: cronBackoff(cron, t.e.now()),
			LastCompletionResult:          result,
			CronSchedule:                  cron,
		}, completedID)
		return
	}
	ev := t.event(api.EventTypeWorkflowExecutionCompleted)
	ev.WorkflowExecutionCompletedEventAttributes = &api.WorkflowExecutionCompletedEventAttributes{
		Result: result, DecisionTaskCompletedEventID: completedID,
	}
	t.add(ev)
	t.closeRun(api.CloseStatusCompleted, ev.EventID)
}

// failWorkflow closes the run as failed unless its retry policy or cron
// schedule starts another run.
func (t *tx) failWorkflow(reason string, details []byte, completedID int64) {
	if t.retryOrCron(reason, details, completedID) {
		return
	}
	ev := t.event(api.EventTypeWorkflowExecutionFailed)
	ev.WorkflowExecutionFailedEventAttributes = &api.WorkflowExecutionFailedEventAttributes{
		Reason: reason, Details: details, DecisionTaskCompletedEventID: completedID,
	}
	t.add(ev)
	t.closeRun(api.CloseStatusFailed, ev.EventID)
}

// retryOrCron continues a failed or timed out run per its retry policy, or
// on its cron schedule. It reports whether it did.
func (t *tx) retryOrCron(reason string, details []byte, completedID int64) bool {
	start := t.s.Start
	if p := start.RetryPolicy; p != nil {
		if backoff, ok := nextBackoff(p, start.Attempt, reason, t.now, start.ExpirationTimestamp); ok {
			t.continueAsNew(&api.ContinueAsNewWorkflowExecutionDecisionAttributes{
				Initiator:                     api.ContinueAsNewInitiatorRetryPolicy,
				BackoffStartIntervalInSeconds: ceilSeconds(backoff),
				FailureReason:                 reason,
				FailureDetails:                details,
				LastCompletionResult:          start.LastCompletionResult,
				CronSchedule:                  start.CronSchedule,
			}, completedID)
			return true
		}
	}
	if cron := start.CronSchedule; cron != "" {
		t.continueAsNew(&api.ContinueAsNewWorkflowExecutionDecisionAttributes{
			Initiator:                     api.ContinueAsNewInitiatorCronSchedule,
			BackoffStartIntervalInSeconds: cronBackoff(cron, t.e.now()),
			FailureReason:                 reason,
			FailureDetails:                details,
			LastCompletionResult:          start.LastCompletionResult,
			CronSchedule:                  cron,
		}, completedID)
		return true
	}
	return false
}

// continueAsNew closes the run and records what the next run starts with.
// Missing fields default to the current run's values.
func (t *tx) continueAsNew(a *api.ContinueAsNewWorkflowExecutionDecisionAttributes, completedID int64) {
	s := t.s
	start := s.Start
	if a.WorkflowType == nil || a.WorkflowType.Name == "" {
		a.WorkflowType = start.WorkflowType
	}
	if a.TaskList == nil || a.TaskList.Name == "" {
		a.TaskList = start.TaskList
	}
	if a.Input == nil && a.Initiator != api.ContinueAsNewInitiatorDecider {
		a.Input = start.Input
	}
	if a.ExecutionStartToCloseTimeoutSeconds <= 0 {
		a.ExecutionStartToCloseTimeoutSeconds = start.ExecutionStartToCloseTimeoutSeconds
	}
	if a.TaskStartToCloseTimeoutSeconds <= 0 {
		a.TaskStartToCloseTimeoutSeconds = start.TaskStartToCloseTimeoutSeconds
	}
	if a.Header == nil {
		a.Header = start.Header
	}
	if a.Memo == nil {
		a.Memo = start.Memo
	}
	if a.SearchAttributes == nil {
		a.SearchAttributes = s.SearchAttributes
	}
	if a.RetryPolicy == nil {
		a.RetryPolicy = start.RetryPolicy
	}
	if a.CronSchedule == "" && a.Initiator != api.ContinueAsNewInitiatorDecider {
		a.CronSchedule = start.CronSchedule
	}
	newRunID := uuid.NewString()
	ev := t.event(api.EventTypeWorkflowExecutionContinuedAsNew)
	ev.WorkflowExecutionContinuedAsNewEventAttributes = &api.WorkflowExecutionContinuedAsNewEventAttributes{
		NewExecutionRunID:                   newRunID,
		WorkflowType:                        a.WorkflowType,
		TaskList:                            a.TaskList,
		Input:                               a.Input,
		ExecutionStartToCloseTimeoutSeconds: a.ExecutionStartToCloseTimeoutSeconds,
		TaskStartToCloseTimeoutSeconds:      a.TaskStartToCloseTimeoutSeconds,
		DecisionTaskCompletedEventID:        completedID,
		BackoffStartIntervalInSeconds:       a.BackoffStartIntervalInSeconds,
		Initiator:                           a.Initiator,
		FailureReason:                       a.FailureReason,
		FailureDetails:                      a.FailureDetails,
		LastCompletionResult:                a.LastCompletionResult,
		Header:                              a.Header,
		Memo:                                a.Memo,
		SearchAttributes:                    a.SearchAttributes,
	}
	t.add(ev)
	s.NewRunID = newRunID
	s.Continue = a
	t.closeRun(api.CloseStatusContinuedAsNew, ev.EventID)
}
