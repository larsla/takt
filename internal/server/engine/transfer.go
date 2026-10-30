package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/larsla/takt/api"
)

// HandleTransfer processes one cross-workflow transfer message. A returned
// error means the message should be redelivered.
func (e *Engine) HandleTransfer(ctx context.Context, data []byte) error {
	var tr Transfer
	if err := json.Unmarshal(data, &tr); err != nil {
		e.log.Warn("dropping malformed transfer", "err", err)
		return nil
	}
	start := time.Now()
	err := e.handleTransfer(ctx, &tr)
	e.queueTask(transferOperations[tr.Kind], tr.Domain, start, err)
	return err
}

func (e *Engine) handleTransfer(ctx context.Context, tr *Transfer) error {
	s, _, err := e.load(ctx, runKey(tr.Domain, tr.WorkflowID, tr.RunID))
	var nf *api.EntityNotExistsError
	if errors.As(err, &nf) {
		return nil
	}
	if err != nil {
		return err
	}
	switch tr.Kind {
	case TransferStartChild:
		return e.startChild(ctx, s, tr.InitiatedID)
	case TransferSignalExternal:
		return e.signalExternal(ctx, s, tr.InitiatedID)
	case TransferCancelExternal:
		return e.cancelExternal(ctx, s, tr.InitiatedID)
	case TransferClosed:
		return e.processClosed(ctx, s)
	}
	e.log.Warn("unknown transfer kind", "kind", tr.Kind)
	return nil
}

func (e *Engine) startChild(ctx context.Context, parent *State, initiatedID int64) error {
	ci := parent.Children[initiatedID]
	if !parent.Running() || ci == nil || ci.StartedID != 0 {
		return nil
	}
	a := ci.Attrs
	attrs := e.startAttrs(a.WorkflowType, a.TaskList, a.Input, a.ExecutionStartToCloseTimeoutSeconds,
		a.TaskStartToCloseTimeoutSeconds, "", "", a.RetryPolicy, a.CronSchedule, a.Memo, a.SearchAttributes, a.Header, startTiming{})
	attrs.ParentWorkflowDomain = parent.Domain
	attrs.ParentWorkflowExecution = parent.Execution()
	attrs.ParentInitiatedEventID = initiatedID
	requestID := fmt.Sprintf("%s-%d", parent.RunID, initiatedID)
	attrs.RequestID = requestID

	var runID string
	_, _, err := e.domain(ctx, ci.Domain)
	if err == nil {
		runID, err = e.startWorkflow(ctx, startParams{
			domain:     ci.Domain,
			workflowID: ci.WorkflowID,
			requestID:  requestID,
			policy:     a.WorkflowIDReusePolicy,
			attrs:      attrs,
		})
	}
	var (
		already *api.WorkflowExecutionAlreadyStartedError
		nf      *api.EntityNotExistsError
	)
	failed := errors.As(err, &already) || errors.As(err, &nf)
	if err != nil && !failed {
		return err
	}
	_, err = e.update(ctx, parent.Domain, parent.WorkflowID, parent.RunID, func(t *tx) error {
		ci := t.s.Children[initiatedID]
		if !t.s.Running() || ci == nil || ci.StartedID != 0 {
			return nil
		}
		if failed {
			ev := t.event(api.EventTypeStartChildWorkflowExecutionFailed)
			ev.StartChildWorkflowExecutionFailedEventAttributes = &api.StartChildWorkflowExecutionFailedEventAttributes{
				Domain:           ci.Domain,
				WorkflowID:       ci.WorkflowID,
				WorkflowType:     &api.WorkflowType{Name: ci.WorkflowType},
				Cause:            api.ChildWorkflowExecutionFailedCauseWorkflowAlreadyRunning,
				Control:          ci.Attrs.Control,
				InitiatedEventID: initiatedID,
			}
			delete(t.s.Children, initiatedID)
			t.addExternal(ev)
			return nil
		}
		t.childStarted(ci, runID)
		return nil
	})
	return ignoreClosed(err)
}

func (t *tx) childStarted(ci *ChildInfo, runID string) {
	ci.RunID = runID
	ev := t.event(api.EventTypeChildWorkflowExecutionStarted)
	ev.ChildWorkflowExecutionStartedEventAttributes = &api.ChildWorkflowExecutionStartedEventAttributes{
		Domain:            ci.Domain,
		InitiatedEventID:  ci.InitiatedID,
		WorkflowExecution: &api.WorkflowExecution{WorkflowID: ci.WorkflowID, RunID: runID},
		WorkflowType:      &api.WorkflowType{Name: ci.WorkflowType},
		Header:            ci.Attrs.Header,
	}
	if t.addExternal(ev) {
		ci.StartedID = bufferedStartedID
	} else {
		ci.StartedID = ev.EventID
	}
}

func ignoreClosed(err error) error {
	var nf *api.EntityNotExistsError
	if errors.As(err, &nf) {
		return nil
	}
	return err
}

func (e *Engine) signalExternal(ctx context.Context, src *State, initiatedID int64) error {
	si := src.Signals[initiatedID]
	if !src.Running() || si == nil {
		return nil
	}
	target := *si.Execution
	err := e.checkChildOnly(ctx, src, si)
	if err == nil {
		err = e.signal(ctx, si.Domain, &target, &api.WorkflowExecutionSignaledEventAttributes{
			SignalName: si.SignalName,
			Input:      si.Input,
			Identity:   "workflow " + src.WorkflowID,
		}, fmt.Sprintf("%s-%d", src.RunID, initiatedID))
	}
	var nf *api.EntityNotExistsError
	unknown := errors.As(err, &nf)
	if err != nil && !unknown {
		return err
	}
	_, err = e.update(ctx, src.Domain, src.WorkflowID, src.RunID, func(t *tx) error {
		si := t.s.Signals[initiatedID]
		if !t.s.Running() || si == nil {
			return nil
		}
		delete(t.s.Signals, initiatedID)
		if unknown {
			ev := t.event(api.EventTypeSignalExternalWorkflowExecutionFailed)
			ev.SignalExternalWorkflowExecutionFailedEventAttributes = &api.SignalExternalWorkflowExecutionFailedEventAttributes{
				Cause:             api.ExternalFailedCauseUnknownExternalWorkflowExecution,
				Domain:            si.Domain,
				WorkflowExecution: si.Execution,
				InitiatedEventID:  initiatedID,
				Control:           si.Control,
			}
			t.addExternal(ev)
			return nil
		}
		ev := t.event(api.EventTypeExternalWorkflowExecutionSignaled)
		ev.ExternalWorkflowExecutionSignaledEventAttributes = &api.ExternalWorkflowExecutionSignaledEventAttributes{
			InitiatedEventID:  initiatedID,
			Domain:            si.Domain,
			WorkflowExecution: si.Execution,
			Control:           si.Control,
		}
		t.addExternal(ev)
		return nil
	})
	return ignoreClosed(err)
}

// checkChildOnly enforces ChildWorkflowOnly: the target must be a child of src.
func (e *Engine) checkChildOnly(ctx context.Context, src *State, x *ExternalInfo) error {
	if !x.ChildOnly {
		return nil
	}
	target, err := e.loadRun(ctx, x.Domain, x.Execution)
	if err != nil {
		return err
	}
	p := target.Start.ParentWorkflowExecution
	if p == nil || p.WorkflowID != src.WorkflowID || p.RunID != src.RunID {
		return notFound("workflow execution is not a child of the requester")
	}
	return nil
}

func (e *Engine) cancelExternal(ctx context.Context, src *State, initiatedID int64) error {
	ci := src.CancelsReqs[initiatedID]
	if !src.Running() || ci == nil {
		return nil
	}
	target := *ci.Execution
	err := e.checkChildOnly(ctx, src, ci)
	if err == nil {
		err = e.requestCancel(ctx, ci.Domain, &target, &api.WorkflowExecutionCancelRequestedEventAttributes{
			ExternalInitiatedEventID:  initiatedID,
			ExternalWorkflowExecution: src.Execution(),
			Identity:                  "workflow " + src.WorkflowID,
		}, fmt.Sprintf("%s-%d", src.RunID, initiatedID))
	}
	var (
		nf     *api.EntityNotExistsError
		repeat *api.CancellationAlreadyRequestedError
	)
	unknown := errors.As(err, &nf)
	if err != nil && !unknown && !errors.As(err, &repeat) {
		return err
	}
	_, err = e.update(ctx, src.Domain, src.WorkflowID, src.RunID, func(t *tx) error {
		ci := t.s.CancelsReqs[initiatedID]
		if !t.s.Running() || ci == nil {
			return nil
		}
		delete(t.s.CancelsReqs, initiatedID)
		if unknown {
			ev := t.event(api.EventTypeRequestCancelExternalWorkflowExecutionFailed)
			ev.RequestCancelExternalWorkflowExecutionFailedEventAttributes = &api.RequestCancelExternalWorkflowExecutionFailedEventAttributes{
				Cause:             api.ExternalFailedCauseUnknownExternalWorkflowExecution,
				Domain:            ci.Domain,
				WorkflowExecution: ci.Execution,
				InitiatedEventID:  initiatedID,
				Control:           ci.Control,
			}
			t.addExternal(ev)
			return nil
		}
		ev := t.event(api.EventTypeExternalWorkflowExecutionCancelRequested)
		ev.ExternalWorkflowExecutionCancelRequestedEventAttributes = &api.ExternalWorkflowExecutionCancelRequestedEventAttributes{
			InitiatedEventID:  initiatedID,
			Domain:            ci.Domain,
			WorkflowExecution: ci.Execution,
		}
		t.addExternal(ev)
		return nil
	})
	return ignoreClosed(err)
}

// processClosed runs after a run closes: it starts the continuation run,
// tells the parent, and applies the parent close policy to children.
func (e *Engine) processClosed(ctx context.Context, s *State) error {
	if s.NewRunID != "" {
		if err := e.startContinuation(ctx, s); err != nil {
			return err
		}
	} else if p := s.Start.ParentWorkflowExecution; p != nil {
		if err := e.notifyParent(ctx, s); err != nil {
			return err
		}
	}
	for _, ci := range s.Children {
		if err := e.applyParentClosePolicy(ctx, s, ci); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) startContinuation(ctx context.Context, s *State) error {
	c := s.Continue
	prev := s.Start
	attrs := &api.WorkflowExecutionStartedEventAttributes{
		WorkflowType:                        c.WorkflowType,
		ParentWorkflowDomain:                prev.ParentWorkflowDomain,
		ParentWorkflowExecution:             prev.ParentWorkflowExecution,
		ParentInitiatedEventID:              prev.ParentInitiatedEventID,
		TaskList:                            c.TaskList,
		Input:                               c.Input,
		ExecutionStartToCloseTimeoutSeconds: c.ExecutionStartToCloseTimeoutSeconds,
		TaskStartToCloseTimeoutSeconds:      c.TaskStartToCloseTimeoutSeconds,
		ContinuedExecutionRunID:             s.RunID,
		Initiator:                           c.Initiator,
		ContinuedFailureReason:              c.FailureReason,
		ContinuedFailureDetails:             c.FailureDetails,
		LastCompletionResult:                c.LastCompletionResult,
		OriginalExecutionRunID:              prev.OriginalExecutionRunID,
		Identity:                            prev.Identity,
		FirstExecutionRunID:                 prev.FirstExecutionRunID,
		RetryPolicy:                         c.RetryPolicy,
		CronSchedule:                        c.CronSchedule,
		CronOverlapPolicy:                   c.CronOverlapPolicy,
		JitterStartSeconds:                  c.JitterStartSeconds,
		FirstDecisionTaskBackoffSeconds:     c.BackoffStartIntervalInSeconds,
		Memo:                                c.Memo,
		SearchAttributes:                    c.SearchAttributes,
		Header:                              c.Header,
	}
	if c.Initiator == api.ContinueAsNewInitiatorRetryPolicy {
		attrs.Attempt = prev.Attempt + 1
		attrs.ExpirationTimestamp = prev.ExpirationTimestamp
	} else if p := attrs.RetryPolicy; p != nil && p.ExpirationIntervalInSeconds > 0 {
		// Cron and decider continuations get a fresh retry budget.
		attrs.ExpirationTimestamp = e.nowNano() + seconds(attrs.FirstDecisionTaskBackoffSeconds) + seconds(p.ExpirationIntervalInSeconds)
	}
	_, err := e.startWorkflow(ctx, startParams{
		domain:     s.Domain,
		workflowID: s.WorkflowID,
		runID:      s.NewRunID,
		requestID:  s.NewRunID,
		attrs:      attrs,
		prevRunID:  s.RunID,
	})
	var already *api.WorkflowExecutionAlreadyStartedError
	if errors.As(err, &already) {
		e.log.Warn("continuation skipped, workflow ID was reused", "workflow", s.WorkflowID, "run", s.RunID)
		return nil
	}
	return err
}

func (e *Engine) notifyParent(ctx context.Context, s *State) error {
	p := s.Start.ParentWorkflowExecution
	initID := s.Start.ParentInitiatedEventID
	closeEv, err := e.closeEvent(ctx, s)
	if err != nil {
		return err
	}
	_, err = e.update(ctx, s.Start.ParentWorkflowDomain, p.WorkflowID, p.RunID, func(t *tx) error {
		ci := t.s.Children[initID]
		if !t.s.Running() || ci == nil {
			return nil
		}
		if ci.StartedID == 0 {
			// The child finished before the parent recorded its start.
			t.childStarted(ci, s.RunID)
		}
		exec := &api.WorkflowExecution{WorkflowID: s.WorkflowID, RunID: s.RunID}
		wt := &api.WorkflowType{Name: ci.WorkflowType}
		ev := childCloseEvent(t, closeEv, ci, exec, wt)
		delete(t.s.Children, initID)
		t.addExternal(ev)
		return nil
	})
	return ignoreClosed(err)
}

func childCloseEvent(t *tx, closeEv *api.HistoryEvent, ci *ChildInfo, exec *api.WorkflowExecution, wt *api.WorkflowType) *api.HistoryEvent {
	var ev *api.HistoryEvent
	switch closeEv.EventType {
	case api.EventTypeWorkflowExecutionCompleted:
		ev = t.event(api.EventTypeChildWorkflowExecutionCompleted)
		ev.ChildWorkflowExecutionCompletedEventAttributes = &api.ChildWorkflowExecutionCompletedEventAttributes{
			Result: closeEv.WorkflowExecutionCompletedEventAttributes.Result,
			Domain: ci.Domain, WorkflowExecution: exec, WorkflowType: wt, InitiatedEventID: ci.InitiatedID, StartedEventID: ci.StartedID,
		}
	case api.EventTypeWorkflowExecutionFailed:
		a := closeEv.WorkflowExecutionFailedEventAttributes
		ev = t.event(api.EventTypeChildWorkflowExecutionFailed)
		ev.ChildWorkflowExecutionFailedEventAttributes = &api.ChildWorkflowExecutionFailedEventAttributes{
			Reason: a.Reason, Details: a.Details,
			Domain: ci.Domain, WorkflowExecution: exec, WorkflowType: wt, InitiatedEventID: ci.InitiatedID, StartedEventID: ci.StartedID,
		}
	case api.EventTypeWorkflowExecutionCanceled:
		ev = t.event(api.EventTypeChildWorkflowExecutionCanceled)
		ev.ChildWorkflowExecutionCanceledEventAttributes = &api.ChildWorkflowExecutionCanceledEventAttributes{
			Details: closeEv.WorkflowExecutionCanceledEventAttributes.Details,
			Domain:  ci.Domain, WorkflowExecution: exec, WorkflowType: wt, InitiatedEventID: ci.InitiatedID, StartedEventID: ci.StartedID,
		}
	case api.EventTypeWorkflowExecutionTimedOut:
		ev = t.event(api.EventTypeChildWorkflowExecutionTimedOut)
		ev.ChildWorkflowExecutionTimedOutEventAttributes = &api.ChildWorkflowExecutionTimedOutEventAttributes{
			TimeoutType: closeEv.WorkflowExecutionTimedOutEventAttributes.TimeoutType,
			Domain:      ci.Domain, WorkflowExecution: exec, WorkflowType: wt, InitiatedEventID: ci.InitiatedID, StartedEventID: ci.StartedID,
		}
	default:
		ev = t.event(api.EventTypeChildWorkflowExecutionTerminated)
		ev.ChildWorkflowExecutionTerminatedEventAttributes = &api.ChildWorkflowExecutionTerminatedEventAttributes{
			Domain: ci.Domain, WorkflowExecution: exec, WorkflowType: wt, InitiatedEventID: ci.InitiatedID, StartedEventID: ci.StartedID,
		}
	}
	return ev
}

func (e *Engine) applyParentClosePolicy(ctx context.Context, parent *State, ci *ChildInfo) error {
	exec := &api.WorkflowExecution{WorkflowID: ci.WorkflowID, RunID: ci.RunID}
	var err error
	switch ci.ParentClosePolicy {
	case api.ParentClosePolicyTerminate:
		err = e.terminate(ctx, ci.Domain, exec, "by parent close policy", nil, "cadence-server")
	case api.ParentClosePolicyRequestCancel:
		err = e.requestCancel(ctx, ci.Domain, exec, &api.WorkflowExecutionCancelRequestedEventAttributes{
			Cause:                     "by parent close policy",
			ExternalWorkflowExecution: parent.Execution(),
		}, fmt.Sprintf("%s-close-%d", parent.RunID, ci.InitiatedID))
	default:
		return nil
	}
	var repeat *api.CancellationAlreadyRequestedError
	if errors.As(err, &repeat) {
		return nil
	}
	return ignoreClosed(err)
}

// closeEvent reads the event that closed s.
func (e *Engine) closeEvent(ctx context.Context, s *State) (*api.HistoryEvent, error) {
	msg, err := e.history.GetLastMsgForSubject(ctx, api.HistorySubject(s.Domain, s.WorkflowID, s.RunID))
	if err != nil {
		return nil, err
	}
	var h api.History
	if err := json.Unmarshal(msg.Data, &h); err != nil {
		return nil, err
	}
	for i := len(h.Events) - 1; i >= 0; i-- {
		if h.Events[i].EventID == s.CloseEventID {
			return h.Events[i], nil
		}
	}
	return nil, fmt.Errorf("close event %d not in last batch", s.CloseEventID)
}
