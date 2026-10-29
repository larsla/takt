package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/larsla/takt/api"
)

// startParams describes a run to create.
type startParams struct {
	domain     string
	workflowID string
	// runID is preset for continue-as-new, cron and retry runs.
	runID     string
	requestID string
	policy    api.WorkflowIDReusePolicy
	attrs     *api.WorkflowExecutionStartedEventAttributes
	// signal is delivered with the start for SignalWithStart.
	signal *api.WorkflowExecutionSignaledEventAttributes
	// prevRunID is the run this one continues. The current pointer only
	// moves if it still points there.
	prevRunID string
}

func validateStart(domain, workflowID string, wt *api.WorkflowType, tl *api.TaskList, execTimeout, taskTimeout int32) error {
	switch {
	case domain == "":
		return badRequest("domain is not set")
	case workflowID == "":
		return badRequest("workflow ID is not set")
	case wt == nil || wt.Name == "":
		return badRequest("workflow type is not set")
	case tl == nil || tl.Name == "":
		return badRequest("task list is not set")
	case execTimeout <= 0:
		return badRequest("invalid ExecutionStartToCloseTimeoutSeconds")
	case taskTimeout < 0:
		return badRequest("invalid TaskStartToCloseTimeoutSeconds")
	}
	return nil
}

func (e *Engine) StartWorkflowExecution(ctx context.Context, req *api.StartWorkflowExecutionRequest) (*api.StartWorkflowExecutionResponse, error) {
	if err := validateStart(req.Domain, req.WorkflowID, req.WorkflowType, req.TaskList, req.ExecutionStartToCloseTimeoutSeconds, req.TaskStartToCloseTimeoutSeconds); err != nil {
		return nil, err
	}
	if _, _, err := e.domain(ctx, req.Domain); err != nil {
		return nil, err
	}
	if err := validateCron(req.CronSchedule); err != nil {
		return nil, err
	}
	runID, err := e.startWorkflow(ctx, startParams{
		domain:     req.Domain,
		workflowID: req.WorkflowID,
		requestID:  req.RequestID,
		policy:     req.WorkflowIDReusePolicy,
		attrs: e.startAttrs(req.WorkflowType, req.TaskList, req.Input, req.ExecutionStartToCloseTimeoutSeconds,
			req.TaskStartToCloseTimeoutSeconds, req.Identity, req.RequestID, req.RetryPolicy, req.CronSchedule,
			req.Memo, req.SearchAttributes, req.Header, req.DelayStartSeconds),
	})
	if err != nil {
		return nil, err
	}
	return &api.StartWorkflowExecutionResponse{RunID: runID}, nil
}

func (e *Engine) startAttrs(wt *api.WorkflowType, tl *api.TaskList, input []byte, execTimeout, taskTimeout int32,
	identity, requestID string, retry *api.RetryPolicy, cron string, memo *api.Memo, sa *api.SearchAttributes,
	header *api.Header, delay int32) *api.WorkflowExecutionStartedEventAttributes {
	if taskTimeout == 0 {
		taskTimeout = 10
	}
	a := &api.WorkflowExecutionStartedEventAttributes{
		WorkflowType:                        wt,
		TaskList:                            &api.TaskList{Name: tl.Name},
		Input:                               input,
		ExecutionStartToCloseTimeoutSeconds: execTimeout,
		TaskStartToCloseTimeoutSeconds:      min(taskTimeout, execTimeout),
		Identity:                            identity,
		RetryPolicy:                         normalizeRetryPolicy(retry),
		CronSchedule:                        cron,
		Memo:                                memo,
		SearchAttributes:                    sa,
		Header:                              header,
		RequestID:                           requestID,
		FirstDecisionTaskBackoffSeconds:     max(delay, 0),
	}
	if a.RetryPolicy != nil && a.RetryPolicy.ExpirationIntervalInSeconds > 0 {
		a.ExpirationTimestamp = e.nowNano() + seconds(a.RetryPolicy.ExpirationIntervalInSeconds)
	}
	if cron != "" && delay <= 0 {
		a.FirstDecisionTaskBackoffSeconds = cronBackoff(cron, e.now())
	}
	return a
}

// startWorkflow claims the workflow ID and creates the run.
func (e *Engine) startWorkflow(ctx context.Context, p startParams) (string, error) {
	if p.policy == "" {
		p.policy = api.WorkflowIDReusePolicyAllowDuplicateFailedOnly
	}
	ckey := currentKey(p.domain, p.workflowID)
	for range e.maxRetry {
		cur, rev, err := e.current(ctx, p.domain, p.workflowID)
		if err != nil {
			return "", err
		}
		if cur != nil {
			if p.prevRunID != "" {
				if cur.RunID == p.runID {
					// Already moved; make sure the run exists.
					return p.runID, e.createRun(ctx, p)
				}
				if cur.RunID != p.prevRunID {
					// A different run took over the ID; don't clobber it.
					return "", &api.WorkflowExecutionAlreadyStartedError{Message: "workflow ID reused before continuation", RunID: cur.RunID}
				}
			} else {
				runID, retry, err := e.checkReuse(ctx, p, cur)
				if err != nil || runID != "" {
					return runID, err
				}
				if retry {
					continue
				}
			}
		}

		runID := p.runID
		if runID == "" {
			runID = uuid.NewString()
		}
		data, _ := json.Marshal(&Current{RunID: runID, StartRequestID: p.requestID})
		if cur == nil {
			_, err = e.execs.Create(ctx, ckey, data)
		} else {
			_, err = e.execs.Update(ctx, ckey, data, rev)
		}
		if isConflict(err) {
			continue
		}
		if err != nil {
			return "", err
		}
		p.runID = runID
		if err := e.createRun(ctx, p); err != nil {
			return "", err
		}
		return runID, nil
	}
	return "", &api.ServiceBusyError{Message: "too much contention starting " + p.workflowID}
}

// checkReuse applies the ID reuse policy against the current run. It returns
// the existing run ID for an idempotent retry, retry=true if it changed
// something and the caller should re-read, or an error.
func (e *Engine) checkReuse(ctx context.Context, p startParams, cur *Current) (runID string, retry bool, err error) {
	prev, _, err := e.load(ctx, runKey(p.domain, p.workflowID, cur.RunID))
	var nf *api.EntityNotExistsError
	if errors.As(err, &nf) {
		// The pointer was claimed but the run never got created.
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if p.requestID != "" && prev.StartRequestID == p.requestID {
		return prev.RunID, false, nil
	}
	already := &api.WorkflowExecutionAlreadyStartedError{
		Message:        fmt.Sprintf("Workflow execution is already running. WorkflowId: %s, RunId: %s.", p.workflowID, prev.RunID),
		StartRequestID: prev.StartRequestID,
		RunID:          prev.RunID,
	}
	if prev.Running() {
		if p.policy != api.WorkflowIDReusePolicyTerminateIfRunning {
			return "", false, already
		}
		err := e.terminate(ctx, prev.Domain, prev.Execution(), "TerminateIfRunning WorkflowIDReusePolicy", nil, "cadence-server")
		if err != nil && !errors.Is(err, errWorkflowClosed) {
			return "", false, err
		}
		return "", true, nil
	}
	switch p.policy {
	case api.WorkflowIDReusePolicyRejectDuplicate:
		already.Message = "Workflow execution already finished. WorkflowId: " + p.workflowID
		return "", false, already
	case api.WorkflowIDReusePolicyAllowDuplicateFailedOnly:
		if prev.CloseStatus == api.CloseStatusCompleted {
			already.Message = "Workflow execution already finished successfully. WorkflowId: " + p.workflowID
			return "", false, already
		}
	}
	return "", false, nil
}

// createRun writes the first events of a new run. It is a no-op if the run
// already exists.
func (e *Engine) createRun(ctx context.Context, p startParams) error {
	now := e.nowNano()
	backoff := seconds(p.attrs.FirstDecisionTaskBackoffSeconds)
	if p.attrs.OriginalExecutionRunID == "" {
		p.attrs.OriginalExecutionRunID = p.runID
	}
	if p.attrs.FirstExecutionRunID == "" {
		p.attrs.FirstExecutionRunID = p.runID
	}
	s := &State{
		Domain:           p.domain,
		WorkflowID:       p.workflowID,
		RunID:            p.runID,
		Start:            p.attrs,
		StartTime:        now,
		ExecutionTime:    now + backoff,
		StartRequestID:   p.requestID,
		NextEventID:      1,
		SearchAttributes: p.attrs.SearchAttributes,
	}
	t := e.newTx(s)
	ev := t.event(api.EventTypeWorkflowExecutionStarted)
	ev.WorkflowExecutionStartedEventAttributes = p.attrs
	t.add(ev)
	if p.signal != nil {
		sig := t.event(api.EventTypeWorkflowExecutionSignaled)
		sig.WorkflowExecutionSignaledEventAttributes = p.signal
		t.add(sig)
	}
	if backoff > 0 {
		t.timer(TimerWorkflowBackoff, 0, 0, "", now+backoff, "wb")
	} else {
		t.scheduleDecision()
	}
	t.timer(TimerWorkflowTimeout, 0, 0, api.TimeoutTypeStartToClose, now+backoff+seconds(p.attrs.ExecutionStartToCloseTimeoutSeconds), "wt")
	t.visible = true
	_, err := e.create(ctx, t)
	return err
}

func (e *Engine) SignalWorkflowExecution(ctx context.Context, req *api.SignalWorkflowExecutionRequest) error {
	if req.SignalName == "" {
		return badRequest("signal name is not set")
	}
	return e.signal(ctx, req.Domain, req.WorkflowExecution, &api.WorkflowExecutionSignaledEventAttributes{
		SignalName: req.SignalName,
		Input:      req.Input,
		Identity:   req.Identity,
	}, req.RequestID)
}

func (e *Engine) signal(ctx context.Context, domain string, exec *api.WorkflowExecution, sig *api.WorkflowExecutionSignaledEventAttributes, requestID string) error {
	if exec == nil || exec.WorkflowID == "" {
		return badRequest("workflow ID is not set")
	}
	runID, err := e.resolveRunID(ctx, domain, exec)
	if err != nil {
		return err
	}
	_, err = e.update(ctx, domain, exec.WorkflowID, runID, func(t *tx) error {
		if !t.s.Running() {
			return errWorkflowClosed
		}
		if requestID != "" {
			if t.s.SignalRequestIDs[requestID] {
				return nil
			}
			if t.s.SignalRequestIDs == nil {
				t.s.SignalRequestIDs = map[string]bool{}
			}
			t.s.SignalRequestIDs[requestID] = true
		}
		ev := t.event(api.EventTypeWorkflowExecutionSignaled)
		ev.WorkflowExecutionSignaledEventAttributes = sig
		t.addExternal(ev)
		return nil
	})
	return err
}

func (e *Engine) SignalWithStartWorkflowExecution(ctx context.Context, req *api.SignalWithStartWorkflowExecutionRequest) (*api.StartWorkflowExecutionResponse, error) {
	if err := validateStart(req.Domain, req.WorkflowID, req.WorkflowType, req.TaskList, req.ExecutionStartToCloseTimeoutSeconds, req.TaskStartToCloseTimeoutSeconds); err != nil {
		return nil, err
	}
	if req.SignalName == "" {
		return nil, badRequest("signal name is not set")
	}
	if _, _, err := e.domain(ctx, req.Domain); err != nil {
		return nil, err
	}
	if err := validateCron(req.CronSchedule); err != nil {
		return nil, err
	}
	sig := &api.WorkflowExecutionSignaledEventAttributes{SignalName: req.SignalName, Input: req.SignalInput, Identity: req.Identity}
	for range e.maxRetry {
		cur, _, err := e.current(ctx, req.Domain, req.WorkflowID)
		if err != nil {
			return nil, err
		}
		if cur != nil {
			err := e.signal(ctx, req.Domain, &api.WorkflowExecution{WorkflowID: req.WorkflowID, RunID: cur.RunID}, sig, req.RequestID)
			if err == nil {
				return &api.StartWorkflowExecutionResponse{RunID: cur.RunID}, nil
			}
			var nf *api.EntityNotExistsError
			if !errors.As(err, &nf) {
				return nil, err
			}
		}
		policy := req.WorkflowIDReusePolicy
		if policy == "" {
			// Signal-with-start always starts when the previous run is closed.
			policy = api.WorkflowIDReusePolicyAllowDuplicate
		}
		runID, err := e.startWorkflow(ctx, startParams{
			domain:     req.Domain,
			workflowID: req.WorkflowID,
			requestID:  req.RequestID,
			policy:     policy,
			signal:     sig,
			attrs: e.startAttrs(req.WorkflowType, req.TaskList, req.Input, req.ExecutionStartToCloseTimeoutSeconds,
				req.TaskStartToCloseTimeoutSeconds, req.Identity, req.RequestID, req.RetryPolicy, req.CronSchedule,
				req.Memo, req.SearchAttributes, req.Header, req.DelayStartSeconds),
		})
		var already *api.WorkflowExecutionAlreadyStartedError
		if errors.As(err, &already) {
			// Raced with another start; signal that run instead.
			continue
		}
		if err != nil {
			return nil, err
		}
		return &api.StartWorkflowExecutionResponse{RunID: runID}, nil
	}
	return nil, &api.ServiceBusyError{Message: "too much contention on " + req.WorkflowID}
}

func (e *Engine) RequestCancelWorkflowExecution(ctx context.Context, req *api.RequestCancelWorkflowExecutionRequest) error {
	attrs := &api.WorkflowExecutionCancelRequestedEventAttributes{Cause: req.Cause, Identity: req.Identity}
	return e.requestCancel(ctx, req.Domain, req.WorkflowExecution, attrs, req.RequestID)
}

func (e *Engine) requestCancel(ctx context.Context, domain string, exec *api.WorkflowExecution, attrs *api.WorkflowExecutionCancelRequestedEventAttributes, requestID string) error {
	if exec == nil || exec.WorkflowID == "" {
		return badRequest("workflow ID is not set")
	}
	runID, err := e.resolveRunID(ctx, domain, exec)
	if err != nil {
		return err
	}
	_, err = e.update(ctx, domain, exec.WorkflowID, runID, func(t *tx) error {
		s := t.s
		if !s.Running() {
			return errWorkflowClosed
		}
		if s.CancelRequested {
			if requestID != "" && s.CancelRequestID == requestID {
				return nil
			}
			return &api.CancellationAlreadyRequestedError{Message: "cancellation already requested for this workflow execution"}
		}
		s.CancelRequested = true
		s.CancelRequestID = requestID
		ev := t.event(api.EventTypeWorkflowExecutionCancelRequested)
		ev.WorkflowExecutionCancelRequestedEventAttributes = attrs
		t.addExternal(ev)
		return nil
	})
	return err
}

func (e *Engine) TerminateWorkflowExecution(ctx context.Context, req *api.TerminateWorkflowExecutionRequest) error {
	return e.terminate(ctx, req.Domain, req.WorkflowExecution, req.Reason, req.Details, req.Identity)
}

func (e *Engine) terminate(ctx context.Context, domain string, exec *api.WorkflowExecution, reason string, details []byte, identity string) error {
	if exec == nil || exec.WorkflowID == "" {
		return badRequest("workflow ID is not set")
	}
	runID, err := e.resolveRunID(ctx, domain, exec)
	if err != nil {
		return err
	}
	_, err = e.update(ctx, domain, exec.WorkflowID, runID, func(t *tx) error {
		if !t.s.Running() {
			return errWorkflowClosed
		}
		t.flushBuffered()
		ev := t.event(api.EventTypeWorkflowExecutionTerminated)
		ev.WorkflowExecutionTerminatedEventAttributes = &api.WorkflowExecutionTerminatedEventAttributes{
			Reason:   reason,
			Details:  details,
			Identity: identity,
		}
		t.add(ev)
		t.closeRun(api.CloseStatusTerminated, ev.EventID)
		return nil
	})
	return err
}
