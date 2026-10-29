package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/larsla/takt/api"
)

// HandleTimer processes one fired timer message. Timers that no longer
// match the state are ignored.
func (e *Engine) HandleTimer(ctx context.Context, data []byte) error {
	var tt TimerTask
	if err := json.Unmarshal(data, &tt); err != nil {
		e.log.Warn("dropping malformed timer", "err", err)
		return nil
	}
	_, err := e.update(ctx, tt.Domain, tt.WorkflowID, tt.RunID, func(t *tx) error {
		if !t.s.Running() {
			return nil
		}
		t.fireTimer(&tt)
		return nil
	})
	var nf *api.EntityNotExistsError
	if errors.As(err, &nf) {
		return nil
	}
	return err
}

func (t *tx) fireTimer(tt *TimerTask) {
	s := t.s
	switch tt.Kind {
	case TimerUser:
		for id, ti := range s.Timers {
			if ti.StartedEventID != tt.EventID {
				continue
			}
			delete(s.Timers, id)
			ev := t.event(api.EventTypeTimerFired)
			ev.TimerFiredEventAttributes = &api.TimerFiredEventAttributes{TimerID: id, StartedEventID: ti.StartedEventID}
			t.addExternal(ev)
			return
		}

	case TimerActivityTimeout:
		t.activityTimeout(tt)

	case TimerActivityRetry:
		ai := s.Activities[tt.EventID]
		if ai == nil || int64(ai.Attempt) != tt.Attempt || ai.Started {
			return
		}
		t.changed = true
		t.dispatchActivity(ai)

	case TimerDecisionTimeout:
		d := s.Decision
		if d == nil || d.ScheduleID != tt.EventID || d.Attempt != tt.Attempt || d.StartedID == 0 {
			return
		}
		t.timeoutDecision(d)

	case TimerDecisionBackoff:
		if s.DecisionAttempt == tt.Attempt {
			t.scheduleDecision()
		}

	case TimerWorkflowBackoff:
		t.scheduleDecision()

	case TimerWorkflowTimeout:
		t.flushBuffered()
		reason := timeoutReason(api.TimeoutTypeStartToClose)
		if t.retryOrCron(reason, nil, 0) {
			return
		}
		ev := t.event(api.EventTypeWorkflowExecutionTimedOut)
		ev.WorkflowExecutionTimedOutEventAttributes = &api.WorkflowExecutionTimedOutEventAttributes{
			TimeoutType: api.TimeoutTypeStartToClose,
		}
		t.add(ev)
		t.closeRun(api.CloseStatusTimedOut, ev.EventID)

	default:
		t.e.log.Warn("unknown timer kind", "kind", tt.Kind, "key", fmt.Sprint(tt.Key))
	}
}
