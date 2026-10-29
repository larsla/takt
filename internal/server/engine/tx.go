package engine

import (
	"fmt"
	"time"

	"github.com/larsla/takt/api"
)

// tx collects the effects of one state change.
type tx struct {
	e       *Engine
	s       *State
	now     int64
	events  []*api.HistoryEvent
	outbox  []*OutboxItem
	changed bool
	visible bool
}

func (e *Engine) newTx(s *State) *tx {
	return &tx{e: e, s: s, now: e.nowNano()}
}

func (t *tx) dirty() bool {
	return t.changed || len(t.events) > 0 || len(t.outbox) > 0
}

// commit moves the collected events and outbox items into the state.
func (t *tx) commit() {
	t.s.Pending = append(t.s.Pending, t.events...)
	t.s.Outbox = append(t.s.Outbox, t.outbox...)
	if t.visible {
		t.s.Outbox = append(t.s.Outbox, &OutboxItem{Kind: OutboxVisible})
	}
	t.events, t.outbox, t.visible = nil, nil, false
}

func (t *tx) event(et api.EventType) *api.HistoryEvent {
	return &api.HistoryEvent{EventType: et, Timestamp: t.now}
}

// add appends ev to history right away.
func (t *tx) add(ev *api.HistoryEvent) *api.HistoryEvent {
	ev.EventID = t.s.NextEventID
	t.s.NextEventID++
	t.events = append(t.events, ev)
	return ev
}

func (t *tx) decisionRunning() bool {
	return t.s.Decision != nil && t.s.Decision.StartedID != 0
}

// addExternal adds an event that the workflow code must react to, such as
// an activity result or a signal. While a decision task runs the event is
// buffered; otherwise it is appended and a decision task scheduled. It
// reports whether the event was buffered.
func (t *tx) addExternal(ev *api.HistoryEvent) bool {
	if t.decisionRunning() {
		t.s.Buffered = append(t.s.Buffered, ev)
		t.changed = true
		return true
	}
	t.add(ev)
	t.scheduleDecision()
	return false
}

// flushBuffered appends buffered events and fixes references to started
// events that had no ID while buffered. It returns how many it appended.
func (t *tx) flushBuffered() int {
	n := len(t.s.Buffered)
	childStarted := map[int64]int64{}
	for _, ev := range t.s.Buffered {
		t.add(ev)
		switch ev.EventType {
		case api.EventTypeActivityTaskCompleted:
			fixPrev(&ev.ActivityTaskCompletedEventAttributes.StartedEventID, ev)
		case api.EventTypeActivityTaskFailed:
			fixPrev(&ev.ActivityTaskFailedEventAttributes.StartedEventID, ev)
		case api.EventTypeActivityTaskTimedOut:
			fixPrev(&ev.ActivityTaskTimedOutEventAttributes.StartedEventID, ev)
		case api.EventTypeActivityTaskCanceled:
			fixPrev(&ev.ActivityTaskCanceledEventAttributes.StartedEventID, ev)
		case api.EventTypeChildWorkflowExecutionStarted:
			init := ev.ChildWorkflowExecutionStartedEventAttributes.InitiatedEventID
			childStarted[init] = ev.EventID
			if ci := t.s.Children[init]; ci != nil && ci.StartedID == bufferedStartedID {
				ci.StartedID = ev.EventID
			}
		case api.EventTypeChildWorkflowExecutionCompleted:
			a := ev.ChildWorkflowExecutionCompletedEventAttributes
			fixChild(&a.StartedEventID, a.InitiatedEventID, childStarted)
		case api.EventTypeChildWorkflowExecutionFailed:
			a := ev.ChildWorkflowExecutionFailedEventAttributes
			fixChild(&a.StartedEventID, a.InitiatedEventID, childStarted)
		case api.EventTypeChildWorkflowExecutionCanceled:
			a := ev.ChildWorkflowExecutionCanceledEventAttributes
			fixChild(&a.StartedEventID, a.InitiatedEventID, childStarted)
		case api.EventTypeChildWorkflowExecutionTimedOut:
			a := ev.ChildWorkflowExecutionTimedOutEventAttributes
			fixChild(&a.StartedEventID, a.InitiatedEventID, childStarted)
		case api.EventTypeChildWorkflowExecutionTerminated:
			a := ev.ChildWorkflowExecutionTerminatedEventAttributes
			fixChild(&a.StartedEventID, a.InitiatedEventID, childStarted)
		}
	}
	t.s.Buffered = nil
	return n
}

// fixPrev resolves a buffered activity started reference: the started event
// is always written right before the close event.
func fixPrev(id *int64, ev *api.HistoryEvent) {
	if *id == bufferedStartedID {
		*id = ev.EventID - 1
	}
}

func fixChild(id *int64, initiated int64, started map[int64]int64) {
	if *id == bufferedStartedID {
		*id = started[initiated]
	}
}

// scheduleDecision schedules a decision task unless one is already pending.
func (t *tx) scheduleDecision() {
	if t.s.Decision != nil || !t.s.Running() {
		return
	}
	s := t.s
	tl := s.TaskList()
	timeout := s.Start.TaskStartToCloseTimeoutSeconds
	ev := t.event(api.EventTypeDecisionTaskScheduled)
	ev.DecisionTaskScheduledEventAttributes = &api.DecisionTaskScheduledEventAttributes{
		TaskList:                   &api.TaskList{Name: tl},
		StartToCloseTimeoutSeconds: timeout,
		Attempt:                    s.DecisionAttempt,
	}
	t.add(ev)
	s.Decision = &DecisionInfo{
		ScheduleID:    ev.EventID,
		Attempt:       s.DecisionAttempt,
		ScheduledTime: t.now,
		TaskList:      tl,
		Timeout:       timeout,
	}
	t.task(api.TaskListTypeDecision, tl, ev.EventID, s.DecisionAttempt)
}

// retryDecision schedules the next decision attempt after a failure,
// backing off once failures repeat.
func (t *tx) retryDecision() {
	t.s.DecisionAttempt++
	if t.s.DecisionAttempt < 3 {
		t.scheduleDecision()
		return
	}
	delay := min(time.Second<<(t.s.DecisionAttempt-3), time.Minute)
	t.timer(TimerDecisionBackoff, 0, t.s.DecisionAttempt, "", t.now+int64(delay), fmt.Sprintf("db-%d", t.s.DecisionAttempt))
}

func (t *tx) task(kind api.TaskListType, taskList string, scheduleID, attempt int64) {
	t.outbox = append(t.outbox, &OutboxItem{
		Kind:     OutboxTask,
		TaskList: taskList,
		Stub: &api.TaskStub{
			Domain:     t.s.Domain,
			WorkflowID: t.s.WorkflowID,
			RunID:      t.s.RunID,
			ScheduleID: scheduleID,
			Attempt:    attempt,
			Type:       kind,
		},
	})
}

func (t *tx) timer(kind TimerKind, eventID, attempt int64, tt api.TimeoutType, fireAt int64, key string) {
	t.outbox = append(t.outbox, &OutboxItem{
		Kind: OutboxTimer,
		Timer: &TimerTask{
			Domain:      t.s.Domain,
			WorkflowID:  t.s.WorkflowID,
			RunID:       t.s.RunID,
			Kind:        kind,
			EventID:     eventID,
			Attempt:     attempt,
			TimeoutType: tt,
			FireAt:      fireAt,
			Key:         key,
		},
	})
}

func (t *tx) transfer(kind TransferKind, initiatedID int64) {
	t.outbox = append(t.outbox, &OutboxItem{
		Kind: OutboxTransfer,
		Transfer: &Transfer{
			Kind:        kind,
			Domain:      t.s.Domain,
			WorkflowID:  t.s.WorkflowID,
			RunID:       t.s.RunID,
			InitiatedID: initiatedID,
		},
	})
}

func seconds(n int32) int64 { return int64(n) * int64(time.Second) }

// closeRun marks the run closed. The close event must already be added.
func (t *tx) closeRun(status api.WorkflowExecutionCloseStatus, closeEventID int64) {
	s := t.s
	s.CloseStatus = status
	s.CloseTime = t.now
	s.CloseEventID = closeEventID
	s.Decision = nil
	t.transfer(TransferClosed, 0)
	t.visible = true
}

// closeActivity writes the started event (if the activity started) and the
// close event built by mk, then forgets the activity.
func (t *tx) closeActivity(ai *ActivityInfo, mk func(startedID int64) *api.HistoryEvent) {
	buffer := t.decisionRunning()
	startedID := int64(0)
	if ai.Started {
		ev := t.event(api.EventTypeActivityTaskStarted)
		ev.ActivityTaskStartedEventAttributes = &api.ActivityTaskStartedEventAttributes{
			ScheduledEventID:   ai.ScheduleID,
			Identity:           ai.StartedIdentity,
			RequestID:          ai.StartedRequestID,
			Attempt:            ai.Attempt,
			LastFailureReason:  ai.LastFailureReason,
			LastFailureDetails: ai.LastFailureDetails,
		}
		ev.Timestamp = ai.StartedTime
		if buffer {
			t.s.Buffered = append(t.s.Buffered, ev)
			startedID = bufferedStartedID
		} else {
			startedID = t.add(ev).EventID
		}
	}
	// addExternal schedules the decision after both events are in.
	t.addExternal(mk(startedID))
	delete(t.s.Activities, ai.ScheduleID)
}
