package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/larsla/takt/api"
)

// Marker names, the same as Cadence's.
const (
	sideEffectMarkerName        = "SideEffect"
	versionMarkerName           = "Version"
	localActivityMarkerName     = "LocalActivity"
	mutableSideEffectMarkerName = "MutableSideEffect"
)

// NonDeterministicError reports workflow code that no longer matches its
// history. Reason is "missing replay decision", "extra replay decision" or
// "mismatch"; HistoryEventText and DecisionText describe the parts that
// don't match.
type NonDeterministicError struct {
	Reason string

	WorkflowType string
	WorkflowID   string
	RunID        string
	TaskList     string
	DomainName   string

	HistoryEventText string
	DecisionText     string
}

func (e *NonDeterministicError) Error() string {
	switch {
	case e.HistoryEventText != "" && e.DecisionText != "":
		return "nondeterministic workflow: history event is " + e.HistoryEventText + ", replay decision is " + e.DecisionText
	case e.HistoryEventText != "":
		return "nondeterministic workflow: " + e.Reason + " for " + e.HistoryEventText
	default:
		return "nondeterministic workflow: " + e.Reason + " for " + e.DecisionText
	}
}

// batch is the history of one decision task: the events it reacted to,
// the DecisionTaskStarted event, and the events its decisions produced.
type batch struct {
	inputs         []*api.HistoryEvent
	started        *api.HistoryEvent
	completed      bool
	decisionEvents []*api.HistoryEvent
}

func isDecisionEvent(t api.EventType) bool {
	switch t {
	case api.EventTypeActivityTaskScheduled,
		api.EventTypeActivityTaskCancelRequested,
		api.EventTypeRequestCancelActivityTaskFailed,
		api.EventTypeTimerStarted,
		api.EventTypeTimerCanceled,
		api.EventTypeCancelTimerFailed,
		api.EventTypeMarkerRecorded,
		api.EventTypeStartChildWorkflowExecutionInitiated,
		api.EventTypeRequestCancelExternalWorkflowExecutionInitiated,
		api.EventTypeSignalExternalWorkflowExecutionInitiated,
		api.EventTypeWorkflowExecutionCompleted,
		api.EventTypeWorkflowExecutionFailed,
		api.EventTypeWorkflowExecutionCanceled,
		api.EventTypeWorkflowExecutionContinuedAsNew,
		api.EventTypeUpsertWorkflowSearchAttributes:
		return true
	}
	return false
}

// splitHistory cuts history into decision batches. Failed and timed out
// decision tasks are skipped: their events stay inputs of the next batch.
func splitHistory(events []*api.HistoryEvent) []*batch {
	var out []*batch
	cur := &batch{}
	for i := 0; i < len(events); i++ {
		e := events[i]
		switch e.EventType {
		case api.EventTypeDecisionTaskScheduled, api.EventTypeDecisionTaskFailed, api.EventTypeDecisionTaskTimedOut,
			api.EventTypeDecisionTaskCompleted:
			continue
		case api.EventTypeDecisionTaskStarted:
			if i+1 == len(events) {
				cur.started = e
				out = append(out, cur)
				return out
			}
			if events[i+1].EventType != api.EventTypeDecisionTaskCompleted {
				continue
			}
			cur.started = e
			cur.completed = true
			j := i + 2
			for j < len(events) && isDecisionEvent(events[j].EventType) {
				cur.decisionEvents = append(cur.decisionEvents, events[j])
				j++
			}
			out = append(out, cur)
			cur = &batch{}
			i = j - 1
		default:
			cur.inputs = append(cur.inputs, e)
		}
	}
	if len(cur.inputs) > 0 {
		out = append(out, cur)
	}
	return out
}

type replayMode int

const (
	// modeTask replays completed batches and runs the last one for real.
	modeTask replayMode = iota
	// modeQuery replays completed batches only.
	modeQuery
	// modeReplay replays every batch, for ReplayWorkflowHistory.
	modeReplay
)

type actHandle struct {
	id         string
	decision   *api.Decision
	done       bool
	waitCancel bool
	cb         resultHandler
}

type timerHandle struct {
	id       string
	decision *api.Decision
	done     bool
	cb       resultHandler
}

type childHandle struct {
	domain     string
	workflowID string
	decision   *api.Decision
	done       bool
	started    bool
	execution  WorkflowExecution
	waitCancel bool
	cb         resultHandler
	startedCb  func(WorkflowExecution, error)
}

type extHandle struct {
	id       string
	decision *api.Decision
	done     bool
	cb       resultHandler
}

type laTask struct {
	params localActivityParams
	cb     resultHandler
	done   bool
}

type laMarker struct {
	ActivityID   string `json:"activityId"`
	ActivityType string `json:"activityType"`
	Result       []byte `json:"result,omitempty"`
	ErrReason    string `json:"errReason,omitempty"`
	ErrDetails   []byte `json:"errDetails,omitempty"`
	Attempt      int32  `json:"attempt,omitempty"`
}

// laOutcome is the result of running a local activity.
type laOutcome struct {
	result  []byte
	err     error
	attempt int32
}

// laExecutor runs local activities for the replayer.
type laExecutor interface {
	executeLocal(ctx context.Context, info *WorkflowInfo, p localActivityParams) laOutcome
}

// replayer is the workflowEnvironment of real executions. It rebuilds the
// workflow's state from history on every decision task.
type replayer struct {
	info     *WorkflowInfo
	reg      *registry
	dc       DataConverter
	logger   *zap.Logger
	scope    tally.Scope
	laExec   laExecutor
	props    []ContextPropagator
	header   *api.Header
	run      *workflowRun
	now      time.Time
	replay   bool
	counter  int64
	seCount  int64
	decs     []*api.Decision
	acts     map[string]*actHandle
	actBySch map[int64]string
	timers   map[string]*timerHandle
	children map[string]*childHandle
	childIni map[int64]string
	sigs     map[string]*extHandle
	sigIni   map[int64]string
	cancels  map[string]*extHandle
	cancIni  map[int64]string

	sideEffects map[int64][]byte
	mutable     map[string][]byte
	versions    map[string]Version
	laMarkers   map[string]*laMarker
	laOrder     []string
	pendingLAs  []*laTask

	signalHandler func(string, []byte)
	cancelHandler func()
	queryHandler  func(string, []byte) ([]byte, error)

	cancelRequested bool
	completed       bool
	closeSent       bool
	result          []byte
	resultErr       error
}

type replayerConfig struct {
	domain                string
	execution             WorkflowExecution
	registry              *registry
	dataConverter         DataConverter
	logger                *zap.Logger
	scope                 tally.Scope
	enableLoggingInReplay bool
	laExec                laExecutor
	propagators           []ContextPropagator
}

func newReplayer(cfg replayerConfig) *replayer {
	r := &replayer{
		info:        &WorkflowInfo{Domain: cfg.domain, WorkflowExecution: cfg.execution},
		reg:         cfg.registry,
		dc:          orDefault(cfg.dataConverter),
		scope:       cfg.scope,
		laExec:      cfg.laExec,
		props:       cfg.propagators,
		acts:        map[string]*actHandle{},
		actBySch:    map[int64]string{},
		timers:      map[string]*timerHandle{},
		children:    map[string]*childHandle{},
		childIni:    map[int64]string{},
		sigs:        map[string]*extHandle{},
		sigIni:      map[int64]string{},
		cancels:     map[string]*extHandle{},
		cancIni:     map[int64]string{},
		sideEffects: map[int64][]byte{},
		mutable:     map[string][]byte{},
		versions:    map[string]Version{},
		laMarkers:   map[string]*laMarker{},
	}
	if r.scope == nil {
		r.scope = tally.NoopScope
	}
	base := cfg.logger
	if base == nil {
		base = zap.NewNop()
	}
	enable := cfg.enableLoggingInReplay
	r.logger = base.WithOptions(zap.WrapCore(func(c zapcore.Core) zapcore.Core {
		return &replayAwareCore{Core: c, isReplay: func() bool { return r.replay && !enable }}
	}))
	return r
}

// replayAwareCore drops log entries while replaying.
type replayAwareCore struct {
	zapcore.Core
	isReplay func() bool
}

func (c *replayAwareCore) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.isReplay() {
		return ce
	}
	return c.Core.Check(ent, ce)
}

func (c *replayAwareCore) With(fields []zapcore.Field) zapcore.Core {
	return &replayAwareCore{Core: c.Core.With(fields), isReplay: c.isReplay}
}

// close stops the workflow's goroutines.
func (r *replayer) close() {
	if r.run != nil {
		r.run.d.close()
	}
}

// replayHistory processes history in the given mode. In modeTask it returns
// the decisions of the last batch.
func (r *replayer) replayHistory(ctx context.Context, events []*api.HistoryEvent, mode replayMode) ([]*api.Decision, error) {
	return r.processBatches(ctx, splitHistory(events), mode)
}

// errCacheMismatch means a cached replayer can't continue with a history.
var errCacheMismatch = errors.New("cached workflow state does not match history")

// resume continues a replayer kept from the previous decision task. events
// start at the DecisionTaskStarted event of that task; sent are the
// decisions it returned.
func (r *replayer) resume(ctx context.Context, events []*api.HistoryEvent, sent []*api.Decision) ([]*api.Decision, error) {
	batches := splitHistory(events)
	if len(batches) < 2 || !batches[0].completed || len(batches[0].inputs) > 0 {
		return nil, errCacheMismatch
	}
	first := batches[0]
	if err := matchReplay(sent, first.decisionEvents); err != nil {
		return nil, errCacheMismatch
	}
	r.applyDecisionEvents(first.decisionEvents)
	return r.processBatches(ctx, batches[1:], modeTask)
}

func (r *replayer) processBatches(ctx context.Context, batches []*batch, mode replayMode) ([]*api.Decision, error) {
	for i, b := range batches {
		live := mode == modeTask && i == len(batches)-1 && !b.completed
		if !b.completed && !live && mode == modeQuery {
			break
		}
		r.replay = !live
		if b.started != nil {
			r.now = time.Unix(0, b.started.Timestamp)
			r.info.DecisionStartedEventID = b.started.EventID
		}
		for _, e := range b.inputs {
			if err := r.processEvent(e); err != nil {
				return nil, err
			}
		}
		if b.started == nil {
			continue
		}
		if r.replay {
			r.preloadMarkers(b.decisionEvents)
		}
		if err := r.execute(ctx); err != nil {
			return nil, err
		}
		decisions := r.decs
		r.decs = nil
		if live {
			return decisions, nil
		}
		if !b.completed {
			continue
		}
		if err := matchReplay(decisions, b.decisionEvents); err != nil {
			var nd *NonDeterministicError
			if errors.As(err, &nd) && r.info != nil {
				nd.WorkflowType, nd.WorkflowID, nd.RunID = r.info.WorkflowType.Name, r.info.WorkflowExecution.ID, r.info.WorkflowExecution.RunID
				nd.TaskList, nd.DomainName = r.info.TaskListName, r.info.Domain
			}
			return nil, err
		}
		r.applyDecisionEvents(b.decisionEvents)
	}
	return nil, nil
}

// execute runs workflow code until it blocks, feeding it local activity
// results along the way.
func (r *replayer) execute(ctx context.Context) error {
	if r.run == nil {
		return errors.New("history does not start with WorkflowExecutionStarted")
	}
	for {
		if r.run.d.closed {
			return nil
		}
		if err := r.run.d.executeUntilAllBlocked(); err != nil {
			return err
		}
		if r.completed {
			r.addCloseDecision()
			r.run.d.close()
			return nil
		}
		progressed, err := r.resolveLocalActivity(ctx)
		if err != nil {
			return err
		}
		if !progressed {
			return nil
		}
	}
}

// resolveLocalActivity delivers one local activity result: from history
// while replaying, by running the activity otherwise.
func (r *replayer) resolveLocalActivity(ctx context.Context) (bool, error) {
	pending := r.pendingLAs[:0]
	for _, la := range r.pendingLAs {
		if !la.done {
			pending = append(pending, la)
		}
	}
	r.pendingLAs = pending
	if len(pending) == 0 {
		return false, nil
	}
	if r.replay {
		for _, id := range r.laOrder {
			for _, la := range pending {
				if la.params.activityID != id {
					continue
				}
				m := r.laMarkers[id]
				delete(r.laMarkers, id)
				r.laOrder = removeString(r.laOrder, id)
				la.done = true
				var err error
				if m.ErrReason != "" {
					err = errorFromDetails(m.ErrReason, m.ErrDetails, r.dc)
				}
				la.cb(m.Result, err)
				return true, nil
			}
		}
		return false, nil
	}
	if r.laExec == nil {
		return false, errors.New("local activities are not supported here")
	}
	// Run local activities one at a time, in the order they were started,
	// so a replay delivers results in the same order.
	la := pending[0]
	la.done = true
	out := r.laExec.executeLocal(ctx, r.info, la.params)
	result, err := out.result, out.err
	m := &laMarker{ActivityID: la.params.activityID, ActivityType: la.params.activityType, Result: result, Attempt: out.attempt}
	if err != nil {
		m.ErrReason, m.ErrDetails = errorDetails(err, r.dc)
		err = errorFromDetails(m.ErrReason, m.ErrDetails, r.dc)
	}
	details, _ := json.Marshal(m)
	r.addDecision(&api.Decision{
		DecisionType:                   api.DecisionTypeRecordMarker,
		RecordMarkerDecisionAttributes: &api.RecordMarkerDecisionAttributes{MarkerName: localActivityMarkerName, Details: details},
	})
	la.cb(result, err)
	return true, nil
}

func removeString(s []string, v string) []string {
	for i, x := range s {
		if x == v {
			return append(s[:i], s[i+1:]...)
		}
	}
	return s
}

func (r *replayer) addDecision(d *api.Decision) { r.decs = append(r.decs, d) }

func (r *replayer) removeDecision(d *api.Decision) bool {
	for i, x := range r.decs {
		if x == d {
			r.decs = append(r.decs[:i], r.decs[i+1:]...)
			return true
		}
	}
	return false
}

func (r *replayer) addCloseDecision() {
	if r.closeSent {
		return
	}
	r.closeSent = true
	var (
		can      *ContinueAsNewError
		canceled *CanceledError
	)
	err := r.resultErr
	switch {
	case err == nil:
		r.addDecision(&api.Decision{
			DecisionType: api.DecisionTypeCompleteWorkflowExecution,
			CompleteWorkflowExecutionDecisionAttributes: &api.CompleteWorkflowExecutionDecisionAttributes{Result: r.result},
		})
	case errors.As(err, &can):
		p := can.params
		r.addDecision(&api.Decision{
			DecisionType: api.DecisionTypeContinueAsNewWorkflowExecution,
			ContinueAsNewWorkflowExecutionDecisionAttributes: &api.ContinueAsNewWorkflowExecutionDecisionAttributes{
				WorkflowType:                        &api.WorkflowType{Name: p.workflowType},
				TaskList:                            &api.TaskList{Name: p.taskList},
				Input:                               p.input,
				ExecutionStartToCloseTimeoutSeconds: p.execTimeout,
				TaskStartToCloseTimeoutSeconds:      p.taskTimeout,
				Header:                              p.header,
			},
		})
	case errors.As(err, &canceled):
		r.addDecision(&api.Decision{
			DecisionType: api.DecisionTypeCancelWorkflowExecution,
			CancelWorkflowExecutionDecisionAttributes: &api.CancelWorkflowExecutionDecisionAttributes{
				Details: encodeDetails(canceled.details, r.dc),
			},
		})
	default:
		reason, details := errorDetails(err, r.dc)
		r.addDecision(&api.Decision{
			DecisionType:                            api.DecisionTypeFailWorkflowExecution,
			FailWorkflowExecutionDecisionAttributes: &api.FailWorkflowExecutionDecisionAttributes{Reason: reason, Details: details},
		})
	}
}

func (r *replayer) startWorkflow(e *api.HistoryEvent) error {
	a := e.WorkflowExecutionStartedEventAttributes
	if a == nil {
		return errors.New("WorkflowExecutionStarted event has no attributes")
	}
	info := r.info
	info.WorkflowType = WorkflowType{Name: a.WorkflowType.Name}
	info.TaskListName = a.TaskList.Name
	info.ExecutionStartToCloseTimeoutSeconds = a.ExecutionStartToCloseTimeoutSeconds
	info.TaskStartToCloseTimeoutSeconds = a.TaskStartToCloseTimeoutSeconds
	info.Attempt = a.Attempt
	info.lastCompletionResult = a.LastCompletionResult
	info.OriginalRunId = a.OriginalExecutionRunID
	info.Memo = a.Memo
	info.SearchAttributes = a.SearchAttributes
	info.RetryPolicy = a.RetryPolicy
	r.header = a.Header
	if a.CronSchedule != "" {
		cs := a.CronSchedule
		info.CronSchedule = &cs
	}
	if a.ContinuedExecutionRunID != "" {
		id := a.ContinuedExecutionRunID
		info.ContinuedExecutionRunID = &id
	}
	if a.ParentWorkflowExecution != nil {
		d := a.ParentWorkflowDomain
		info.ParentWorkflowDomain = &d
		info.ParentWorkflowExecution = &WorkflowExecution{ID: a.ParentWorkflowExecution.WorkflowID, RunID: a.ParentWorkflowExecution.RunID}
	}
	fn, ok := r.reg.getWorkflow(info.WorkflowType.Name)
	if !ok {
		return fmt.Errorf("unable to find workflow type: %v. Supported types: %v", info.WorkflowType.Name, r.reg.workflowNames())
	}
	r.run = newWorkflowRun(r, fn, a.Input)
	return nil
}

// processEvent feeds one input event to the workflow.
func (r *replayer) processEvent(e *api.HistoryEvent) error {
	switch e.EventType {
	case api.EventTypeWorkflowExecutionStarted:
		return r.startWorkflow(e)
	case api.EventTypeWorkflowExecutionSignaled:
		a := e.WorkflowExecutionSignaledEventAttributes
		if r.signalHandler != nil {
			r.signalHandler(a.SignalName, a.Input)
		}
	case api.EventTypeWorkflowExecutionCancelRequested:
		r.cancelRequested = true
		if r.cancelHandler != nil {
			r.cancelHandler()
		}
	case api.EventTypeActivityTaskCompleted:
		a := e.ActivityTaskCompletedEventAttributes
		r.activityDone(a.ScheduledEventID, a.Result, nil)
	case api.EventTypeActivityTaskFailed:
		a := e.ActivityTaskFailedEventAttributes
		r.activityDone(a.ScheduledEventID, nil, errorFromDetails(a.Reason, a.Details, r.dc))
	case api.EventTypeActivityTaskTimedOut:
		a := e.ActivityTaskTimedOutEventAttributes
		r.activityDone(a.ScheduledEventID, nil, &TimeoutError{timeoutType: a.TimeoutType, details: newEncodedValues(a.Details, r.dc)})
	case api.EventTypeActivityTaskCanceled:
		a := e.ActivityTaskCanceledEventAttributes
		r.activityDone(a.ScheduledEventID, nil, &CanceledError{details: newEncodedValues(a.Details, r.dc)})
	case api.EventTypeTimerFired:
		if h := r.timers[e.TimerFiredEventAttributes.TimerID]; h != nil && !h.done {
			h.done = true
			h.cb(nil, nil)
		}
	case api.EventTypeChildWorkflowExecutionStarted:
		a := e.ChildWorkflowExecutionStartedEventAttributes
		if h := r.children[r.childIni[a.InitiatedEventID]]; h != nil {
			h.started = true
			h.execution = WorkflowExecution{ID: a.WorkflowExecution.WorkflowID, RunID: a.WorkflowExecution.RunID}
			h.startedCb(h.execution, nil)
		}
	case api.EventTypeStartChildWorkflowExecutionFailed:
		a := e.StartChildWorkflowExecutionFailedEventAttributes
		err := &api.WorkflowExecutionAlreadyStartedError{Message: "Workflow execution already started"}
		if h := r.children[r.childIni[a.InitiatedEventID]]; h != nil && !h.done {
			h.done = true
			h.startedCb(WorkflowExecution{}, err)
			h.cb(nil, err)
		}
	case api.EventTypeChildWorkflowExecutionCompleted:
		a := e.ChildWorkflowExecutionCompletedEventAttributes
		r.childDone(a.InitiatedEventID, a.Result, nil)
	case api.EventTypeChildWorkflowExecutionFailed:
		a := e.ChildWorkflowExecutionFailedEventAttributes
		r.childDone(a.InitiatedEventID, nil, errorFromDetails(a.Reason, a.Details, r.dc))
	case api.EventTypeChildWorkflowExecutionCanceled:
		a := e.ChildWorkflowExecutionCanceledEventAttributes
		r.childDone(a.InitiatedEventID, nil, &CanceledError{details: newEncodedValues(a.Details, r.dc)})
	case api.EventTypeChildWorkflowExecutionTimedOut:
		a := e.ChildWorkflowExecutionTimedOutEventAttributes
		r.childDone(a.InitiatedEventID, nil, &TimeoutError{timeoutType: a.TimeoutType})
	case api.EventTypeChildWorkflowExecutionTerminated:
		r.childDone(e.ChildWorkflowExecutionTerminatedEventAttributes.InitiatedEventID, nil, &TerminatedError{})
	case api.EventTypeExternalWorkflowExecutionSignaled:
		r.extDone(r.sigs, r.sigIni, e.ExternalWorkflowExecutionSignaledEventAttributes.InitiatedEventID, nil)
	case api.EventTypeSignalExternalWorkflowExecutionFailed:
		r.extDone(r.sigs, r.sigIni, e.SignalExternalWorkflowExecutionFailedEventAttributes.InitiatedEventID, &UnknownExternalWorkflowExecutionError{})
	case api.EventTypeExternalWorkflowExecutionCancelRequested:
		r.extDone(r.cancels, r.cancIni, e.ExternalWorkflowExecutionCancelRequestedEventAttributes.InitiatedEventID, nil)
	case api.EventTypeRequestCancelExternalWorkflowExecutionFailed:
		r.extDone(r.cancels, r.cancIni, e.RequestCancelExternalWorkflowExecutionFailedEventAttributes.InitiatedEventID, &UnknownExternalWorkflowExecutionError{})
	}
	return nil
}

func (r *replayer) activityDone(scheduledID int64, result []byte, err error) {
	h := r.acts[r.actBySch[scheduledID]]
	if h == nil || h.done {
		return
	}
	h.done = true
	h.cb(result, err)
}

func (r *replayer) childDone(initiatedID int64, result []byte, err error) {
	h := r.children[r.childIni[initiatedID]]
	if h == nil || h.done {
		return
	}
	h.done = true
	h.cb(result, err)
}

func (r *replayer) extDone(handles map[string]*extHandle, byInit map[int64]string, initiatedID int64, err error) {
	h := handles[byInit[initiatedID]]
	if h == nil || h.done {
		return
	}
	h.done = true
	h.cb(nil, err)
}

// preloadMarkers makes recorded markers of a batch available before its
// workflow code runs, so side effects and versions return recorded values.
func (r *replayer) preloadMarkers(events []*api.HistoryEvent) {
	for _, e := range events {
		if e.EventType != api.EventTypeMarkerRecorded {
			continue
		}
		a := e.MarkerRecordedEventAttributes
		switch a.MarkerName {
		case sideEffectMarkerName:
			var id int64
			var result []byte
			if defaultConverter.FromData(a.Details, &id, &result) == nil {
				r.sideEffects[id] = result
			}
		case versionMarkerName:
			var changeID string
			var v Version
			if defaultConverter.FromData(a.Details, &changeID, &v) == nil {
				r.versions[changeID] = v
			}
		case mutableSideEffectMarkerName:
			var id string
			var result []byte
			if defaultConverter.FromData(a.Details, &id, &result) == nil {
				r.mutable[id] = result
			}
		case localActivityMarkerName:
			var m laMarker
			if json.Unmarshal(a.Details, &m) == nil {
				r.laMarkers[m.ActivityID] = &m
				r.laOrder = append(r.laOrder, m.ActivityID)
			}
		}
	}
}

// skipped markers are not compared during replay, because they are only
// written when not replaying (or their order may legitimately vary).
func skipMarker(name string) bool {
	return name == versionMarkerName || name == mutableSideEffectMarkerName || name == localActivityMarkerName
}

func matchReplay(decisions []*api.Decision, events []*api.HistoryEvent) error {
	var ds []*api.Decision
	for _, d := range decisions {
		if d.DecisionType == api.DecisionTypeRecordMarker && skipMarker(d.RecordMarkerDecisionAttributes.MarkerName) {
			continue
		}
		ds = append(ds, d)
	}
	var es []*api.HistoryEvent
	for _, e := range events {
		if e.EventType == api.EventTypeMarkerRecorded && skipMarker(e.MarkerRecordedEventAttributes.MarkerName) {
			continue
		}
		es = append(es, e)
	}
	for i := 0; i < len(ds) || i < len(es); i++ {
		switch {
		case i >= len(ds):
			return &NonDeterministicError{Reason: "missing replay decision", HistoryEventText: fmt.Sprintf("%s (event %d)", es[i].EventType, es[i].EventID)}
		case i >= len(es):
			return &NonDeterministicError{Reason: "extra replay decision", DecisionText: describeDecision(ds[i])}
		case !decisionMatchesEvent(ds[i], es[i]):
			return &NonDeterministicError{Reason: "mismatch", HistoryEventText: fmt.Sprintf("%s (event %d)", es[i].EventType, es[i].EventID), DecisionText: describeDecision(ds[i])}
		}
	}
	return nil
}

func describeDecision(d *api.Decision) string {
	switch d.DecisionType {
	case api.DecisionTypeScheduleActivityTask:
		a := d.ScheduleActivityTaskDecisionAttributes
		return fmt.Sprintf("ScheduleActivityTask(ActivityID: %s, ActivityType: %s)", a.ActivityID, a.ActivityType.Name)
	case api.DecisionTypeStartTimer:
		return fmt.Sprintf("StartTimer(TimerID: %s)", d.StartTimerDecisionAttributes.TimerID)
	case api.DecisionTypeStartChildWorkflowExecution:
		a := d.StartChildWorkflowExecutionDecisionAttributes
		return fmt.Sprintf("StartChildWorkflowExecution(WorkflowID: %s, WorkflowType: %s)", a.WorkflowID, a.WorkflowType.Name)
	case api.DecisionTypeRecordMarker:
		return fmt.Sprintf("RecordMarker(%s)", d.RecordMarkerDecisionAttributes.MarkerName)
	}
	return string(d.DecisionType)
}

func decisionMatchesEvent(d *api.Decision, e *api.HistoryEvent) bool {
	switch d.DecisionType {
	case api.DecisionTypeScheduleActivityTask:
		a, ea := d.ScheduleActivityTaskDecisionAttributes, e.ActivityTaskScheduledEventAttributes
		return ea != nil && a.ActivityID == ea.ActivityID && a.ActivityType.Name == ea.ActivityType.Name
	case api.DecisionTypeRequestCancelActivityTask:
		id := d.RequestCancelActivityTaskDecisionAttributes.ActivityID
		switch e.EventType {
		case api.EventTypeActivityTaskCancelRequested:
			return e.ActivityTaskCancelRequestedEventAttributes.ActivityID == id
		case api.EventTypeRequestCancelActivityTaskFailed:
			return e.RequestCancelActivityTaskFailedEventAttributes.ActivityID == id
		}
	case api.DecisionTypeStartTimer:
		ea := e.TimerStartedEventAttributes
		return ea != nil && ea.TimerID == d.StartTimerDecisionAttributes.TimerID
	case api.DecisionTypeCancelTimer:
		id := d.CancelTimerDecisionAttributes.TimerID
		switch e.EventType {
		case api.EventTypeTimerCanceled:
			return e.TimerCanceledEventAttributes.TimerID == id
		case api.EventTypeCancelTimerFailed:
			return e.CancelTimerFailedEventAttributes.TimerID == id
		}
	case api.DecisionTypeCompleteWorkflowExecution:
		return e.EventType == api.EventTypeWorkflowExecutionCompleted || e.EventType == api.EventTypeWorkflowExecutionContinuedAsNew
	case api.DecisionTypeFailWorkflowExecution:
		return e.EventType == api.EventTypeWorkflowExecutionFailed || e.EventType == api.EventTypeWorkflowExecutionContinuedAsNew
	case api.DecisionTypeCancelWorkflowExecution:
		return e.EventType == api.EventTypeWorkflowExecutionCanceled
	case api.DecisionTypeContinueAsNewWorkflowExecution:
		return e.EventType == api.EventTypeWorkflowExecutionContinuedAsNew
	case api.DecisionTypeRecordMarker:
		ea := e.MarkerRecordedEventAttributes
		return ea != nil && ea.MarkerName == d.RecordMarkerDecisionAttributes.MarkerName
	case api.DecisionTypeStartChildWorkflowExecution:
		a, ea := d.StartChildWorkflowExecutionDecisionAttributes, e.StartChildWorkflowExecutionInitiatedEventAttributes
		return ea != nil && a.WorkflowID == ea.WorkflowID && a.WorkflowType.Name == ea.WorkflowType.Name
	case api.DecisionTypeSignalExternalWorkflowExecution:
		a, ea := d.SignalExternalWorkflowExecutionDecisionAttributes, e.SignalExternalWorkflowExecutionInitiatedEventAttributes
		return ea != nil && a.Execution.WorkflowID == ea.WorkflowExecution.WorkflowID && a.SignalName == ea.SignalName
	case api.DecisionTypeRequestCancelExternalWorkflowExecution:
		ea := e.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes
		return ea != nil && ea.WorkflowExecution.WorkflowID == d.RequestCancelExternalWorkflowExecutionDecisionAttributes.WorkflowID
	case api.DecisionTypeUpsertWorkflowSearchAttributes:
		return e.EventType == api.EventTypeUpsertWorkflowSearchAttributes
	}
	return false
}

// applyDecisionEvents links the events written for matched decisions to
// their handles, so later completion events find them.
func (r *replayer) applyDecisionEvents(events []*api.HistoryEvent) {
	for _, e := range events {
		switch e.EventType {
		case api.EventTypeActivityTaskScheduled:
			id := e.ActivityTaskScheduledEventAttributes.ActivityID
			if h := r.acts[id]; h != nil {
				h.decision = nil
				r.actBySch[e.EventID] = id
			}
		case api.EventTypeTimerStarted:
			if h := r.timers[e.TimerStartedEventAttributes.TimerID]; h != nil {
				h.decision = nil
			}
		case api.EventTypeStartChildWorkflowExecutionInitiated:
			id := e.StartChildWorkflowExecutionInitiatedEventAttributes.WorkflowID
			if h := r.children[id]; h != nil {
				h.decision = nil
				r.childIni[e.EventID] = id
			}
		case api.EventTypeSignalExternalWorkflowExecutionInitiated:
			id := string(e.SignalExternalWorkflowExecutionInitiatedEventAttributes.Control)
			if h := r.sigs[id]; h != nil {
				h.decision = nil
				r.sigIni[e.EventID] = id
			}
		case api.EventTypeRequestCancelExternalWorkflowExecutionInitiated:
			id := string(e.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes.Control)
			if h := r.cancels[id]; h != nil {
				h.decision = nil
				r.cancIni[e.EventID] = id
			}
		}
	}
}

// workflowEnvironment implementation

func (r *replayer) Now() time.Time              { return r.now }
func (r *replayer) IsReplaying() bool           { return r.replay }
func (r *replayer) WorkflowInfo() *WorkflowInfo { return r.info }
func (r *replayer) Registry() *registry         { return r.reg }
func (r *replayer) DataConverter() DataConverter {
	return r.dc
}
func (r *replayer) Logger() *zap.Logger       { return r.logger }
func (r *replayer) MetricsScope() tally.Scope { return r.scope }

func (r *replayer) ContextPropagators() []ContextPropagator { return r.props }

func (r *replayer) StartHeader() *api.Header { return r.header }

func (r *replayer) NextID() string {
	r.counter++
	return strconv.FormatInt(r.counter, 10)
}

func (r *replayer) RegisterSignalHandler(h func(string, []byte))                { r.signalHandler = h }
func (r *replayer) RegisterCancelHandler(h func())                              { r.cancelHandler = h }
func (r *replayer) RegisterQueryHandler(h func(string, []byte) ([]byte, error)) { r.queryHandler = h }

func (r *replayer) Complete(result []byte, err error) {
	if r.completed {
		return
	}
	r.completed = true
	r.result = result
	r.resultErr = err
}

func (r *replayer) ExecuteActivity(p activityParams, cb resultHandler) string {
	d := &api.Decision{
		DecisionType: api.DecisionTypeScheduleActivityTask,
		ScheduleActivityTaskDecisionAttributes: &api.ScheduleActivityTaskDecisionAttributes{
			ActivityID:                    p.activityID,
			ActivityType:                  &api.ActivityType{Name: p.activityType},
			Domain:                        r.info.Domain,
			TaskList:                      &api.TaskList{Name: p.taskList},
			Input:                         p.input,
			ScheduleToCloseTimeoutSeconds: durationSeconds(p.scheduleToClose),
			ScheduleToStartTimeoutSeconds: durationSeconds(p.scheduleToStart),
			StartToCloseTimeoutSeconds:    durationSeconds(p.startToClose),
			HeartbeatTimeoutSeconds:       durationSeconds(p.heartbeat),
			RetryPolicy:                   p.retryPolicy,
			Header:                        p.header,
		},
	}
	r.addDecision(d)
	r.acts[p.activityID] = &actHandle{id: p.activityID, decision: d, waitCancel: p.waitForCancellation, cb: cb}
	return p.activityID
}

func (r *replayer) RequestCancelActivity(id string) {
	h := r.acts[id]
	if h == nil || h.done {
		return
	}
	if h.decision != nil && r.removeDecision(h.decision) {
		h.done = true
		h.cb(nil, ErrCanceled)
		return
	}
	r.addDecision(&api.Decision{
		DecisionType: api.DecisionTypeRequestCancelActivityTask,
		RequestCancelActivityTaskDecisionAttributes: &api.RequestCancelActivityTaskDecisionAttributes{ActivityID: id},
	})
	if !h.waitCancel {
		h.done = true
		h.cb(nil, ErrCanceled)
	}
}

func (r *replayer) ExecuteLocalActivity(p localActivityParams, cb resultHandler) string {
	r.pendingLAs = append(r.pendingLAs, &laTask{params: p, cb: cb})
	return p.activityID
}

func (r *replayer) RequestCancelLocalActivity(id string) {
	for _, la := range r.pendingLAs {
		if la.params.activityID == id && !la.done {
			la.done = true
			la.cb(nil, ErrCanceled)
		}
	}
}

func (r *replayer) NewTimer(d time.Duration, cb resultHandler) string {
	id := r.NextID()
	dec := &api.Decision{
		DecisionType: api.DecisionTypeStartTimer,
		StartTimerDecisionAttributes: &api.StartTimerDecisionAttributes{
			TimerID:                   id,
			StartToFireTimeoutSeconds: int64(durationSeconds(d)),
		},
	}
	r.addDecision(dec)
	r.timers[id] = &timerHandle{id: id, decision: dec, cb: cb}
	return id
}

func (r *replayer) RequestCancelTimer(id string) {
	h := r.timers[id]
	if h == nil || h.done {
		return
	}
	h.done = true
	if h.decision == nil || !r.removeDecision(h.decision) {
		r.addDecision(&api.Decision{
			DecisionType:                  api.DecisionTypeCancelTimer,
			CancelTimerDecisionAttributes: &api.CancelTimerDecisionAttributes{TimerID: id},
		})
	}
	h.cb(nil, ErrCanceled)
}

func (r *replayer) ExecuteChildWorkflow(p childParams, cb resultHandler, started func(WorkflowExecution, error)) {
	d := &api.Decision{
		DecisionType: api.DecisionTypeStartChildWorkflowExecution,
		StartChildWorkflowExecutionDecisionAttributes: &api.StartChildWorkflowExecutionDecisionAttributes{
			Domain:                              p.domain,
			WorkflowID:                          p.workflowID,
			WorkflowType:                        &api.WorkflowType{Name: p.workflowType},
			TaskList:                            &api.TaskList{Name: p.taskList},
			Input:                               p.input,
			ExecutionStartToCloseTimeoutSeconds: durationSeconds(p.execTimeout),
			TaskStartToCloseTimeoutSeconds:      durationSeconds(p.taskTimeout),
			ParentClosePolicy:                   p.parentClosePolicy.toAPI(),
			WorkflowIDReusePolicy:               p.reusePolicy.toAPI(),
			RetryPolicy:                         p.retryPolicy,
			CronSchedule:                        p.cronSchedule,
			Memo:                                p.memo,
			SearchAttributes:                    p.searchAttributes,
			Header:                              p.header,
		},
	}
	r.addDecision(d)
	r.children[p.workflowID] = &childHandle{
		domain: p.domain, workflowID: p.workflowID, decision: d, waitCancel: p.waitForCancellation, cb: cb, startedCb: started,
	}
}

func (r *replayer) RequestCancelChildWorkflow(_, workflowID string) {
	h := r.children[workflowID]
	if h == nil || h.done {
		return
	}
	if h.decision != nil && r.removeDecision(h.decision) {
		h.done = true
		h.startedCb(WorkflowExecution{}, ErrCanceled)
		h.cb(nil, ErrCanceled)
		return
	}
	r.requestCancelExternal(h.domain, h.workflowID, "", true, func([]byte, error) {})
	if !h.waitCancel {
		h.done = true
		h.cb(nil, ErrCanceled)
	}
}

func (r *replayer) RequestCancelExternalWorkflow(domain, workflowID, runID string, cb resultHandler) {
	r.requestCancelExternal(domain, workflowID, runID, false, cb)
}

func (r *replayer) requestCancelExternal(domain, workflowID, runID string, childOnly bool, cb resultHandler) {
	id := r.NextID()
	d := &api.Decision{
		DecisionType: api.DecisionTypeRequestCancelExternalWorkflowExecution,
		RequestCancelExternalWorkflowExecutionDecisionAttributes: &api.RequestCancelExternalWorkflowExecutionDecisionAttributes{
			Domain: domain, WorkflowID: workflowID, RunID: runID, Control: []byte(id), ChildWorkflowOnly: childOnly,
		},
	}
	r.addDecision(d)
	r.cancels[id] = &extHandle{id: id, decision: d, cb: cb}
}

func (r *replayer) SignalExternalWorkflow(domain, workflowID, runID, signalName string, input []byte, _ interface{}, childOnly bool, cb resultHandler) {
	id := r.NextID()
	d := &api.Decision{
		DecisionType: api.DecisionTypeSignalExternalWorkflowExecution,
		SignalExternalWorkflowExecutionDecisionAttributes: &api.SignalExternalWorkflowExecutionDecisionAttributes{
			Domain:            domain,
			Execution:         &api.WorkflowExecution{WorkflowID: workflowID, RunID: runID},
			SignalName:        signalName,
			Input:             input,
			Control:           []byte(id),
			ChildWorkflowOnly: childOnly,
		},
	}
	r.addDecision(d)
	r.sigs[id] = &extHandle{id: id, decision: d, cb: cb}
}

func (r *replayer) SideEffect(f func() ([]byte, error), cb resultHandler) {
	r.seCount++
	id := r.seCount
	var result []byte
	if r.replay {
		var ok bool
		result, ok = r.sideEffects[id]
		if !ok {
			cb(nil, fmt.Errorf("no cached result found for side effect %d", id))
			return
		}
	} else {
		var err error
		if result, err = f(); err != nil {
			cb(nil, err)
			return
		}
	}
	details, _ := defaultConverter.ToData(id, result)
	r.addDecision(&api.Decision{
		DecisionType:                   api.DecisionTypeRecordMarker,
		RecordMarkerDecisionAttributes: &api.RecordMarkerDecisionAttributes{MarkerName: sideEffectMarkerName, Details: details},
	})
	cb(result, nil)
}

func (r *replayer) MutableSideEffect(id string, f func() interface{}, equals func(a, b interface{}) bool) Value {
	existing, have := r.mutable[id]
	if r.replay && have {
		return newEncodedValue(existing, r.dc)
	}
	v := f()
	if have && equals != nil {
		old := newEncodedValue(existing, r.dc)
		if same := compareMutable(old, v, equals); same {
			return old
		}
	}
	data, err := r.dc.ToData(v)
	if err != nil {
		panic(err)
	}
	r.mutable[id] = data
	if !r.replay {
		details, _ := defaultConverter.ToData(id, data)
		r.addDecision(&api.Decision{
			DecisionType:                   api.DecisionTypeRecordMarker,
			RecordMarkerDecisionAttributes: &api.RecordMarkerDecisionAttributes{MarkerName: mutableSideEffectMarkerName, Details: details},
		})
	}
	return newEncodedValue(data, r.dc)
}

// compareMutable decodes the old value into a value of v's type and compares.
func compareMutable(old Value, v interface{}, equals func(a, b interface{}) bool) bool {
	if v == nil {
		return false
	}
	ptr := newPtrLike(v)
	if ptr == nil || old.Get(ptr) != nil {
		return false
	}
	return equals(derefPtr(ptr), v)
}

func (r *replayer) GetVersion(changeID string, minSupported, maxSupported Version) Version {
	if v, ok := r.versions[changeID]; ok {
		validateVersion(changeID, v, minSupported, maxSupported)
		return v
	}
	v := DefaultVersion
	if !r.replay {
		v = maxSupported
		details, _ := defaultConverter.ToData(changeID, v)
		r.addDecision(&api.Decision{
			DecisionType:                   api.DecisionTypeRecordMarker,
			RecordMarkerDecisionAttributes: &api.RecordMarkerDecisionAttributes{MarkerName: versionMarkerName, Details: details},
		})
	}
	validateVersion(changeID, v, minSupported, maxSupported)
	r.versions[changeID] = v
	return v
}

func (r *replayer) UpsertSearchAttributes(attrs map[string]interface{}) error {
	fields, err := encodeMap(defaultConverter, attrs)
	if err != nil {
		return err
	}
	r.addDecision(&api.Decision{
		DecisionType: api.DecisionTypeUpsertWorkflowSearchAttributes,
		UpsertWorkflowSearchAttributesDecisionAttributes: &api.UpsertWorkflowSearchAttributesDecisionAttributes{
			SearchAttributes: &api.SearchAttributes{IndexedFields: fields},
		},
	})
	if r.info.SearchAttributes == nil {
		r.info.SearchAttributes = &api.SearchAttributes{IndexedFields: map[string][]byte{}}
	}
	if r.info.SearchAttributes.IndexedFields == nil {
		r.info.SearchAttributes.IndexedFields = map[string][]byte{}
	}
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		r.info.SearchAttributes.IndexedFields[k] = fields[k]
	}
	return nil
}
