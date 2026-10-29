// Package engine implements workflow state transitions on top of JetStream.
//
// Each run's mutable state lives in the takt_executions KV bucket and is the
// source of truth. A state change is committed in two steps:
//
//  1. compare-and-set the KV entry with the new state, including the new
//     history events (State.Pending) and messages to publish (State.Outbox);
//  2. finalize: append Pending to the history stream, publish the outbox and
//     clear both with another compare-and-set.
//
// Any reader that finds a state with Pending or Outbox set finalizes it
// first, so a crash between the two steps only delays the second.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/larsla/takt/api"
)

// Config configures an Engine.
type Config struct {
	NC     *nats.Conn
	JS     jetstream.JetStream
	Logger *slog.Logger
	// Now returns the current time. Defaults to time.Now.
	Now func() time.Time
	// LongPollTimeout bounds history long polls. Defaults to 30s.
	LongPollTimeout time.Duration
	// QueryTimeout bounds how long a query waits for a worker. Defaults to 10s.
	QueryTimeout time.Duration
	// HistoryPageSize is the default number of events per history page.
	HistoryPageSize int
}

// Engine applies API calls, timers and transfers to workflow state.
type Engine struct {
	nc  *nats.Conn
	js  jetstream.JetStream
	log *slog.Logger
	now func() time.Time

	longPoll     time.Duration
	queryTimeout time.Duration
	pageSize     int

	execs    jetstream.KeyValue
	domains  jetstream.KeyValue
	vis      jetstream.KeyValue
	pollers  jetstream.KeyValue
	history  jetstream.Stream
	tasks    jetstream.Stream
	maxRetry int
}

const (
	bucketExecutions = "takt_executions"
	bucketDomains    = "takt_domains"
	bucketVisibility = "takt_visibility"
	bucketPollers    = "takt_pollers"
)

// New opens the buckets and streams created by the migrations.
func New(ctx context.Context, cfg Config) (*Engine, error) {
	e := &Engine{
		nc:           cfg.NC,
		js:           cfg.JS,
		log:          cfg.Logger,
		now:          cfg.Now,
		longPoll:     cfg.LongPollTimeout,
		queryTimeout: cfg.QueryTimeout,
		pageSize:     cfg.HistoryPageSize,
		maxRetry:     50,
	}
	if e.log == nil {
		e.log = slog.Default()
	}
	if e.now == nil {
		e.now = time.Now
	}
	if e.longPoll == 0 {
		e.longPoll = 30 * time.Second
	}
	if e.queryTimeout == 0 {
		e.queryTimeout = 10 * time.Second
	}
	if e.pageSize == 0 {
		e.pageSize = 1000
	}
	var err error
	for name, dst := range map[string]*jetstream.KeyValue{
		bucketExecutions: &e.execs,
		bucketDomains:    &e.domains,
		bucketVisibility: &e.vis,
		bucketPollers:    &e.pollers,
	} {
		if *dst, err = e.js.KeyValue(ctx, name); err != nil {
			return nil, fmt.Errorf("open bucket %s: %w", name, err)
		}
	}
	if e.history, err = e.js.Stream(ctx, api.HistoryStream); err != nil {
		return nil, fmt.Errorf("open stream %s: %w", api.HistoryStream, err)
	}
	if e.tasks, err = e.js.Stream(ctx, api.TaskStream); err != nil {
		return nil, fmt.Errorf("open stream %s: %w", api.TaskStream, err)
	}
	return e, nil
}

func (e *Engine) nowNano() int64 { return e.now().UnixNano() }

// isConflict reports a lost compare-and-set on a KV key or stream subject.
func isConflict(err error) bool {
	if errors.Is(err, jetstream.ErrKeyExists) || errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
		return true
	}
	var apiErr *jetstream.APIError
	return errors.As(err, &apiErr) &&
		(apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence ||
			apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequenceConstant)
}

func notFound(format string, args ...any) error {
	return &api.EntityNotExistsError{Message: fmt.Sprintf(format, args...)}
}

func badRequest(format string, args ...any) error {
	return &api.BadRequestError{Message: fmt.Sprintf(format, args...)}
}

var errWorkflowClosed = &api.EntityNotExistsError{Message: "workflow execution already completed"}

// get reads a run without finalizing it.
func (e *Engine) get(ctx context.Context, key string) (*State, uint64, error) {
	entry, err := e.execs.Get(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, 0, notFound("workflow execution not found")
	}
	if err != nil {
		return nil, 0, err
	}
	s, err := decodeState(entry.Value())
	if err != nil {
		return nil, 0, fmt.Errorf("decode state %s: %w", key, err)
	}
	return s, entry.Revision(), nil
}

// load reads a run and finalizes it if an earlier commit was interrupted.
func (e *Engine) load(ctx context.Context, key string) (*State, uint64, error) {
	for range e.maxRetry {
		s, rev, err := e.get(ctx, key)
		if err != nil {
			return nil, 0, err
		}
		if len(s.Pending) == 0 && len(s.Outbox) == 0 {
			return s, rev, nil
		}
		s, rev, err = e.finalize(ctx, key, s, rev)
		if err != nil {
			return nil, 0, err
		}
		if s != nil {
			return s, rev, nil
		}
		// Lost a race with another finalizer; read again.
	}
	return nil, 0, &api.ServiceBusyError{Message: "too much contention on workflow " + key}
}

// loadRun resolves an execution (empty run ID means the current run) and loads it.
func (e *Engine) loadRun(ctx context.Context, domain string, exec *api.WorkflowExecution) (*State, error) {
	if exec == nil || exec.WorkflowID == "" {
		return nil, badRequest("workflow ID is not set")
	}
	runID, err := e.resolveRunID(ctx, domain, exec)
	if err != nil {
		return nil, err
	}
	s, _, err := e.load(ctx, runKey(domain, exec.WorkflowID, runID))
	return s, err
}

func (e *Engine) resolveRunID(ctx context.Context, domain string, exec *api.WorkflowExecution) (string, error) {
	if exec.RunID != "" {
		return exec.RunID, nil
	}
	cur, _, err := e.current(ctx, domain, exec.WorkflowID)
	if err != nil {
		return "", err
	}
	if cur == nil {
		return "", notFound("workflow execution not found")
	}
	return cur.RunID, nil
}

func (e *Engine) current(ctx context.Context, domain, workflowID string) (*Current, uint64, error) {
	entry, err := e.execs.Get(ctx, currentKey(domain, workflowID))
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	var cur Current
	if err := json.Unmarshal(entry.Value(), &cur); err != nil {
		return nil, 0, err
	}
	return &cur, entry.Revision(), nil
}

// update loads a run, applies fn and commits the result. fn may run several
// times when it loses a race, so it must only change the state it is given.
// It returns the state after the commit.
func (e *Engine) update(ctx context.Context, domain, workflowID, runID string, fn func(t *tx) error) (*State, error) {
	key := runKey(domain, workflowID, runID)
	for range e.maxRetry {
		s, rev, err := e.load(ctx, key)
		if err != nil {
			return nil, err
		}
		t := e.newTx(s)
		if err := fn(t); err != nil {
			return nil, err
		}
		if !t.dirty() {
			return s, nil
		}
		t.commit()
		newRev, err := e.execs.Update(ctx, key, encodeState(s), rev)
		if isConflict(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		final, _, err := e.finalize(ctx, key, s, newRev)
		if err != nil {
			// The change is committed; finalize will be retried by the
			// next reader or the sweeper.
			e.log.Warn("finalize failed", "key", key, "err", err)
			return s, nil
		}
		if t.continued {
			// Create the next run now so callers can follow the chain right
			// away. The close transfer does the same if this fails.
			if err := e.startContinuation(ctx, s); err != nil {
				e.log.Warn("start continuation", "key", key, "err", err)
			}
		}
		if final == nil {
			return s, nil
		}
		return final, nil
	}
	return nil, &api.ServiceBusyError{Message: "too much contention on workflow " + workflowID}
}

// create stores a brand new run and finalizes it. It returns false if the
// run already exists.
func (e *Engine) create(ctx context.Context, t *tx) (bool, error) {
	s := t.s
	key := runKey(s.Domain, s.WorkflowID, s.RunID)
	t.commit()
	rev, err := e.execs.Create(ctx, key, encodeState(s))
	if isConflict(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if _, _, err := e.finalize(ctx, key, s, rev); err != nil {
		e.log.Warn("finalize failed", "key", key, "err", err)
	}
	return true, nil
}

// finalize appends pending history, publishes the outbox and clears both.
// It returns nil state if another writer changed the run meanwhile.
func (e *Engine) finalize(ctx context.Context, key string, s *State, rev uint64) (*State, uint64, error) {
	if len(s.Pending) > 0 {
		seq, err := e.appendHistory(ctx, s)
		if errors.Is(err, errAppendConflict) {
			_, curRev, gerr := e.get(ctx, key)
			if gerr != nil {
				return nil, 0, gerr
			}
			if curRev != rev {
				// Another writer finalized this batch and moved on.
				return nil, 0, nil
			}
			seq, err = e.recoverAppend(ctx, s)
		}
		if err != nil {
			return nil, 0, err
		}
		s.HistorySeq = seq
		s.Pending = nil
	}
	var failed []*OutboxItem
	for _, item := range s.Outbox {
		if err := e.publish(ctx, s, item); err != nil {
			e.log.Warn("publish outbox item", "key", key, "kind", item.Kind, "err", err)
			failed = append(failed, item)
		}
	}
	s.Outbox = failed
	newRev, err := e.execs.Update(ctx, key, encodeState(s), rev)
	if isConflict(err) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	return s, newRev, nil
}

func (e *Engine) appendHistory(ctx context.Context, s *State) (uint64, error) {
	subject := api.HistorySubject(s.Domain, s.WorkflowID, s.RunID)
	data, err := json.Marshal(&api.History{Events: s.Pending})
	if err != nil {
		return 0, err
	}
	first := s.Pending[0].EventID
	msg := nats.NewMsg(subject)
	msg.Data = data
	msg.Header.Set(jetstream.MsgIDHeader, s.RunID+"-h-"+strconv.FormatInt(first, 10))
	msg.Header.Set(jetstream.ExpectedLastSubjSeqHeader, strconv.FormatUint(s.HistorySeq, 10))
	ack, err := e.js.PublishMsg(ctx, msg)
	if err == nil {
		return ack.Sequence, nil
	}
	if isConflict(err) {
		return 0, errAppendConflict
	}
	return 0, fmt.Errorf("append history: %w", err)
}

var errAppendConflict = errors.New("history append conflict")

// recoverAppend handles an append conflict while the state is unchanged:
// an earlier finalize appended this batch but crashed before updating the
// state. Then the batch is the last one on the subject.
func (e *Engine) recoverAppend(ctx context.Context, s *State) (uint64, error) {
	last, err := e.history.GetLastMsgForSubject(ctx, api.HistorySubject(s.Domain, s.WorkflowID, s.RunID))
	if err != nil {
		return 0, fmt.Errorf("read last history batch: %w", err)
	}
	var h api.History
	if json.Unmarshal(last.Data, &h) == nil && len(h.Events) > 0 && h.Events[0].EventID == s.Pending[0].EventID {
		return last.Sequence, nil
	}
	return 0, fmt.Errorf("history of %s diverged from state", key(s))
}

func key(s *State) string { return runKey(s.Domain, s.WorkflowID, s.RunID) }

func (e *Engine) publish(ctx context.Context, s *State, item *OutboxItem) error {
	switch item.Kind {
	case OutboxTask:
		st := item.Stub
		data, _ := json.Marshal(st)
		msg := nats.NewMsg(api.TaskSubject(st.Domain, item.TaskList, st.Type))
		msg.Data = data
		msg.Header.Set(jetstream.MsgIDHeader, fmt.Sprintf("%s-%s-%d-%d", st.RunID, st.Type, st.ScheduleID, st.Attempt))
		_, err := e.js.PublishMsg(ctx, msg)
		return err
	case OutboxTimer:
		return e.publishTimer(ctx, item.Timer)
	case OutboxTransfer:
		tr := item.Transfer
		data, _ := json.Marshal(tr)
		msg := nats.NewMsg(transferSubject(tr.Domain))
		msg.Data = data
		msg.Header.Set(jetstream.MsgIDHeader, fmt.Sprintf("%s-x-%s-%d", tr.RunID, tr.Kind, tr.InitiatedID))
		_, err := e.js.PublishMsg(ctx, msg)
		return err
	case OutboxVisible:
		return e.writeVisibility(ctx, s)
	default:
		return fmt.Errorf("unknown outbox kind %q", item.Kind)
	}
}

const (
	timerSchedPrefix = "takt.timer.sched."
	timerFirePrefix  = "takt.timer.fire."
	transferPrefix   = "takt.xfer."
	// TimerFireSubjects is what the timer consumer listens on.
	TimerFireSubjects = timerFirePrefix + ">"
	// TransferSubjects is what the transfer consumer listens on.
	TransferSubjects = transferPrefix + ">"
)

func transferSubject(domain string) string { return transferPrefix + api.Token(domain) }

func (e *Engine) publishTimer(ctx context.Context, tt *TimerTask) error {
	data, _ := json.Marshal(tt)
	suffix := fmt.Sprintf("%s.%s.%s.%s", api.Token(tt.Domain), api.Token(tt.WorkflowID), tt.RunID, tt.Key)
	fire := timerFirePrefix + suffix
	fireAt := time.Unix(0, tt.FireAt)

	var msg *nats.Msg
	if time.Until(fireAt) <= 50*time.Millisecond {
		msg = nats.NewMsg(fire)
		msg.Header.Set(jetstream.MsgIDHeader, tt.RunID+"-tf-"+tt.Key)
	} else {
		msg = nats.NewMsg(timerSchedPrefix + suffix)
		msg.Header.Set("Nats-Schedule", "@at "+fireAt.UTC().Format(time.RFC3339Nano))
		msg.Header.Set("Nats-Schedule-Target", fire)
		msg.Header.Set(jetstream.MsgIDHeader, tt.RunID+"-ts-"+tt.Key)
	}
	msg.Data = data
	_, err := e.js.PublishMsg(ctx, msg)
	return err
}
