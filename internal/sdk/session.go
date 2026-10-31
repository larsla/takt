package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/larsla/takt/api"
)

// Sessions pin a series of activities to one worker, as in Cadence. They
// are built from activities: CreateSession schedules a long-running
// creation activity on "<task list>__internal_session_creation". The
// session worker that takes it signals the workflow with the name of a task
// list only it polls, then heartbeats until the session completes. The
// session's activities go to that task list. When the worker dies, the
// creation activity times out on its heartbeat and the session fails.

// SessionInfo describes a session. SessionID identifies it and HostName is
// the host of the worker running it.
type SessionInfo struct {
	SessionID string
	HostName  string

	taskList          string
	sessionState      sessionState
	sessionCancelFunc CancelFunc
	completionCtx     Context
}

// SessionOptions configures a session.
//
// ExecutionTimeout (required) is the longest the session may run.
// CreationTimeout (required) is how long CreateSession waits for a worker
// with room for another session. HeartbeatTimeout (default 20s) is how soon
// the session fails after its worker stops heartbeating.
type SessionOptions struct {
	ExecutionTimeout time.Duration
	CreationTimeout  time.Duration
	HeartbeatTimeout time.Duration
}

type sessionState int

const (
	sessionStateOpen sessionState = iota
	sessionStateFailed
	sessionStateClosed
)

const (
	sessionInfoKey contextKey = "sessionInfo"

	sessionCreationActivityName   = "internalSessionCreationActivity"
	sessionCompletionActivityName = "internalSessionCompletionActivity"
	sessionCreationSuffix         = "__internal_session_creation"
	errTooManySessionsMsg         = "too many outstanding sessions"

	defaultSessionHeartbeatTimeout = 20 * time.Second
	maxSessionHeartbeatInterval    = 10 * time.Second
	defaultMaxConcurrentSessions   = 1000
)

// QueryTypeOpenSessions returns the workflow's open sessions.
const QueryTypeOpenSessions = "__open_sessions"

var (
	// ErrSessionFailed is returned by an activity run in a session that has
	// failed. The activity is not scheduled.
	ErrSessionFailed            = errors.New("session has failed")
	errFoundExistingOpenSession = errors.New("found existing open session in the context")
)

type sessionCreationResponse struct {
	Tasklist   string
	HostName   string
	ResourceID string
}

type recreateSessionParams struct {
	Tasklist string
}

// CreateSession creates a session on a worker polling the activity task
// list of ctx (the workflow's task list by default) and returns a context
// for its activities. All activities run with that context go to the same
// worker. It fails when ctx already holds an open session, or when no
// worker has room for a session within CreationTimeout.
//
// An activity in a failed session returns ErrSessionFailed. The context is
// canceled when the session fails or CompleteSession is called.
func CreateSession(ctx Context, sessionOptions *SessionOptions) (Context, error) {
	tl := activityOptions(ctx).TaskList
	if tl == "" {
		tl = getEnv(ctx).WorkflowInfo().TaskListName
	}
	return createSession(ctx, tl+sessionCreationSuffix, sessionOptions, false)
}

// RecreateSession creates a session on the worker of an earlier session,
// identified by the token from SessionInfo.GetRecreateToken. Use it to
// carry a session across continue-as-new.
func RecreateSession(ctx Context, recreateToken []byte, sessionOptions *SessionOptions) (Context, error) {
	var params recreateSessionParams
	if err := json.Unmarshal(recreateToken, &params); err != nil {
		return nil, fmt.Errorf("failed to deserialize recreate token: %w", err)
	}
	return createSession(ctx, params.Tasklist, sessionOptions, true)
}

// CompleteSession ends the session in ctx and frees its slot on the
// worker. It does nothing when ctx holds no open session. Activities run
// with ctx afterwards go to the normal task list.
func CompleteSession(ctx Context) {
	info := getSessionInfo(ctx)
	if info == nil || info.sessionState != sessionStateOpen {
		return
	}
	// Cancel the creation activity and the session's activities, then tell
	// the worker, which otherwise notices only on its next heartbeat.
	info.sessionCancelFunc()
	cctx := WithActivityOptions(info.completionCtx, ActivityOptions{
		ScheduleToStartTimeout: 3 * time.Second,
		StartToCloseTimeout:    3 * time.Second,
	})
	if err := ExecuteActivity(cctx, sessionCompletionActivityName, info.SessionID).Get(cctx, nil); err != nil {
		GetLogger(ctx).Warn("Complete session activity failed", zap.Error(err))
	}
	info.sessionState = sessionStateClosed
	delete(getRun(ctx).sessions, info.SessionID)
}

// GetSessionInfo returns the most recent session in ctx, or nil.
func GetSessionInfo(ctx Context) *SessionInfo {
	info := getSessionInfo(ctx)
	if info == nil {
		GetLogger(ctx).Warn("Context contains no session information")
	}
	return info
}

// GetRecreateToken returns the token RecreateSession takes.
func (s *SessionInfo) GetRecreateToken() []byte {
	token, _ := json.Marshal(recreateSessionParams{Tasklist: s.taskList})
	return token
}

func getSessionInfo(ctx Context) *SessionInfo {
	info, _ := ctx.Value(sessionInfoKey).(*SessionInfo)
	return info
}

func createSession(ctx Context, creationTaskList string, options *SessionOptions, retryable bool) (Context, error) {
	if prev := getSessionInfo(ctx); prev != nil && prev.sessionState == sessionStateOpen {
		return nil, errFoundExistingOpenSession
	}
	if options == nil || options.ExecutionTimeout <= 0 || options.CreationTimeout <= 0 {
		return nil, errors.New("session ExecutionTimeout and CreationTimeout must be positive")
	}
	var sessionID string
	if err := SideEffect(ctx, func(Context) interface{} { return uuid.NewString() }).Get(&sessionID); err != nil {
		return nil, err
	}
	responses := GetSignalChannel(ctx, sessionID)

	heartbeat := options.HeartbeatTimeout
	if heartbeat <= 0 {
		heartbeat = defaultSessionHeartbeatTimeout
	}
	ao := ActivityOptions{
		TaskList:               creationTaskList,
		ScheduleToStartTimeout: options.CreationTimeout,
		StartToCloseTimeout:    options.ExecutionTimeout,
		HeartbeatTimeout:       heartbeat,
	}
	if retryable {
		// The worker of a recreated session may be full for a while.
		ao.RetryPolicy = &RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 1.1,
			MaximumInterval:    10 * time.Second,
			ExpirationInterval: options.CreationTimeout,
			NonRetriableErrorReasons: []string{
				errReasonPanic, errReasonGeneric,
				errReasonTimeout + " " + string(api.TimeoutTypeStartToClose),
				errReasonTimeout + " " + string(api.TimeoutTypeHeartbeat),
			},
		}
	}

	info := &SessionInfo{SessionID: sessionID, sessionState: sessionStateOpen}
	completionCtx := WithValue(ctx, sessionInfoKey, info)
	info.completionCtx = completionCtx
	// The session context is a child of completionCtx, so CompleteSession
	// can cancel the session and still run the completion activity.
	sessionCtx, cancel := WithCancel(completionCtx)
	creationCtx := WithActivityOptions(sessionCtx, ao)
	creation := ExecuteActivity(creationCtx, sessionCreationActivityName, sessionID)

	var creationErr error
	var resp sessionCreationResponse
	s := NewSelector(creationCtx)
	s.AddReceive(responses, func(c Channel, more bool) { c.Receive(creationCtx, &resp) })
	s.AddFuture(creation, func(f Future) {
		// The activity ended before the worker answered.
		creationErr = f.Get(creationCtx, nil)
		if creationErr == nil {
			creationErr = errors.New("session creation activity returned before the session started")
		}
	})
	s.Select(creationCtx)
	if creationErr != nil {
		cancel()
		return nil, creationErr
	}

	info.taskList = resp.Tasklist
	info.HostName = resp.HostName
	info.sessionCancelFunc = cancel
	run := getRun(ctx)
	run.sessions[sessionID] = info
	Go(creationCtx, func(c Context) {
		err := creation.Get(c, nil)
		if err == nil || IsCanceledError(err) {
			return
		}
		GetLogger(c).Info("Session failed", zap.String("SessionID", sessionID), zap.Error(err))
		info.sessionState = sessionStateFailed
		delete(run.sessions, sessionID)
		cancel()
	})
	return sessionCtx, nil
}

// sessionTaskList routes an activity run with ctx: to the session's task
// list while the session is open, and nowhere once it failed.
func sessionTaskList(ctx Context, activityType, taskList string) (string, error) {
	info := getSessionInfo(ctx)
	if info == nil || activityType == sessionCreationActivityName {
		return taskList, nil
	}
	switch info.sessionState {
	case sessionStateFailed:
		return "", ErrSessionFailed
	case sessionStateOpen:
		return info.taskList, nil
	}
	return taskList, nil
}

// openSessions answers QueryTypeOpenSessions.
func (r *workflowRun) openSessions() []*SessionInfo {
	out := make([]*SessionInfo, 0, len(r.sessions))
	for _, s := range r.sessions {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SessionID < out[j].SessionID })
	return out
}

// sessionWorker runs a worker's sessions. Its creation pollers hold a slot
// of slots for each session they start, so a full worker stops taking
// creation tasks and they wait for another worker.
type sessionWorker struct {
	creationTaskList string
	// taskList is only polled by this worker.
	taskList string
	host     string
	slots    chan struct{}

	mu   sync.Mutex
	done map[string]chan struct{}
}

func newSessionWorker(taskList string, maxSessions int) *sessionWorker {
	if maxSessions <= 0 {
		maxSessions = defaultMaxConcurrentSessions
	}
	host, _ := os.Hostname()
	return &sessionWorker{
		creationTaskList: taskList + sessionCreationSuffix,
		taskList:         api.SessionTaskListPrefix + uuid.NewString() + "@" + host,
		host:             host,
		slots:            make(chan struct{}, maxSessions),
		done:             map[string]chan struct{}{},
	}
}

// activity returns the session activity called name, if it is one.
// fromCreationList says whether the task came from the creation task list,
// whose poller already holds a slot.
func (s *sessionWorker) activity(name string, fromCreationList bool) (interface{}, bool) {
	switch name {
	case sessionCreationActivityName:
		return func(ctx context.Context, sessionID string) error {
			return s.run(ctx, sessionID, fromCreationList)
		}, true
	case sessionCompletionActivityName:
		return func(ctx context.Context, sessionID string) error {
			s.complete(sessionID)
			return nil
		}, true
	}
	return nil, false
}

// run is the creation activity: it tells the workflow where to send the
// session's activities and heartbeats until the session ends.
func (s *sessionWorker) run(ctx context.Context, sessionID string, haveSlot bool) error {
	if !haveSlot {
		// A recreated session arrives on the session task list.
		select {
		case s.slots <- struct{}{}:
			defer func() { <-s.slots }()
		default:
			return NewCustomError(errTooManySessionsMsg)
		}
	}
	done := make(chan struct{})
	s.mu.Lock()
	s.done[sessionID] = done
	s.mu.Unlock()
	defer s.complete(sessionID)

	env := getActivityEnv(ctx)
	input, err := encodeArgs(env.dc, []interface{}{sessionCreationResponse{
		Tasklist: s.taskList, HostName: s.host, ResourceID: s.taskList,
	}})
	if err != nil {
		return err
	}
	err = env.service.SignalWorkflowExecution(ctx, &api.SignalWorkflowExecutionRequest{
		Domain:            env.info.WorkflowDomain,
		WorkflowExecution: &api.WorkflowExecution{WorkflowID: env.info.WorkflowExecution.ID, RunID: env.info.WorkflowExecution.RunID},
		SignalName:        sessionID,
		Input:             input,
		Identity:          env.identity,
	})
	if err != nil {
		return err
	}

	interval := min(env.info.HeartbeatTimeout/3, maxSessionHeartbeatInterval)
	if interval <= 0 {
		interval = maxSessionHeartbeatInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-env.workerStop:
			return errors.New("session worker stopped")
		case <-done:
			return nil
		case <-ticker.C:
			// Skip throttling: the session fails if heartbeats lapse.
			env.send(nil)
		}
	}
}

func (s *sessionWorker) complete(sessionID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if done, ok := s.done[sessionID]; ok {
		delete(s.done, sessionID)
		close(done)
	}
}
