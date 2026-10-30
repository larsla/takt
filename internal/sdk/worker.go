package sdk

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"os/signal"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
	"golang.org/x/time/rate"

	"github.com/larsla/takt/api"
)

// NonDeterministicWorkflowPolicy says what to do when workflow code no
// longer matches its history.
type NonDeterministicWorkflowPolicy int

const (
	// NonDeterministicWorkflowPolicyBlockWorkflow fails the decision task so
	// it is retried, giving time to deploy a fix.
	NonDeterministicWorkflowPolicyBlockWorkflow NonDeterministicWorkflowPolicy = iota
	// NonDeterministicWorkflowPolicyFailWorkflow fails the workflow.
	NonDeterministicWorkflowPolicyFailWorkflow
)

// WorkflowPanicPolicy says what to do when workflow code panics.
type WorkflowPanicPolicy int

const (
	// BlockWorkflow fails the decision task so it is retried.
	BlockWorkflow WorkflowPanicPolicy = iota
	// FailWorkflow fails the workflow.
	FailWorkflow
)

// WorkerOptions configures a worker. Field names follow Cadence's.
type WorkerOptions struct {
	MaxConcurrentActivityExecutionSize      int
	WorkerActivitiesPerSecond               float64
	MaxConcurrentLocalActivityExecutionSize int
	WorkerLocalActivitiesPerSecond          float64
	// TaskListActivitiesPerSecond is accepted for compatibility; it is
	// applied per worker like WorkerActivitiesPerSecond.
	TaskListActivitiesPerSecond            float64
	MaxConcurrentActivityTaskPollers       int
	MaxConcurrentDecisionTaskExecutionSize int
	WorkerDecisionTasksPerSecond           float64
	MaxConcurrentDecisionTaskPollers       int
	Identity                               string
	MetricsScope                           tally.Scope
	Logger                                 *zap.Logger
	EnableLoggingInReplay                  bool
	DisableWorkflowWorker                  bool
	DisableActivityWorker                  bool
	// DisableStickyExecution turns off the in-memory cache of workflow runs
	// (see SetStickyWorkflowCacheSize). StickyScheduleToStartTimeout is
	// accepted for compatibility and ignored.
	DisableStickyExecution         bool
	StickyScheduleToStartTimeout   time.Duration
	BackgroundActivityContext      context.Context
	NonDeterministicWorkflowPolicy NonDeterministicWorkflowPolicy
	WorkflowPanicPolicy            WorkflowPanicPolicy
	DataConverter                  DataConverter
	WorkerStopTimeout              time.Duration
	// EnableSessionWorker and MaxConcurrentSessionExecutionSize are accepted
	// for compatibility; sessions are not supported.
	EnableSessionWorker               bool
	MaxConcurrentSessionExecutionSize int
}

func (o *WorkerOptions) defaults(taskList string) {
	if o.MaxConcurrentActivityExecutionSize <= 0 {
		o.MaxConcurrentActivityExecutionSize = 1000
	}
	if o.MaxConcurrentLocalActivityExecutionSize <= 0 {
		o.MaxConcurrentLocalActivityExecutionSize = 1000
	}
	if o.MaxConcurrentActivityTaskPollers <= 0 {
		o.MaxConcurrentActivityTaskPollers = 2
	}
	if o.MaxConcurrentDecisionTaskExecutionSize <= 0 {
		o.MaxConcurrentDecisionTaskExecutionSize = 1000
	}
	if o.MaxConcurrentDecisionTaskPollers <= 0 {
		o.MaxConcurrentDecisionTaskPollers = 2
	}
	if o.Identity == "" {
		host, _ := os.Hostname()
		o.Identity = fmt.Sprintf("%d@%s@%s", os.Getpid(), host, taskList)
	}
	if o.MetricsScope == nil {
		o.MetricsScope = tally.NoopScope
	}
	if o.Logger == nil {
		o.Logger = zap.NewNop()
	}
	if o.BackgroundActivityContext == nil {
		o.BackgroundActivityContext = context.Background()
	}
	if o.WorkerStopTimeout == 0 {
		o.WorkerStopTimeout = 10 * time.Second
	}
	o.DataConverter = orDefault(o.DataConverter)
}

// queryListener is implemented by services that can deliver queries to
// workers (the NATS transport does).
type queryListener interface {
	ListenQueries(domain, taskList string, handle func(context.Context, *api.WorkerQueryRequest) *api.WorkerQueryResponse) (stop func(), err error)
}

// Worker polls a task list and runs workflows and activities.
type Worker struct {
	*registry
	service  api.Service
	domain   string
	taskList string
	opts     WorkerOptions
	logger   *zap.Logger

	mu        sync.Mutex
	started   bool
	ctx       context.Context
	cancel    context.CancelFunc
	stopC     chan struct{}
	wg        sync.WaitGroup
	laSem     chan struct{}
	laLimit   *rate.Limiter
	queryStop func()
	cache     *stickyCache
	// fullReplays counts decision tasks that replayed the whole history.
	fullReplays atomic.Int64
}

// NewWorker creates a worker for a domain and task list.
func NewWorker(service api.Service, domain, taskList string, options WorkerOptions) *Worker {
	options.defaults(taskList)
	w := &Worker{
		registry: newRegistry(globalRegistry),
		service:  service,
		domain:   domain,
		taskList: taskList,
		opts:     options,
		logger:   options.Logger.With(zap.String("Domain", domain), zap.String("TaskList", taskList), zap.String("WorkerID", options.Identity)),
		stopC:    make(chan struct{}),
		laSem:    make(chan struct{}, options.MaxConcurrentLocalActivityExecutionSize),
		laLimit:  limiter(options.WorkerLocalActivitiesPerSecond),
	}
	if !options.DisableStickyExecution {
		w.cache = newStickyCache(int(stickyCacheSize.Load()))
	}
	return w
}

func limiter(perSecond float64) *rate.Limiter {
	if perSecond <= 0 {
		return nil
	}
	return rate.NewLimiter(rate.Limit(perSecond), int(math.Max(1, perSecond)))
}

// Start starts polling. It returns without blocking.
func (w *Worker) Start() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.started {
		return errors.New("worker already started")
	}
	if w.domain == "" || w.taskList == "" {
		return errors.New("worker needs a domain and a task list")
	}
	workflows, activities := w.workflowNames(), w.activityNames()
	if len(workflows) == 0 && len(activities) == 0 {
		return errors.New("worker has no workflows or activities registered")
	}
	w.started = true
	w.ctx, w.cancel = context.WithCancel(context.Background())
	if !w.opts.DisableWorkflowWorker && len(workflows) > 0 {
		sem := make(chan struct{}, w.opts.MaxConcurrentDecisionTaskExecutionSize)
		lim := limiter(w.opts.WorkerDecisionTasksPerSecond)
		for range w.opts.MaxConcurrentDecisionTaskPollers {
			w.wg.Go(func() { w.pollLoop(sem, lim, w.pollDecision) })
		}
		if l, ok := w.service.(queryListener); ok {
			stop, err := l.ListenQueries(w.domain, w.taskList, w.handleQuery)
			if err != nil {
				w.cancel()
				return err
			}
			w.queryStop = stop
		}
	}
	if !w.opts.DisableActivityWorker && len(activities) > 0 {
		sem := make(chan struct{}, w.opts.MaxConcurrentActivityExecutionSize)
		perSecond := w.opts.WorkerActivitiesPerSecond
		if perSecond <= 0 {
			perSecond = w.opts.TaskListActivitiesPerSecond
		}
		lim := limiter(perSecond)
		for range w.opts.MaxConcurrentActivityTaskPollers {
			w.wg.Go(func() { w.pollLoop(sem, lim, w.pollActivity) })
		}
	}
	w.logger.Info("Started Worker", zap.Strings("workflows", workflows), zap.Strings("activities", activities))
	return nil
}

// Run starts the worker and blocks until SIGINT or SIGTERM.
func (w *Worker) Run() error {
	if err := w.Start(); err != nil {
		return err
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	w.Stop()
	return nil
}

// Stop stops polling and waits for running tasks up to WorkerStopTimeout.
func (w *Worker) Stop() {
	w.mu.Lock()
	if !w.started {
		w.mu.Unlock()
		return
	}
	w.started = false
	w.mu.Unlock()
	close(w.stopC)
	if w.queryStop != nil {
		w.queryStop()
	}
	w.cancel()
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(w.opts.WorkerStopTimeout):
		w.logger.Warn("worker stop timed out with tasks still running")
	}
	w.cache.clear()
	w.logger.Info("Stopped Worker")
}

// pollLoop polls with poll until the worker stops. poll returns a function
// that handles the task, or nil when there was no task.
func (w *Worker) pollLoop(sem chan struct{}, lim *rate.Limiter, poll func() (func(), error)) {
	for w.ctx.Err() == nil {
		if lim != nil && lim.Wait(w.ctx) != nil {
			return
		}
		select {
		case sem <- struct{}{}:
		case <-w.ctx.Done():
			return
		}
		handle, err := poll()
		if err != nil || handle == nil {
			<-sem
			if err != nil && w.ctx.Err() == nil {
				w.logger.Warn("poll failed", zap.Error(err))
				select {
				case <-time.After(time.Second):
				case <-w.ctx.Done():
				}
			}
			continue
		}
		w.wg.Go(func() {
			defer func() { <-sem }()
			handle()
		})
	}
}

func (w *Worker) pollDecision() (func(), error) {
	task, err := w.service.PollForDecisionTask(w.ctx, &api.PollForDecisionTaskRequest{
		Domain: w.domain, TaskList: &api.TaskList{Name: w.taskList}, Identity: w.opts.Identity,
	})
	if err != nil || len(task.TaskToken) == 0 {
		return nil, err
	}
	return func() { w.handleDecision(task) }, nil
}

func (w *Worker) pollActivity() (func(), error) {
	task, err := w.service.PollForActivityTask(w.ctx, &api.PollForActivityTaskRequest{
		Domain: w.domain, TaskList: &api.TaskList{Name: w.taskList}, Identity: w.opts.Identity,
	})
	if err != nil || len(task.TaskToken) == 0 {
		return nil, err
	}
	return func() { w.handleActivity(task) }, nil
}

// fullHistory fetches the remaining pages of a history.
func fullHistory(ctx context.Context, service api.Service, domain string, exec *api.WorkflowExecution, first []*api.HistoryEvent, token []byte) ([]*api.HistoryEvent, error) {
	events := slices.Clone(first)
	for len(token) > 0 {
		resp, err := service.GetWorkflowExecutionHistory(ctx, &api.GetWorkflowExecutionHistoryRequest{
			Domain: domain, Execution: exec, NextPageToken: token,
		})
		if err != nil {
			return nil, err
		}
		events = append(events, resp.History.Events...)
		token = resp.NextPageToken
	}
	return events, nil
}

// fetchHistory reads a whole history.
func fetchHistory(ctx context.Context, service api.Service, domain string, exec *api.WorkflowExecution) ([]*api.HistoryEvent, error) {
	resp, err := service.GetWorkflowExecutionHistory(ctx, &api.GetWorkflowExecutionHistoryRequest{Domain: domain, Execution: exec})
	if err != nil {
		return nil, err
	}
	return fullHistory(ctx, service, domain, exec, resp.History.Events, resp.NextPageToken)
}

func (w *Worker) newReplayer(exec *api.WorkflowExecution, logger *zap.Logger, la laExecutor) *replayer {
	return newReplayer(replayerConfig{
		domain:                w.domain,
		execution:             WorkflowExecution{ID: exec.WorkflowID, RunID: exec.RunID},
		registry:              w.registry,
		dataConverter:         w.opts.DataConverter,
		logger:                logger,
		scope:                 w.opts.MetricsScope,
		enableLoggingInReplay: w.opts.EnableLoggingInReplay,
		laExec:                la,
	})
}

func (w *Worker) handleDecision(task *api.PollForDecisionTaskResponse) {
	exec := task.WorkflowExecution
	logger := w.logger.With(zap.String("WorkflowType", task.WorkflowType.Name),
		zap.String("WorkflowID", exec.WorkflowID), zap.String("RunID", exec.RunID))
	timeout := 10 * time.Second
	if len(task.History.Events) > 0 {
		if a := task.History.Events[0].WorkflowExecutionStartedEventAttributes; a != nil && a.TaskStartToCloseTimeoutSeconds > 0 {
			timeout = time.Duration(a.TaskStartToCloseTimeoutSeconds) * time.Second
		}
	}
	ctx, cancel := context.WithTimeout(w.ctx, timeout)
	defer cancel()

	events, err := fullHistory(ctx, w.service, w.domain, exec, task.History.Events, task.NextPageToken)
	if err != nil {
		logger.Error("failed to fetch history", zap.Error(err))
		return
	}
	r, decisions, err := w.runDecisionTask(ctx, task, events, logger)
	if err != nil {
		r.close()
		w.decisionFailed(ctx, logger, task, err)
		return
	}
	_, err = w.service.RespondDecisionTaskCompleted(ctx, &api.RespondDecisionTaskCompletedRequest{
		TaskToken: task.TaskToken, Decisions: decisions, Identity: w.opts.Identity,
	})
	if err != nil || r.completed {
		r.close()
		if err != nil {
			logger.Warn("failed to complete decision task", zap.Error(err))
		}
		return
	}
	w.cache.put(&stickyEntry{runID: exec.RunID, r: r, consumed: len(events), lastStarted: task.StartedEventID, sent: decisions})
}

// runDecisionTask continues the cached replayer of the run when the
// previous decision task completed as cached, and replays from scratch
// otherwise.
func (w *Worker) runDecisionTask(ctx context.Context, task *api.PollForDecisionTaskResponse, events []*api.HistoryEvent, logger *zap.Logger) (*replayer, []*api.Decision, error) {
	if e := w.cache.take(task.WorkflowExecution.RunID); e != nil {
		if e.lastStarted == task.PreviousStartedEventID && e.consumed > 0 && e.consumed <= len(events) &&
			events[e.consumed-1].EventID == e.lastStarted {
			decisions, err := e.r.resume(ctx, events[e.consumed-1:], e.sent)
			if !errors.Is(err, errCacheMismatch) {
				return e.r, decisions, err
			}
		}
		e.r.close()
	}
	w.fullReplays.Add(1)
	r := w.newReplayer(task.WorkflowExecution, logger, w)
	decisions, err := r.replayHistory(ctx, events, modeTask)
	return r, decisions, err
}

func (w *Worker) decisionFailed(ctx context.Context, logger *zap.Logger, task *api.PollForDecisionTaskResponse, err error) {
	var (
		nd *nondeterminismError
		pe *PanicError
	)
	failWorkflow := (errors.As(err, &nd) && w.opts.NonDeterministicWorkflowPolicy == NonDeterministicWorkflowPolicyFailWorkflow) ||
		(errors.As(err, &pe) && w.opts.WorkflowPanicPolicy == FailWorkflow)
	if failWorkflow {
		logger.Error("failing workflow", zap.Error(err))
		reason, details := errorDetails(err, w.opts.DataConverter)
		if nd != nil {
			reason, details = "NonDeterministicWorkflowPolicyFailWorkflow", []byte(err.Error())
		}
		_, rerr := w.service.RespondDecisionTaskCompleted(ctx, &api.RespondDecisionTaskCompletedRequest{
			TaskToken: task.TaskToken,
			Identity:  w.opts.Identity,
			Decisions: []*api.Decision{{
				DecisionType:                            api.DecisionTypeFailWorkflowExecution,
				FailWorkflowExecutionDecisionAttributes: &api.FailWorkflowExecutionDecisionAttributes{Reason: reason, Details: details},
			}},
		})
		if rerr != nil {
			logger.Warn("failed to complete decision task", zap.Error(rerr))
		}
		return
	}
	logger.Error("decision task failed", zap.Error(err))
	details := err.Error()
	if pe != nil {
		details += "\n" + pe.StackTrace()
	}
	if rerr := w.service.RespondDecisionTaskFailed(ctx, &api.RespondDecisionTaskFailedRequest{
		TaskToken: task.TaskToken,
		Cause:     api.DecisionTaskFailedCauseWorkflowWorkerUnhandledFailure,
		Details:   []byte(details),
		Identity:  w.opts.Identity,
	}); rerr != nil {
		logger.Warn("failed to fail decision task", zap.Error(rerr))
	}
}

func (w *Worker) handleQuery(ctx context.Context, req *api.WorkerQueryRequest) *api.WorkerQueryResponse {
	events, err := fetchHistory(ctx, w.service, req.Domain, req.Execution)
	if err != nil {
		return &api.WorkerQueryResponse{ErrorMessage: err.Error()}
	}
	r := w.newReplayer(req.Execution, w.logger, nil)
	defer r.close()
	if _, err := r.replayHistory(ctx, events, modeQuery); err != nil {
		return &api.WorkerQueryResponse{ErrorMessage: err.Error()}
	}
	if r.queryHandler == nil {
		return &api.WorkerQueryResponse{ErrorMessage: "workflow has not run its first decision task yet"}
	}
	result, err := r.queryHandler(req.Query.QueryType, req.Query.QueryArgs)
	if err != nil {
		return &api.WorkerQueryResponse{ErrorMessage: err.Error()}
	}
	return &api.WorkerQueryResponse{Result: result}
}

func (w *Worker) handleActivity(task *api.PollForActivityTaskResponse) {
	name := task.ActivityType.Name
	logger := w.logger.With(zap.String("ActivityType", name), zap.String("ActivityID", task.ActivityID),
		zap.String("WorkflowID", task.WorkflowExecution.WorkflowID), zap.String("RunID", task.WorkflowExecution.RunID))
	scheduled := time.Unix(0, task.ScheduledTimestampOfThisAttempt)
	started := time.Unix(0, task.StartedTimestamp)
	deadline := started.Add(time.Duration(task.StartToCloseTimeoutSeconds) * time.Second)
	if s2c := scheduled.Add(time.Duration(task.ScheduleToCloseTimeoutSeconds) * time.Second); task.ScheduleToCloseTimeoutSeconds > 0 && s2c.Before(deadline) {
		deadline = s2c
	}
	env := &activityEnv{
		info: ActivityInfo{
			TaskToken:          task.TaskToken,
			WorkflowType:       &WorkflowType{Name: task.WorkflowType.Name},
			WorkflowDomain:     task.WorkflowDomain,
			WorkflowExecution:  WorkflowExecution{ID: task.WorkflowExecution.WorkflowID, RunID: task.WorkflowExecution.RunID},
			ActivityID:         task.ActivityID,
			ActivityType:       ActivityType{Name: name},
			TaskList:           w.taskList,
			HeartbeatTimeout:   time.Duration(task.HeartbeatTimeoutSeconds) * time.Second,
			ScheduledTimestamp: scheduled,
			StartedTimestamp:   started,
			Deadline:           deadline,
			Attempt:            task.Attempt,
		},
		service:          w.service,
		dc:               w.opts.DataConverter,
		logger:           logger,
		scope:            w.opts.MetricsScope,
		identity:         w.opts.Identity,
		heartbeatDetails: task.HeartbeatDetails,
		workerStop:       w.stopC,
	}
	ctx, cancel := context.WithDeadline(w.opts.BackgroundActivityContext, deadline)
	defer cancel()
	env.cancel = cancel
	ctx = context.WithValue(ctx, activityEnvKey, env)

	var result []byte
	var err error
	if fn, ok := w.getActivity(name); ok {
		result, err = runActivity(ctx, fn, task.Input, w.opts.DataConverter)
	} else {
		err = fmt.Errorf("unable to find activityType=%v. Supported types: %v", name, w.activityNames())
	}
	ctxErr := ctx.Err()
	env.stopHeartbeats()

	rctx, rcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer rcancel()
	var respondErr error
	switch {
	case errors.Is(err, ErrActivityResultPending):
		return
	case err != nil && errors.Is(ctxErr, context.DeadlineExceeded):
		// The server times the activity out; nothing to report.
		return
	case err != nil && env.wasCancelRequested() && (errors.Is(err, context.Canceled) || IsCanceledError(err)):
		var details []byte
		var ce *CanceledError
		if errors.As(err, &ce) {
			details = encodeDetails(ce.details, w.opts.DataConverter)
		}
		respondErr = w.service.RespondActivityTaskCanceled(rctx, &api.RespondActivityTaskCanceledRequest{
			TaskToken: task.TaskToken, Details: details, Identity: w.opts.Identity,
		})
	case err != nil:
		reason, details := errorDetails(err, w.opts.DataConverter)
		respondErr = w.service.RespondActivityTaskFailed(rctx, &api.RespondActivityTaskFailedRequest{
			TaskToken: task.TaskToken, Reason: reason, Details: details, Identity: w.opts.Identity,
		})
	default:
		respondErr = w.service.RespondActivityTaskCompleted(rctx, &api.RespondActivityTaskCompletedRequest{
			TaskToken: task.TaskToken, Result: result, Identity: w.opts.Identity,
		})
	}
	if respondErr != nil {
		logger.Warn("failed to report activity result", zap.Error(respondErr))
	}
}

// executeLocal implements laExecutor.
func (w *Worker) executeLocal(ctx context.Context, info *WorkflowInfo, p localActivityParams) laOutcome {
	select {
	case w.laSem <- struct{}{}:
		defer func() { <-w.laSem }()
	case <-ctx.Done():
		return laOutcome{err: ctx.Err()}
	}
	if w.laLimit != nil {
		if err := w.laLimit.Wait(ctx); err != nil {
			return laOutcome{err: err}
		}
	}
	return runLocalActivity(ctx, info, p, w.opts.DataConverter, w.logger, w.opts.MetricsScope, w.stopC)
}

// runLocalActivity runs a local activity with its retry policy.
func runLocalActivity(ctx context.Context, info *WorkflowInfo, p localActivityParams, dc DataConverter,
	logger *zap.Logger, scope tally.Scope, stop <-chan struct{}) laOutcome {
	scheduled := time.Now()
	deadline := scheduled.Add(p.options.ScheduleToCloseTimeout)
	var attempt int32
	for {
		env := &activityEnv{
			info: ActivityInfo{
				WorkflowType:       &info.WorkflowType,
				WorkflowDomain:     info.Domain,
				WorkflowExecution:  info.WorkflowExecution,
				ActivityID:         p.activityID,
				ActivityType:       ActivityType{Name: p.activityType},
				TaskList:           info.TaskListName,
				ScheduledTimestamp: scheduled,
				StartedTimestamp:   time.Now(),
				Deadline:           deadline,
				Attempt:            attempt,
			},
			dc:         dc,
			logger:     logger.With(zap.String("ActivityType", p.activityType), zap.String("ActivityID", p.activityID)),
			scope:      scope,
			isLocal:    true,
			workerStop: stop,
		}
		actx, cancel := context.WithDeadline(ctx, deadline)
		actx = context.WithValue(actx, activityEnvKey, env)
		result, err := runActivityArgs(actx, p.fn, p.args, dc)
		ctxErr := actx.Err()
		cancel()
		if err == nil {
			return laOutcome{result: result, attempt: attempt}
		}
		if errors.Is(ctxErr, context.DeadlineExceeded) {
			return laOutcome{err: NewTimeoutError(api.TimeoutTypeScheduleToClose), attempt: attempt}
		}
		backoff, ok := localBackoff(p.options.RetryPolicy, attempt, err, scheduled)
		if !ok || time.Now().Add(backoff).After(deadline) {
			return laOutcome{err: err, attempt: attempt}
		}
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return laOutcome{err: err, attempt: attempt}
		}
		attempt++
	}
}

func localBackoff(p *RetryPolicy, attempt int32, err error, start time.Time) (time.Duration, bool) {
	if p == nil {
		return 0, false
	}
	if p.MaximumAttempts > 0 && attempt+1 >= p.MaximumAttempts {
		return 0, false
	}
	reason, _ := errorDetails(err, nil)
	if slices.Contains(p.NonRetriableErrorReasons, reason) {
		return 0, false
	}
	coeff := p.BackoffCoefficient
	if coeff == 0 {
		coeff = 2
	}
	d := time.Duration(float64(p.InitialInterval) * math.Pow(coeff, float64(attempt)))
	if p.MaximumInterval > 0 && d > p.MaximumInterval {
		d = p.MaximumInterval
	}
	if p.ExpirationInterval > 0 && time.Since(start)+d > p.ExpirationInterval {
		return 0, false
	}
	return d, true
}
