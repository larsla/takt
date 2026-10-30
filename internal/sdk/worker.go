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

	"github.com/google/uuid"
	"github.com/opentracing/opentracing-go"
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
	// TaskListActivitiesPerSecond limits the activities started per second
	// on the task list across all its workers. Each worker takes an equal
	// share, recounting the task list's pollers every 10 seconds, so the
	// limit is approximate while workers come and go.
	TaskListActivitiesPerSecond      float64
	MaxConcurrentActivityTaskPollers int
	// MinConcurrentActivityTaskPollers, MinConcurrentDecisionTaskPollers,
	// AutoScalerOptions and the PollerAutoScaler fields are accepted for
	// compatibility. takt has no poller autoscaler: a worker always runs
	// MaxConcurrent*TaskPollers pollers.
	MinConcurrentActivityTaskPollers       int
	MaxConcurrentDecisionTaskExecutionSize int
	WorkerDecisionTasksPerSecond           float64
	MaxConcurrentDecisionTaskPollers       int
	MinConcurrentDecisionTaskPollers       int
	PollerAutoScalerCooldown               time.Duration
	PollerAutoScalerTargetUtilization      float64
	PollerAutoScalerDryRun                 bool
	AutoScalerOptions                      AutoScalerOptions
	Identity                               string
	// IsolationGroup is accepted for compatibility and ignored: takt has no
	// isolation groups, and any worker on the task list can get any task.
	IsolationGroup        string
	MetricsScope          tally.Scope
	Logger                *zap.Logger
	EnableLoggingInReplay bool
	// EnableWorkflowLifecycleLogging logs when a workflow run starts and
	// closes on this worker.
	EnableWorkflowLifecycleLogging bool
	DisableWorkflowWorker          bool
	DisableActivityWorker          bool
	// DisableStickyExecution turns off the in-memory cache of workflow runs
	// (see SetStickyWorkflowCacheSize) and the worker's sticky task list.
	// StickyScheduleToStartTimeout is how long a decision task waits on the
	// sticky task list before it goes to any worker; it defaults to 5s.
	DisableStickyExecution         bool
	StickyScheduleToStartTimeout   time.Duration
	BackgroundActivityContext      context.Context
	NonDeterministicWorkflowPolicy NonDeterministicWorkflowPolicy
	WorkflowPanicPolicy            WorkflowPanicPolicy
	DataConverter                  DataConverter
	WorkerStopTimeout              time.Duration
	// ContextPropagators carry context values into workflows and activities
	// through headers; see ContextPropagator.
	ContextPropagators []ContextPropagator
	// WorkflowInterceptorChainFactories build an interceptor chain for each
	// workflow run; the first factory's interceptor is called first.
	WorkflowInterceptorChainFactories []WorkflowInterceptorFactory
	// Tracer, when set, records takt-ExecuteWorkflow, takt-ExecuteActivity
	// and takt-ExecuteLocalActivity spans and carries span contexts in
	// headers.
	Tracer opentracing.Tracer
	// EnableSessionWorker makes Start fail: takt doesn't support sessions.
	// MaxConcurrentSessionExecutionSize is ignored.
	EnableSessionWorker               bool
	MaxConcurrentSessionExecutionSize int
	// FeatureFlags and WorkerBugPorts are accepted for compatibility and
	// ignored.
	FeatureFlags   FeatureFlags
	WorkerBugPorts WorkerBugPorts
}

// AutoScalerOptions configures Cadence's poller autoscaler. takt accepts
// it for compatibility and ignores it.
type AutoScalerOptions struct {
	Enabled                  bool
	Cooldown                 time.Duration
	PollerMinCount           int
	PollerMaxCount           int
	PollerInitCount          int
	PollerWaitTimeLowerBound time.Duration
	PollerWaitTimeUpperBound time.Duration
}

// FeatureFlags toggles Cadence client features. takt accepts them for
// compatibility and ignores them.
type FeatureFlags struct {
	WorkflowExecutionAlreadyCompletedErrorEnabled bool
	AutoforwardingEnabled                         bool
	PollerAutoScalerEnabled                       bool
	EphemeralTaskListsEnabled                     bool
}

// WorkerBugPorts turns on old Cadence client behavior. takt accepts it for
// compatibility and ignores it.
type WorkerBugPorts struct {
	DisableStrictNonDeterminismCheck bool
}

// Validate reports options that a worker can't run with.
func (o WorkerOptions) Validate() error {
	switch {
	case !o.DisableStickyExecution && o.MaxConcurrentDecisionTaskPollers == 1:
		return errors.New("DecisionTaskPollers must be >= 2 or use default value")
	case o.EnableSessionWorker:
		return errors.New("sessions are not supported by takt: EnableSessionWorker must be false")
	}
	return nil
}

// AugmentWorkerOptions returns options with defaults filled in.
func AugmentWorkerOptions(o WorkerOptions) WorkerOptions {
	o.defaults("")
	return o
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
	if o.StickyScheduleToStartTimeout <= 0 {
		o.StickyScheduleToStartTimeout = 5 * time.Second
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
	// stickyTaskList is the task list only this worker polls, where the
	// server sends decision tasks of runs it has cached. Empty when sticky
	// execution is off.
	stickyTaskList string
	// props are the context propagators, the tracing one included.
	props []ContextPropagator
	// raw is the service as given; service records metrics around it.
	raw           api.Service
	scope         tally.Scope // tagged with domain and task list
	decisionScope tally.Scope
	activityScope tally.Scope
	// fullReplays counts decision tasks that replayed the whole history.
	fullReplays atomic.Int64
}

// NewWorker creates a worker for a domain and task list.
func NewWorker(service api.Service, domain, taskList string, options WorkerOptions) *Worker {
	options.defaults(taskList)
	scope := tagged(options.MetricsScope, tagDomain, domain, tagTaskList, taskList)
	w := &Worker{
		registry: newRegistry(globalRegistry),
		service:  withServiceMetrics(service, scope),
		raw:      service,
		domain:   domain,
		taskList: taskList,
		opts:     options,
		logger:   options.Logger.With(zap.String("Domain", domain), zap.String("TaskList", taskList), zap.String("WorkerID", options.Identity)),
		stopC:    make(chan struct{}),
		props:    withTracing(options.ContextPropagators, options.Tracer),
		laSem:    make(chan struct{}, options.MaxConcurrentLocalActivityExecutionSize),
		laLimit:  limiter(options.WorkerLocalActivitiesPerSecond),

		scope:         scope,
		decisionScope: tagged(scope, tagWorkerType, workerTypeDecision),
		activityScope: tagged(scope, tagWorkerType, workerTypeActivity),
	}
	if !options.DisableStickyExecution {
		w.cache = newStickyCache(int(stickyCacheSize.Load()))
		w.cache.scope = w.decisionScope
		w.cache.onEvict = func(es []*stickyEntry) { go w.resetSticky(es, 10*time.Second) }
		w.stickyTaskList = api.StickyTaskListPrefix + options.Identity + ":" + uuid.NewString()
	}
	return w
}

// resetSticky tells the server to stop sending the decision tasks of runs
// this worker dropped from its cache to its sticky task list, so they
// don't wait for the sticky timeout.
func (w *Worker) resetSticky(entries []*stickyEntry, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	sem := make(chan struct{}, 8)
	var wg sync.WaitGroup
	for _, e := range entries {
		if e.exec == nil {
			continue
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return
		}
		wg.Go(func() {
			defer func() { <-sem }()
			_, err := w.service.ResetStickyTaskList(ctx, &api.ResetStickyTaskListRequest{Domain: w.domain, Execution: e.exec})
			var nf *api.EntityNotExistsError
			if err != nil && !errors.As(err, &nf) && ctx.Err() == nil {
				w.logger.Debug("failed to reset sticky task list", zap.String("WorkflowID", e.exec.WorkflowID), zap.Error(err))
			}
		})
	}
	wg.Wait()
}

var binaryChecksum atomic.Value

// SetBinaryChecksum sets the checksum workers report with their decision
// tasks and show in WorkflowInfo.BinaryChecksum. Call it before creating
// workers.
func SetBinaryChecksum(checksum string) { binaryChecksum.Store(checksum) }

func getBinaryChecksum() string {
	s, _ := binaryChecksum.Load().(string)
	return s
}

// decisionPollers splits n decision pollers between the task list and
// the worker's sticky task list.
func decisionPollers(n int, sticky bool) (normal, onSticky int) {
	if !sticky {
		return n, 0
	}
	onSticky = max(1, n/2)
	return max(1, n-onSticky), onSticky
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
	if w.opts.EnableSessionWorker {
		return errors.New("sessions are not supported by takt: EnableSessionWorker must be false")
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
		w.decisionScope.Counter(MetricWorkerStart).Inc(1)
		normal, sticky := decisionPollers(w.opts.MaxConcurrentDecisionTaskPollers, w.stickyTaskList != "")
		for range normal {
			w.decisionScope.Counter(MetricPollerStart).Inc(1)
			tl := &api.TaskList{Name: w.taskList, Kind: api.TaskListKindNormal}
			w.wg.Go(func() { w.pollLoop(sem, func() (func(), error) { return w.pollDecision(tl) }, lim) })
		}
		for range sticky {
			w.decisionScope.Counter(MetricPollerStart).Inc(1)
			tl := &api.TaskList{Name: w.stickyTaskList, Kind: api.TaskListKindSticky}
			w.wg.Go(func() { w.pollLoop(sem, func() (func(), error) { return w.pollDecision(tl) }, lim) })
		}
		if l, ok := w.raw.(queryListener); ok {
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
		lim := limiter(w.opts.WorkerActivitiesPerSecond)
		tlLim := limiter(w.opts.TaskListActivitiesPerSecond)
		if tlLim != nil {
			w.wg.Go(func() { w.shareTaskListRate(tlLim) })
		}
		w.activityScope.Counter(MetricWorkerStart).Inc(1)
		for range w.opts.MaxConcurrentActivityTaskPollers {
			w.activityScope.Counter(MetricPollerStart).Inc(1)
			w.wg.Go(func() { w.pollLoop(sem, w.pollActivity, lim, tlLim) })
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
	// Hand the cached runs back to the task list right away.
	w.resetSticky(w.cache.clear(), 5*time.Second)
	w.logger.Info("Stopped Worker")
}

// taskListRateInterval is how often a worker recounts the pollers of its
// task list to share TaskListActivitiesPerSecond.
var taskListRateInterval = 10 * time.Second

// shareTaskListRate sets lim to this worker's share of
// TaskListActivitiesPerSecond: the total divided by the number of workers
// polling the task list for activities.
func (w *Worker) shareTaskListRate(lim *rate.Limiter) {
	total := w.opts.TaskListActivitiesPerSecond
	for {
		workers := 1
		resp, err := w.service.DescribeTaskList(w.ctx, &api.DescribeTaskListRequest{
			Domain: w.domain, TaskList: &api.TaskList{Name: w.taskList}, TaskListType: api.TaskListTypeActivity,
		})
		if err == nil {
			ids := map[string]bool{w.opts.Identity: true}
			for _, p := range resp.Pollers {
				ids[p.Identity] = true
			}
			workers = len(ids)
		}
		share := total / float64(workers)
		lim.SetLimit(rate.Limit(share))
		lim.SetBurst(int(math.Max(1, share)))
		select {
		case <-w.ctx.Done():
			return
		case <-time.After(taskListRateInterval):
		}
	}
}

// pollLoop polls with poll until the worker stops. poll returns a function
// that handles the task, or nil when there was no task. Each poll waits
// for all limiters.
func (w *Worker) pollLoop(sem chan struct{}, poll func() (func(), error), limiters ...*rate.Limiter) {
	for w.ctx.Err() == nil {
		for _, lim := range limiters {
			if lim != nil && lim.Wait(w.ctx) != nil {
				return
			}
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

func (w *Worker) pollDecision(tl *api.TaskList) (func(), error) {
	start := time.Now()
	task, err := w.service.PollForDecisionTask(w.ctx, &api.PollForDecisionTaskRequest{
		Domain: w.domain, TaskList: tl, Identity: w.opts.Identity,
	})
	gotTask := err == nil && len(task.TaskToken) > 0
	if w.ctx.Err() == nil {
		pollMetrics(w.decisionScope, MetricDecisionPoll, MetricDecisionPollFailed, MetricDecisionPollNoTask,
			MetricDecisionPollSucceed, MetricDecisionPollLatency, start, gotTask, err)
	}
	if !gotTask {
		return nil, err
	}
	if task.ScheduledTimestamp > 0 && task.StartedTimestamp >= task.ScheduledTimestamp {
		tagged(w.decisionScope, tagWorkflowType, task.WorkflowType.Name).Timer(MetricDecisionScheduledToStart).
			Record(time.Duration(task.StartedTimestamp - task.ScheduledTimestamp))
	}
	return func() { w.handleDecision(task) }, nil
}

func (w *Worker) pollActivity() (func(), error) {
	start := time.Now()
	task, err := w.service.PollForActivityTask(w.ctx, &api.PollForActivityTaskRequest{
		Domain: w.domain, TaskList: &api.TaskList{Name: w.taskList}, Identity: w.opts.Identity,
	})
	gotTask := err == nil && len(task.TaskToken) > 0
	if w.ctx.Err() == nil {
		pollMetrics(w.activityScope, MetricActivityPoll, MetricActivityPollFailed, MetricActivityPollNoTask,
			MetricActivityPollSucceed, MetricActivityPollLatency, start, gotTask, err)
	}
	if !gotTask {
		return nil, err
	}
	if task.ScheduledTimestampOfThisAttempt > 0 && task.StartedTimestamp >= task.ScheduledTimestampOfThisAttempt {
		tagged(w.activityScope, tagActivityType, task.ActivityType.Name).Timer(MetricActivityScheduledToStart).
			Record(time.Duration(task.StartedTimestamp - task.ScheduledTimestampOfThisAttempt))
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

// historyPages fetches the remaining pages of a decision task's history
// and records Cadence's get-history metrics when there were any.
func (w *Worker) historyPages(ctx context.Context, scope tally.Scope, exec *api.WorkflowExecution, first []*api.HistoryEvent, token []byte) ([]*api.HistoryEvent, error) {
	if len(token) == 0 {
		return first, nil
	}
	start := time.Now()
	scope.Counter(MetricWorkflowGetHistory).Inc(1)
	events, err := fullHistory(ctx, w.service, w.domain, exec, first, token)
	scope.Timer(MetricWorkflowGetHistoryLatency).Record(time.Since(start))
	if err != nil {
		scope.Counter(MetricWorkflowGetHistoryFailed).Inc(1)
	} else {
		scope.Counter(MetricWorkflowGetHistorySucceed).Inc(1)
	}
	return events, err
}

func (w *Worker) newReplayer(exec *api.WorkflowExecution, wfType string, logger *zap.Logger, la laExecutor) *replayer {
	return newReplayer(replayerConfig{
		domain:                w.domain,
		execution:             WorkflowExecution{ID: exec.WorkflowID, RunID: exec.RunID},
		registry:              w.registry,
		dataConverter:         w.opts.DataConverter,
		logger:                logger,
		scope:                 tagged(w.scope, tagWorkflowType, wfType),
		enableLoggingInReplay: w.opts.EnableLoggingInReplay,
		laExec:                la,
		propagators:           w.props,
		tracer:                w.opts.Tracer,
		interceptors:          w.opts.WorkflowInterceptorChainFactories,
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
	scope := tagged(w.decisionScope, tagWorkflowType, task.WorkflowType.Name)

	events, err := w.historyPages(ctx, scope, exec, task.History.Events, task.NextPageToken)
	if err != nil {
		logger.Error("failed to fetch history", zap.Error(err))
		return
	}
	start := time.Now()
	r, decisions, err := w.runDecisionTask(ctx, task, events, logger)
	scope.Timer(MetricDecisionExecutionLatency).Record(time.Since(start))
	if err != nil {
		r.close()
		scope.Counter(MetricDecisionExecutionFailed).Inc(1)
		w.decisionFailed(ctx, logger, scope, task, err)
		return
	}
	req := &api.RespondDecisionTaskCompletedRequest{
		TaskToken: task.TaskToken, Decisions: decisions, Identity: w.opts.Identity, BinaryChecksum: getBinaryChecksum(),
	}
	// Cache the run before answering: once the server has the answer, the
	// run's next decision task can reach this worker's sticky task list.
	var entry *stickyEntry
	if w.cache != nil && !r.completed {
		entry = &stickyEntry{runID: exec.RunID, exec: exec, r: r, consumed: len(events), lastStarted: task.StartedEventID, sent: decisions}
		w.cache.put(entry)
		req.StickyAttributes = &api.StickyExecutionAttributes{
			WorkerTaskList:                &api.TaskList{Name: w.stickyTaskList, Kind: api.TaskListKindSticky},
			ScheduleToStartTimeoutSeconds: durationSeconds(w.opts.StickyScheduleToStartTimeout),
		}
	}
	start = time.Now()
	// Stopping the worker doesn't cancel the answer: Stop waits for it and
	// then hands the cached runs back, which must come after it.
	rctx, rcancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer rcancel()
	_, err = w.service.RespondDecisionTaskCompleted(rctx, req)
	scope.Timer(MetricDecisionResponseLatency).Record(time.Since(start))
	if err == nil {
		scope.Counter(MetricDecisionTaskCompleted).Inc(1)
		workflowCloseMetrics(scope, decisions, workflowStartTime(events))
		if w.opts.EnableWorkflowLifecycleLogging {
			logLifecycle(logger, task, decisions)
		}
	} else {
		scope.Counter(MetricDecisionResponseFailed).Inc(1)
	}
	if err != nil {
		logger.Warn("failed to complete decision task", zap.Error(err))
	}
	switch {
	case entry == nil:
		r.close()
	case err != nil:
		// The server may have taken the answer anyway.
		if w.cache.drop(entry) {
			w.resetSticky([]*stickyEntry{entry}, 5*time.Second)
		}
	}
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
				w.cache.hit()
				return e.r, decisions, err
			}
		}
		e.r.close()
	}
	w.cache.miss()
	w.fullReplays.Add(1)
	r := w.newReplayer(task.WorkflowExecution, task.WorkflowType.Name, logger, w)
	decisions, err := r.replayHistory(ctx, events, modeTask)
	return r, decisions, err
}

// logLifecycle logs a run's first decision task and its close.
func logLifecycle(logger *zap.Logger, task *api.PollForDecisionTaskResponse, decisions []*api.Decision) {
	if task.PreviousStartedEventID == 0 {
		logger.Info("Workflow started")
	}
	for _, d := range decisions {
		switch d.DecisionType {
		case api.DecisionTypeCompleteWorkflowExecution, api.DecisionTypeFailWorkflowExecution,
			api.DecisionTypeCancelWorkflowExecution, api.DecisionTypeContinueAsNewWorkflowExecution:
			logger.Info("Workflow closed", zap.String("Decision", string(d.DecisionType)))
		}
	}
}

func (w *Worker) decisionFailed(ctx context.Context, logger *zap.Logger, scope tally.Scope, task *api.PollForDecisionTaskResponse, err error) {
	var (
		nd *NonDeterministicError
		pe *PanicError
	)
	if errors.As(err, &nd) {
		scope.Counter(MetricNonDeterministicError).Inc(1)
	}
	if errors.As(err, &pe) {
		scope.Counter(MetricDecisionTaskPanic).Inc(1)
	}
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
	r := w.newReplayer(req.Execution, "", w.logger, nil)
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
		scope:            tagged(w.scope, tagActivityType, name),
		identity:         w.opts.Identity,
		heartbeatDetails: task.HeartbeatDetails,
		lastDetails:      task.HeartbeatDetails,
		workerStop:       w.stopC,
	}
	ctx, cancel := context.WithDeadline(w.opts.BackgroundActivityContext, deadline)
	defer cancel()
	env.cancel = cancel
	ctx = context.WithValue(ctx, activityEnvKey, env)
	stopAuto := func() {}
	if env.info.HeartbeatTimeout > 0 && w.isAutoHeartbeat(name) {
		hbCtx, hbCancel := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			env.autoHeartbeat(hbCtx, env.info.HeartbeatTimeout/2)
		}()
		stopAuto = func() { hbCancel(); <-done }
	}

	scope := tagged(w.activityScope, tagActivityType, name)
	var result []byte
	execStart := time.Now()
	fn, ok := w.getActivity(name)
	pctx, err := extractHeader(ctx, w.props, task.Header)
	switch {
	case err != nil:
	case ok:
		if w.opts.Tracer != nil {
			var span opentracing.Span
			pctx, span = startSpanFromHeader(pctx, w.opts.Tracer, spanNameExecuteActivity, opentracing.Tags{
				tagSpanWorkflowType: task.WorkflowType.Name,
				tagSpanWorkflowID:   task.WorkflowExecution.WorkflowID,
				tagSpanRunID:        task.WorkflowExecution.RunID,
				tagSpanActivityType: name,
			})
			defer span.Finish()
		}
		result, err = runActivity(pctx, fn, task.Input, w.opts.DataConverter)
	default:
		err = fmt.Errorf("unable to find activityType=%v. Supported types: %v", name, w.activityNames())
	}
	ctxErr := ctx.Err()
	stopAuto()
	env.stopHeartbeats()
	scope.Timer(MetricActivityExecutionLatency).Record(time.Since(execStart))
	var pe *PanicError
	if errors.As(err, &pe) {
		scope.Counter(MetricActivityTaskPanic).Inc(1)
	}
	if err != nil && !errors.Is(err, ErrActivityResultPending) {
		scope.Counter(MetricActivityExecutionFailed).Inc(1)
	}

	rctx, rcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer rcancel()
	var respondErr error
	var outcome string
	respondStart := time.Now()
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
		outcome = MetricActivityTaskCanceled
		respondErr = w.service.RespondActivityTaskCanceled(rctx, &api.RespondActivityTaskCanceledRequest{
			TaskToken: task.TaskToken, Details: details, Identity: w.opts.Identity,
		})
	case err != nil:
		reason, details := errorDetails(err, w.opts.DataConverter)
		outcome = MetricActivityTaskFailed
		respondErr = w.service.RespondActivityTaskFailed(rctx, &api.RespondActivityTaskFailedRequest{
			TaskToken: task.TaskToken, Reason: reason, Details: details, Identity: w.opts.Identity, FailureOptions: failureOptions(err),
		})
	default:
		outcome = MetricActivityTaskCompleted
		respondErr = w.service.RespondActivityTaskCompleted(rctx, &api.RespondActivityTaskCompletedRequest{
			TaskToken: task.TaskToken, Result: result, Identity: w.opts.Identity,
		})
	}
	scope.Timer(MetricActivityResponseLatency).Record(time.Since(respondStart))
	if respondErr != nil {
		scope.Counter(MetricActivityResponseFailed).Inc(1)
		logger.Warn("failed to report activity result", zap.Error(respondErr))
		return
	}
	scope.Counter(outcome).Inc(1)
	scope.Timer(MetricActivityEndToEndLatency).Record(time.Since(scheduled))
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
	scope := tagged(w.scope, tagWorkerType, workerTypeLocalActivity, tagActivityType, p.activityType)
	scope.Counter(MetricLocalActivityTotal).Inc(1)
	start := time.Now()
	ctx, err := extractHeader(ctx, w.props, p.header)
	if err != nil {
		scope.Counter(MetricLocalActivityFailed).Inc(1)
		return laOutcome{err: err}
	}
	if w.opts.Tracer != nil {
		var span opentracing.Span
		ctx, span = startSpanFromHeader(ctx, w.opts.Tracer, spanNameExecuteLocalActivity, opentracing.Tags{
			tagSpanWorkflowType: info.WorkflowType.Name,
			tagSpanWorkflowID:   info.WorkflowExecution.ID,
			tagSpanRunID:        info.WorkflowExecution.RunID,
			tagSpanActivityType: p.activityType,
		})
		defer span.Finish()
	}
	out := runLocalActivity(ctx, info, p, w.opts.DataConverter, w.logger, tagged(w.scope, tagActivityType, p.activityType), w.stopC)
	scope.Timer(MetricLocalActivityExecutionLatency).Record(time.Since(start))
	var (
		pe *PanicError
		te *TimeoutError
	)
	switch {
	case out.err == nil:
	case errors.As(out.err, &pe):
		scope.Counter(MetricLocalActivityPanic).Inc(1)
	case errors.As(out.err, &te):
		scope.Counter(MetricLocalActivityTimeout).Inc(1)
	case errors.Is(out.err, context.Canceled) || IsCanceledError(out.err):
		scope.Counter(MetricLocalActivityCanceled).Inc(1)
	default:
		scope.Counter(MetricLocalActivityFailed).Inc(1)
	}
	return out
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
