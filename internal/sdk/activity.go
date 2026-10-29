package sdk

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/uber-go/tally"
	"go.uber.org/zap"

	"github.com/larsla/takt/api"
)

// ActivityType names an activity.
type ActivityType struct {
	Name string
}

// ActivityInfo describes the running activity.
type ActivityInfo struct {
	TaskToken          []byte
	WorkflowType       *WorkflowType
	WorkflowDomain     string
	WorkflowExecution  WorkflowExecution
	ActivityID         string
	ActivityType       ActivityType
	TaskList           string
	HeartbeatTimeout   time.Duration
	ScheduledTimestamp time.Time
	StartedTimestamp   time.Time
	Deadline           time.Time
	Attempt            int32
}

// activityEnv is stored in an activity's context.
type activityEnv struct {
	info             ActivityInfo
	service          api.Service
	dc               DataConverter
	logger           *zap.Logger
	scope            tally.Scope
	identity         string
	heartbeatDetails []byte
	workerStop       <-chan struct{}
	isLocal          bool

	mu              sync.Mutex
	cancel          context.CancelFunc
	cancelRequested bool
	lastSent        time.Time
	pending         []byte
	hasPending      bool
	flushTimer      *time.Timer
}

// GetActivityInfo returns information about the running activity.
func GetActivityInfo(ctx context.Context) ActivityInfo { return getActivityEnv(ctx).info }

// GetActivityLogger returns the activity's logger.
func GetActivityLogger(ctx context.Context) *zap.Logger { return getActivityEnv(ctx).logger }

// GetActivityMetricsScope returns the activity's metrics scope.
func GetActivityMetricsScope(ctx context.Context) tally.Scope { return getActivityEnv(ctx).scope }

// GetWorkerStopChannel returns a channel closed when the worker stops.
func GetWorkerStopChannel(ctx context.Context) <-chan struct{} { return getActivityEnv(ctx).workerStop }

// HasHeartbeatDetails reports whether a previous attempt left heartbeat details.
func HasHeartbeatDetails(ctx context.Context) bool {
	return len(getActivityEnv(ctx).heartbeatDetails) > 0
}

// GetHeartbeatDetails decodes the heartbeat details of the previous attempt.
func GetHeartbeatDetails(ctx context.Context, d ...interface{}) error {
	env := getActivityEnv(ctx)
	if len(env.heartbeatDetails) == 0 {
		return ErrNoData
	}
	return env.dc.FromData(env.heartbeatDetails, d...)
}

// RecordActivityHeartbeat reports progress. Calls are throttled; the latest
// details are always sent. A canceled activity sees its context canceled.
func RecordActivityHeartbeat(ctx context.Context, details ...interface{}) {
	env := getActivityEnv(ctx)
	if env.isLocal {
		return
	}
	data, err := encodeArgs(env.dc, details)
	if err != nil {
		env.logger.Error("failed to encode heartbeat details", zap.Error(err))
		return
	}
	env.heartbeat(data)
}

func (env *activityEnv) throttle() time.Duration {
	if env.info.HeartbeatTimeout > 0 {
		return env.info.HeartbeatTimeout * 4 / 5
	}
	return 30 * time.Second
}

func (env *activityEnv) heartbeat(data []byte) {
	env.mu.Lock()
	if wait := env.throttle() - time.Since(env.lastSent); wait > 0 {
		env.pending, env.hasPending = data, true
		if env.flushTimer == nil {
			env.flushTimer = time.AfterFunc(wait, env.flush)
		}
		env.mu.Unlock()
		return
	}
	env.lastSent = time.Now()
	env.mu.Unlock()
	env.send(data)
}

func (env *activityEnv) flush() {
	env.mu.Lock()
	env.flushTimer = nil
	if !env.hasPending {
		env.mu.Unlock()
		return
	}
	data := env.pending
	env.pending, env.hasPending = nil, false
	env.lastSent = time.Now()
	env.mu.Unlock()
	env.send(data)
}

func (env *activityEnv) stopHeartbeats() {
	env.mu.Lock()
	if env.flushTimer != nil {
		env.flushTimer.Stop()
		env.flushTimer = nil
	}
	env.hasPending = false
	env.mu.Unlock()
}

func (env *activityEnv) send(data []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := env.service.RecordActivityTaskHeartbeat(ctx, &api.RecordActivityTaskHeartbeatRequest{
		TaskToken: env.info.TaskToken, Details: data, Identity: env.identity,
	})
	var nf *api.EntityNotExistsError
	switch {
	case errors.As(err, &nf):
		// The activity timed out or the workflow is gone; stop the work.
		env.requestCancel()
	case err != nil:
		env.logger.Warn("heartbeat failed", zap.Error(err))
	case resp.CancelRequested:
		env.requestCancel()
	}
}

func (env *activityEnv) requestCancel() {
	env.mu.Lock()
	env.cancelRequested = true
	cancel := env.cancel
	env.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (env *activityEnv) wasCancelRequested() bool {
	env.mu.Lock()
	defer env.mu.Unlock()
	return env.cancelRequested
}

// runActivity calls an activity function with its encoded input.
func runActivity(ctx context.Context, fn interface{}, input []byte, dc DataConverter) (result []byte, err error) {
	fv := reflect.ValueOf(fn)
	ft := fv.Type()
	skip := skipContext(fn)
	args, err := decodeArgs(dc, ft, input, skip)
	if err != nil {
		return nil, fmt.Errorf("unable to decode the activity function input: %w", err)
	}
	if skip == 1 {
		args = append([]reflect.Value{reflect.ValueOf(ctx)}, args...)
	}
	defer func() {
		if p := recover(); p != nil {
			result, err = nil, newPanicError(p, stack())
		}
	}()
	out, err := callFn(fv, args)
	if err != nil {
		return nil, err
	}
	if ft.NumOut() == 1 {
		return nil, nil
	}
	return encodeArgs(dc, []interface{}{out})
}

// runActivityArgs calls an activity with in-process arguments (local activities).
func runActivityArgs(ctx context.Context, fn interface{}, args []interface{}, dc DataConverter) ([]byte, error) {
	input, err := encodeArgs(dc, args)
	if err != nil {
		return nil, err
	}
	return runActivity(ctx, fn, input, dc)
}
