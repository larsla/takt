// Package activity mirrors go.uber.org/cadence/activity: helpers for
// activity implementations.
package activity

import (
	"context"

	"github.com/uber-go/tally"
	"go.uber.org/zap"

	"github.com/larsla/takt/internal/sdk"
)

type (
	// Info describes the running activity.
	Info = sdk.ActivityInfo
	// Type names an activity.
	Type = sdk.ActivityType
	// RegisterOptions configures activity registration.
	RegisterOptions = sdk.RegisterActivityOptions
	// RegistryInfo describes a registered activity.
	RegistryInfo = sdk.RegistryActivityInfo
)

// ErrResultPending, returned from an activity, means it will be completed
// later through client.CompleteActivity.
var ErrResultPending = sdk.ErrActivityResultPending

// Register registers an activity function or struct with all workers.
func Register(activityFunc interface{}) { sdk.RegisterActivity(activityFunc) }

// RegisterWithOptions registers an activity with options.
func RegisterWithOptions(activityFunc interface{}, opts RegisterOptions) {
	sdk.RegisterActivityWithOptions(activityFunc, opts)
}

// GetInfo returns information about the running activity.
func GetInfo(ctx context.Context) Info { return sdk.GetActivityInfo(ctx) }

// HasInfo reports whether ctx is an activity's context.
func HasInfo(ctx context.Context) bool { return sdk.HasActivityInfo(ctx) }

// GetLogger returns the activity's logger.
func GetLogger(ctx context.Context) *zap.Logger { return sdk.GetActivityLogger(ctx) }

// GetMetricsScope returns the activity's metrics scope.
func GetMetricsScope(ctx context.Context) tally.Scope { return sdk.GetActivityMetricsScope(ctx) }

// RecordHeartbeat reports progress and lets the activity notice cancellation.
func RecordHeartbeat(ctx context.Context, details ...interface{}) {
	sdk.RecordActivityHeartbeat(ctx, details...)
}

// HasHeartbeatDetails reports whether a previous attempt left heartbeat details.
func HasHeartbeatDetails(ctx context.Context) bool { return sdk.HasHeartbeatDetails(ctx) }

// GetHeartbeatDetails decodes the heartbeat details of the previous attempt.
func GetHeartbeatDetails(ctx context.Context, d ...interface{}) error {
	return sdk.GetHeartbeatDetails(ctx, d...)
}

// GetWorkerStopChannel returns a channel closed when the worker stops.
func GetWorkerStopChannel(ctx context.Context) <-chan struct{} { return sdk.GetWorkerStopChannel(ctx) }
