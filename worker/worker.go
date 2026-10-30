// Package worker mirrors go.uber.org/cadence/worker: hosting workflow and
// activity implementations.
package worker

import (
	"context"
	"io"

	"go.uber.org/zap"

	"github.com/larsla/takt/api"
	"github.com/larsla/takt/internal/sdk"
)

type (
	// Registry registers workflows and activities.
	Registry interface {
		WorkflowRegistry
		ActivityRegistry
	}

	// WorkflowRegistry registers workflows.
	WorkflowRegistry interface {
		RegisterWorkflow(w interface{})
		RegisterWorkflowWithOptions(w interface{}, options sdk.RegisterWorkflowOptions)
		GetRegisteredWorkflows() []sdk.RegistryWorkflowInfo
	}

	// ActivityRegistry registers activities.
	ActivityRegistry interface {
		RegisterActivity(a interface{})
		RegisterActivityWithOptions(a interface{}, options sdk.RegisterActivityOptions)
		GetRegisteredActivities() []sdk.RegistryActivityInfo
	}

	// Worker polls a task list and runs workflows and activities.
	Worker interface {
		Registry
		// Start starts polling and returns.
		Start() error
		// Run starts polling and blocks until SIGINT or SIGTERM.
		Run() error
		// Stop stops polling and waits for running tasks.
		Stop()
	}

	// WorkflowReplayer replays histories against workflow code.
	WorkflowReplayer interface {
		WorkflowRegistry
		ActivityRegistry
		ReplayWorkflowHistory(logger *zap.Logger, history *api.History) error
		ReplayWorkflowHistoryFromJSON(logger *zap.Logger, reader io.Reader) error
		ReplayWorkflowHistoryFromJSONFile(logger *zap.Logger, jsonfileName string) error
		ReplayPartialWorkflowHistoryFromJSON(logger *zap.Logger, reader io.Reader, lastEventID int64) error
		ReplayPartialWorkflowHistoryFromJSONFile(logger *zap.Logger, jsonfileName string, lastEventID int64) error
		ReplayWorkflowExecution(ctx context.Context, service api.Service, logger *zap.Logger, domain string, execution sdk.WorkflowExecution) error
	}

	// ReplayOptions configures a WorkflowReplayer.
	ReplayOptions = sdk.ReplayOptions

	// Options configures a worker.
	Options = sdk.WorkerOptions
	// NonDeterministicWorkflowPolicy says what to do with nondeterministic workflows.
	NonDeterministicWorkflowPolicy = sdk.NonDeterministicWorkflowPolicy
	// WorkflowPanicPolicy says what to do when workflow code panics.
	WorkflowPanicPolicy = sdk.WorkflowPanicPolicy
	// AutoScalerOptions is accepted for compatibility; takt has no poller
	// autoscaler.
	AutoScalerOptions = sdk.AutoScalerOptions
)

const (
	NonDeterministicWorkflowPolicyBlockWorkflow = sdk.NonDeterministicWorkflowPolicyBlockWorkflow
	NonDeterministicWorkflowPolicyFailWorkflow  = sdk.NonDeterministicWorkflowPolicyFailWorkflow

	BlockWorkflow = sdk.BlockWorkflow
	FailWorkflow  = sdk.FailWorkflow
)

// New creates a worker. Build service with takt.NewServiceClient.
func New(service api.Service, domain, taskList string, options Options) Worker {
	return sdk.NewWorker(service, domain, taskList, options)
}

// NewV2 creates a worker after checking its options.
func NewV2(service api.Service, domain, taskList string, options Options) (Worker, error) {
	if err := options.Validate(); err != nil {
		return nil, err
	}
	return sdk.NewWorker(service, domain, taskList, options), nil
}

// AugmentWorkerOptions returns options with defaults filled in.
func AugmentWorkerOptions(options Options) Options { return sdk.AugmentWorkerOptions(options) }

// NewWorkflowReplayer creates a replayer with its own registry.
func NewWorkflowReplayer() WorkflowReplayer { return sdk.NewWorkflowReplayer() }

// NewWorkflowReplayerWithOptions creates a replayer with options.
func NewWorkflowReplayerWithOptions(options ReplayOptions) WorkflowReplayer {
	return sdk.NewWorkflowReplayerWithOptions(options)
}

// ReplayPartialWorkflowHistoryFromJSONFile replays a JSON history up to
// lastEventID against workflows registered globally.
func ReplayPartialWorkflowHistoryFromJSONFile(logger *zap.Logger, jsonfileName string, lastEventID int64) error {
	return sdk.ReplayPartialWorkflowHistoryFromJSONFile(logger, jsonfileName, lastEventID)
}

// ReplayWorkflowExecution fetches a workflow's history and replays it
// against workflows registered globally.
func ReplayWorkflowExecution(ctx context.Context, service api.Service, logger *zap.Logger, domain string, execution sdk.WorkflowExecution) error {
	return sdk.ReplayWorkflowExecution(ctx, service, logger, domain, execution)
}

// SetBinaryChecksum sets the checksum workers report with their decision
// tasks. Call it before creating workers.
func SetBinaryChecksum(checksum string) { sdk.SetBinaryChecksum(checksum) }

// ReplayWorkflowHistory replays a history against workflows registered
// globally (workflow.Register).
func ReplayWorkflowHistory(logger *zap.Logger, history *api.History) error {
	return sdk.ReplayWorkflowHistory(logger, history)
}

// ReplayWorkflowHistoryFromJSONFile replays a history stored as JSON.
func ReplayWorkflowHistoryFromJSONFile(logger *zap.Logger, jsonfileName string) error {
	return sdk.ReplayWorkflowHistoryFromJSONFile(logger, jsonfileName)
}

// EnableVerboseLogging is accepted for compatibility and does nothing.
func EnableVerboseLogging(bool) {}

// SetStickyWorkflowCacheSize sets how many workflow runs each worker keeps
// in memory between decision tasks. Call it before creating workers.
func SetStickyWorkflowCacheSize(cacheSize int) { sdk.SetStickyWorkflowCacheSize(cacheSize) }
