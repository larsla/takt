// Package worker mirrors go.uber.org/cadence/worker: hosting workflow and
// activity implementations.
package worker

import (
	"context"

	"go.uber.org/zap"

	"github.com/larsla/takt/api"
	"github.com/larsla/takt/internal/sdk"
)

type (
	// Registry registers workflows and activities.
	Registry interface {
		RegisterWorkflow(w interface{})
		RegisterWorkflowWithOptions(w interface{}, options sdk.RegisterWorkflowOptions)
		RegisterActivity(a interface{})
		RegisterActivityWithOptions(a interface{}, options sdk.RegisterActivityOptions)
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
		RegisterWorkflow(w interface{})
		RegisterWorkflowWithOptions(w interface{}, options sdk.RegisterWorkflowOptions)
		ReplayWorkflowHistory(logger *zap.Logger, history *api.History) error
		ReplayWorkflowHistoryFromJSONFile(logger *zap.Logger, jsonfileName string) error
		ReplayWorkflowExecution(ctx context.Context, service api.Service, logger *zap.Logger, domain string, execution sdk.WorkflowExecution) error
	}

	// Options configures a worker.
	Options = sdk.WorkerOptions
	// NonDeterministicWorkflowPolicy says what to do with nondeterministic workflows.
	NonDeterministicWorkflowPolicy = sdk.NonDeterministicWorkflowPolicy
	// WorkflowPanicPolicy says what to do when workflow code panics.
	WorkflowPanicPolicy = sdk.WorkflowPanicPolicy
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

// NewWorkflowReplayer creates a replayer with its own registry.
func NewWorkflowReplayer() WorkflowReplayer { return sdk.NewWorkflowReplayer() }

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
