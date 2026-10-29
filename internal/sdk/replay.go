package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"go.uber.org/zap"

	"github.com/larsla/takt/api"
)

// WorkflowReplayer replays recorded histories against workflow code to
// catch nondeterministic changes before they are deployed.
type WorkflowReplayer struct {
	*registry
}

// NewWorkflowReplayer creates a replayer with its own registry, backed by
// the global one.
func NewWorkflowReplayer() *WorkflowReplayer {
	return &WorkflowReplayer{registry: newRegistry(globalRegistry)}
}

// ReplayWorkflowHistory replays a history and returns an error if the
// workflow code does not produce the same decisions.
func (r *WorkflowReplayer) ReplayWorkflowHistory(logger *zap.Logger, history *api.History) error {
	if history == nil || len(history.Events) == 0 {
		return errors.New("empty history")
	}
	first := history.Events[0]
	if first.EventType != api.EventTypeWorkflowExecutionStarted || first.WorkflowExecutionStartedEventAttributes == nil {
		return errors.New("first event is not WorkflowExecutionStarted")
	}
	runID := "ReplayRunId"
	if a := first.WorkflowExecutionStartedEventAttributes; a.Initiator == "" && a.FirstExecutionRunID != "" {
		runID = a.FirstExecutionRunID
	}
	rep := newReplayer(replayerConfig{
		domain:    "ReplayDomain",
		execution: WorkflowExecution{ID: "ReplayId", RunID: runID},
		registry:  r.registry,
		logger:    logger,
	})
	defer rep.close()
	_, err := rep.replayHistory(context.Background(), history.Events, modeReplay)
	return err
}

// ReplayWorkflowHistoryFromJSONFile replays a history stored as JSON, either
// a list of events (as the Cadence CLI writes) or {"events": [...]}.
func (r *WorkflowReplayer) ReplayWorkflowHistoryFromJSONFile(logger *zap.Logger, jsonfileName string) error {
	h, err := historyFromFile(jsonfileName)
	if err != nil {
		return err
	}
	return r.ReplayWorkflowHistory(logger, h)
}

func historyFromFile(name string) (*api.History, error) {
	data, err := os.ReadFile(filepath.Clean(name))
	if err != nil {
		return nil, err
	}
	var events []*api.HistoryEvent
	if err := json.Unmarshal(data, &events); err == nil {
		return &api.History{Events: events}, nil
	}
	var h api.History
	if err := json.Unmarshal(data, &h); err != nil {
		return nil, fmt.Errorf("parse history file %s: %w", name, err)
	}
	return &h, nil
}

// ReplayWorkflowHistory replays a history against the global registry.
func ReplayWorkflowHistory(logger *zap.Logger, history *api.History) error {
	return (&WorkflowReplayer{registry: globalRegistry}).ReplayWorkflowHistory(logger, history)
}

// ReplayWorkflowHistoryFromJSONFile replays a JSON history against the
// global registry.
func ReplayWorkflowHistoryFromJSONFile(logger *zap.Logger, jsonfileName string) error {
	return (&WorkflowReplayer{registry: globalRegistry}).ReplayWorkflowHistoryFromJSONFile(logger, jsonfileName)
}
