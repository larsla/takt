package sdk

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/larsla/takt/api"
)

// recordHistory runs a workflow to completion and returns its history.
func recordHistory(t *testing.T, e *stackEnv, run WorkflowRun) *api.History {
	t.Helper()
	if err := run.Get(e.ctx, nil); err != nil {
		t.Fatal(err)
	}
	it := e.client.GetWorkflowHistory(e.ctx, run.GetID(), run.GetRunID(), false, api.HistoryEventFilterTypeAllEvent)
	h := &api.History{}
	for it.HasNext() {
		ev, err := it.Next()
		if err != nil {
			t.Fatal(err)
		}
		h.Events = append(h.Events, ev)
	}
	return h
}

func TestReplayDetectsChanges(t *testing.T) {
	t.Parallel()
	e := newStackEnv(t)
	e.worker(WorkerOptions{}, func(w *Worker) {
		w.RegisterWorkflowWithOptions(helloWorkflow, RegisterWorkflowOptions{Name: "greeter"})
		w.RegisterWorkflow(replayHeavyWorkflow)
		w.RegisterWorkflow(localActivityWorkflow)
		w.RegisterWorkflow(childWorkflow)
		w.RegisterWorkflow(parentWorkflow)
		w.RegisterActivity(greet)
	})
	hello := recordHistory(t, e, e.run("replay-hello", "greeter", "x"))
	heavy := recordHistory(t, e, e.run("replay-heavy", replayHeavyWorkflow))
	local := recordHistory(t, e, e.run("replay-local", localActivityWorkflow))
	parent := recordHistory(t, e, e.run("replay-parent", parentWorkflow, 2))

	same := NewWorkflowReplayer()
	same.RegisterWorkflowWithOptions(helloWorkflow, RegisterWorkflowOptions{Name: "greeter"})
	same.RegisterWorkflow(replayHeavyWorkflow)
	same.RegisterWorkflow(localActivityWorkflow)
	same.RegisterWorkflow(parentWorkflow)
	for name, h := range map[string]*api.History{"hello": hello, "heavy": heavy, "local": local, "parent": parent} {
		if err := same.ReplayWorkflowHistory(nil, h); err != nil {
			t.Errorf("%s: unchanged code failed to replay: %v", name, err)
		}
	}

	changed := NewWorkflowReplayer()
	changed.RegisterWorkflowWithOptions(waitThenGreetV2, RegisterWorkflowOptions{Name: "greeter"})
	err := changed.ReplayWorkflowHistory(nil, hello)
	if err == nil || !strings.Contains(err.Error(), "nondeterministic") {
		t.Fatalf("changed code replayed without error: %v", err)
	}
	var nd *NonDeterministicError
	if !errors.As(err, &nd) || nd.Reason == "" || nd.WorkflowType != "greeter" || nd.WorkflowID == "" {
		t.Fatalf("nondeterministic error %+v", nd)
	}

	// File variants: a bare event list and a {"events": ...} object.
	dir := t.TempDir()
	list, _ := json.Marshal(hello.Events)
	obj, _ := json.Marshal(hello)
	for name, data := range map[string][]byte{"list.json": list, "object.json": obj} {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := same.ReplayWorkflowHistoryFromJSONFile(nil, path); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	if err := same.ReplayWorkflowHistoryFromJSONFile(nil, filepath.Join(dir, "missing.json")); err == nil {
		t.Error("missing file should fail")
	}
	if err := same.ReplayWorkflowHistory(nil, &api.History{}); err == nil {
		t.Error("empty history should fail")
	}
}

func TestSplitHistory(t *testing.T) {
	ev := func(id int64, et api.EventType) *api.HistoryEvent {
		return &api.HistoryEvent{EventID: id, EventType: et}
	}
	events := []*api.HistoryEvent{
		ev(1, api.EventTypeWorkflowExecutionStarted),
		ev(2, api.EventTypeDecisionTaskScheduled),
		ev(3, api.EventTypeDecisionTaskStarted),
		ev(4, api.EventTypeDecisionTaskFailed),
		ev(5, api.EventTypeWorkflowExecutionSignaled),
		ev(6, api.EventTypeDecisionTaskScheduled),
		ev(7, api.EventTypeDecisionTaskStarted),
		ev(8, api.EventTypeDecisionTaskCompleted),
		ev(9, api.EventTypeTimerStarted),
		ev(10, api.EventTypeMarkerRecorded),
		ev(11, api.EventTypeTimerFired),
		ev(12, api.EventTypeDecisionTaskScheduled),
		ev(13, api.EventTypeDecisionTaskStarted),
	}
	bs := splitHistory(events)
	if len(bs) != 2 {
		t.Fatalf("got %d batches", len(bs))
	}
	if !bs[0].completed || bs[0].started.EventID != 7 || len(bs[0].inputs) != 2 || len(bs[0].decisionEvents) != 2 {
		t.Fatalf("first batch %+v", bs[0])
	}
	if bs[1].completed || bs[1].started.EventID != 13 || len(bs[1].inputs) != 1 || bs[1].inputs[0].EventID != 11 {
		t.Fatalf("second batch %+v", bs[1])
	}
}
