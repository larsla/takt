package sdk

import (
	"context"
	"testing"
	"time"
)

func aliasedActivity(_ context.Context, s string) (string, error) { return "activity:" + s, nil }

func aliasedWorkflow(ctx Context, s string) (string, error) { return "workflow:" + s, nil }

// remoteAliasedWorkflow runs an activity by the name a process that didn't
// register it would send: the function name.
func remoteAliasedWorkflow(ctx Context, s string) (string, error) {
	ctx = WithActivityOptions(ctx, shortActivity)
	var out string
	err := ExecuteActivity(ctx, getFunctionName(aliasedActivity), s).Get(ctx, &out)
	return out, err
}

// A client starts a workflow by function value that only a worker in the
// same process registered, under another name.
func TestStartByFunctionWorkerAlias(t *testing.T) {
	t.Parallel()
	e := newStackEnv(t)
	e.worker(WorkerOptions{}, func(w *Worker) {
		w.RegisterWorkflowWithOptions(aliasedWorkflow, RegisterWorkflowOptions{Name: "analysis"})
	})
	run := e.run("alias-local", aliasedWorkflow, "x")
	var out string
	if err := run.Get(e.ctx, &out); err != nil || out != "workflow:x" {
		t.Fatalf("result %q, err %v", out, err)
	}
	desc, err := e.client.DescribeWorkflowExecution(e.ctx, run.GetID(), run.GetRunID())
	if err != nil {
		t.Fatal(err)
	}
	if got := desc.WorkflowExecutionInfo.Type.Name; got != "analysis" {
		t.Fatalf("workflow type %q, want analysis", got)
	}
}

// A client in another process sends the function name; the worker finds
// the workflow and activity registered under other names.
func TestStartByFunctionNameFallback(t *testing.T) {
	t.Parallel()
	e := newStackEnv(t)
	e.worker(WorkerOptions{}, func(w *Worker) {
		w.RegisterWorkflowWithOptions(remoteAliasedWorkflow, RegisterWorkflowOptions{Name: "remote-analysis"})
		w.RegisterActivityWithOptions(aliasedActivity, RegisterActivityOptions{Name: "analyse"})
	})
	run, err := e.client.ExecuteWorkflow(e.ctx, e.options("alias-remote"), getFunctionName(remoteAliasedWorkflow), "y")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(e.ctx, 30*time.Second)
	defer cancel()
	var out string
	if err := run.Get(ctx, &out); err != nil || out != "activity:y" {
		t.Fatalf("result %q, err %v", out, err)
	}
}

func TestWorkerAliasAmbiguous(t *testing.T) {
	shareWorkflowAlias("example.com/x.Ambiguous", "a")
	if n, ok := workerAlias("example.com/x.Ambiguous"); !ok || n != "a" {
		t.Fatalf("got %q %v", n, ok)
	}
	shareWorkflowAlias("example.com/x.Ambiguous", "b")
	if _, ok := workerAlias("example.com/x.Ambiguous"); ok {
		t.Fatal("two names should not resolve")
	}
	if got := clientWorkflowName("plain-name"); got != "plain-name" {
		t.Fatalf("got %q", got)
	}
}
