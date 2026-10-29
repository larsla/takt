package sdk

import (
	"context"
	"testing"
	"time"

	"github.com/larsla/takt/internal/servertest"
)

const testTaskList = "sdk-tl"

type testEnv struct {
	t      *testing.T
	stack  *servertest.Env
	client *Client
	ctx    context.Context
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	stack := servertest.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	return &testEnv{t: t, stack: stack, client: NewClient(stack.Client, servertest.Domain, nil), ctx: ctx}
}

// worker starts a worker with the given registrations.
func (e *testEnv) worker(opts WorkerOptions, register func(w *Worker)) *Worker {
	e.t.Helper()
	w := NewWorker(e.stack.Client, servertest.Domain, testTaskList, opts)
	register(w)
	if err := w.Start(); err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(w.Stop)
	return w
}

func (e *testEnv) options(id string) StartWorkflowOptions {
	return StartWorkflowOptions{
		ID:                              id,
		TaskList:                        testTaskList,
		ExecutionStartToCloseTimeout:    time.Minute,
		DecisionTaskStartToCloseTimeout: 10 * time.Second,
	}
}

func (e *testEnv) run(id string, wf interface{}, args ...interface{}) WorkflowRun {
	e.t.Helper()
	run, err := e.client.ExecuteWorkflow(e.ctx, e.options(id), wf, args...)
	if err != nil {
		e.t.Fatal(err)
	}
	return run
}

func greet(_ context.Context, name string) (string, error) { return "Hello " + name + "!", nil }

func helloWorkflow(ctx Context, name string) (string, error) {
	ctx = WithActivityOptions(ctx, ActivityOptions{ScheduleToStartTimeout: time.Minute, StartToCloseTimeout: time.Minute})
	var greeting string
	if err := ExecuteActivity(ctx, greet, name).Get(ctx, &greeting); err != nil {
		return "", err
	}
	return greeting, nil
}

func TestHelloWorld(t *testing.T) {
	t.Parallel()
	e := newTestEnv(t)
	e.worker(WorkerOptions{}, func(w *Worker) {
		w.RegisterWorkflow(helloWorkflow)
		w.RegisterActivity(greet)
	})
	var result string
	if err := e.run("hello", helloWorkflow, "takt").Get(e.ctx, &result); err != nil {
		t.Fatal(err)
	}
	if result != "Hello takt!" {
		t.Fatalf("result %q", result)
	}
}
