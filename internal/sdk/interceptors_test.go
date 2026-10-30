package sdk

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// recordingFactory builds interceptors that log the calls they see and
// upper-case the workflow's string result.
type recordingFactory struct {
	mu    sync.Mutex
	calls []string
}

func (f *recordingFactory) record(s string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, s)
}

func (f *recordingFactory) seen() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return strings.Join(f.calls, ",")
}

func (f *recordingFactory) NewInterceptor(info *WorkflowInfo, next WorkflowInterceptor) WorkflowInterceptor {
	return &recordingInterceptor{WorkflowInterceptorBase: WorkflowInterceptorBase{Next: next}, f: f}
}

type recordingInterceptor struct {
	WorkflowInterceptorBase
	f *recordingFactory
}

func (i *recordingInterceptor) ExecuteWorkflow(ctx Context, workflowType string, args ...interface{}) []interface{} {
	if !i.IsReplaying(ctx) {
		i.f.record("workflow")
	}
	out := i.Next.ExecuteWorkflow(ctx, workflowType, args...)
	if s, ok := out[0].(string); ok {
		out[0] = strings.ToUpper(s)
	}
	return out
}

func (i *recordingInterceptor) ExecuteActivity(ctx Context, activityType string, args ...interface{}) Future {
	if !i.IsReplaying(ctx) {
		i.f.record("activity:" + activityType[strings.LastIndex(activityType, ".")+1:])
	}
	return i.Next.ExecuteActivity(ctx, activityType, args...)
}

func (i *recordingInterceptor) GetVersion(ctx Context, changeID string, minSupported, maxSupported Version, opts ...GetVersionOption) Version {
	return i.Next.GetVersion(ctx, changeID, minSupported, maxSupported, append(opts, ExecuteWithMinVersion())...)
}

func interceptedWorkflow(ctx Context, name string) (string, error) {
	v := GetVersion(ctx, "change", 1, 3)
	ctx = WithActivityOptions(ctx, ActivityOptions{ScheduleToStartTimeout: time.Minute, StartToCloseTimeout: time.Minute})
	var greeting string
	if err := ExecuteActivity(ctx, greet, name).Get(ctx, &greeting); err != nil {
		return "", err
	}
	return greeting + " v" + string(rune('0'+v)), nil
}

func TestInterceptors(t *testing.T) {
	t.Parallel()
	e := newStackEnv(t)
	f := &recordingFactory{}
	e.worker(WorkerOptions{WorkflowInterceptorChainFactories: []WorkflowInterceptorFactory{f}}, func(w *Worker) {
		w.RegisterWorkflow(interceptedWorkflow)
		w.RegisterActivity(greet)
	})
	var result string
	if err := e.run("intercepted", interceptedWorkflow, "bob").Get(e.ctx, &result); err != nil {
		t.Fatal(err)
	}
	if result != "HELLO BOB! V1" {
		t.Fatalf("result %q", result)
	}
	if got := f.seen(); got != "workflow,activity:greet" {
		t.Fatalf("calls %q", got)
	}
}

func TestInterceptorsTestSuite(t *testing.T) {
	var s WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	f := &recordingFactory{}
	env.SetWorkerOptions(WorkerOptions{WorkflowInterceptorChainFactories: []WorkflowInterceptorFactory{f}})
	env.RegisterWorkflow(interceptedWorkflow)
	env.RegisterActivity(greet)
	env.ExecuteWorkflow(interceptedWorkflow, "ann")
	var result string
	if err := env.GetWorkflowResult(&result); err != nil || result != "HELLO ANN! V1" {
		t.Fatalf("result %q, err %v", result, err)
	}
}

func versionOptionsWorkflow(ctx Context) ([]Version, error) {
	return []Version{
		GetVersion(ctx, "a", DefaultVersion, 3),
		GetVersion(ctx, "b", 1, 3, ExecuteWithVersion(2)),
		GetVersion(ctx, "c", 1, 3, ExecuteWithMinVersion()),
		GetVersion(ctx, "b", 1, 3),
		GetVersion(ctx, "d", DefaultVersion, 3, ExecuteWithVersion(DefaultVersion)),
	}, nil
}

func TestGetVersionOptions(t *testing.T) {
	t.Parallel()
	want := "[3 2 1 2 -1]"
	var s WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(versionOptionsWorkflow)
	env.ExecuteWorkflow(versionOptionsWorkflow)
	var got []Version
	if err := env.GetWorkflowResult(&got); err != nil || fmtVersions(got) != want {
		t.Fatalf("testsuite: %v, err %v", got, err)
	}

	e := newStackEnv(t)
	e.worker(WorkerOptions{}, func(w *Worker) { w.RegisterWorkflow(versionOptionsWorkflow) })
	run := e.run("versions", versionOptionsWorkflow)
	if err := run.Get(e.ctx, &got); err != nil || fmtVersions(got) != want {
		t.Fatalf("worker: %v, err %v", got, err)
	}
	// The history replays to the same versions; DefaultVersion left no
	// marker.
	r := NewWorkflowReplayer()
	r.RegisterWorkflow(versionOptionsWorkflow)
	h := recordHistory(t, e, run)
	if err := r.ReplayWorkflowHistory(nil, h); err != nil {
		t.Fatal(err)
	}
	markers := 0
	for _, ev := range h.Events {
		if a := ev.MarkerRecordedEventAttributes; a != nil && a.MarkerName == versionMarkerName {
			markers++
		}
	}
	if markers != 3 {
		t.Fatalf("%d version markers, want 3", markers)
	}
}

func fmtVersions(v []Version) string { return fmt.Sprint(v) }
