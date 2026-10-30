package sdk

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/larsla/takt/api"
	"github.com/larsla/takt/internal/servertest"
)

type propKey struct{}

// stringPropagator carries a string stored under propKey in the header
// field "prop".
type stringPropagator struct{}

func (stringPropagator) Inject(ctx context.Context, w HeaderWriter) error {
	if v, ok := ctx.Value(propKey{}).(string); ok {
		w.Set("prop", []byte(v))
	}
	return nil
}

func (stringPropagator) Extract(ctx context.Context, r HeaderReader) (context.Context, error) {
	err := r.ForEachKey(func(k string, v []byte) error {
		if k == "prop" {
			if string(v) == "bad" {
				return errors.New("bad value")
			}
			ctx = context.WithValue(ctx, propKey{}, string(v))
		}
		return nil
	})
	return ctx, err
}

func (stringPropagator) InjectFromWorkflow(ctx Context, w HeaderWriter) error {
	if v, ok := ctx.Value(propKey{}).(string); ok {
		w.Set("prop", []byte(v))
	}
	return nil
}

func (stringPropagator) ExtractToWorkflow(ctx Context, r HeaderReader) (Context, error) {
	err := r.ForEachKey(func(k string, v []byte) error {
		if k == "prop" {
			ctx = WithValue(ctx, propKey{}, string(v))
		}
		return nil
	})
	return ctx, err
}

func propActivity(ctx context.Context) (string, error) {
	v, _ := ctx.Value(propKey{}).(string)
	return v, nil
}

func propChild(ctx Context) (string, error) {
	v, _ := ctx.Value(propKey{}).(string)
	return v, nil
}

// propWorkflow returns the propagated value as seen by itself, an
// activity, a local activity and a child workflow.
func propWorkflow(ctx Context) ([]string, error) {
	v, _ := ctx.Value(propKey{}).(string)
	out := []string{v}
	actx := WithActivityOptions(ctx, ActivityOptions{ScheduleToStartTimeout: time.Minute, StartToCloseTimeout: time.Minute})
	var s string
	if err := ExecuteActivity(actx, propActivity).Get(ctx, &s); err != nil {
		return nil, err
	}
	out = append(out, s)
	lctx := WithLocalActivityOptions(ctx, LocalActivityOptions{ScheduleToCloseTimeout: time.Minute})
	if err := ExecuteLocalActivity(lctx, propActivity).Get(ctx, &s); err != nil {
		return nil, err
	}
	out = append(out, s)
	cctx := WithChildOptions(ctx, ChildWorkflowOptions{ExecutionStartToCloseTimeout: time.Minute})
	if err := ExecuteChildWorkflow(cctx, propChild).Get(ctx, &s); err != nil {
		return nil, err
	}
	return append(out, s), nil
}

func TestContextPropagation(t *testing.T) {
	t.Parallel()
	e := newStackEnv(t)
	props := []ContextPropagator{stringPropagator{}}
	e.worker(WorkerOptions{ContextPropagators: props}, func(w *Worker) {
		w.RegisterWorkflow(propWorkflow)
		w.RegisterWorkflow(propChild)
		w.RegisterActivity(propActivity)
	})
	c := NewClient(e.stack.Client, servertest.Domain, &ClientOptions{ContextPropagators: props})
	opts := e.options("propagation")
	run, err := c.ExecuteWorkflow(context.WithValue(e.ctx, propKey{}, "corr-1"), opts, propWorkflow)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	if err := run.Get(e.ctx, &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 || got[0] != "corr-1" || got[1] != "corr-1" || got[2] != "corr-1" || got[3] != "corr-1" {
		t.Fatalf("propagated values %q", got)
	}
}

func TestContextPropagationTestSuite(t *testing.T) {
	var s WorkflowTestSuite
	s.SetContextPropagators([]ContextPropagator{stringPropagator{}})
	s.SetHeader(&api.Header{Fields: map[string][]byte{"prop": []byte("corr-2")}})
	env := s.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(propWorkflow)
	env.RegisterWorkflow(propChild)
	env.RegisterActivity(propActivity)
	env.ExecuteWorkflow(propWorkflow)
	var got []string
	if err := env.GetWorkflowResult(&got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 || got[0] != "corr-2" || got[3] != "corr-2" || got[1] != "corr-2" || got[2] != "corr-2" {
		t.Fatalf("propagated values %q", got)
	}

	aenv := s.NewTestActivityEnvironment()
	aenv.RegisterActivity(propActivity)
	val, err := aenv.ExecuteActivity(propActivity)
	var v string
	if err != nil || val.Get(&v) != nil || v != "corr-2" {
		t.Fatalf("activity env: %q %v", v, err)
	}

	s.SetHeader(&api.Header{Fields: map[string][]byte{"prop": []byte("bad")}})
	aenv = s.NewTestActivityEnvironment()
	aenv.RegisterActivity(propActivity)
	if _, err := aenv.ExecuteActivity(propActivity); err == nil {
		t.Fatal("bad header should fail the activity")
	}
}
