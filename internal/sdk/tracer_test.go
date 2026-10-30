package sdk

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/mocktracer"

	"github.com/larsla/takt/internal/servertest"
)

func tracedActivity(ctx context.Context) (bool, error) {
	return opentracing.SpanFromContext(ctx) != nil, nil
}

func tracedWorkflow(ctx Context) (bool, error) {
	if GetSpanContext(ctx) == nil {
		return false, nil
	}
	actx := WithActivityOptions(ctx, ActivityOptions{ScheduleToStartTimeout: time.Minute, StartToCloseTimeout: time.Minute})
	var traced bool
	if err := ExecuteActivity(actx, tracedActivity).Get(ctx, &traced); err != nil || !traced {
		return false, err
	}
	lctx := WithLocalActivityOptions(ctx, LocalActivityOptions{ScheduleToCloseTimeout: time.Minute})
	err := ExecuteLocalActivity(lctx, tracedActivity).Get(ctx, &traced)
	return traced, err
}

func TestTracer(t *testing.T) {
	t.Parallel()
	e := newStackEnv(t)
	tracer := mocktracer.New()
	e.worker(WorkerOptions{Tracer: tracer}, func(w *Worker) {
		w.RegisterWorkflow(tracedWorkflow)
		w.RegisterActivity(tracedActivity)
	})
	c := NewClient(e.stack.Client, servertest.Domain, &ClientOptions{Tracer: tracer})
	parent := tracer.StartSpan("caller")
	ctx := opentracing.ContextWithSpan(e.ctx, parent)
	run, err := c.ExecuteWorkflow(ctx, e.options("traced"), tracedWorkflow)
	if err != nil {
		t.Fatal(err)
	}
	var traced bool
	if err := run.Get(e.ctx, &traced); err != nil || !traced {
		t.Fatalf("traced %v, err %v", traced, err)
	}
	parent.Finish()

	// The workflow span finishes when the workflow function returns, just
	// after the result is reported.
	deadline := time.Now().Add(5 * time.Second)
	var spans map[string]*mocktracer.MockSpan
	for {
		spans = map[string]*mocktracer.MockSpan{}
		for _, s := range tracer.FinishedSpans() {
			spans[s.OperationName] = s
		}
		if spans[spanNameExecuteWorkflow] != nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	var start *mocktracer.MockSpan
	for name, s := range spans {
		if strings.HasPrefix(name, "StartWorkflow-") {
			start = s
		}
	}
	wf, act, la := spans[spanNameExecuteWorkflow], spans[spanNameExecuteActivity], spans[spanNameExecuteLocalActivity]
	if start == nil || wf == nil || act == nil || la == nil {
		t.Fatalf("missing spans: %v", spans)
	}
	trace := parent.Context().(mocktracer.MockSpanContext).TraceID
	for _, s := range []*mocktracer.MockSpan{start, wf, act, la} {
		if s.SpanContext.TraceID != trace {
			t.Errorf("%s is in trace %d, want %d", s.OperationName, s.SpanContext.TraceID, trace)
		}
	}
	if wf.ParentID != start.SpanContext.SpanID || act.ParentID != wf.SpanContext.SpanID || la.ParentID != wf.SpanContext.SpanID {
		t.Errorf("parents: workflow %d (start %d), activity %d and local %d (workflow %d)",
			wf.ParentID, start.SpanContext.SpanID, act.ParentID, la.ParentID, wf.SpanContext.SpanID)
	}
}
