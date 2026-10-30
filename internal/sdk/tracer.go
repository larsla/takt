package sdk

import (
	"context"
	"time"

	"github.com/opentracing/opentracing-go"
)

// Span names and tags, Cadence's with a takt prefix.
const (
	spanNameExecuteWorkflow      = "takt-ExecuteWorkflow"
	spanNameExecuteActivity      = "takt-ExecuteActivity"
	spanNameExecuteLocalActivity = "takt-ExecuteLocalActivity"

	tagSpanWorkflowType = "taktWorkflowType"
	tagSpanActivityType = "taktActivityType"
	tagSpanWorkflowID   = "taktWorkflowID"
	tagSpanRunID        = "taktRunID"
	tagSpanIsCron       = "taktIsCron"
)

const activeSpanContextKey contextKey = "activeSpanContext"

// GetSpanContext returns the tracing span context of a workflow context,
// or nil.
func GetSpanContext(ctx Context) opentracing.SpanContext {
	sc, _ := ctx.Value(activeSpanContextKey).(opentracing.SpanContext)
	return sc
}

// WithSpanContext returns a workflow context with a tracing span context,
// which activities and child workflows started from it continue.
func WithSpanContext(ctx Context, sc opentracing.SpanContext) Context {
	return WithValue(ctx, activeSpanContextKey, sc)
}

type tracingReader struct{ r HeaderReader }

func (t tracingReader) ForeachKey(handler func(key, val string) error) error {
	return t.r.ForEachKey(func(k string, v []byte) error { return handler(k, string(v)) })
}

type tracingWriter struct{ w HeaderWriter }

func (t tracingWriter) Set(key, val string) { t.w.Set(key, []byte(val)) }

// tracingPropagator carries span contexts in headers. The worker and
// client add it when they have a Tracer.
type tracingPropagator struct{ tracer opentracing.Tracer }

func (t tracingPropagator) Inject(ctx context.Context, w HeaderWriter) error {
	span := opentracing.SpanFromContext(ctx)
	if span == nil {
		return nil
	}
	return t.tracer.Inject(span.Context(), opentracing.TextMap, tracingWriter{w})
}

func (t tracingPropagator) Extract(ctx context.Context, r HeaderReader) (context.Context, error) {
	sc, err := t.tracer.Extract(opentracing.TextMap, tracingReader{r})
	if err != nil {
		// No span in the header.
		return ctx, nil
	}
	return context.WithValue(ctx, activeSpanContextKey, sc), nil
}

func (t tracingPropagator) InjectFromWorkflow(ctx Context, w HeaderWriter) error {
	sc := GetSpanContext(ctx)
	if sc == nil {
		return nil
	}
	return t.tracer.Inject(sc, opentracing.TextMap, tracingWriter{w})
}

func (t tracingPropagator) ExtractToWorkflow(ctx Context, r HeaderReader) (Context, error) {
	sc, err := t.tracer.Extract(opentracing.TextMap, tracingReader{r})
	if err != nil {
		return ctx, nil
	}
	return WithSpanContext(ctx, sc), nil
}

// withTracing returns props with the tracing propagator added when there
// is a tracer.
func withTracing(props []ContextPropagator, tracer opentracing.Tracer) []ContextPropagator {
	if tracer == nil {
		return props
	}
	return append(append([]ContextPropagator(nil), props...), tracingPropagator{tracer})
}

func isNoopTracer(tracer opentracing.Tracer) bool {
	if tracer == nil {
		return true
	}
	_, ok := tracer.(opentracing.NoopTracer)
	return ok
}

// startSpan starts a span that follows from parent, if any, and returns
// ctx carrying it.
func startSpan(ctx context.Context, tracer opentracing.Tracer, name string, tags opentracing.Tags,
	parent opentracing.SpanContext) (context.Context, opentracing.Span) {
	opts := []opentracing.StartSpanOption{opentracing.StartTime(time.Now()), tags}
	if parent != nil {
		opts = append(opts, opentracing.FollowsFrom(parent))
	}
	span := tracer.StartSpan(name, opts...)
	if !isNoopTracer(tracer) {
		ctx = opentracing.ContextWithSpan(ctx, span)
	}
	return ctx, span
}

// startSpanFromHeader starts a span for work that a header's span context
// (put in ctx by tracingPropagator.Extract) asked for.
func startSpanFromHeader(ctx context.Context, tracer opentracing.Tracer, name string, tags opentracing.Tags) (context.Context, opentracing.Span) {
	parent, _ := ctx.Value(activeSpanContextKey).(opentracing.SpanContext)
	return startSpan(ctx, tracer, name, tags, parent)
}

// startClientSpan starts and finishes the span of a workflow start. The
// returned context carries it, so the start's header points at it.
func startClientSpan(ctx context.Context, tracer opentracing.Tracer, name, workflowID string, cron bool) context.Context {
	if tracer == nil {
		return ctx
	}
	var parent opentracing.SpanContext
	if span := opentracing.SpanFromContext(ctx); span != nil {
		parent = span.Context()
	}
	ctx, span := startSpan(ctx, tracer, name, opentracing.Tags{tagSpanWorkflowID: workflowID, tagSpanIsCron: cron}, parent)
	// Finish right away: the span can't be recreated during replay, so it
	// only marks the start.
	span.Finish()
	return ctx
}
