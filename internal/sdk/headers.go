package sdk

import (
	"context"
	"fmt"

	"github.com/larsla/takt/api"
)

// HeaderWriter writes header fields.
type HeaderWriter interface {
	Set(string, []byte)
}

// HeaderReader reads header fields.
type HeaderReader interface {
	ForEachKey(handler func(string, []byte) error) error
}

// ContextPropagator carries values from a context across workflow and
// activity boundaries in headers. Inject and InjectFromWorkflow write a
// context's values into a header when a workflow, activity or child
// workflow is started; Extract and ExtractToWorkflow put them into the
// context the activity or workflow runs with. An error from an Extract
// method fails the activity, or the decision task of the workflow.
//
// The values travel: client (Inject) -> workflow (ExtractToWorkflow) ->
// activity, local activity, child or continued run (InjectFromWorkflow) ->
// activity (Extract) or child workflow (ExtractToWorkflow).
type ContextPropagator interface {
	Inject(context.Context, HeaderWriter) error
	Extract(context.Context, HeaderReader) (context.Context, error)
	InjectFromWorkflow(Context, HeaderWriter) error
	ExtractToWorkflow(Context, HeaderReader) (Context, error)
}

type headerReader struct{ header *api.Header }

func (r *headerReader) ForEachKey(handler func(string, []byte) error) error {
	if r.header == nil {
		return nil
	}
	for k, v := range r.header.Fields {
		if err := handler(k, v); err != nil {
			return err
		}
	}
	return nil
}

// NewHeaderReader returns a HeaderReader over header, which may be nil.
func NewHeaderReader(header *api.Header) HeaderReader { return &headerReader{header} }

type headerWriter struct{ header *api.Header }

func (w *headerWriter) Set(key string, value []byte) {
	if w.header == nil {
		return
	}
	w.header.Fields[key] = value
}

// NewHeaderWriter returns a HeaderWriter that writes into header.
func NewHeaderWriter(header *api.Header) HeaderWriter {
	if header != nil && header.Fields == nil {
		header.Fields = map[string][]byte{}
	}
	return &headerWriter{header}
}

const propagatorsKey contextKey = "contextPropagators"

func contextPropagators(ctx Context) []ContextPropagator {
	props, _ := ctx.Value(propagatorsKey).([]ContextPropagator)
	return props
}

// workflowHeader runs InjectFromWorkflow of the workflow's propagators. It
// returns nil when there are none or they wrote nothing.
func workflowHeader(ctx Context) (*api.Header, error) {
	props := contextPropagators(ctx)
	if len(props) == 0 {
		return nil, nil
	}
	h := &api.Header{Fields: map[string][]byte{}}
	for _, p := range props {
		if err := p.InjectFromWorkflow(ctx, NewHeaderWriter(h)); err != nil {
			return nil, fmt.Errorf("unable to propagate context: %w", err)
		}
	}
	if len(h.Fields) == 0 {
		return nil, nil
	}
	return h, nil
}

// extractToWorkflow sets up a workflow's root context from its start
// header.
func extractToWorkflow(ctx Context, props []ContextPropagator, header *api.Header) (Context, error) {
	ctx = WithValue(ctx, propagatorsKey, props)
	for _, p := range props {
		var err error
		if ctx, err = p.ExtractToWorkflow(ctx, NewHeaderReader(header)); err != nil {
			return nil, fmt.Errorf("unable to propagate context: %w", err)
		}
	}
	return ctx, nil
}

// injectHeader runs Inject of the client's propagators.
func injectHeader(ctx context.Context, props []ContextPropagator) (*api.Header, error) {
	if len(props) == 0 {
		return nil, nil
	}
	h := &api.Header{Fields: map[string][]byte{}}
	for _, p := range props {
		if err := p.Inject(ctx, NewHeaderWriter(h)); err != nil {
			return nil, fmt.Errorf("unable to propagate context: %w", err)
		}
	}
	if len(h.Fields) == 0 {
		return nil, nil
	}
	return h, nil
}

// extractHeader runs Extract of the worker's propagators for an activity.
func extractHeader(ctx context.Context, props []ContextPropagator, header *api.Header) (context.Context, error) {
	for _, p := range props {
		var err error
		if ctx, err = p.Extract(ctx, NewHeaderReader(header)); err != nil {
			return nil, fmt.Errorf("unable to propagate context: %w", err)
		}
	}
	return ctx, nil
}
