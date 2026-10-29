package sdk

import (
	"time"
)

// Context is the workflow counterpart of context.Context. Its Done channel
// is a workflow Channel.
type Context interface {
	Deadline() (deadline time.Time, ok bool)
	Done() Channel
	Err() error
	Value(key interface{}) interface{}
}

// CancelFunc cancels a Context.
type CancelFunc func()

type emptyCtx struct{}

func (emptyCtx) Deadline() (time.Time, bool)   { return time.Time{}, false }
func (emptyCtx) Done() Channel                 { return nil }
func (emptyCtx) Err() error                    { return nil }
func (emptyCtx) Value(interface{}) interface{} { return nil }
func (emptyCtx) String() string                { return "workflow.Background" }

var background Context = emptyCtx{}

type valueCtx struct {
	Context
	key, val interface{}
}

// WithValue returns a copy of parent carrying key and val.
func WithValue(parent Context, key, val interface{}) Context {
	return &valueCtx{parent, key, val}
}

func (c *valueCtx) Value(key interface{}) interface{} {
	if c.key == key {
		return c.val
	}
	return c.Context.Value(key)
}

type cancelCtx struct {
	Context
	done      Channel
	err       error
	children  map[*cancelCtx]struct{}
	callbacks []func()
}

// WithCancel returns a copy of parent that can be canceled on its own and
// is canceled with its parent.
func WithCancel(parent Context) (Context, CancelFunc) {
	c := &cancelCtx{Context: parent}
	propagateCancel(parent, c)
	return c, func() { c.cancel(true, ErrCanceled) }
}

// NewDisconnectedContext returns a context with parent's values that is not
// canceled with parent. Use it for cleanup after cancellation.
func NewDisconnectedContext(parent Context) (Context, CancelFunc) {
	return WithCancel(&disconnectedCtx{parent})
}

type disconnectedCtx struct{ parent Context }

func (c *disconnectedCtx) Deadline() (time.Time, bool)       { return time.Time{}, false }
func (c *disconnectedCtx) Done() Channel                     { return nil }
func (c *disconnectedCtx) Err() error                        { return nil }
func (c *disconnectedCtx) Value(key interface{}) interface{} { return c.parent.Value(key) }

func (c *cancelCtx) Done() Channel {
	if c.done == nil {
		c.done = &channelImpl{name: "ctx.Done"}
		if c.err != nil {
			c.done.Close()
		}
	}
	return c.done
}

func (c *cancelCtx) Err() error { return c.err }

func (c *cancelCtx) cancel(removeFromParent bool, err error) {
	if c.err != nil {
		return
	}
	c.err = err
	if c.done != nil {
		c.done.Close()
	}
	for child := range c.children {
		child.cancel(false, err)
	}
	c.children = nil
	cbs := c.callbacks
	c.callbacks = nil
	for _, cb := range cbs {
		cb()
	}
	if removeFromParent {
		if p := parentCancelCtx(c.Context); p != nil {
			delete(p.children, c)
		}
	}
}

func propagateCancel(parent Context, child *cancelCtx) {
	p := parentCancelCtx(parent)
	if p == nil {
		return
	}
	if p.err != nil {
		child.cancel(false, p.err)
		return
	}
	if p.children == nil {
		p.children = map[*cancelCtx]struct{}{}
	}
	p.children[child] = struct{}{}
}

// parentCancelCtx finds the nearest cancelCtx, stopping at disconnects.
func parentCancelCtx(parent Context) *cancelCtx {
	for {
		switch c := parent.(type) {
		case *cancelCtx:
			return c
		case *valueCtx:
			parent = c.Context
		default:
			return nil
		}
	}
}

// onCancel runs cb when ctx is canceled, right away if it already is.
func onCancel(ctx Context, cb func()) {
	c := parentCancelCtx(ctx)
	if c == nil {
		return
	}
	if c.err != nil {
		cb()
		return
	}
	c.callbacks = append(c.callbacks, cb)
}
