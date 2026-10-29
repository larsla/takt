package sdk

// Channel is a workflow-safe channel. Use it instead of Go channels inside
// workflow code.
type Channel interface {
	// Receive blocks until a value arrives and assigns it to valuePtr (which
	// may be nil). It returns false once the channel is closed and drained.
	Receive(ctx Context, valuePtr interface{}) (more bool)
	// ReceiveAsync takes a value if one is ready.
	ReceiveAsync(valuePtr interface{}) (ok bool)
	// ReceiveAsyncWithMoreFlag is ReceiveAsync that also reports closing.
	ReceiveAsyncWithMoreFlag(valuePtr interface{}) (ok bool, more bool)
	// Send blocks until the value is accepted.
	Send(ctx Context, v interface{})
	// SendAsync sends if that can be done without blocking.
	SendAsync(v interface{}) (ok bool)
	// Close closes the channel.
	Close()
}

type blockedSend struct {
	value     interface{}
	delivered bool
}

type channelImpl struct {
	name   string
	size   int
	buffer []interface{}
	sends  []*blockedSend
	closed bool
	// waiting counts receivers (and selectors) currently blocked on it, so an
	// unbuffered SendAsync can hand a value over.
	waiting int
	dc      DataConverter
}

// NewChannel creates an unbuffered channel.
func NewChannel(ctx Context) Channel { return NewNamedChannel(ctx, "") }

// NewNamedChannel creates an unbuffered channel with a name shown in stack traces.
func NewNamedChannel(ctx Context, name string) Channel {
	return &channelImpl{name: name, dc: contextDataConverter(ctx)}
}

// NewBufferedChannel creates a channel with a buffer.
func NewBufferedChannel(ctx Context, size int) Channel { return NewNamedBufferedChannel(ctx, "", size) }

// NewNamedBufferedChannel creates a named channel with a buffer.
func NewNamedBufferedChannel(ctx Context, name string, size int) Channel {
	return &channelImpl{name: name, size: size, dc: contextDataConverter(ctx)}
}

func (c *channelImpl) label(op string) string {
	if c.name == "" {
		return "blocked on chan." + op
	}
	return "blocked on " + c.name + "." + op
}

// take removes the next value. ok is false if none is ready.
func (c *channelImpl) take() (v interface{}, ok bool, more bool) {
	if len(c.buffer) > 0 {
		v = c.buffer[0]
		c.buffer[0] = nil
		c.buffer = c.buffer[1:]
		c.promoteSend()
		return v, true, true
	}
	if len(c.sends) > 0 {
		s := c.sends[0]
		c.sends = c.sends[1:]
		s.delivered = true
		return s.value, true, true
	}
	if c.closed {
		return nil, false, false
	}
	return nil, false, true
}

// promoteSend moves a blocked sender into freed buffer space.
func (c *channelImpl) promoteSend() {
	if len(c.sends) > 0 && len(c.buffer) < c.size {
		s := c.sends[0]
		c.sends = c.sends[1:]
		s.delivered = true
		c.buffer = append(c.buffer, s.value)
	}
}

// ready reports whether Receive would return without blocking.
func (c *channelImpl) ready() bool {
	return len(c.buffer) > 0 || len(c.sends) > 0 || c.closed
}

func (c *channelImpl) Receive(ctx Context, valuePtr interface{}) bool {
	st := getState(ctx)
	c.waiting++
	defer func() { c.waiting-- }()
	for {
		v, ok, more := c.take()
		if ok || !more {
			st.unblocked()
			if ok {
				c.assign(v, valuePtr)
			}
			return more
		}
		st.yield(c.label("Receive"))
	}
}

func (c *channelImpl) assign(v, valuePtr interface{}) {
	if err := decodeAndAssign(c.dc, v, valuePtr); err != nil {
		panic(err)
	}
}

func (c *channelImpl) ReceiveAsync(valuePtr interface{}) bool {
	ok, _ := c.ReceiveAsyncWithMoreFlag(valuePtr)
	return ok
}

func (c *channelImpl) ReceiveAsyncWithMoreFlag(valuePtr interface{}) (bool, bool) {
	v, ok, more := c.take()
	if ok {
		c.assign(v, valuePtr)
	}
	return ok, more
}

func (c *channelImpl) canAccept() bool {
	return len(c.buffer) < c.size+c.waiting
}

func (c *channelImpl) Send(ctx Context, v interface{}) {
	if c.closed {
		panic("Closed channel")
	}
	st := getState(ctx)
	if c.canAccept() {
		c.buffer = append(c.buffer, v)
		st.unblocked()
		return
	}
	s := &blockedSend{value: v}
	c.sends = append(c.sends, s)
	for !s.delivered {
		if c.closed {
			panic("Closed channel")
		}
		st.yield(c.label("Send"))
	}
	st.unblocked()
}

func (c *channelImpl) SendAsync(v interface{}) bool {
	if c.closed {
		panic("Closed channel")
	}
	if !c.canAccept() {
		return false
	}
	c.buffer = append(c.buffer, v)
	return true
}

func (c *channelImpl) Close() { c.closed = true }

// Future is the result of an asynchronous operation.
type Future interface {
	// Get blocks until the future is ready and decodes its value into valuePtr.
	Get(ctx Context, valuePtr interface{}) error
	// IsReady reports whether Get would return without blocking.
	IsReady() bool
}

// Settable completes a Future.
type Settable interface {
	Set(value interface{}, err error)
	SetValue(value interface{})
	SetError(err error)
	// Chain makes the future complete with the result of another one.
	Chain(future Future)
}

type futureImpl struct {
	value   interface{}
	err     error
	ready   bool
	dc      DataConverter
	onReady []func()
}

// NewFuture creates a Future and the Settable that completes it.
func NewFuture(ctx Context) (Future, Settable) {
	f := &futureImpl{dc: contextDataConverter(ctx)}
	return f, f
}

func newFuture(dc DataConverter) *futureImpl { return &futureImpl{dc: dc} }

func (f *futureImpl) Get(ctx Context, valuePtr interface{}) error {
	st := getState(ctx)
	for !f.ready {
		st.yield("blocked on future.Get")
	}
	st.unblocked()
	if f.err != nil || f.value == nil || valuePtr == nil {
		return f.err
	}
	return decodeAndAssign(f.dc, f.value, valuePtr)
}

func (f *futureImpl) IsReady() bool { return f.ready }

func (f *futureImpl) Set(value interface{}, err error) {
	if f.ready {
		panic("already set")
	}
	f.value, f.err, f.ready = value, err, true
	cbs := f.onReady
	f.onReady = nil
	for _, cb := range cbs {
		cb()
	}
}

func (f *futureImpl) SetValue(value interface{}) { f.Set(value, nil) }

func (f *futureImpl) SetError(err error) { f.Set(nil, err) }

func (f *futureImpl) Chain(other Future) {
	if f.ready {
		panic("already set")
	}
	o, ok := other.(asyncFuture)
	if !ok {
		panic("cannot chain a future of a different implementation")
	}
	o.whenReady(func() {
		v, err := o.result()
		f.Set(v, err)
	})
}

// asyncFuture lets futures of different kinds chain to each other.
type asyncFuture interface {
	Future
	whenReady(func())
	result() (interface{}, error)
}

func (f *futureImpl) whenReady(cb func()) {
	if f.ready {
		cb()
		return
	}
	f.onReady = append(f.onReady, cb)
}

func (f *futureImpl) result() (interface{}, error) { return f.value, f.err }

// Selector waits on several channels and futures at once.
type Selector interface {
	AddReceive(c Channel, f func(c Channel, more bool)) Selector
	AddSend(c Channel, v interface{}, f func()) Selector
	AddFuture(future Future, f func(f Future)) Selector
	AddDefault(f func())
	// Select runs the callback of one ready case, blocking until there is one.
	Select(ctx Context)
	// HasPending reports whether a case is ready.
	HasPending() bool
}

type selectCase struct {
	receive   *channelImpl
	receiveFn func(c Channel, more bool)
	send      *channelImpl
	sendValue interface{}
	sendFn    func()
	future    Future
	futureFn  func(f Future)
	fired     bool
}

type selectorImpl struct {
	name      string
	cases     []*selectCase
	defaultFn func()
}

// NewSelector creates a Selector.
func NewSelector(Context) Selector { return &selectorImpl{} }

// NewNamedSelector creates a Selector with a name shown in stack traces.
func NewNamedSelector(_ Context, name string) Selector { return &selectorImpl{name: name} }

func (s *selectorImpl) AddReceive(c Channel, f func(c Channel, more bool)) Selector {
	s.cases = append(s.cases, &selectCase{receive: c.(*channelImpl), receiveFn: f})
	return s
}

func (s *selectorImpl) AddSend(c Channel, v interface{}, f func()) Selector {
	s.cases = append(s.cases, &selectCase{send: c.(*channelImpl), sendValue: v, sendFn: f})
	return s
}

func (s *selectorImpl) AddFuture(future Future, f func(f Future)) Selector {
	s.cases = append(s.cases, &selectCase{future: future, futureFn: f})
	return s
}

func (s *selectorImpl) AddDefault(f func()) { s.defaultFn = f }

func (s *selectorImpl) readyCase() *selectCase {
	for _, c := range s.cases {
		switch {
		case c.receive != nil:
			if c.receive.ready() {
				return c
			}
		case c.send != nil:
			if c.send.canAccept() {
				return c
			}
		case c.future != nil:
			if !c.fired && c.future.IsReady() {
				return c
			}
		}
	}
	return nil
}

func (s *selectorImpl) HasPending() bool { return s.readyCase() != nil }

func (s *selectorImpl) Select(ctx Context) {
	st := getState(ctx)
	for _, c := range s.cases {
		if c.receive != nil {
			c.receive.waiting++
		}
	}
	defer func() {
		for _, c := range s.cases {
			if c.receive != nil {
				c.receive.waiting--
			}
		}
	}()
	for {
		if c := s.readyCase(); c != nil {
			st.unblocked()
			s.run(c)
			return
		}
		if s.defaultFn != nil {
			st.unblocked()
			s.defaultFn()
			return
		}
		label := "blocked on selector"
		if s.name != "" {
			label = "blocked on " + s.name + ".Select"
		}
		st.yield(label)
	}
}

func (s *selectorImpl) run(c *selectCase) {
	switch {
	case c.receive != nil:
		if c.receiveFn != nil {
			c.receiveFn(c.receive, !c.receive.closed || len(c.receive.buffer) > 0 || len(c.receive.sends) > 0)
		}
	case c.send != nil:
		c.send.buffer = append(c.send.buffer, c.sendValue)
		if c.sendFn != nil {
			c.sendFn()
		}
	case c.future != nil:
		c.fired = true
		if c.futureFn != nil {
			c.futureFn(c.future)
		}
	}
}
