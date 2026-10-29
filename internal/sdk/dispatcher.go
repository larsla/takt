package sdk

import (
	"fmt"
	"runtime"
	"strings"
	"time"
)

// The dispatcher runs workflow goroutines (coroutines) one at a time, in
// creation order, until all of them are blocked. That keeps workflow code
// deterministic across replays.

// deadlockTimeout is how long a coroutine may run without yielding before
// the dispatcher gives up on it.
var deadlockTimeout = time.Second

type dispatcher struct {
	coroutines []*coroutine
	sequence   int
	closed     bool
	executing  bool
}

type coroutine struct {
	name    string
	d       *dispatcher
	unblock chan func(status string) bool
	// aboutToBlock receives when the coroutine yields or exits.
	aboutToBlock chan struct{}
	closed       bool
	keptBlocked  bool
	panicErr     *PanicError
}

func newDispatcher() *dispatcher { return &dispatcher{} }

// newCoroutine creates a coroutine that runs f the next time the dispatcher
// executes.
func (d *dispatcher) newCoroutine(ctx Context, name string, f func(ctx Context)) {
	d.sequence++
	if name == "" {
		name = fmt.Sprintf("%d", d.sequence)
	}
	c := &coroutine{
		name:         name,
		d:            d,
		unblock:      make(chan func(string) bool),
		aboutToBlock: make(chan struct{}),
	}
	ctx = WithValue(ctx, coroutineKey, c)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				c.panicErr = newPanicError(r, stack())
			}
			c.closed = true
			c.aboutToBlock <- struct{}{}
		}()
		c.initialYield("created")
		f(ctx)
	}()
	d.coroutines = append(d.coroutines, c)
}

// executeUntilAllBlocked runs coroutines until none can make progress.
func (d *dispatcher) executeUntilAllBlocked() error {
	if d.closed {
		return nil
	}
	d.executing = true
	defer func() { d.executing = false }()
	allBlocked := false
	for !allBlocked {
		allBlocked = true
		last := d.sequence
		for i := 0; i < len(d.coroutines); i++ {
			c := d.coroutines[i]
			if err := c.call(); err != nil {
				return err
			}
			if c.panicErr != nil {
				return c.panicErr
			}
			if c.closed {
				d.coroutines = append(d.coroutines[:i], d.coroutines[i+1:]...)
				i--
				allBlocked = false
				continue
			}
			allBlocked = allBlocked && c.keptBlocked
		}
		if last != d.sequence {
			allBlocked = false
		}
		if d.closed {
			return nil
		}
	}
	return nil
}

// close stops all coroutines.
func (d *dispatcher) close() {
	if d.closed {
		return
	}
	d.closed = true
	for _, c := range d.coroutines {
		c.exit()
	}
	d.coroutines = nil
}

// stackTrace returns the stacks of all blocked coroutines.
func (d *dispatcher) stackTrace() string {
	var b strings.Builder
	for _, c := range d.coroutines {
		if c.closed {
			continue
		}
		b.WriteString(c.stackTrace())
		b.WriteString("\n")
	}
	return b.String()
}

func (c *coroutine) call() error {
	c.unblock <- func(string) bool { return false }
	t := time.NewTimer(deadlockTimeout)
	defer t.Stop()
	select {
	case <-c.aboutToBlock:
		return nil
	case <-t.C:
		return fmt.Errorf("potential deadlock detected: workflow goroutine %q didn't yield for over %v", c.name, deadlockTimeout)
	}
}

// yield hands control back to the dispatcher until the next pass.
func (c *coroutine) yield(status string) {
	c.aboutToBlock <- struct{}{}
	c.initialYield(status)
	c.keptBlocked = true
}

func (c *coroutine) initialYield(status string) {
	for {
		f := <-c.unblock
		if !f(status) {
			return
		}
	}
}

// unblocked records progress after a blocking call returns.
func (c *coroutine) unblocked() { c.keptBlocked = false }

func (c *coroutine) exit() {
	if c.closed {
		return
	}
	c.unblock <- func(string) bool {
		runtime.Goexit()
		return false
	}
	<-c.aboutToBlock
}

func (c *coroutine) stackTrace() string {
	out := make(chan string, 1)
	c.unblock <- func(status string) bool {
		buf := make([]byte, 64<<10)
		n := runtime.Stack(buf, false)
		out <- fmt.Sprintf("coroutine %s [%s]:\n%s", c.name, status, buf[:n])
		return true
	}
	return <-out
}

type contextKey string

const coroutineKey contextKey = "coroutine"

func getState(ctx Context) *coroutine {
	c, ok := ctx.Value(coroutineKey).(*coroutine)
	if !ok || c == nil {
		panic("blocking call outside of a workflow goroutine; use workflow.Go to start goroutines")
	}
	return c
}
