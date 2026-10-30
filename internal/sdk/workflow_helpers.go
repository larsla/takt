package sdk

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
)

// WaitGroup waits for workflow goroutines to finish.
type WaitGroup interface {
	Add(delta int)
	Done()
	Wait(ctx Context)
}

type waitGroup struct{ n int }

// NewWaitGroup creates a WaitGroup.
func NewWaitGroup(ctx Context) WaitGroup { return &waitGroup{} }

func (w *waitGroup) Add(delta int) {
	w.n += delta
	if w.n < 0 {
		panic("negative WaitGroup counter")
	}
}

func (w *waitGroup) Done() { w.Add(-1) }

// Wait blocks until the counter is zero or ctx is canceled.
func (w *waitGroup) Wait(ctx Context) { _ = Await(ctx, func() bool { return w.n == 0 }) }

// BatchFuture runs futures with limited concurrency and collects them.
type BatchFuture interface {
	IsReady() bool
	// Get waits for all futures and decodes their results into valuePtr, a
	// pointer to a slice, or nil. The errors of failed futures are joined.
	Get(ctx Context, valuePtr interface{}) error
	// GetFutures returns the wrapped futures, in the order of the factories.
	GetFutures() []Future
}

type batchFuture struct {
	futures []Future
	wg      WaitGroup
}

// NewBatchFuture calls the factories, at most batchSize at a time, and
// returns a future for all their results.
func NewBatchFuture(ctx Context, batchSize int, factories []func(ctx Context) Future) (BatchFuture, error) {
	if batchSize <= 0 {
		return nil, errors.New("batchSize must be positive")
	}
	b := &batchFuture{wg: NewWaitGroup(ctx)}
	settables := make([]Settable, len(factories))
	for i := range factories {
		b.futures = append(b.futures, nil)
		b.futures[i], settables[i] = NewFuture(ctx)
	}
	running := 0
	b.wg.Add(1)
	GoNamed(ctx, "batch-future", func(ctx Context) {
		defer b.wg.Done()
		for i, factory := range factories {
			_ = Await(ctx, func() bool { return running < batchSize })
			running++
			b.wg.Add(1)
			GoNamed(ctx, fmt.Sprintf("batch-future-%d", i), func(ctx Context) {
				defer b.wg.Done()
				f := factory(ctx)
				settables[i].Chain(f)
				_ = f.Get(ctx, nil)
				running--
			})
		}
	})
	return b, nil
}

func (b *batchFuture) GetFutures() []Future { return b.futures }

func (b *batchFuture) IsReady() bool {
	for _, f := range b.futures {
		if !f.IsReady() {
			return false
		}
	}
	return true
}

func (b *batchFuture) Get(ctx Context, valuePtr interface{}) error {
	b.wg.Wait(ctx)
	var errs []error
	if valuePtr == nil {
		for _, f := range b.futures {
			errs = append(errs, f.Get(ctx, nil))
		}
		return errors.Join(errs...)
	}
	v := reflect.ValueOf(valuePtr)
	if v.Kind() != reflect.Pointer || v.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("valuePtr must be a pointer to a slice, got %v", v.Kind())
	}
	slice := v.Elem()
	if slice.Len() < len(b.futures) {
		slice.Set(reflect.MakeSlice(slice.Type(), len(b.futures), len(b.futures)))
	} else {
		slice.SetLen(len(b.futures))
	}
	for i, f := range b.futures {
		errs = append(errs, f.Get(ctx, slice.Index(i).Addr().Interface()))
	}
	return errors.Join(errs...)
}

// GetActivityOptions returns the activity options set on ctx.
func GetActivityOptions(ctx Context) *ActivityOptions {
	o := activityOptions(ctx)
	return &o
}

// GetActivityTaskList returns the task list activities started with ctx go
// to.
func GetActivityTaskList(ctx Context) string {
	if tl := activityOptions(ctx).TaskList; tl != "" {
		return tl
	}
	return GetInfo(ctx).TaskListName
}

// GetWorkflowTaskList returns the task list child workflows started with
// ctx go to.
func GetWorkflowTaskList(ctx Context) string {
	if tl := childOptions(ctx).TaskList; tl != "" {
		return tl
	}
	return GetInfo(ctx).TaskListName
}

// WithCronSchedule sets the cron schedule of child workflows started with
// ctx, and of the run a ContinueAsNewError built with ctx starts.
func WithCronSchedule(ctx Context, cronSchedule string) Context {
	return withChildOptions(ctx, func(o *ChildWorkflowOptions) { o.CronSchedule = cronSchedule })
}

// GetHistoryCount returns the number of events in the workflow's history
// when the current decision task started.
func GetHistoryCount(ctx Context) int64 { return GetInfo(ctx).HistoryCount }

// GetTotalHistoryBytes estimates the encoded size of the workflow's history
// when the current decision task started.
func GetTotalHistoryBytes(ctx Context) int64 { return GetInfo(ctx).TotalHistoryBytes }

// GetUnhandledSignalNames returns the names of signals that arrived and
// that the workflow hasn't received yet.
func GetUnhandledSignalNames(ctx Context) []string {
	var names []string
	for name, ch := range getRun(ctx).signals {
		if len(ch.buffer) > 0 || len(ch.sends) > 0 {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

// RegistryWorkflowInfo describes a registered workflow.
type RegistryWorkflowInfo interface {
	WorkflowType() WorkflowType
	GetFunction() interface{}
}

// RegistryActivityInfo describes a registered activity.
type RegistryActivityInfo interface {
	ActivityType() ActivityType
	GetFunction() interface{}
}

type registeredWorkflow struct {
	name string
	fn   interface{}
}

func (w registeredWorkflow) WorkflowType() WorkflowType { return WorkflowType{Name: w.name} }
func (w registeredWorkflow) GetFunction() interface{}   { return w.fn }

type registeredActivity struct {
	name string
	fn   interface{}
}

func (a registeredActivity) ActivityType() ActivityType { return ActivityType{Name: a.name} }
func (a registeredActivity) GetFunction() interface{}   { return a.fn }

// GetRegisteredWorkflows returns the workflows of the registry, sorted by
// name.
func (r *registry) GetRegisteredWorkflows() []RegistryWorkflowInfo {
	var out []RegistryWorkflowInfo
	for _, name := range r.workflowNames() {
		fn, _ := r.getWorkflow(name)
		out = append(out, registeredWorkflow{name, fn})
	}
	return out
}

// GetRegisteredActivities returns the activities of the registry, sorted by
// name.
func (r *registry) GetRegisteredActivities() []RegistryActivityInfo {
	var out []RegistryActivityInfo
	for _, name := range r.activityNames() {
		fn, _ := r.getActivity(name)
		out = append(out, registeredActivity{name, fn})
	}
	return out
}

// GetRegisteredWorkflowTypes returns the workflow types registered with
// RegisterWorkflow, sorted.
func GetRegisteredWorkflowTypes() []string { return globalRegistry.workflowNames() }

// GetRegisteredActivities returns the activities registered with
// RegisterActivity.
func GetRegisteredActivities() []RegistryActivityInfo {
	return globalRegistry.GetRegisteredActivities()
}

// GetRegisteredWorkflows returns the workflows registered with
// RegisterWorkflow.
func GetRegisteredWorkflows() []RegistryWorkflowInfo { return globalRegistry.GetRegisteredWorkflows() }

// WorkflowExecutionAsync is the result of StartWorkflowAsync.
type WorkflowExecutionAsync struct {
	ID string
}
