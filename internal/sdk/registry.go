package sdk

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
)

// RegisterWorkflowOptions configures workflow registration.
type RegisterWorkflowOptions struct {
	// Name overrides the workflow type name, which defaults to the fully
	// qualified function name.
	Name                          string
	DisableAlreadyRegisteredCheck bool
}

// RegisterActivityOptions configures activity registration.
type RegisterActivityOptions struct {
	// Name overrides the activity type name. For a struct it is used as a
	// prefix for each method name.
	Name                          string
	DisableAlreadyRegisteredCheck bool
	// EnableShortName registers struct methods under their bare method name.
	EnableShortName bool
	// EnableAutoHeartbeat sends heartbeats for the activity at half its
	// HeartbeatTimeout while it runs, repeating the last recorded details.
	// It has no effect when the activity has no HeartbeatTimeout.
	EnableAutoHeartbeat bool
}

type registry struct {
	mu            sync.RWMutex
	workflows     map[string]interface{}
	workflowAlias map[string]string
	activities    map[string]interface{}
	activityAlias map[string]string
	// autoHeartbeat holds the activity types registered with
	// EnableAutoHeartbeat.
	autoHeartbeat map[string]bool
	parent        *registry
}

func newRegistry(parent *registry) *registry {
	return &registry{
		workflows:     map[string]interface{}{},
		workflowAlias: map[string]string{},
		activities:    map[string]interface{}{},
		activityAlias: map[string]string{},
		autoHeartbeat: map[string]bool{},
		parent:        parent,
	}
}

var globalRegistry = newRegistry(nil)

// getFunctionName returns the name Cadence would register fn under.
func getFunctionName(fn interface{}) string {
	if s, ok := fn.(string); ok {
		return s
	}
	full := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
	return strings.TrimSuffix(full, "-fm")
}

var (
	workflowContextType = reflect.TypeOf((*Context)(nil)).Elem()
	goContextType       = reflect.TypeOf((*context.Context)(nil)).Elem()
	errorType           = reflect.TypeOf((*error)(nil)).Elem()
)

func validateResults(t reflect.Type) error {
	switch t.NumOut() {
	case 1:
		if t.Out(0) != errorType {
			return fmt.Errorf("expected function to return error, got %v", t.Out(0))
		}
	case 2:
		if t.Out(1) != errorType {
			return fmt.Errorf("expected function's second return value to be error, got %v", t.Out(1))
		}
	default:
		return fmt.Errorf("expected function to return error or (result, error), got %d results", t.NumOut())
	}
	return nil
}

func validateWorkflowFn(fn interface{}) error {
	t := reflect.TypeOf(fn)
	if t == nil || t.Kind() != reflect.Func {
		return fmt.Errorf("expected a func as workflow, got %T", fn)
	}
	if t.NumIn() < 1 || t.In(0) != workflowContextType {
		return errors.New("expected first argument of workflow function to be workflow.Context")
	}
	return validateResults(t)
}

func validateActivityFn(t reflect.Type) error {
	if t.Kind() != reflect.Func {
		return fmt.Errorf("expected a func as activity, got %v", t)
	}
	return validateResults(t)
}

func (r *registry) RegisterWorkflow(fn interface{}) {
	r.RegisterWorkflowWithOptions(fn, RegisterWorkflowOptions{})
}

func (r *registry) RegisterWorkflowWithOptions(fn interface{}, opts RegisterWorkflowOptions) {
	if err := validateWorkflowFn(fn); err != nil {
		panic(err)
	}
	fnName := getFunctionName(fn)
	name := opts.Name
	if name == "" {
		name = fnName
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !opts.DisableAlreadyRegisteredCheck {
		if _, ok := r.workflows[name]; ok {
			panic(fmt.Sprintf("workflow name \"%v\" is already registered", name))
		}
	}
	r.workflows[name] = fn
	if name != fnName {
		r.workflowAlias[fnName] = name
	}
}

func (r *registry) RegisterActivity(a interface{}) {
	r.RegisterActivityWithOptions(a, RegisterActivityOptions{})
}

func (r *registry) RegisterActivityWithOptions(a interface{}, opts RegisterActivityOptions) {
	t := reflect.TypeOf(a)
	if t != nil && t.Kind() == reflect.Pointer && t.Elem().Kind() == reflect.Struct {
		if err := r.registerActivityStruct(a, opts); err != nil {
			panic(err)
		}
		return
	}
	if t == nil {
		panic("activity is nil")
	}
	if err := validateActivityFn(t); err != nil {
		panic(err)
	}
	fnName := getFunctionName(a)
	name := opts.Name
	if name == "" {
		name = fnName
	}
	if err := r.addActivity(name, fnName, a, opts); err != nil {
		panic(err)
	}
}

func (r *registry) registerActivityStruct(a interface{}, opts RegisterActivityOptions) error {
	v := reflect.ValueOf(a)
	t := v.Type()
	count := 0
	for i := range v.NumMethod() {
		m := t.Method(i)
		if m.PkgPath != "" {
			continue
		}
		mv := v.Method(i)
		if validateActivityFn(mv.Type()) != nil {
			continue
		}
		// A method value such as a.Do has the same runtime name as the method
		// expression (*T).Do, which is how ExecuteActivity(ctx, a.Do) finds it.
		fnName := strings.TrimSuffix(runtime.FuncForPC(m.Func.Pointer()).Name(), "-fm")
		var name string
		switch {
		case opts.Name != "":
			name = opts.Name + m.Name
		case opts.EnableShortName:
			name = m.Name
		default:
			name = fnName
		}
		if err := r.addActivity(name, fnName, mv.Interface(), opts); err != nil {
			return err
		}
		count++
	}
	if count == 0 {
		return fmt.Errorf("no activities (public methods) found in %v", t)
	}
	return nil
}

func (r *registry) addActivity(name, fnName string, fn interface{}, opts RegisterActivityOptions) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !opts.DisableAlreadyRegisteredCheck {
		if _, ok := r.activities[name]; ok {
			return fmt.Errorf("activity type \"%v\" is already registered", name)
		}
	}
	r.activities[name] = fn
	r.autoHeartbeat[name] = opts.EnableAutoHeartbeat
	if name != fnName {
		r.activityAlias[fnName] = name
	}
	return nil
}

func (r *registry) getWorkflow(name string) (interface{}, bool) {
	r.mu.RLock()
	fn, ok := r.workflows[name]
	r.mu.RUnlock()
	if !ok && r.parent != nil {
		return r.parent.getWorkflow(name)
	}
	return fn, ok
}

func (r *registry) getActivity(name string) (interface{}, bool) {
	r.mu.RLock()
	fn, ok := r.activities[name]
	r.mu.RUnlock()
	if !ok && r.parent != nil {
		return r.parent.getActivity(name)
	}
	return fn, ok
}

// isAutoHeartbeat reports whether an activity type was registered with
// EnableAutoHeartbeat.
func (r *registry) isAutoHeartbeat(name string) bool {
	r.mu.RLock()
	auto, ok := r.autoHeartbeat[name]
	r.mu.RUnlock()
	if !ok && r.parent != nil {
		return r.parent.isAutoHeartbeat(name)
	}
	return auto
}

func (r *registry) alias(fnName string, activity bool) (string, bool) {
	r.mu.RLock()
	var name string
	var ok bool
	if activity {
		name, ok = r.activityAlias[fnName]
	} else {
		name, ok = r.workflowAlias[fnName]
	}
	r.mu.RUnlock()
	if !ok && r.parent != nil {
		return r.parent.alias(fnName, activity)
	}
	return name, ok
}

// workflowName resolves a workflow function or name to its type name.
func (r *registry) workflowName(wf interface{}) string {
	name := getFunctionName(wf)
	if _, isString := wf.(string); !isString {
		if a, ok := r.alias(name, false); ok {
			return a
		}
	}
	return name
}

// activityName resolves an activity function or name to its type name.
func (r *registry) activityName(a interface{}) string {
	name := getFunctionName(a)
	if _, isString := a.(string); !isString {
		if al, ok := r.alias(name, true); ok {
			return al
		}
	}
	return name
}

func (r *registry) workflowNames() []string {
	seen := map[string]bool{}
	for reg := r; reg != nil; reg = reg.parent {
		reg.mu.RLock()
		for n := range reg.workflows {
			seen[n] = true
		}
		reg.mu.RUnlock()
	}
	return sortedKeys(seen)
}

func (r *registry) activityNames() []string {
	seen := map[string]bool{}
	for reg := r; reg != nil; reg = reg.parent {
		reg.mu.RLock()
		for n := range reg.activities {
			seen[n] = true
		}
		reg.mu.RUnlock()
	}
	return sortedKeys(seen)
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// RegisterWorkflow registers a workflow with the global registry used by
// all workers.
func RegisterWorkflow(fn interface{}) { globalRegistry.RegisterWorkflow(fn) }

// RegisterWorkflowWithOptions registers a workflow with the global registry.
func RegisterWorkflowWithOptions(fn interface{}, opts RegisterWorkflowOptions) {
	globalRegistry.RegisterWorkflowWithOptions(fn, opts)
}

// RegisterActivity registers an activity with the global registry.
func RegisterActivity(a interface{}) { globalRegistry.RegisterActivity(a) }

// RegisterActivityWithOptions registers an activity with the global registry.
func RegisterActivityWithOptions(a interface{}, opts RegisterActivityOptions) {
	globalRegistry.RegisterActivityWithOptions(a, opts)
}

// validateArgs checks call arguments against a function's parameters.
func validateArgs(fn interface{}, args []interface{}, skip int) error {
	t := reflect.TypeOf(fn)
	if t == nil || t.Kind() != reflect.Func {
		return nil
	}
	n := t.NumIn() - skip
	if t.IsVariadic() {
		return nil
	}
	if len(args) != n {
		return fmt.Errorf("expected %d args for function %v, got %d", n, getFunctionName(fn), len(args))
	}
	for i, a := range args {
		want := t.In(i + skip)
		if a == nil {
			switch want.Kind() {
			case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
				continue
			}
			return fmt.Errorf("cannot assign nil to argument %d of type %v", i, want)
		}
		if !reflect.TypeOf(a).AssignableTo(want) {
			return fmt.Errorf("cannot assign argument %d of type %T to %v", i, a, want)
		}
	}
	return nil
}

// callFn calls fn with args and splits its results.
func callFn(fn reflect.Value, args []reflect.Value) (interface{}, error) {
	out := fn.Call(args)
	var err error
	if e := out[len(out)-1].Interface(); e != nil {
		err = e.(error)
	}
	if len(out) == 1 {
		return nil, err
	}
	return out[0].Interface(), err
}
