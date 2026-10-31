package engine

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/uber-go/tally"

	"github.com/larsla/takt/api"
)

// Task list gauges. Every server reports the same values; aggregate them
// with max, not sum.
const (
	MetricTaskListBacklog    = "takt_tasklist_backlog"
	MetricTaskListBacklogAge = "takt_tasklist_backlog_age_seconds"
	MetricTaskListPollers    = "takt_tasklist_pollers"
)

// maxTaskListGauges caps how many task lists get gauges, to bound the
// cost of a pass and the number of series.
const maxTaskListGauges = 1000

type taskListKey struct {
	domain, name string
	t            api.TaskListType
}

type taskListGauges struct {
	mu       sync.Mutex
	reported map[taskListKey]bool
}

// ReportTaskLists sets the task list gauges: pending task stubs, the age
// of the oldest one and the pollers seen in the last minute, for each task
// list that has a backlog or pollers. Sticky and session task lists are
// left out. A task list that disappears is reported as zero once.
func (e *Engine) ReportTaskLists(ctx context.Context) error {
	if e.scope == tally.NoopScope {
		return nil
	}
	type stats struct {
		backlog, pollers int64
		ageSeconds       float64
	}
	found := map[taskListKey]*stats{}
	get := func(k taskListKey) *stats {
		if found[k] == nil {
			found[k] = &stats{}
		}
		return found[k]
	}

	info, err := e.tasks.Info(ctx, jetstream.WithSubjectFilter("takt.task.>"))
	if err != nil {
		return err
	}
	now := e.now()
	for subject, n := range info.State.Subjects {
		k, ok := taskListFromSubject(subject)
		if !ok || len(found) >= maxTaskListGauges {
			continue
		}
		st := get(k)
		st.backlog = clampInt64(n)
		if msg, err := e.tasks.GetMsg(ctx, 1, jetstream.WithGetMsgSubject(subject)); err == nil {
			st.ageSeconds = max(0, now.Sub(msg.Time).Seconds())
		}
	}

	lister, err := e.pollers.ListKeys(ctx)
	if err != nil && !errors.Is(err, jetstream.ErrNoKeysFound) {
		return err
	}
	if lister != nil {
		for key := range lister.Keys() {
			k, ok := taskListFromPollerKey(key)
			if !ok || (found[k] == nil && len(found) >= maxTaskListGauges) {
				continue
			}
			get(k).pollers++
		}
	}

	g := &e.tlGauges
	g.mu.Lock()
	defer g.mu.Unlock()
	report := func(k taskListKey, st stats) {
		scope := e.scope.Tagged(map[string]string{"domain": k.domain, "tasklist": k.name, "tasklisttype": string(k.t)})
		scope.Gauge(MetricTaskListBacklog).Update(float64(st.backlog))
		scope.Gauge(MetricTaskListBacklogAge).Update(st.ageSeconds)
		scope.Gauge(MetricTaskListPollers).Update(float64(st.pollers))
	}
	for k := range g.reported {
		if found[k] == nil {
			report(k, stats{})
		}
	}
	g.reported = map[taskListKey]bool{}
	for k, st := range found {
		report(k, *st)
		g.reported[k] = true
	}
	return nil
}

// taskListFromSubject parses takt.task.<domain>.<tasklist>.<kind>.
func taskListFromSubject(subject string) (taskListKey, bool) {
	parts := strings.Split(subject, ".")
	if len(parts) != 5 {
		return taskListKey{}, false
	}
	domain, err1 := api.FromToken(parts[2])
	name, err2 := api.FromToken(parts[3])
	if err1 != nil || err2 != nil || api.IsWorkerTaskList(name) {
		return taskListKey{}, false
	}
	t := api.TaskListTypeDecision
	if parts[4] == "activity" {
		t = api.TaskListTypeActivity
	}
	return taskListKey{domain, name, t}, true
}

// taskListFromPollerKey parses <domain>.<tasklist>.<d|a>.<identity>.
func taskListFromPollerKey(key string) (taskListKey, bool) {
	parts := strings.Split(key, ".")
	if len(parts) != 4 {
		return taskListKey{}, false
	}
	domain, err1 := api.FromToken(parts[0])
	name, err2 := api.FromToken(parts[1])
	if err1 != nil || err2 != nil || api.IsWorkerTaskList(name) {
		return taskListKey{}, false
	}
	t := api.TaskListTypeDecision
	if parts[2] == "a" {
		t = api.TaskListTypeActivity
	}
	return taskListKey{domain, name, t}, true
}
