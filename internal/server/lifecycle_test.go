package server_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/larsla/takt/api"
	"github.com/larsla/takt/internal/server"
	"github.com/larsla/takt/internal/server/engine"
	"github.com/larsla/takt/internal/transport"
)

func TestReusePolicies(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	first := h.start("wf-reuse")

	// Same request ID is idempotent; a new one is rejected while running.
	if again := h.start("wf-reuse"); again != first {
		t.Fatalf("idempotent start returned %s, want %s", again, first)
	}
	req := h.startReq("wf-reuse")
	req.RequestID = "other"
	_, err := h.c.StartWorkflowExecution(h.ctx, req)
	var already *api.WorkflowExecutionAlreadyStartedError
	if !errors.As(err, &already) || already.RunID != first {
		t.Fatalf("expected already started, got %v", err)
	}

	h.respond(h.pollDecision(), completeWorkflow(`1`))
	h.waitClosed("wf-reuse", first)

	// Default policy rejects reuse after success; AllowDuplicate permits it.
	req.RequestID = "r2"
	if _, err := h.c.StartWorkflowExecution(h.ctx, req); !errors.As(err, &already) {
		t.Fatalf("AllowDuplicateFailedOnly after success: %v", err)
	}
	req.WorkflowIDReusePolicy = api.WorkflowIDReusePolicyRejectDuplicate
	if _, err := h.c.StartWorkflowExecution(h.ctx, req); !errors.As(err, &already) {
		t.Fatalf("RejectDuplicate: %v", err)
	}
	req.WorkflowIDReusePolicy = api.WorkflowIDReusePolicyAllowDuplicate
	second, err := h.c.StartWorkflowExecution(h.ctx, req)
	if err != nil || second.RunID == first {
		t.Fatalf("AllowDuplicate: %v", err)
	}

	req.RequestID = "r3"
	req.WorkflowIDReusePolicy = api.WorkflowIDReusePolicyTerminateIfRunning
	third, err := h.c.StartWorkflowExecution(h.ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if ev := h.waitClosed("wf-reuse", second.RunID); ev.EventType != api.EventTypeWorkflowExecutionTerminated {
		t.Fatalf("previous run closed with %s", ev.EventType)
	}
	if h.describe("wf-reuse", "").WorkflowExecutionInfo.Execution.RunID != third.RunID {
		t.Fatal("current run did not move")
	}
}

func TestStartValidation(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	cases := map[string]func(*api.StartWorkflowExecutionRequest){
		"no id":      func(r *api.StartWorkflowExecutionRequest) { r.WorkflowID = "" },
		"no type":    func(r *api.StartWorkflowExecutionRequest) { r.WorkflowType = nil },
		"no list":    func(r *api.StartWorkflowExecutionRequest) { r.TaskList = nil },
		"no timeout": func(r *api.StartWorkflowExecutionRequest) { r.ExecutionStartToCloseTimeoutSeconds = 0 },
		"bad cron":   func(r *api.StartWorkflowExecutionRequest) { r.CronSchedule = "every day" },
	}
	for name, mut := range cases {
		req := h.startReq("v")
		mut(req)
		_, err := h.c.StartWorkflowExecution(h.ctx, req)
		var br *api.BadRequestError
		if !errors.As(err, &br) {
			t.Errorf("%s: %v", name, err)
		}
	}
	req := h.startReq("v")
	req.Domain = "missing"
	_, err := h.c.StartWorkflowExecution(h.ctx, req)
	var nf *api.EntityNotExistsError
	if !errors.As(err, &nf) {
		t.Errorf("missing domain: %v", err)
	}
}

func TestCancelAndTerminate(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	runID := h.start("wf-cancel")
	cancelReq := &api.RequestCancelWorkflowExecutionRequest{
		Domain: domain, WorkflowExecution: &api.WorkflowExecution{WorkflowID: "wf-cancel"}, RequestID: "c1",
	}
	if err := h.c.RequestCancelWorkflowExecution(h.ctx, cancelReq); err != nil {
		t.Fatal(err)
	}
	if err := h.c.RequestCancelWorkflowExecution(h.ctx, cancelReq); err != nil {
		t.Fatalf("same request ID must be idempotent: %v", err)
	}
	cancelReq.RequestID = "c2"
	var dup *api.CancellationAlreadyRequestedError
	if err := h.c.RequestCancelWorkflowExecution(h.ctx, cancelReq); !errors.As(err, &dup) {
		t.Fatalf("second cancel: %v", err)
	}
	d := h.pollDecision()
	if hasType(d.History.Events, api.EventTypeWorkflowExecutionCancelRequested) == nil {
		t.Fatalf("history %v", types(d.History.Events))
	}
	h.respond(d, &api.Decision{
		DecisionType: api.DecisionTypeCancelWorkflowExecution,
		CancelWorkflowExecutionDecisionAttributes: &api.CancelWorkflowExecutionDecisionAttributes{Details: []byte("bye")},
	})
	if ev := h.waitClosed("wf-cancel", runID); ev.EventType != api.EventTypeWorkflowExecutionCanceled {
		t.Fatalf("close %s", ev.EventType)
	}

	runID = h.start("wf-term")
	if err := h.c.TerminateWorkflowExecution(h.ctx, &api.TerminateWorkflowExecutionRequest{
		Domain: domain, WorkflowExecution: &api.WorkflowExecution{WorkflowID: "wf-term"}, Reason: "stop",
	}); err != nil {
		t.Fatal(err)
	}
	ev := h.waitClosed("wf-term", runID)
	if ev.EventType != api.EventTypeWorkflowExecutionTerminated || ev.WorkflowExecutionTerminatedEventAttributes.Reason != "stop" {
		t.Fatalf("close %+v", ev)
	}
	err := h.c.SignalWorkflowExecution(h.ctx, &api.SignalWorkflowExecutionRequest{
		Domain: domain, WorkflowExecution: &api.WorkflowExecution{WorkflowID: "wf-term"}, SignalName: "late",
	})
	var nf *api.EntityNotExistsError
	if !errors.As(err, &nf) {
		t.Fatalf("signal closed workflow: %v", err)
	}
	if err := h.c.TerminateWorkflowExecution(h.ctx, &api.TerminateWorkflowExecutionRequest{
		Domain: domain, WorkflowExecution: &api.WorkflowExecution{WorkflowID: "wf-term"},
	}); !errors.As(err, &nf) {
		t.Fatalf("terminate closed workflow: %v", err)
	}
}

func TestSignalDedupAndSignalWithStart(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	sws := &api.SignalWithStartWorkflowExecutionRequest{
		Domain:                              domain,
		WorkflowID:                          "wf-sws",
		WorkflowType:                        &api.WorkflowType{Name: "wf"},
		TaskList:                            &api.TaskList{Name: taskList},
		ExecutionStartToCloseTimeoutSeconds: 60,
		SignalName:                          "go",
		SignalInput:                         []byte(`1`),
	}
	first, err := h.c.SignalWithStartWorkflowExecution(h.ctx, sws)
	if err != nil {
		t.Fatal(err)
	}
	second, err := h.c.SignalWithStartWorkflowExecution(h.ctx, sws)
	if err != nil || second.RunID != first.RunID {
		t.Fatalf("second signal-with-start: %v %v", err, second)
	}
	sig := &api.SignalWorkflowExecutionRequest{
		Domain: domain, WorkflowExecution: &api.WorkflowExecution{WorkflowID: "wf-sws"}, SignalName: "dup", RequestID: "once",
	}
	for range 2 {
		if err := h.c.SignalWorkflowExecution(h.ctx, sig); err != nil {
			t.Fatal(err)
		}
	}
	d := h.pollDecision()
	requireTypes(t, d.History.Events,
		api.EventTypeWorkflowExecutionStarted,
		api.EventTypeWorkflowExecutionSignaled,
		api.EventTypeDecisionTaskScheduled,
		api.EventTypeWorkflowExecutionSignaled,
		api.EventTypeWorkflowExecutionSignaled,
		api.EventTypeDecisionTaskStarted,
	)
}

func TestChildWorkflow(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	parentRun := h.start("parent")
	h.respond(h.pollDecision(), &api.Decision{
		DecisionType: api.DecisionTypeStartChildWorkflowExecution,
		StartChildWorkflowExecutionDecisionAttributes: &api.StartChildWorkflowExecutionDecisionAttributes{
			WorkflowID:   "child",
			WorkflowType: &api.WorkflowType{Name: "childwf"},
			Input:        []byte(`"c"`),
		},
	})

	// Two decision tasks show up: the child's first and the parent's
	// reaction to ChildWorkflowExecutionStarted.
	var child, parent *api.PollForDecisionTaskResponse
	for child == nil || parent == nil {
		d := h.pollDecision()
		if d.WorkflowType.Name == "childwf" {
			child = d
		} else {
			parent = d
		}
	}
	start := child.History.Events[0].WorkflowExecutionStartedEventAttributes
	if start.ParentWorkflowExecution.RunID != parentRun || start.ParentInitiatedEventID != 5 || start.TaskList.Name != taskList {
		t.Fatalf("child start attrs: %+v", start)
	}
	started := hasType(parent.History.Events, api.EventTypeChildWorkflowExecutionStarted)
	if started == nil || started.ChildWorkflowExecutionStartedEventAttributes.WorkflowExecution.RunID != child.WorkflowExecution.RunID {
		t.Fatalf("parent history %v", types(parent.History.Events))
	}
	h.respond(parent)
	h.respond(child, completeWorkflow(`"child done"`))

	d := h.pollDecision()
	done := hasType(d.History.Events, api.EventTypeChildWorkflowExecutionCompleted)
	if done == nil || string(done.ChildWorkflowExecutionCompletedEventAttributes.Result) != `"child done"` ||
		done.ChildWorkflowExecutionCompletedEventAttributes.StartedEventID != started.EventID {
		t.Fatalf("parent history %v", types(d.History.Events))
	}
	// A second child with the same ID fails to start.
	h.respond(d, &api.Decision{
		DecisionType: api.DecisionTypeStartChildWorkflowExecution,
		StartChildWorkflowExecutionDecisionAttributes: &api.StartChildWorkflowExecutionDecisionAttributes{
			WorkflowID: "child", WorkflowType: &api.WorkflowType{Name: "childwf"},
			WorkflowIDReusePolicy: api.WorkflowIDReusePolicyRejectDuplicate,
		},
	})
	d = h.pollDecision()
	if hasType(d.History.Events, api.EventTypeStartChildWorkflowExecutionFailed) == nil {
		t.Fatalf("history %v", types(d.History.Events))
	}
	h.respond(d, completeWorkflow(`null`))
	h.waitClosed("parent", parentRun)
}

func TestParentClosePolicyTerminatesChild(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.start("p2")
	h.respond(h.pollDecision(), &api.Decision{
		DecisionType: api.DecisionTypeStartChildWorkflowExecution,
		StartChildWorkflowExecutionDecisionAttributes: &api.StartChildWorkflowExecutionDecisionAttributes{
			WorkflowID: "c2", WorkflowType: &api.WorkflowType{Name: "childwf"}, TaskList: &api.TaskList{Name: "child-tl"},
		},
	})
	d := h.pollDecision()
	if hasType(d.History.Events, api.EventTypeChildWorkflowExecutionStarted) == nil {
		t.Fatalf("history %v", types(d.History.Events))
	}
	h.respond(d, completeWorkflow(`null`))
	childRun := hasType(d.History.Events, api.EventTypeChildWorkflowExecutionStarted).ChildWorkflowExecutionStartedEventAttributes.WorkflowExecution.RunID
	if ev := h.waitClosed("c2", childRun); ev.EventType != api.EventTypeWorkflowExecutionTerminated {
		t.Fatalf("child closed with %s", ev.EventType)
	}
}

func TestSignalAndCancelExternal(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.start("src")
	h.start("dst")
	h.respond(h.pollDecision()) // whichever comes first
	h.respond(h.pollDecision())
	h.signal("src", "go")
	d := h.pollDecision()
	if d.WorkflowExecution.WorkflowID != "src" {
		t.Fatalf("unexpected decision for %s", d.WorkflowExecution.WorkflowID)
	}
	h.respond(d,
		&api.Decision{DecisionType: api.DecisionTypeSignalExternalWorkflowExecution, SignalExternalWorkflowExecutionDecisionAttributes: &api.SignalExternalWorkflowExecutionDecisionAttributes{
			Execution: &api.WorkflowExecution{WorkflowID: "dst"}, SignalName: "hello", Input: []byte(`"hi"`),
		}},
		&api.Decision{DecisionType: api.DecisionTypeSignalExternalWorkflowExecution, SignalExternalWorkflowExecutionDecisionAttributes: &api.SignalExternalWorkflowExecutionDecisionAttributes{
			Execution: &api.WorkflowExecution{WorkflowID: "ghost"}, SignalName: "hello",
		}},
		&api.Decision{DecisionType: api.DecisionTypeRequestCancelExternalWorkflowExecution, RequestCancelExternalWorkflowExecutionDecisionAttributes: &api.RequestCancelExternalWorkflowExecutionDecisionAttributes{
			WorkflowID: "dst",
		}},
	)
	got := map[string][]*api.HistoryEvent{}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && (countType(got["src"], api.EventTypeExternalWorkflowExecutionSignaled) == 0 ||
		countType(got["src"], api.EventTypeSignalExternalWorkflowExecutionFailed) == 0 ||
		countType(got["src"], api.EventTypeExternalWorkflowExecutionCancelRequested) == 0 ||
		countType(got["dst"], api.EventTypeWorkflowExecutionCancelRequested) == 0 ||
		countType(got["dst"], api.EventTypeWorkflowExecutionSignaled) == 0) {
		d := h.pollDecision()
		got[d.WorkflowExecution.WorkflowID] = d.History.Events
		h.respond(d)
	}
	sig := hasType(got["dst"], api.EventTypeWorkflowExecutionSignaled)
	if sig == nil || sig.WorkflowExecutionSignaledEventAttributes.SignalName != "hello" {
		t.Fatalf("dst history %v", types(got["dst"]))
	}
	cr := hasType(got["dst"], api.EventTypeWorkflowExecutionCancelRequested)
	if cr == nil || cr.WorkflowExecutionCancelRequestedEventAttributes.ExternalWorkflowExecution.WorkflowID != "src" {
		t.Fatalf("dst history %v", types(got["dst"]))
	}
}

func TestContinueAsNew(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	first := h.start("wf-can")
	h.respond(h.pollDecision(), &api.Decision{
		DecisionType: api.DecisionTypeContinueAsNewWorkflowExecution,
		ContinueAsNewWorkflowExecutionDecisionAttributes: &api.ContinueAsNewWorkflowExecutionDecisionAttributes{
			Input: []byte(`"next"`),
		},
	})
	ev := h.waitClosed("wf-can", first)
	if ev.EventType != api.EventTypeWorkflowExecutionContinuedAsNew {
		t.Fatalf("close %s", ev.EventType)
	}
	newRun := ev.WorkflowExecutionContinuedAsNewEventAttributes.NewExecutionRunID
	d := h.pollDecision()
	start := d.History.Events[0].WorkflowExecutionStartedEventAttributes
	if d.WorkflowExecution.RunID != newRun || start.ContinuedExecutionRunID != first || string(start.Input) != `"next"` ||
		start.FirstExecutionRunID != first || start.Initiator != api.ContinueAsNewInitiatorDecider {
		t.Fatalf("new run %s start %+v", d.WorkflowExecution.RunID, start)
	}
	if cur := h.describe("wf-can", "").WorkflowExecutionInfo.Execution.RunID; cur != newRun {
		t.Fatalf("current run %s", cur)
	}
}

func TestWorkflowTimeoutAndDelayedStart(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	req := h.startReq("wf-timeout")
	req.ExecutionStartToCloseTimeoutSeconds = 1
	resp, err := h.c.StartWorkflowExecution(h.ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if ev := h.waitClosed("wf-timeout", resp.RunID); ev.EventType != api.EventTypeWorkflowExecutionTimedOut {
		t.Fatalf("close %s", ev.EventType)
	}

	req = h.startReq("wf-delay")
	req.DelayStartSeconds = 1
	began := time.Now()
	if _, err := h.c.StartWorkflowExecution(h.ctx, req); err != nil {
		t.Fatal(err)
	}
	var d *api.PollForDecisionTaskResponse
	for d == nil || d.WorkflowExecution.WorkflowID != "wf-delay" {
		d = h.pollDecision()
	}
	if time.Since(began) < 900*time.Millisecond {
		t.Fatalf("delayed start ran after %v", time.Since(began))
	}
}

func TestQuery(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.start("wf-query")
	q := &api.QueryWorkflowRequest{
		Domain: domain, Execution: &api.WorkflowExecution{WorkflowID: "wf-query"}, Query: &api.WorkflowQuery{QueryType: "state"},
	}
	_, err := h.c.QueryWorkflow(h.ctx, q)
	var qf *api.QueryFailedError
	if !errors.As(err, &qf) {
		t.Fatalf("query without worker: %v", err)
	}
	sub, err := h.env.NC.Subscribe(api.QuerySubject(domain, taskList), func(m *nats.Msg) {
		var req api.WorkerQueryRequest
		_ = json.Unmarshal(m.Data, &req)
		resp := api.WorkerQueryResponse{Result: []byte(`"` + req.Query.QueryType + `:` + req.Execution.WorkflowID + `"`)}
		if req.Query.QueryType == "bad" {
			resp = api.WorkerQueryResponse{ErrorMessage: "unknown query"}
		}
		b, _ := json.Marshal(resp)
		_ = m.Respond(b)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	resp, err := h.c.QueryWorkflow(h.ctx, q)
	if err != nil || string(resp.QueryResult) != `"state:wf-query"` {
		t.Fatalf("query: %v %s", err, resp.QueryResult)
	}
	q.Query.QueryType = "bad"
	if _, err := h.c.QueryWorkflow(h.ctx, q); !errors.As(err, &qf) || qf.Message != "unknown query" {
		t.Fatalf("bad query: %v", err)
	}
	if err := h.c.TerminateWorkflowExecution(h.ctx, &api.TerminateWorkflowExecutionRequest{Domain: domain, WorkflowExecution: q.Execution}); err != nil {
		t.Fatal(err)
	}
	q.QueryRejectCondition = api.QueryRejectConditionNotOpen
	resp, err = h.c.QueryWorkflow(h.ctx, q)
	if err != nil || resp.QueryRejected == nil || *resp.QueryRejected.CloseStatus != api.CloseStatusTerminated {
		t.Fatalf("rejected query: %v %+v", err, resp)
	}
}

func TestTaskListsAndDomains(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.start("wf-tl")
	h.start("wf-tl2")
	time.Sleep(200 * time.Millisecond)
	desc, err := h.c.DescribeTaskList(h.ctx, &api.DescribeTaskListRequest{
		Domain: domain, TaskList: &api.TaskList{Name: taskList}, IncludeTaskListStatus: true,
	})
	if err != nil || desc.TaskListStatus.BacklogCountHint != 2 {
		t.Fatalf("describe: %v %+v", err, desc)
	}
	h.pollDecision()
	desc, err = h.c.DescribeTaskList(h.ctx, &api.DescribeTaskListRequest{Domain: domain, TaskList: &api.TaskList{Name: taskList}})
	if err != nil || len(desc.Pollers) != 1 || desc.Pollers[0].Identity != "decider" {
		t.Fatalf("pollers: %v %+v", err, desc)
	}
	lists, err := h.c.ListTaskLists(h.ctx, &api.ListTaskListsRequest{Domain: domain})
	if err != nil || len(lists.TaskLists) == 0 || lists.TaskLists[0].Name != taskList {
		t.Fatalf("list task lists: %v %+v", err, lists)
	}

	var exists *api.DomainAlreadyExistsError
	if err := h.c.RegisterDomain(h.ctx, &api.RegisterDomainRequest{Name: domain}); !errors.As(err, &exists) {
		t.Fatalf("duplicate domain: %v", err)
	}
	desc2 := "updated"
	upd, err := h.c.UpdateDomain(h.ctx, &api.UpdateDomainRequest{
		Name: domain, UpdatedInfo: &api.UpdateDomainInfo{Description: &desc2, Data: map[string]string{"k": "v"}},
		Configuration: &api.DomainConfiguration{WorkflowExecutionRetentionPeriodInDays: 7},
	})
	if err != nil || upd.DomainInfo.Description != "updated" || upd.Configuration.WorkflowExecutionRetentionPeriodInDays != 7 {
		t.Fatalf("update: %v %+v", err, upd)
	}
	all, err := h.c.ListDomains(h.ctx, &api.ListDomainsRequest{})
	if err != nil || len(all.Domains) != 1 || all.Domains[0].DomainInfo.Data["k"] != "v" {
		t.Fatalf("list: %v %+v", err, all)
	}
	var nf *api.EntityNotExistsError
	if _, err := h.c.DescribeDomain(h.ctx, &api.DescribeDomainRequest{Name: "nope"}); !errors.As(err, &nf) {
		t.Fatalf("describe missing: %v", err)
	}
}

func TestListOpenAndClosed(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.start("open-1")
	closed := h.start("closed-1")
	if err := h.c.TerminateWorkflowExecution(h.ctx, &api.TerminateWorkflowExecutionRequest{
		Domain: domain, WorkflowExecution: &api.WorkflowExecution{WorkflowID: "closed-1"},
	}); err != nil {
		t.Fatal(err)
	}
	h.waitClosed("closed-1", closed)
	open, err := h.c.ListOpenWorkflowExecutions(h.ctx, &api.ListOpenWorkflowExecutionsRequest{Domain: domain})
	if err != nil || len(open.Executions) != 1 || open.Executions[0].Execution.WorkflowID != "open-1" {
		t.Fatalf("open: %v %+v", err, open)
	}
	status := api.CloseStatusTerminated
	cl, err := h.c.ListClosedWorkflowExecutions(h.ctx, &api.ListClosedWorkflowExecutionsRequest{Domain: domain, StatusFilter: &status})
	if err != nil || len(cl.Executions) != 1 || *cl.Executions[0].CloseStatus != api.CloseStatusTerminated {
		t.Fatalf("closed: %v %+v", err, cl)
	}
}

// Two servers on one NATS race to append signals to the same run.
func TestConcurrentSignalsAcrossServers(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	second, err := server.Start(context.Background(), server.Config{NC: h.env.NC, JS: h.env.JS})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Stop()
	runID := h.start("wf-race")

	const n = 40
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := range n {
		wg.Go(func() {
			errs <- h.c.SignalWorkflowExecution(h.ctx, &api.SignalWorkflowExecutionRequest{
				Domain: domain, WorkflowExecution: &api.WorkflowExecution{WorkflowID: "wf-race"},
				SignalName: fmt.Sprintf("s%d", i),
			})
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	events := h.history("wf-race", runID)
	if got := countType(events, api.EventTypeWorkflowExecutionSignaled); got != n {
		t.Fatalf("got %d signals, want %d", got, n)
	}
	for i, e := range events {
		if e.EventID != int64(i+1) {
			t.Fatalf("event %d has ID %d", i, e.EventID)
		}
	}
}

// A commit whose history append never happened is completed by the sweeper.
func TestSweepFinishesInterruptedCommit(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	runID := h.start("wf-crash")
	kv, err := h.env.JS.KeyValue(h.ctx, "takt_executions")
	if err != nil {
		t.Fatal(err)
	}
	key := api.Token(domain) + "." + api.Token("wf-crash") + "." + runID
	entry, err := kv.Get(h.ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	var s engine.State
	if err := json.Unmarshal(entry.Value(), &s); err != nil {
		t.Fatal(err)
	}
	s.Pending = []*api.HistoryEvent{{
		EventID: s.NextEventID, EventType: api.EventTypeWorkflowExecutionSignaled,
		WorkflowExecutionSignaledEventAttributes: &api.WorkflowExecutionSignaledEventAttributes{SignalName: "crashed"},
	}}
	s.NextEventID++
	data, _ := json.Marshal(&s)
	if _, err := kv.Update(h.ctx, key, data, entry.Revision()); err != nil {
		t.Fatal(err)
	}
	n, err := h.env.Server.Engine().Sweep(h.ctx)
	if err != nil || n != 1 {
		t.Fatalf("sweep: %d %v", n, err)
	}
	events := h.history("wf-crash", runID)
	if last := events[len(events)-1]; last.WorkflowExecutionSignaledEventAttributes == nil || last.WorkflowExecutionSignaledEventAttributes.SignalName != "crashed" {
		t.Fatalf("history %v", types(events))
	}
	// Running the same append again (an earlier finalize that did write
	// history but lost its state update) is recognised and not duplicated.
	entry, _ = kv.Get(h.ctx, key)
	_ = json.Unmarshal(entry.Value(), &s)
	s.Pending = []*api.HistoryEvent{events[len(events)-1]}
	s.HistorySeq--
	data, _ = json.Marshal(&s)
	if _, err := kv.Update(h.ctx, key, data, entry.Revision()); err != nil {
		t.Fatal(err)
	}
	if n, err := h.env.Server.Engine().Sweep(h.ctx); err != nil || n != 1 {
		t.Fatalf("second sweep: %d %v", n, err)
	}
	if got := len(h.history("wf-crash", runID)); got != len(events) {
		t.Fatalf("history grew from %d to %d", len(events), got)
	}
}

func TestNoServer(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.env.Server.Stop()
	c, err := transport.New(h.env.NC, transport.Options{})
	if err != nil {
		t.Fatal(err)
	}
	var busy *api.ServiceBusyError
	if _, err := c.DescribeDomain(h.ctx, &api.DescribeDomainRequest{Name: domain}); !errors.As(err, &busy) {
		t.Fatalf("no server: %v", err)
	}
}
