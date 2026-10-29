package takt_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/larsla/takt"
	"github.com/larsla/takt/.gen/go/shared"
	"github.com/larsla/takt/activity"
	"github.com/larsla/takt/client"
	"github.com/larsla/takt/encoded"
	"github.com/larsla/takt/internal/servertest"
	"github.com/larsla/takt/worker"
	"github.com/larsla/takt/workflow"
)

const taskList = "public-api"

func waitForSignal(ctx workflow.Context, name string) (string, error) {
	var v string
	workflow.GetSignalChannel(ctx, name).Receive(ctx, &v)
	return v, nil
}

var pendingIDs = make(chan string, 1)

func pendingActivity(ctx context.Context) (string, error) {
	pendingIDs <- activity.GetInfo(ctx).ActivityID
	return "", activity.ErrResultPending
}

func pendingWorkflow(ctx workflow.Context) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute, StartToCloseTimeout: time.Minute,
	})
	var s string
	err := workflow.ExecuteActivity(ctx, pendingActivity).Get(ctx, &s)
	return s, err
}

func TestPublicAPI(t *testing.T) {
	env := servertest.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	service, err := takt.NewServiceClient(env.NC, &takt.ServiceOptions{PollTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	domains := client.NewDomainClient(service, nil)
	if err := domains.Register(ctx, &shared.RegisterDomainRequest{Name: "public"}); err != nil {
		t.Fatal(err)
	}
	var exists *shared.DomainAlreadyExistsError
	if err := domains.Register(ctx, &shared.RegisterDomainRequest{Name: "public"}); !errors.As(err, &exists) {
		t.Fatalf("duplicate register: %v", err)
	}
	desc := "public domain"
	if err := domains.Update(ctx, &shared.UpdateDomainRequest{Name: "public", UpdatedInfo: &shared.UpdateDomainInfo{Description: &desc}}); err != nil {
		t.Fatal(err)
	}
	d, err := domains.Describe(ctx, "public")
	if err != nil || d.DomainInfo.Description != desc {
		t.Fatalf("describe: %v %+v", err, d)
	}

	w := worker.New(service, "public", taskList, worker.Options{})
	w.RegisterWorkflow(waitForSignal)
	w.RegisterWorkflow(pendingWorkflow)
	w.RegisterActivity(pendingActivity)
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	if err := w.Start(); err == nil {
		t.Fatal("second Start should fail")
	}

	c := client.NewClient(service, "public", &client.Options{Identity: "tester"})
	opts := client.StartWorkflowOptions{ID: "sws", TaskList: taskList, ExecutionStartToCloseTimeout: time.Minute}
	exec, err := c.SignalWithStartWorkflow(ctx, "sws", "go", "payload", opts, waitForSignal, "go")
	if err != nil {
		t.Fatal(err)
	}
	var got string
	if err := c.GetWorkflow(ctx, exec.ID, exec.RunID).Get(ctx, &got); err != nil || got != "payload" {
		t.Fatalf("signal-with-start: %q %v", got, err)
	}

	// Long-poll history until the close event arrives.
	it := c.GetWorkflowHistory(ctx, exec.ID, exec.RunID, true, shared.HistoryEventFilterTypeCloseEvent)
	if !it.HasNext() {
		t.Fatal("no close event")
	}
	if ev, err := it.Next(); err != nil || ev.EventType != shared.EventTypeWorkflowExecutionCompleted {
		t.Fatalf("close event: %v %v", ev, err)
	}

	// Async completion by activity ID, with a heartbeat on the way.
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "pending", TaskList: taskList, ExecutionStartToCloseTimeout: time.Minute}, pendingWorkflow)
	if err != nil {
		t.Fatal(err)
	}
	actID := <-pendingIDs
	if err := c.RecordActivityHeartbeatByID(ctx, "public", run.GetID(), "", actID, "half"); err != nil {
		t.Fatal(err)
	}
	if err := c.CompleteActivityByID(ctx, "public", run.GetID(), "", actID, "async", nil); err != nil {
		t.Fatal(err)
	}
	if err := run.Get(ctx, &got); err != nil || got != "async" {
		t.Fatalf("async completion: %q %v", got, err)
	}

	// ExecuteWorkflow with a running ID returns the existing run.
	first, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "dup", TaskList: taskList, ExecutionStartToCloseTimeout: time.Minute}, waitForSignal, "never")
	if err != nil {
		t.Fatal(err)
	}
	again, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "dup", TaskList: taskList, ExecutionStartToCloseTimeout: time.Minute}, waitForSignal, "never")
	if err != nil || again.GetRunID() != first.GetRunID() {
		t.Fatalf("duplicate execute: %v", err)
	}
	if _, err := c.StartWorkflow(ctx, client.StartWorkflowOptions{ID: "dup", TaskList: taskList, ExecutionStartToCloseTimeout: time.Minute}, waitForSignal, "never"); err == nil {
		t.Fatal("StartWorkflow with a running ID should fail")
	} else {
		var already *shared.WorkflowExecutionAlreadyStartedError
		if !errors.As(err, &already) {
			t.Fatalf("got %T", err)
		}
	}

	open, err := c.ListOpenWorkflow(ctx, &shared.ListOpenWorkflowExecutionsRequest{})
	if err != nil || len(open.Executions) != 1 || open.Executions[0].Execution.WorkflowID != "dup" {
		t.Fatalf("open: %v %+v", err, open)
	}
	closed, err := c.ListClosedWorkflow(ctx, &shared.ListClosedWorkflowExecutionsRequest{})
	if err != nil || len(closed.Executions) != 2 {
		t.Fatalf("closed: %v %+v", err, closed)
	}
	tl, err := c.DescribeTaskList(ctx, taskList, shared.TaskListTypeDecision)
	if err != nil || len(tl.Pollers) == 0 {
		t.Fatalf("task list: %v %+v", err, tl)
	}

	if err := c.TerminateWorkflow(ctx, "dup", "", "done", nil); err != nil {
		t.Fatal(err)
	}
	reject := shared.QueryRejectConditionNotOpen
	qr, err := c.QueryWorkflowWithOptions(ctx, &client.QueryWorkflowWithOptionsRequest{WorkflowID: "dup", QueryType: client.QueryTypeStackTrace, QueryRejectCondition: &reject})
	if err != nil || qr.QueryRejected == nil || *qr.QueryRejected.CloseStatus != shared.WorkflowExecutionCloseStatusTerminated {
		t.Fatalf("rejected query: %v %+v", err, qr)
	}
	var term *takt.TerminatedError
	if err := first.Get(ctx, nil); !errors.As(err, &term) {
		t.Fatalf("terminated: %v", err)
	}

	var nf *shared.EntityNotExistsError
	if err := c.CancelWorkflow(ctx, "missing", ""); !errors.As(err, &nf) {
		t.Fatalf("cancel missing: %v", err)
	}
	if _, err := c.StartWorkflow(ctx, client.StartWorkflowOptions{}, waitForSignal); err == nil {
		t.Fatal("missing task list accepted")
	}
}

func TestRootHelpers(t *testing.T) {
	if !takt.IsCanceledError(takt.NewCanceledError()) {
		t.Fatal("canceled")
	}
	if takt.NewTimeoutError(shared.TimeoutTypeStartToClose).TimeoutType() != shared.TimeoutTypeStartToClose {
		t.Fatal("timeout type")
	}
	if takt.NewHeartbeatTimeoutError().TimeoutType() != shared.TimeoutTypeHeartbeat {
		t.Fatal("heartbeat timeout")
	}
	var details string
	if err := takt.NewCustomError("r", "d").Details(&details); err != nil || details != "d" {
		t.Fatal("custom details")
	}
	data, err := encoded.GetDefaultDataConverter().ToData(1)
	if err != nil || string(data) != "1\n" {
		t.Fatalf("converter: %q %v", data, err)
	}
	if !errors.Is(takt.ErrNoData, takt.ErrNoData) {
		t.Fatal("ErrNoData")
	}
	if _, _, err := takt.Dial("nats://127.0.0.1:1", nil); err == nil {
		t.Fatal("dial to closed port should fail")
	}
	env := servertest.New(t)
	service, closeConn, err := takt.Dial(env.NATS.ClientURL(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeConn()
	if _, err := client.NewDomainClient(service, nil).Describe(context.Background(), servertest.Domain); err != nil {
		t.Fatal(err)
	}
}
