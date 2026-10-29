// Command cancel cancels a running workflow, which cleans up with a
// disconnected context before it returns.
package main

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/larsla/takt"
	"github.com/larsla/takt/activity"
	"github.com/larsla/takt/client"
	"github.com/larsla/takt/examples/common"
	"github.com/larsla/takt/worker"
	"github.com/larsla/takt/workflow"
)

const taskList = "cancelGroup"

func cancelWorkflow(ctx workflow.Context) (retErr error) {
	ao := workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute,
		StartToCloseTimeout:    time.Hour,
		HeartbeatTimeout:       5 * time.Second,
		WaitForCancellation:    true,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	defer func() {
		if !takt.IsCanceledError(retErr) {
			return
		}
		// The workflow context is canceled; clean up on a new one.
		newCtx, _ := workflow.NewDisconnectedContext(ctx)
		if err := workflow.ExecuteActivity(newCtx, cleanupActivity).Get(newCtx, nil); err != nil {
			retErr = err
		}
	}()
	return workflow.ExecuteActivity(ctx, longRunningActivity).Get(ctx, nil)
}

func longRunningActivity(ctx context.Context) error {
	for {
		activity.RecordHeartbeat(ctx, "")
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

var cleanedUp = make(chan struct{}, 1)

func cleanupActivity() error {
	cleanedUp <- struct{}{}
	return nil
}

func run(ctx context.Context, service takt.ServiceClient, domain string, out io.Writer) error {
	w := worker.New(service, domain, taskList, worker.Options{})
	w.RegisterWorkflow(cancelWorkflow)
	w.RegisterActivity(longRunningActivity)
	w.RegisterActivity(cleanupActivity)
	if err := w.Start(); err != nil {
		return err
	}
	defer w.Stop()

	c := client.NewClient(service, domain, nil)
	we, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskList: taskList, ExecutionStartToCloseTimeout: time.Hour,
	}, cancelWorkflow)
	if err != nil {
		return err
	}
	time.Sleep(time.Second)
	if err := c.CancelWorkflow(ctx, we.GetID(), ""); err != nil {
		return err
	}
	err = we.Get(ctx, nil)
	if !takt.IsCanceledError(err) {
		return fmt.Errorf("expected cancellation, got %w", err)
	}
	select {
	case <-cleanedUp:
	case <-ctx.Done():
		return ctx.Err()
	}
	_, err = fmt.Fprintln(out, "workflow canceled and cleaned up")
	return err
}

func main() { common.Main(run) }
