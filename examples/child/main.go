// Command child runs a parent workflow that starts a child workflow.
package main

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/larsla/takt"
	"github.com/larsla/takt/client"
	"github.com/larsla/takt/examples/common"
	"github.com/larsla/takt/worker"
	"github.com/larsla/takt/workflow"
)

const taskList = "childWorkflowGroup"

func sampleParentWorkflow(ctx workflow.Context) (string, error) {
	cwo := workflow.ChildWorkflowOptions{
		WorkflowID:                   "child-of-" + workflow.GetInfo(ctx).WorkflowExecution.ID,
		ExecutionStartToCloseTimeout: time.Minute,
	}
	ctx = workflow.WithChildOptions(ctx, cwo)
	var result string
	if err := workflow.ExecuteChildWorkflow(ctx, sampleChildWorkflow, 0, 5).Get(ctx, &result); err != nil {
		return "", err
	}
	return result, nil
}

// sampleChildWorkflow continues as new until it has run runs times.
func sampleChildWorkflow(ctx workflow.Context, total, runs int) (string, error) {
	total++
	runs--
	if runs == 0 {
		return fmt.Sprintf("child workflow completed after %d runs", total), nil
	}
	return "", workflow.NewContinueAsNewError(ctx, sampleChildWorkflow, total, runs)
}

func run(ctx context.Context, service takt.ServiceClient, domain string, out io.Writer) error {
	w := worker.New(service, domain, taskList, worker.Options{})
	w.RegisterWorkflow(sampleParentWorkflow)
	w.RegisterWorkflow(sampleChildWorkflow)
	if err := w.Start(); err != nil {
		return err
	}
	defer w.Stop()

	c := client.NewClient(service, domain, nil)
	we, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskList: taskList, ExecutionStartToCloseTimeout: time.Minute,
	}, sampleParentWorkflow)
	if err != nil {
		return err
	}
	var result string
	if err := we.Get(ctx, &result); err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, result)
	return err
}

func main() { common.Main(run) }
