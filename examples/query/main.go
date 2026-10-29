// Command query exposes workflow state through a query handler.
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

const taskList = "queryGroup"

func queryWorkflow(ctx workflow.Context) error {
	state := "started"
	if err := workflow.SetQueryHandler(ctx, "state", func() (string, error) {
		return state, nil
	}); err != nil {
		return err
	}
	state = "waiting for timer"
	if err := workflow.Sleep(ctx, 2*time.Second); err != nil {
		return err
	}
	state = "done"
	return nil
}

func queryState(ctx context.Context, c client.Client, id string) (string, error) {
	v, err := c.QueryWorkflow(ctx, id, "", "state")
	if err != nil {
		return "", err
	}
	var s string
	err = v.Get(&s)
	return s, err
}

func run(ctx context.Context, service takt.ServiceClient, domain string, out io.Writer) error {
	w := worker.New(service, domain, taskList, worker.Options{})
	w.RegisterWorkflow(queryWorkflow)
	if err := w.Start(); err != nil {
		return err
	}
	defer w.Stop()

	c := client.NewClient(service, domain, nil)
	we, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskList: taskList, ExecutionStartToCloseTimeout: time.Minute,
	}, queryWorkflow)
	if err != nil {
		return err
	}
	// The first decision task has to finish before queries see the handler.
	var state string
	for range 50 {
		if state, err = queryState(ctx, c, we.GetID()); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "state while running:", state)
	if err := we.Get(ctx, nil); err != nil {
		return err
	}
	if state, err = queryState(ctx, c, we.GetID()); err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, "state after completion:", state)
	return err
}

func main() { common.Main(run) }
