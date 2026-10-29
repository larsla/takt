// Command helloworld is the hello world sample: a workflow that calls one
// activity.
package main

import (
	"context"
	"fmt"
	"io"
	"time"

	"go.uber.org/zap"

	"github.com/larsla/takt"
	"github.com/larsla/takt/activity"
	"github.com/larsla/takt/client"
	"github.com/larsla/takt/examples/common"
	"github.com/larsla/takt/worker"
	"github.com/larsla/takt/workflow"
)

const taskList = "helloWorldGroup"

func helloWorldWorkflow(ctx workflow.Context, name string) (string, error) {
	ao := workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute,
		StartToCloseTimeout:    time.Minute,
		HeartbeatTimeout:       20 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	logger := workflow.GetLogger(ctx)
	logger.Info("helloworld workflow started")

	var result string
	if err := workflow.ExecuteActivity(ctx, helloWorldActivity, name).Get(ctx, &result); err != nil {
		logger.Error("activity failed", zap.Error(err))
		return "", err
	}
	logger.Info("workflow completed", zap.String("result", result))
	return result, nil
}

func helloWorldActivity(ctx context.Context, name string) (string, error) {
	activity.GetLogger(ctx).Info("helloworld activity started")
	return "Hello " + name + "!", nil
}

func run(ctx context.Context, service takt.ServiceClient, domain string, out io.Writer) error {
	w := worker.New(service, domain, taskList, worker.Options{})
	w.RegisterWorkflow(helloWorldWorkflow)
	w.RegisterActivity(helloWorldActivity)
	if err := w.Start(); err != nil {
		return err
	}
	defer w.Stop()

	c := client.NewClient(service, domain, nil)
	we, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskList:                        taskList,
		ExecutionStartToCloseTimeout:    time.Minute,
		DecisionTaskStartToCloseTimeout: time.Minute,
	}, helloWorldWorkflow, "Cadence")
	if err != nil {
		return err
	}
	var result string
	if err := we.Get(ctx, &result); err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "workflow %s finished: %s\n", we.GetID(), result)
	return err
}

func main() { common.Main(run) }
