// Command localactivity uses local activities to pick which regular
// activities to run for a signal.
package main

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/larsla/takt"
	"github.com/larsla/takt/client"
	"github.com/larsla/takt/examples/common"
	"github.com/larsla/takt/worker"
	"github.com/larsla/takt/workflow"
)

const taskList = "localActivityGroup"

func processingWorkflow(ctx workflow.Context, data string) ([]string, error) {
	lao := workflow.LocalActivityOptions{ScheduleToCloseTimeout: time.Second}
	ctx = workflow.WithLocalActivityOptions(ctx, lao)
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute, StartToCloseTimeout: time.Minute,
	})
	var handled []string
	for _, cond := range []string{"_1_", "_2_", "_3_"} {
		var match bool
		if err := workflow.ExecuteLocalActivity(ctx, checkCondition, data, cond).Get(ctx, &match); err != nil {
			return nil, err
		}
		if !match {
			continue
		}
		var r string
		if err := workflow.ExecuteActivity(ctx, processActivity, cond).Get(ctx, &r); err != nil {
			return nil, err
		}
		handled = append(handled, r)
	}
	return handled, nil
}

func checkCondition(_ context.Context, data, cond string) (bool, error) {
	return strings.Contains(data, cond), nil
}

func processActivity(_ context.Context, cond string) (string, error) {
	return "processed " + cond, nil
}

func run(ctx context.Context, service takt.ServiceClient, domain string, out io.Writer) error {
	w := worker.New(service, domain, taskList, worker.Options{})
	w.RegisterWorkflow(processingWorkflow)
	w.RegisterActivity(processActivity)
	if err := w.Start(); err != nil {
		return err
	}
	defer w.Stop()

	c := client.NewClient(service, domain, nil)
	we, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskList: taskList, ExecutionStartToCloseTimeout: time.Minute,
	}, processingWorkflow, "data_1__3_")
	if err != nil {
		return err
	}
	var handled []string
	if err := we.Get(ctx, &handled); err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, strings.Join(handled, ", "))
	return err
}

func main() { common.Main(run) }
