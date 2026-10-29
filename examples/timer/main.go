// Command timer sends a notification when processing takes longer than a
// threshold, then waits for processing to finish.
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

const taskList = "timerGroup"

func sampleTimerWorkflow(ctx workflow.Context, threshold time.Duration) ([]string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute, StartToCloseTimeout: time.Minute,
	})
	var events []string
	childCtx, cancelTimer := workflow.WithCancel(ctx)
	processing := workflow.ExecuteActivity(ctx, orderProcessingActivity)
	timer := workflow.NewTimer(childCtx, threshold)

	s := workflow.NewSelector(ctx)
	s.AddFuture(processing, func(f workflow.Future) {
		events = append(events, "processing done before threshold")
		cancelTimer()
	})
	s.AddFuture(timer, func(f workflow.Future) {
		if f.Get(ctx, nil) == nil {
			_ = workflow.ExecuteActivity(ctx, sendEmailActivity).Get(ctx, nil)
			events = append(events, "sent slow-processing email")
		}
	})
	s.Select(ctx)
	if !processing.IsReady() {
		s.Select(ctx)
		events = append(events, "processing done")
	}
	return events, nil
}

func orderProcessingActivity() error {
	time.Sleep(2 * time.Second)
	return nil
}

func sendEmailActivity() error { return nil }

func run(ctx context.Context, service takt.ServiceClient, domain string, out io.Writer) error {
	w := worker.New(service, domain, taskList, worker.Options{})
	w.RegisterWorkflow(sampleTimerWorkflow)
	w.RegisterActivity(orderProcessingActivity)
	w.RegisterActivity(sendEmailActivity)
	if err := w.Start(); err != nil {
		return err
	}
	defer w.Stop()

	c := client.NewClient(service, domain, nil)
	we, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskList: taskList, ExecutionStartToCloseTimeout: time.Minute,
	}, sampleTimerWorkflow, time.Second)
	if err != nil {
		return err
	}
	var events []string
	if err := we.Get(ctx, &events); err != nil {
		return err
	}
	for _, e := range events {
		if _, err := fmt.Fprintln(out, e); err != nil {
			return err
		}
	}
	return nil
}

func main() { common.Main(run) }
