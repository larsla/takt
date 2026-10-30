// Command cron runs a workflow on a cron schedule. Each run picks up where
// the previous one stopped through its last completion result.
package main

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/larsla/takt"
	"github.com/larsla/takt/.gen/go/shared"
	"github.com/larsla/takt/client"
	"github.com/larsla/takt/examples/common"
	"github.com/larsla/takt/worker"
	"github.com/larsla/takt/workflow"
)

const taskList = "cronGroup"

func sampleCronWorkflow(ctx workflow.Context) (time.Time, error) {
	var lastRun time.Time
	if workflow.HasLastCompletionResult(ctx) {
		if err := workflow.GetLastCompletionResult(ctx, &lastRun); err != nil {
			return time.Time{}, err
		}
	}
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute, StartToCloseTimeout: time.Minute,
	})
	now := workflow.Now(ctx)
	if err := workflow.ExecuteActivity(ctx, sampleCronActivity, lastRun, now).Get(ctx, nil); err != nil {
		return time.Time{}, err
	}
	return now, nil
}

func sampleCronActivity(_ context.Context, lastRun, now time.Time) error {
	if lastRun.IsZero() {
		fmt.Println("first run")
		return nil
	}
	fmt.Printf("%v since the last run\n", now.Sub(lastRun).Round(time.Second))
	return nil
}

func run(ctx context.Context, service takt.ServiceClient, domain string, out io.Writer) error {
	w := worker.New(service, domain, taskList, worker.Options{})
	w.RegisterWorkflow(sampleCronWorkflow)
	w.RegisterActivity(sampleCronActivity)
	if err := w.Start(); err != nil {
		return err
	}
	defer w.Stop()

	c := client.NewClient(service, domain, nil)
	id := fmt.Sprintf("cron_%d", time.Now().UnixNano())
	if _, err := c.StartWorkflow(ctx, client.StartWorkflowOptions{
		ID:                           id,
		TaskList:                     taskList,
		ExecutionStartToCloseTimeout: time.Minute,
		CronSchedule:                 "@every 2s",
	}, sampleCronWorkflow); err != nil {
		return err
	}
	// Let it run three times, then stop the schedule.
	query := fmt.Sprintf("WorkflowID = '%s' AND CloseStatus = 'CONTINUED_AS_NEW'", id)
	for {
		resp, err := c.CountWorkflow(ctx, &shared.CountWorkflowExecutionsRequest{Query: query})
		if err != nil {
			return err
		}
		if resp.Count >= 3 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	if err := c.TerminateWorkflow(ctx, id, "", "sample done", nil); err != nil {
		return err
	}
	_, err := fmt.Fprintln(out, "cron workflow ran 3 times")
	return err
}

func main() { common.Main(run) }
