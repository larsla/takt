// Command retry runs an activity that fails a few times. Its heartbeat
// details let each retry resume where the previous attempt stopped.
package main

import (
	"context"
	"errors"
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

const taskList = "retryActivityGroup"

func retryWorkflow(ctx workflow.Context) (int, error) {
	ao := workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute,
		StartToCloseTimeout:    time.Minute,
		HeartbeatTimeout:       10 * time.Second,
		RetryPolicy: &takt.RetryPolicy{
			InitialInterval:          time.Second,
			BackoffCoefficient:       2.0,
			MaximumInterval:          time.Minute,
			ExpirationInterval:       5 * time.Minute,
			MaximumAttempts:          5,
			NonRetriableErrorReasons: []string{"bad-input"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	var processed int
	err := workflow.ExecuteActivity(ctx, batchProcessingActivity, 0, 20, time.Millisecond).Get(ctx, &processed)
	return processed, err
}

// batchProcessingActivity processes tasks firstTask..batchSize-1 and fails
// every 6th task on the first attempts.
func batchProcessingActivity(ctx context.Context, firstTask, batchSize int, delay time.Duration) (int, error) {
	if batchSize <= 0 {
		return 0, takt.NewCustomError("bad-input", batchSize)
	}
	i := firstTask
	if activity.HasHeartbeatDetails(ctx) {
		var done int
		if err := activity.GetHeartbeatDetails(ctx, &done); err == nil {
			i = done + 1
		}
	}
	info := activity.GetInfo(ctx)
	for ; i < batchSize; i++ {
		time.Sleep(delay)
		activity.RecordHeartbeat(ctx, i)
		if i > 0 && i%6 == 0 && int(info.Attempt) < i/6 {
			return 0, errors.New("simulated transient failure")
		}
	}
	return i, nil
}

func run(ctx context.Context, service takt.ServiceClient, domain string, out io.Writer) error {
	w := worker.New(service, domain, taskList, worker.Options{})
	w.RegisterWorkflow(retryWorkflow)
	w.RegisterActivity(batchProcessingActivity)
	if err := w.Start(); err != nil {
		return err
	}
	defer w.Stop()

	c := client.NewClient(service, domain, nil)
	we, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskList: taskList, ExecutionStartToCloseTimeout: 5 * time.Minute,
	}, retryWorkflow)
	if err != nil {
		return err
	}
	var processed int
	if err := we.Get(ctx, &processed); err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "processed %d tasks\n", processed)
	return err
}

func main() { common.Main(run) }
