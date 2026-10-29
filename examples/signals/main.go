// Command signals counts signals until it receives "exit".
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

const taskList = "signalsGroup"

func signalCounterWorkflow(ctx workflow.Context) (int, error) {
	total := 0
	ch := workflow.GetSignalChannel(ctx, "add")
	exit := workflow.GetSignalChannel(ctx, "exit")
	for {
		done := false
		s := workflow.NewSelector(ctx)
		s.AddReceive(ch, func(c workflow.Channel, more bool) {
			var n int
			c.Receive(ctx, &n)
			total += n
		})
		s.AddReceive(exit, func(c workflow.Channel, more bool) {
			c.Receive(ctx, nil)
			done = true
		})
		s.Select(ctx)
		if done {
			return total, nil
		}
	}
}

func run(ctx context.Context, service takt.ServiceClient, domain string, out io.Writer) error {
	w := worker.New(service, domain, taskList, worker.Options{})
	w.RegisterWorkflow(signalCounterWorkflow)
	if err := w.Start(); err != nil {
		return err
	}
	defer w.Stop()

	c := client.NewClient(service, domain, nil)
	we, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskList: taskList, ExecutionStartToCloseTimeout: time.Minute,
	}, signalCounterWorkflow)
	if err != nil {
		return err
	}
	for _, n := range []int{1, 2, 3} {
		if err := c.SignalWorkflow(ctx, we.GetID(), "", "add", n); err != nil {
			return err
		}
	}
	if err := c.SignalWorkflow(ctx, we.GetID(), "", "exit", nil); err != nil {
		return err
	}
	var total int
	if err := we.Get(ctx, &total); err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "total of signals: %d\n", total)
	return err
}

func main() { common.Main(run) }
