package mocks_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/larsla/takt/client"
	"github.com/larsla/takt/encoded"
	"github.com/larsla/takt/mocks"
)

var (
	_ client.Client               = (*mocks.Client)(nil)
	_ client.DomainClient         = (*mocks.DomainClient)(nil)
	_ client.WorkflowRun          = (*mocks.WorkflowRun)(nil)
	_ client.HistoryEventIterator = (*mocks.HistoryEventIterator)(nil)
	_ encoded.Value               = (*mocks.Value)(nil)
)

func TestClientMock(t *testing.T) {
	c := mocks.NewClient(t)
	run := mocks.NewWorkflowRun(t)
	run.On("GetID").Return("wf-1")
	c.On("ExecuteWorkflow", mock.Anything, mock.Anything, "flow", 1, "two").Return(run, nil)
	c.On("CancelWorkflow", mock.Anything, "wf-1", "", mock.Anything).Return(errors.New("nope"))
	c.On("SignalWorkflow", mock.Anything, "wf-1", "", "s", nil).Return(func(context.Context, string, string, string, interface{}) error { return nil })

	r, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{}, "flow", 1, "two")
	if err != nil || r.GetID() != "wf-1" {
		t.Fatalf("run %v, err %v", r, err)
	}
	if err := c.CancelWorkflow(context.Background(), "wf-1", "", client.WithCancelReason("r")); err == nil {
		t.Fatal("expected the mocked error")
	}
	if err := c.SignalWorkflow(context.Background(), "wf-1", "", "s", nil); err != nil {
		t.Fatal(err)
	}
}
