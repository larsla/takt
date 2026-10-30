package client_test

import (
	"errors"
	"testing"

	"github.com/larsla/takt"
	"github.com/larsla/takt/client"
)

func TestIsWorkflowError(t *testing.T) {
	if !client.IsWorkflowError(takt.NewCustomError("r")) || !client.IsWorkflowError(takt.NewCanceledError()) {
		t.Fatal("workflow errors not recognized")
	}
	if client.IsWorkflowError(errors.New("connection refused")) {
		t.Fatal("plain error taken for a workflow error")
	}
}
