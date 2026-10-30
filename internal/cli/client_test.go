package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/larsla/takt/api"
	"github.com/larsla/takt/internal/servertest"
)

func TestClientCommands(t *testing.T) {
	env := servertest.New(t)
	url := env.NATS.ClientURL()
	run := func(args ...string) (string, error) {
		t.Helper()
		var out, errOut bytes.Buffer
		args = append(args, "--nats-url", url)
		code := Run(context.Background(), args, &out, &errOut)
		if code != 0 {
			return out.String(), &cliError{code: code, stderr: errOut.String()}
		}
		return out.String(), nil
	}
	must := func(want string, args ...string) string {
		t.Helper()
		out, err := run(args...)
		if err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		if !strings.Contains(out, want) {
			t.Fatalf("%v: output %q lacks %q", args, out, want)
		}
		return out
	}

	must("registered", "domain", "register", "--domain", "cli", "--desc", "from the CLI", "--rd", "7")
	must("RetentionInDays:  7", "domain", "describe", "--do", "cli")
	must("cli", "domain", "list")
	if out := must("[", "domain", "list", "--json"); !strings.Contains(out, `"name": "cli"`) {
		t.Fatalf("json list: %s", out)
	}
	if _, err := run("domain", "register", "--domain", "cli"); err == nil {
		t.Fatal("duplicate register should fail")
	}

	must("Started workflow. Workflow ID: wf-1", "workflow", "start", "--do", "cli", "--tl", "tl", "--wt", "MyWorkflow", "--et", "60", "-w", "wf-1", "-i", `"a" {"b":1}`)
	out := must(`"runId"`, "wf", "start", "--do", "cli", "--tl", "tl", "--wt", "MyWorkflow", "--et", "60", "--json")
	var started api.WorkflowExecution
	if err := json.Unmarshal([]byte(out), &started); err != nil || started.RunID == "" {
		t.Fatalf("json start: %s", out)
	}
	must("wf-1", "workflow", "list", "--do", "cli", "--open")
	must("MyWorkflow", "workflow", "list", "--do", "cli", "--query", "WorkflowID = 'wf-1'")
	must("Signal workflow succeeded", "workflow", "signal", "--do", "cli", "-w", "wf-1", "-n", "go", "-i", "42")
	must("WorkflowExecutionSignaled", "workflow", "show", "--do", "cli", "-w", "wf-1")
	must(`"eventType": "WorkflowExecutionStarted"`, "workflow", "show", "--do", "cli", "-w", "wf-1", "--json")
	must(`"historyLength"`, "workflow", "describe", "--do", "cli", "-w", "wf-1")
	must("Cancel workflow succeeded", "workflow", "cancel", "--do", "cli", "-w", "wf-1")
	must("Terminate workflow succeeded", "workflow", "terminate", "--do", "cli", "-w", "wf-1", "--reason", "test")
	must("TERMINATED", "workflow", "list", "--do", "cli", "--closed")

	for _, args := range [][]string{
		{"workflow", "start", "--do", "cli"},
		{"workflow", "start", "--do", "cli", "--tl", "x", "--wt", "y", "--et", "5", "-i", "{bad"},
		{"workflow", "list", "--do", "cli", "--open", "--closed"},
		{"workflow", "describe", "--do", "cli"},
		{"workflow", "signal", "--do", "cli", "-w", "wf-1"},
		{"workflow", "query", "--do", "cli", "-w", "wf-1"},
		{"workflow", "query", "--do", "cli", "-w", "wf-1", "--qt", "state"},
		{"workflow", "bogus", "--do", "cli"},
		{"domain", "bogus"},
		{"domain", "describe", "--do", "missing"},
	} {
		if _, err := run(args...); err == nil {
			t.Errorf("%v should fail", args)
		}
	}
	var out2, errOut bytes.Buffer
	if code := Run(context.Background(), []string{"workflow", "list", "--nats-url", "nats://127.0.0.1:1"}, &out2, &errOut); code != 1 {
		t.Fatalf("unreachable NATS: code %d", code)
	}
	if code := Run(context.Background(), []string{"workflow"}, &out2, &errOut); code != 0 || !strings.Contains(errOut.String(), "takt workflow start") {
		t.Fatalf("workflow usage: %d %q", code, errOut.String())
	}
	if code := Run(context.Background(), []string{"domain"}, &out2, &errOut); code != 0 {
		t.Fatalf("domain usage: %d", code)
	}
}

type cliError struct {
	code   int
	stderr string
}

func (e *cliError) Error() string { return e.stderr }

func TestPayload(t *testing.T) {
	for in, want := range map[string]string{
		"":             "",
		`1`:            "1\n",
		`"a" {"b": 1}`: "\"a\"\n{\"b\": 1}\n",
		` [1,2] `:      "[1,2]\n",
	} {
		got, err := payload(in)
		if err != nil || string(got) != want {
			t.Errorf("payload(%q) = %q, %v", in, got, err)
		}
	}
	if _, err := payload("{"); err == nil {
		t.Error("bad JSON accepted")
	}
	if joinQuery("a", " ") != "a" || joinQuery("a", "b") != "a AND (b)" {
		t.Error("joinQuery")
	}
	if stamp(0) != "" {
		t.Error("stamp")
	}
}
