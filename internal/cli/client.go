package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"

	"github.com/larsla/takt/api"
	"github.com/larsla/takt/internal/transport"
)

const domainUsage = `Usage:
  takt domain register [flags]   register a domain
  takt domain describe [flags]   show a domain
  takt domain list [flags]       list domains
`

const workflowUsage = `Usage:
  takt workflow start [flags]      start a workflow
  takt workflow list [flags]       list workflows
  takt workflow describe [flags]   show a workflow's state
  takt workflow show [flags]       print a workflow's history
  takt workflow signal [flags]     signal a workflow
  takt workflow query [flags]      query a workflow
  takt workflow cancel [flags]     request cancellation
  takt workflow terminate [flags]  terminate a workflow
`

// clientFlags are shared by the domain and workflow commands.
type clientFlags struct {
	fs      *flag.FlagSet
	natsURL string
	domain  string
	json    bool
	timeout time.Duration
}

func newClientFlags(name string, stderr io.Writer) *clientFlags {
	c := &clientFlags{fs: flag.NewFlagSet(name, flag.ContinueOnError)}
	c.fs.SetOutput(stderr)
	url := os.Getenv("TAKT_NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}
	dom := os.Getenv("TAKT_DOMAIN")
	if dom == "" {
		dom = "default"
	}
	c.fs.StringVar(&c.natsURL, "nats-url", url, "NATS server (env TAKT_NATS_URL)")
	c.stringVar(&c.domain, dom, "domain (env TAKT_DOMAIN)", "domain", "do")
	c.fs.BoolVar(&c.json, "json", false, "print JSON")
	c.fs.DurationVar(&c.timeout, "timeout", 30*time.Second, "give up after this long")
	return c
}

// stringVar registers a flag under several names, like the Cadence CLI's aliases.
func (c *clientFlags) stringVar(p *string, value, usage string, names ...string) {
	*p = value
	for _, n := range names {
		c.fs.StringVar(p, n, value, usage)
	}
}

func (c *clientFlags) intVar(p *int, value int, usage string, names ...string) {
	*p = value
	for _, n := range names {
		c.fs.IntVar(p, n, value, usage)
	}
}

func (c *clientFlags) connect(ctx context.Context) (api.Service, func(), context.Context, context.CancelFunc, error) {
	nc, err := nats.Connect(c.natsURL, nats.Name("takt-cli"))
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("connect to nats: %w", err)
	}
	svc, err := transport.New(nc, transport.Options{RequestTimeout: c.timeout})
	if err != nil {
		nc.Close()
		return nil, nil, nil, nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	return svc, nc.Close, ctx, cancel, nil
}

func printJSON(w io.Writer, v interface{}) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// payload turns a CLI JSON argument into the newline-separated encoding
// the default data converter uses.
func payload(s string) ([]byte, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	dec := json.NewDecoder(strings.NewReader(s))
	var out []byte
	for dec.More() {
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			return nil, fmt.Errorf("input is not JSON: %w", err)
		}
		out = append(out, v...)
		out = append(out, '\n')
	}
	return out, nil
}

func runDomain(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		_, _ = fmt.Fprint(stderr, domainUsage)
		return flag.ErrHelp
	}
	c := newClientFlags("domain "+args[0], stderr)
	var desc, owner string
	var retention int
	c.stringVar(&desc, "", "description", "description", "desc")
	c.stringVar(&owner, "", "owner email", "owner_email", "oe")
	c.intVar(&retention, 3, "retention in days", "retention", "rd")
	if err := c.fs.Parse(args[1:]); err != nil {
		return err
	}
	svc, closeConn, ctx, cancel, err := c.connect(ctx)
	if err != nil {
		return err
	}
	defer closeConn()
	defer cancel()

	switch args[0] {
	case "register":
		err := svc.RegisterDomain(ctx, &api.RegisterDomainRequest{
			Name: c.domain, Description: desc, OwnerEmail: owner,
			WorkflowExecutionRetentionPeriodInDays: clamp32(retention),
		})
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(stdout, "Domain %s registered.\n", c.domain)
		return err
	case "describe":
		d, err := svc.DescribeDomain(ctx, &api.DescribeDomainRequest{Name: c.domain})
		if err != nil {
			return err
		}
		if c.json {
			return printJSON(stdout, d)
		}
		tw := tabwriter.NewWriter(stdout, 0, 2, 2, ' ', 0)
		fmt.Fprintf(tw, "Name:\t%s\n", d.DomainInfo.Name)
		fmt.Fprintf(tw, "UUID:\t%s\n", d.DomainInfo.UUID)
		fmt.Fprintf(tw, "Description:\t%s\n", d.DomainInfo.Description)
		fmt.Fprintf(tw, "OwnerEmail:\t%s\n", d.DomainInfo.OwnerEmail)
		fmt.Fprintf(tw, "Status:\t%s\n", d.DomainInfo.Status)
		fmt.Fprintf(tw, "RetentionInDays:\t%d\n", d.Configuration.WorkflowExecutionRetentionPeriodInDays)
		return tw.Flush()
	case "list":
		resp, err := svc.ListDomains(ctx, &api.ListDomainsRequest{})
		if err != nil {
			return err
		}
		if c.json {
			return printJSON(stdout, resp.Domains)
		}
		tw := tabwriter.NewWriter(stdout, 0, 2, 2, ' ', 0)
		fmt.Fprintln(tw, "NAME\tRETENTION\tDESCRIPTION")
		for _, d := range resp.Domains {
			fmt.Fprintf(tw, "%s\t%dd\t%s\n", d.DomainInfo.Name, d.Configuration.WorkflowExecutionRetentionPeriodInDays, d.DomainInfo.Description)
		}
		return tw.Flush()
	}
	_, _ = fmt.Fprint(stderr, domainUsage)
	return fmt.Errorf("unknown domain command %q", args[0])
}

func runWorkflow(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		_, _ = fmt.Fprint(stderr, workflowUsage)
		return flag.ErrHelp
	}
	c := newClientFlags("workflow "+args[0], stderr)
	var (
		workflowID, runID, taskList, wfType, input, cron, query, name, reason string
		execTimeout, taskTimeout, pageSize                                    int
		open, closed                                                          bool
	)
	c.stringVar(&workflowID, "", "workflow ID", "workflow_id", "wid", "w")
	c.stringVar(&runID, "", "run ID", "run_id", "rid", "r")
	c.stringVar(&taskList, "", "task list", "tasklist", "tl")
	c.stringVar(&wfType, "", "workflow type", "workflow_type", "wt")
	c.stringVar(&input, "", "input as JSON; several values separated by spaces", "input", "i")
	c.stringVar(&cron, "", "cron schedule", "cron")
	c.stringVar(&query, "", "visibility query for list, query type for query", "query", "q")
	c.stringVar(&name, "", "signal name or query type", "name", "n", "query_type", "qt")
	c.stringVar(&reason, "", "termination reason", "reason", "re")
	c.intVar(&execTimeout, 0, "execution timeout in seconds", "execution_timeout", "et")
	c.intVar(&taskTimeout, 10, "decision task timeout in seconds", "decision_timeout", "dt")
	c.intVar(&pageSize, 100, "maximum workflows to list", "pagesize", "ps")
	c.fs.BoolVar(&open, "open", false, "list only open workflows")
	c.fs.BoolVar(&closed, "closed", false, "list only closed workflows")
	if err := c.fs.Parse(args[1:]); err != nil {
		return err
	}
	svc, closeConn, ctx, cancel, err := c.connect(ctx)
	if err != nil {
		return err
	}
	defer closeConn()
	defer cancel()
	exec := &api.WorkflowExecution{WorkflowID: workflowID, RunID: runID}
	needID := func() error {
		if workflowID == "" {
			return errors.New("--workflow_id is required")
		}
		return nil
	}

	switch args[0] {
	case "start":
		if taskList == "" || wfType == "" || execTimeout <= 0 {
			return errors.New("--tasklist, --workflow_type and --execution_timeout are required")
		}
		in, err := payload(input)
		if err != nil {
			return err
		}
		if workflowID == "" {
			workflowID = uuid.NewString()
		}
		resp, err := svc.StartWorkflowExecution(ctx, &api.StartWorkflowExecutionRequest{
			Domain:                              c.domain,
			WorkflowID:                          workflowID,
			WorkflowType:                        &api.WorkflowType{Name: wfType},
			TaskList:                            &api.TaskList{Name: taskList},
			Input:                               in,
			ExecutionStartToCloseTimeoutSeconds: clamp32(execTimeout),
			TaskStartToCloseTimeoutSeconds:      clamp32(taskTimeout),
			Identity:                            "takt-cli",
			RequestID:                           uuid.NewString(),
			CronSchedule:                        cron,
		})
		if err != nil {
			return err
		}
		if c.json {
			return printJSON(stdout, api.WorkflowExecution{WorkflowID: workflowID, RunID: resp.RunID})
		}
		_, err = fmt.Fprintf(stdout, "Started workflow. Workflow ID: %s, Run ID: %s\n", workflowID, resp.RunID)
		return err

	case "list":
		q := query
		switch {
		case open && closed:
			return errors.New("--open and --closed are exclusive")
		case open:
			q = joinQuery("CloseTime = missing", q)
		case closed:
			q = joinQuery("CloseTime != missing", q)
		}
		resp, err := svc.ListWorkflowExecutions(ctx, &api.ListWorkflowExecutionsRequest{Domain: c.domain, Query: q, PageSize: clamp32(pageSize)})
		if err != nil {
			return err
		}
		if c.json {
			return printJSON(stdout, resp.Executions)
		}
		tw := tabwriter.NewWriter(stdout, 0, 2, 2, ' ', 0)
		fmt.Fprintln(tw, "WORKFLOW TYPE\tWORKFLOW ID\tRUN ID\tSTATUS\tSTART TIME\tCLOSE TIME")
		for _, e := range resp.Executions {
			status := "RUNNING"
			if e.CloseStatus != nil {
				status = string(*e.CloseStatus)
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n", e.Type.Name, e.Execution.WorkflowID, e.Execution.RunID,
				status, stamp(e.StartTime), stamp(e.CloseTime))
		}
		return tw.Flush()

	case "describe":
		if err := needID(); err != nil {
			return err
		}
		d, err := svc.DescribeWorkflowExecution(ctx, &api.DescribeWorkflowExecutionRequest{Domain: c.domain, Execution: exec})
		if err != nil {
			return err
		}
		return printJSON(stdout, d)

	case "show":
		if err := needID(); err != nil {
			return err
		}
		var events []*api.HistoryEvent
		var token []byte
		for {
			resp, err := svc.GetWorkflowExecutionHistory(ctx, &api.GetWorkflowExecutionHistoryRequest{Domain: c.domain, Execution: exec, NextPageToken: token})
			if err != nil {
				return err
			}
			events = append(events, resp.History.Events...)
			if token = resp.NextPageToken; len(token) == 0 {
				break
			}
		}
		if c.json {
			return printJSON(stdout, events)
		}
		tw := tabwriter.NewWriter(stdout, 0, 2, 2, ' ', 0)
		for _, e := range events {
			fmt.Fprintf(tw, "%d\t%s\t%s\n", e.EventID, stamp(e.Timestamp), e.EventType)
		}
		return tw.Flush()

	case "signal":
		if err := needID(); err != nil {
			return err
		}
		if name == "" {
			return errors.New("--name is required")
		}
		in, err := payload(input)
		if err != nil {
			return err
		}
		if err := svc.SignalWorkflowExecution(ctx, &api.SignalWorkflowExecutionRequest{
			Domain: c.domain, WorkflowExecution: exec, SignalName: name, Input: in, Identity: "takt-cli", RequestID: uuid.NewString(),
		}); err != nil {
			return err
		}
		_, err = fmt.Fprintln(stdout, "Signal workflow succeeded.")
		return err

	case "query":
		if err := needID(); err != nil {
			return err
		}
		qt := name
		if qt == "" {
			qt = query
		}
		if qt == "" {
			return errors.New("--query_type is required")
		}
		in, err := payload(input)
		if err != nil {
			return err
		}
		resp, err := svc.QueryWorkflow(ctx, &api.QueryWorkflowRequest{
			Domain: c.domain, Execution: exec, Query: &api.WorkflowQuery{QueryType: qt, QueryArgs: in},
		})
		if err != nil {
			return err
		}
		if qt == "__stack_trace" {
			var trace string
			if json.Unmarshal(resp.QueryResult, &trace) == nil {
				_, err = fmt.Fprintln(stdout, trace)
				return err
			}
		}
		_, err = stdout.Write(resp.QueryResult)
		return err

	case "cancel":
		if err := needID(); err != nil {
			return err
		}
		if err := svc.RequestCancelWorkflowExecution(ctx, &api.RequestCancelWorkflowExecutionRequest{
			Domain: c.domain, WorkflowExecution: exec, Identity: "takt-cli", RequestID: uuid.NewString(),
		}); err != nil {
			return err
		}
		_, err = fmt.Fprintln(stdout, "Cancel workflow succeeded.")
		return err

	case "terminate":
		if err := needID(); err != nil {
			return err
		}
		if err := svc.TerminateWorkflowExecution(ctx, &api.TerminateWorkflowExecutionRequest{
			Domain: c.domain, WorkflowExecution: exec, Reason: reason, Identity: "takt-cli",
		}); err != nil {
			return err
		}
		_, err = fmt.Fprintln(stdout, "Terminate workflow succeeded.")
		return err
	}
	_, _ = fmt.Fprint(stderr, workflowUsage)
	return fmt.Errorf("unknown workflow command %q", args[0])
}

func joinQuery(a, b string) string {
	if strings.TrimSpace(b) == "" {
		return a
	}
	return a + " AND (" + b + ")"
}

func stamp(n int64) string {
	if n == 0 {
		return ""
	}
	return time.Unix(0, n).Format(time.RFC3339)
}

// clamp32 converts a flag value to int32, clamping it to [0, MaxInt32].
func clamp32(n int) int32 {
	if n < 0 {
		return 0
	}
	if n > math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(n)
}
