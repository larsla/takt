// Package web serves the takt web UI.
package web

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/a-h/templ"
	"github.com/gorilla/mux"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/larsla/takt/api"
	"github.com/larsla/takt/internal/server/migrations"
	"github.com/larsla/takt/web/views"
)

//go:embed static
var staticFiles embed.FS

// Backend is what the UI reads from and acts on. The server's engine
// implements it.
type Backend interface {
	ListDomains(ctx context.Context, req *api.ListDomainsRequest) (*api.ListDomainsResponse, error)
	ListWorkflowExecutions(ctx context.Context, req *api.ListWorkflowExecutionsRequest) (*api.ListWorkflowExecutionsResponse, error)
	CountWorkflowExecutions(ctx context.Context, req *api.CountWorkflowExecutionsRequest) (*api.CountWorkflowExecutionsResponse, error)
	DescribeWorkflowExecution(ctx context.Context, req *api.DescribeWorkflowExecutionRequest) (*api.DescribeWorkflowExecutionResponse, error)
	GetWorkflowExecutionHistory(ctx context.Context, req *api.GetWorkflowExecutionHistoryRequest) (*api.GetWorkflowExecutionHistoryResponse, error)
	QueryWorkflow(ctx context.Context, req *api.QueryWorkflowRequest) (*api.QueryWorkflowResponse, error)
	DescribeTaskList(ctx context.Context, req *api.DescribeTaskListRequest) (*api.DescribeTaskListResponse, error)
	ListTaskLists(ctx context.Context, req *api.ListTaskListsRequest) (*api.ListTaskListsResponse, error)
	SignalWorkflowExecution(ctx context.Context, req *api.SignalWorkflowExecutionRequest) error
	RequestCancelWorkflowExecution(ctx context.Context, req *api.RequestCancelWorkflowExecutionRequest) error
	TerminateWorkflowExecution(ctx context.Context, req *api.TerminateWorkflowExecutionRequest) error
}

// Options configures the UI handler.
type Options struct {
	NC      *nats.Conn
	JS      jetstream.JetStream
	Backend Backend
	Logger  *slog.Logger
	// AllowWrite enables signal, cancel and terminate actions.
	AllowWrite bool
}

// UI is the HTTP handler for the web UI.
type UI struct {
	nc     *nats.Conn
	js     jetstream.JetStream
	b      Backend
	log    *slog.Logger
	write  bool
	router *mux.Router
}

// New builds the UI handler.
func New(opts Options) *UI {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	u := &UI{nc: opts.NC, js: opts.JS, b: opts.Backend, log: opts.Logger, write: opts.AllowWrite, router: mux.NewRouter()}

	static, err := fs.Sub(staticFiles, "static")
	if err != nil {
		// The embed directive guarantees the directory exists.
		panic(err)
	}
	r := u.router
	r.PathPrefix("/static/").Handler(http.StripPrefix("/static/", http.FileServerFS(static)))
	r.HandleFunc("/healthz", u.healthz).Methods(http.MethodGet)
	r.Handle("/", http.RedirectHandler("/domains", http.StatusFound)).Methods(http.MethodGet)
	r.HandleFunc("/cluster", u.overview).Methods(http.MethodGet)
	r.HandleFunc("/partials/overview", u.overviewPartial).Methods(http.MethodGet)
	r.HandleFunc("/domains", u.domains).Methods(http.MethodGet)
	r.HandleFunc("/domains/{domain}/workflows", u.workflows(false)).Methods(http.MethodGet)
	r.HandleFunc("/partials/domains/{domain}/workflows", u.workflows(true)).Methods(http.MethodGet)
	r.HandleFunc("/domains/{domain}/cron", u.cron).Methods(http.MethodGet)
	r.HandleFunc("/domains/{domain}/workflow", u.workflow("page")).Methods(http.MethodGet)
	r.HandleFunc("/partials/domains/{domain}/workflow/summary", u.workflow("summary")).Methods(http.MethodGet)
	r.HandleFunc("/partials/domains/{domain}/workflow/history", u.workflow("history")).Methods(http.MethodGet)
	r.HandleFunc("/partials/domains/{domain}/workflow/stack", u.stack).Methods(http.MethodGet)
	r.HandleFunc("/domains/{domain}/workflow/query", u.query).Methods(http.MethodPost)
	r.HandleFunc("/domains/{domain}/workflow/{action:signal|cancel|terminate}", u.action).Methods(http.MethodPost)
	r.HandleFunc("/domains/{domain}/tasklists", u.taskLists(false)).Methods(http.MethodGet)
	r.HandleFunc("/partials/domains/{domain}/tasklists", u.taskLists(true)).Methods(http.MethodGet)
	r.HandleFunc("/domains/{domain}/tasklists/{kind:decision|activity}/{name}", u.taskList).Methods(http.MethodGet)
	return u
}

func (u *UI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	u.router.ServeHTTP(w, r)
}

func (u *UI) healthz(w http.ResponseWriter, _ *http.Request) {
	if u.nc == nil || !u.nc.IsConnected() {
		http.Error(w, "nats disconnected", http.StatusServiceUnavailable)
		return
	}
	_, _ = w.Write([]byte("ok"))
}

func (u *UI) overview(w http.ResponseWriter, r *http.Request) {
	o, err := u.loadOverview(r.Context())
	if err != nil {
		u.fail(w, "load overview", err)
		return
	}
	u.render(w, r, views.OverviewPage(o))
}

func (u *UI) overviewPartial(w http.ResponseWriter, r *http.Request) {
	o, err := u.loadOverview(r.Context())
	if err != nil {
		u.fail(w, "load overview", err)
		return
	}
	u.render(w, r, views.OverviewBody(o))
}

func (u *UI) loadOverview(ctx context.Context) (views.Overview, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	o := views.Overview{
		NATSURL:       u.nc.ConnectedUrlRedacted(),
		ServerVersion: u.nc.ConnectedServerVersion(),
	}
	v, err := migrations.Version(ctx, u.js)
	if err != nil {
		return o, err
	}
	o.SchemaVersion = v

	streams := u.js.ListStreams(ctx)
	for info := range streams.Info() {
		s := views.Store{
			Messages: info.State.Msgs,
			Bytes:    info.State.Bytes,
			Subjects: strings.Join(info.Config.Subjects, " "),
		}
		switch name := info.Config.Name; {
		case strings.HasPrefix(name, "KV_"):
			s.Name = strings.TrimPrefix(name, "KV_")
			o.KeyValues = append(o.KeyValues, s)
		case strings.HasPrefix(name, "OBJ_"):
			s.Name = strings.TrimPrefix(name, "OBJ_")
			o.ObjectStores = append(o.ObjectStores, s)
		default:
			s.Name = name
			o.Streams = append(o.Streams, s)
		}
	}
	if err := streams.Err(); err != nil {
		return o, err
	}
	for _, list := range [][]views.Store{o.Streams, o.KeyValues, o.ObjectStores} {
		sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })
	}
	return o, nil
}

func (u *UI) nav(ctx context.Context, domain, active string) views.Nav {
	n := views.Nav{Domain: domain, Active: active}
	if resp, err := u.b.ListDomains(ctx, &api.ListDomainsRequest{}); err == nil {
		for _, d := range resp.Domains {
			n.Domains = append(n.Domains, d.DomainInfo.Name)
		}
	}
	return n
}

func (u *UI) domains(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	resp, err := u.b.ListDomains(ctx, &api.ListDomainsRequest{})
	var rows []views.DomainRow
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	} else {
		for _, d := range resp.Domains {
			row := views.DomainRow{
				Name:          d.DomainInfo.Name,
				Description:   d.DomainInfo.Description,
				RetentionDays: d.Configuration.WorkflowExecutionRetentionPeriodInDays,
			}
			if c, err := u.b.CountWorkflowExecutions(ctx, &api.CountWorkflowExecutionsRequest{Domain: row.Name, Query: "CloseTime = missing"}); err == nil {
				row.Open = c.Count
			}
			rows = append(rows, row)
		}
	}
	u.render(w, r, views.DomainsPage(views.Nav{Active: "domains"}, rows, errMsg))
}

// listQuery builds a visibility query from the workflow list filters.
func listQuery(status, workflowID, workflowType, extra string) string {
	var parts []string
	switch status {
	case "open":
		parts = append(parts, "CloseTime = missing")
	case "closed":
		parts = append(parts, "CloseTime != missing")
	}
	if workflowID != "" {
		parts = append(parts, "WorkflowID = "+quote(workflowID))
	}
	if workflowType != "" {
		parts = append(parts, "WorkflowType = "+quote(workflowType))
	}
	if extra = strings.TrimSpace(extra); extra != "" {
		parts = append(parts, "("+extra+")")
	}
	return strings.Join(parts, " AND ")
}

func quote(s string) string {
	if !strings.Contains(s, "'") {
		return "'" + s + "'"
	}
	return `"` + s + `"`
}

func workflowRow(info *api.WorkflowExecutionInfo) views.WorkflowRow {
	row := views.WorkflowRow{
		WorkflowID:   info.Execution.WorkflowID,
		RunID:        info.Execution.RunID,
		Type:         info.Type.Name,
		Status:       "RUNNING",
		Start:        unixNano(info.StartTime),
		Close:        unixNano(info.CloseTime),
		HistoryLen:   info.HistoryLength,
		CronSchedule: info.CronSchedule,
		Execution:    unixNano(info.ExecutionTime),
	}
	if info.CloseStatus != nil {
		row.Status = string(*info.CloseStatus)
	}
	return row
}

func unixNano(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}

func (u *UI) workflows(partial bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		domain := mux.Vars(r)["domain"]
		q := r.URL.Query()
		l := views.WorkflowList{
			Nav:        u.nav(ctx, domain, "workflows"),
			Status:     q.Get("status"),
			WorkflowID: q.Get("id"),
			Type:       q.Get("type"),
			Query:      q.Get("query"),
		}
		if l.Status == "" {
			l.Status = "open"
		}
		query := listQuery(l.Status, l.WorkflowID, l.Type, l.Query)
		resp, err := u.b.ListWorkflowExecutions(ctx, &api.ListWorkflowExecutionsRequest{
			Domain: domain, PageSize: 50, Query: query, NextPageToken: []byte(q.Get("page")),
		})
		if err != nil {
			l.Error = err.Error()
		} else {
			for _, info := range resp.Executions {
				l.Rows = append(l.Rows, workflowRow(info))
			}
			l.NextToken = string(resp.NextPageToken)
			if c, err := u.b.CountWorkflowExecutions(ctx, &api.CountWorkflowExecutionsRequest{Domain: domain, Query: query}); err == nil {
				l.Count = c.Count
			}
		}
		if partial {
			u.render(w, r, views.WorkflowResults(l))
			return
		}
		u.render(w, r, views.WorkflowsPage(l))
	}
}

func (u *UI) cron(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	domain := mux.Vars(r)["domain"]
	var rows []views.WorkflowRow
	errMsg := ""
	var token []byte
	for {
		resp, err := u.b.ListWorkflowExecutions(ctx, &api.ListWorkflowExecutionsRequest{
			Domain: domain, PageSize: 1000, Query: "CloseTime = missing", NextPageToken: token,
		})
		if err != nil {
			errMsg = err.Error()
			break
		}
		for _, info := range resp.Executions {
			if info.CronSchedule != "" {
				rows = append(rows, workflowRow(info))
			}
		}
		if token = resp.NextPageToken; len(token) == 0 {
			break
		}
	}
	u.render(w, r, views.CronPage(u.nav(ctx, domain, "cron"), rows, errMsg))
}

// loadWorkflow builds the workflow page model.
func (u *UI) loadWorkflow(ctx context.Context, domain, workflowID, runID string) views.Workflow {
	wf := views.Workflow{Nav: u.nav(ctx, domain, "workflows"), WorkflowID: workflowID, RunID: runID, CanWrite: u.write}
	exec := &api.WorkflowExecution{WorkflowID: workflowID, RunID: runID}
	desc, err := u.b.DescribeWorkflowExecution(ctx, &api.DescribeWorkflowExecutionRequest{Domain: domain, Execution: exec})
	if err != nil {
		wf.Error = err.Error()
		return wf
	}
	info := desc.WorkflowExecutionInfo
	wf.RunID = info.Execution.RunID
	exec.RunID = wf.RunID
	row := workflowRow(info)
	wf.Type, wf.Status, wf.Running = row.Type, row.Status, info.CloseStatus == nil

	events, err := u.history(ctx, domain, exec)
	if err != nil {
		wf.Error = err.Error()
		return wf
	}
	wf.Fields = []views.Field{
		{Label: "Workflow type", Value: row.Type, Mono: true},
		{Label: "Task list", Value: info.TaskList, Mono: true, Href: "/domains/" + url.PathEscape(domain) + "/tasklists/decision/" + url.PathEscape(info.TaskList)},
		{Label: "Started", Value: views.Stamp(row.Start)},
	}
	if !row.Close.IsZero() {
		wf.Fields = append(wf.Fields, views.Field{Label: "Closed", Value: views.Stamp(row.Close)})
	}
	if row.Execution.After(row.Start) {
		wf.Fields = append(wf.Fields, views.Field{Label: "First decision", Value: views.Stamp(row.Execution)})
	}
	wf.Fields = append(wf.Fields,
		views.Field{Label: "History length", Value: fmt.Sprint(info.HistoryLength)},
		views.Field{Label: "Timeout", Value: (time.Duration(desc.ExecutionConfiguration.ExecutionStartToCloseTimeoutSeconds) * time.Second).String()},
	)
	if info.CronSchedule != "" {
		wf.Fields = append(wf.Fields, views.Field{Label: "Cron schedule", Value: info.CronSchedule, Mono: true})
	}
	if p := info.ParentExecution; p != nil {
		wf.Fields = append(wf.Fields, views.Field{Label: "Parent", Value: p.WorkflowID, Mono: true, Href: views.WorkflowHref(info.ParentDomain, p.WorkflowID, p.RunID)})
	}
	if len(events) > 0 {
		start := events[0]
		if a := start.WorkflowExecutionStartedEventAttributes; a != nil {
			if a.ContinuedExecutionRunID != "" {
				wf.Fields = append(wf.Fields, views.Field{Label: "Continued from", Value: a.ContinuedExecutionRunID, Mono: true,
					Href: views.WorkflowHref(domain, workflowID, a.ContinuedExecutionRunID)})
			}
			if a.Attempt > 0 {
				wf.Fields = append(wf.Fields, views.Field{Label: "Attempt", Value: fmt.Sprint(a.Attempt + 1)})
			}
			wf.Payloads = append(wf.Payloads, views.Payload{Label: "Input", Body: orNone(views.DecodePayload(a.Input))})
		}
		wf.Payloads = append(wf.Payloads, closePayloads(&wf, domain, workflowID, events[len(events)-1])...)
	}
	if sa := info.SearchAttributes; sa != nil && len(sa.IndexedFields) > 0 {
		wf.Payloads = append(wf.Payloads, views.Payload{Label: "Search attributes", Body: fieldsText(sa.IndexedFields)})
	}
	if m := info.Memo; m != nil && len(m.Fields) > 0 {
		wf.Payloads = append(wf.Payloads, views.Payload{Label: "Memo", Body: fieldsText(m.Fields)})
	}
	for _, a := range desc.PendingActivities {
		wf.Activities = append(wf.Activities, views.Activity{
			ID:            a.ActivityID,
			Type:          a.ActivityType.Name,
			State:         string(a.State),
			Attempt:       a.Attempt,
			MaxAttempts:   a.MaximumAttempts,
			Scheduled:     unixNano(a.ScheduledTimestamp),
			LastStarted:   unixNano(a.LastStartedTimestamp),
			LastHeartbeat: unixNano(a.LastHeartbeatTimestamp),
			Heartbeat:     views.DecodePayload(a.HeartbeatDetails),
			LastFailure:   a.LastFailureReason,
			Worker:        a.LastWorkerIdentity,
		})
	}
	for _, c := range desc.PendingChildren {
		wf.Children = append(wf.Children, views.Child{
			WorkflowID: c.WorkflowID, RunID: c.RunID, Type: c.WorkflowTypeName,
			Href: views.WorkflowHref(c.Domain, c.WorkflowID, c.RunID),
		})
	}
	if d := desc.PendingDecision; d != nil {
		wf.Decision = strings.ToLower(string(d.State))
		if d.Attempt > 0 {
			wf.Decision += fmt.Sprintf(", attempt %d", d.Attempt+1)
		}
	}
	var first time.Time
	for _, e := range events {
		t := unixNano(e.Timestamp)
		if first.IsZero() {
			first = t
		}
		wf.Events = append(wf.Events, views.Event{
			ID:       e.EventID,
			Type:     string(e.EventType),
			Category: views.EventCategory(e.EventType),
			Time:     t,
			Offset:   "+" + t.Sub(first).Round(time.Millisecond).String(),
			Summary:  views.EventSummary(e),
			JSON:     views.EventJSON(e),
		})
	}
	return wf
}

func orNone(s string) string {
	if s == "" {
		return "(none)"
	}
	return s
}

func fieldsText(fields map[string][]byte) string {
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "%s: %s\n", k, views.DecodePayload(fields[k]))
	}
	return b.String()
}

// closePayloads describes how a closed run ended.
func closePayloads(wf *views.Workflow, domain, workflowID string, ev *api.HistoryEvent) []views.Payload {
	switch {
	case ev.WorkflowExecutionCompletedEventAttributes != nil:
		return []views.Payload{{Label: "Result", Body: orNone(views.DecodePayload(ev.WorkflowExecutionCompletedEventAttributes.Result))}}
	case ev.WorkflowExecutionFailedEventAttributes != nil:
		a := ev.WorkflowExecutionFailedEventAttributes
		return []views.Payload{{Label: "Failure: " + a.Reason, Body: orNone(views.DecodePayload(a.Details))}}
	case ev.WorkflowExecutionTerminatedEventAttributes != nil:
		a := ev.WorkflowExecutionTerminatedEventAttributes
		return []views.Payload{{Label: "Terminated", Body: orNone(a.Reason)}}
	case ev.WorkflowExecutionCanceledEventAttributes != nil:
		return []views.Payload{{Label: "Canceled", Body: orNone(views.DecodePayload(ev.WorkflowExecutionCanceledEventAttributes.Details))}}
	case ev.WorkflowExecutionTimedOutEventAttributes != nil:
		return []views.Payload{{Label: "Timed out", Body: string(ev.WorkflowExecutionTimedOutEventAttributes.TimeoutType)}}
	case ev.WorkflowExecutionContinuedAsNewEventAttributes != nil:
		a := ev.WorkflowExecutionContinuedAsNewEventAttributes
		wf.Fields = append(wf.Fields, views.Field{Label: "Continued as", Value: a.NewExecutionRunID, Mono: true,
			Href: views.WorkflowHref(domain, workflowID, a.NewExecutionRunID)})
		if len(a.LastCompletionResult) > 0 {
			return []views.Payload{{Label: "Result", Body: views.DecodePayload(a.LastCompletionResult)}}
		}
	}
	return nil
}

func (u *UI) history(ctx context.Context, domain string, exec *api.WorkflowExecution) ([]*api.HistoryEvent, error) {
	var events []*api.HistoryEvent
	var token []byte
	for {
		resp, err := u.b.GetWorkflowExecutionHistory(ctx, &api.GetWorkflowExecutionHistoryRequest{Domain: domain, Execution: exec, NextPageToken: token})
		if err != nil {
			return nil, err
		}
		events = append(events, resp.History.Events...)
		if token = resp.NextPageToken; len(token) == 0 {
			return events, nil
		}
	}
}

func (u *UI) workflow(part string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		wf := u.loadWorkflow(r.Context(), mux.Vars(r)["domain"], q.Get("id"), q.Get("run"))
		switch part {
		case "summary":
			u.render(w, r, views.WorkflowSummary(wf))
		case "history":
			u.render(w, r, views.WorkflowHistory(wf))
		default:
			u.render(w, r, views.WorkflowPage(wf))
		}
	}
}

func (u *UI) runQuery(ctx context.Context, domain, workflowID, runID, queryType string, args []byte) ([]byte, error) {
	resp, err := u.b.QueryWorkflow(ctx, &api.QueryWorkflowRequest{
		Domain:    domain,
		Execution: &api.WorkflowExecution{WorkflowID: workflowID, RunID: runID},
		Query:     &api.WorkflowQuery{QueryType: queryType, QueryArgs: args},
	})
	if err != nil {
		return nil, err
	}
	if resp.QueryRejected != nil {
		return nil, errors.New("query rejected")
	}
	return resp.QueryResult, nil
}

func (u *UI) stack(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	result, err := u.runQuery(r.Context(), mux.Vars(r)["domain"], q.Get("id"), q.Get("run"), "__stack_trace", nil)
	if err != nil {
		u.render(w, r, views.StackTrace("", err.Error()))
		return
	}
	var trace string
	if err := json.Unmarshal(result, &trace); err != nil {
		trace = string(result)
	}
	u.render(w, r, views.StackTrace(trace, ""))
}

func (u *UI) query(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		u.render(w, r, views.ActionResult(views.QueryResult{Message: err.Error()}))
		return
	}
	args := strings.TrimSpace(r.PostForm.Get("args"))
	var input []byte
	if args != "" {
		input = []byte(args + "\n")
	}
	result, err := u.runQuery(r.Context(), mux.Vars(r)["domain"], r.PostForm.Get("id"), r.PostForm.Get("run"), r.PostForm.Get("type"), input)
	if err != nil {
		u.render(w, r, views.ActionResult(views.QueryResult{Message: err.Error()}))
		return
	}
	u.render(w, r, views.ActionResult(views.QueryResult{OK: true, Message: "Query result", Body: orNone(views.DecodePayload(result))}))
}

// sameOrigin rejects cross-site form posts.
func sameOrigin(r *http.Request) bool {
	if r.Header.Get("HX-Request") != "true" {
		return false
	}
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	o, err := url.Parse(origin)
	return err == nil && o.Host == r.Host
}

func (u *UI) action(w http.ResponseWriter, r *http.Request) {
	if !u.write {
		http.Error(w, "write actions are disabled; start the server with --ui-write", http.StatusForbidden)
		return
	}
	if !sameOrigin(r) {
		http.Error(w, "cross-site request rejected", http.StatusForbidden)
		return
	}
	if err := r.ParseForm(); err != nil {
		u.render(w, r, views.ActionResult(views.QueryResult{Message: err.Error()}))
		return
	}
	ctx := r.Context()
	domain := mux.Vars(r)["domain"]
	f := r.PostForm
	exec := &api.WorkflowExecution{WorkflowID: f.Get("id"), RunID: f.Get("run")}
	var err error
	var done string
	switch mux.Vars(r)["action"] {
	case "signal":
		var input []byte
		if s := strings.TrimSpace(f.Get("input")); s != "" {
			input = []byte(s + "\n")
		}
		err = u.b.SignalWorkflowExecution(ctx, &api.SignalWorkflowExecutionRequest{
			Domain: domain, WorkflowExecution: exec, SignalName: f.Get("name"), Input: input, Identity: "takt-web",
		})
		done = "Signal sent"
	case "cancel":
		err = u.b.RequestCancelWorkflowExecution(ctx, &api.RequestCancelWorkflowExecutionRequest{
			Domain: domain, WorkflowExecution: exec, Identity: "takt-web",
		})
		done = "Cancellation requested"
	case "terminate":
		reason := f.Get("reason")
		if reason == "" {
			reason = "terminated from the web UI"
		}
		err = u.b.TerminateWorkflowExecution(ctx, &api.TerminateWorkflowExecutionRequest{
			Domain: domain, WorkflowExecution: exec, Reason: reason, Identity: "takt-web",
		})
		done = "Workflow terminated"
	}
	if err != nil {
		u.render(w, r, views.ActionResult(views.QueryResult{Message: err.Error()}))
		return
	}
	u.render(w, r, views.ActionResult(views.QueryResult{OK: true, Message: done}))
}

func (u *UI) taskLists(partial bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		domain := mux.Vars(r)["domain"]
		t := views.TaskLists{Nav: u.nav(ctx, domain, "tasklists")}
		resp, err := u.b.ListTaskLists(ctx, &api.ListTaskListsRequest{Domain: domain})
		if err != nil {
			t.Error = err.Error()
		} else {
			for _, l := range resp.TaskLists {
				kind := strings.ToLower(string(l.TaskListType))
				t.Lists = append(t.Lists, views.TaskList{
					Name: l.Name, Type: kind, Pollers: l.Pollers, Backlog: l.Backlog,
					Href: "/domains/" + url.PathEscape(domain) + "/tasklists/" + kind + "/" + url.PathEscape(l.Name),
				})
			}
		}
		if partial {
			u.render(w, r, views.TaskListsBody(t))
			return
		}
		u.render(w, r, views.TaskListsPage(t))
	}
}

func (u *UI) taskList(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	domain, kind, name := vars["domain"], vars["kind"], vars["name"]
	t := views.TaskLists{Nav: u.nav(ctx, domain, "tasklists"), Name: name, Kind: kind}
	tlType := api.TaskListTypeDecision
	if kind == "activity" {
		tlType = api.TaskListTypeActivity
	}
	resp, err := u.b.DescribeTaskList(ctx, &api.DescribeTaskListRequest{Domain: domain, TaskList: &api.TaskList{Name: name}, TaskListType: tlType})
	if err != nil {
		t.Error = err.Error()
	} else {
		for _, p := range resp.Pollers {
			t.Polls = append(t.Polls, views.Poller{Identity: p.Identity, LastAccess: unixNano(p.LastAccessTime)})
		}
	}
	u.render(w, r, views.TaskListPage(t))
}

func (u *UI) render(w http.ResponseWriter, r *http.Request, c templ.Component) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := c.Render(r.Context(), w); err != nil {
		u.log.Error("render", "path", r.URL.Path, "err", err)
	}
}

func (u *UI) fail(w http.ResponseWriter, what string, err error) {
	u.log.Error(what, "err", err)
	http.Error(w, "internal error", http.StatusInternalServerError)
}
