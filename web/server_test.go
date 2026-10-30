package web

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/larsla/takt/internal/sdk"
	"github.com/larsla/takt/internal/servertest"
)

const domain = servertest.Domain

func waitForSignal(ctx sdk.Context, name string) (string, error) {
	if err := sdk.SetQueryHandler(ctx, "name", func() (string, error) { return name, nil }); err != nil {
		return "", err
	}
	ctx = sdk.WithActivityOptions(ctx, sdk.ActivityOptions{ScheduleToStartTimeout: time.Minute, StartToCloseTimeout: time.Minute})
	_ = sdk.ExecuteActivity(ctx, "neverRegistered")
	var v string
	sdk.GetSignalChannel(ctx, "go").Receive(ctx, &v)
	return "hello " + name + " " + v, nil
}

type fixture struct {
	t   *testing.T
	env *servertest.Env
	ui  *UI
	ctx context.Context
}

func setup(t *testing.T, write bool) *fixture {
	t.Helper()
	env := servertest.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	w := sdk.NewWorker(env.Client, domain, "web-tl", sdk.WorkerOptions{})
	w.RegisterWorkflow(waitForSignal)
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Stop)
	c := sdk.NewClient(env.Client, domain, nil)
	opts := sdk.StartWorkflowOptions{ID: "wf-open", TaskList: "web-tl", ExecutionStartToCloseTimeout: time.Hour}
	if _, err := c.StartWorkflow(ctx, opts, waitForSignal, "web"); err != nil {
		t.Fatal(err)
	}
	opts.ID = "wf-done"
	run, err := c.ExecuteWorkflow(ctx, opts, waitForSignal, "done")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.SignalWorkflow(ctx, "wf-done", "", "go", "now"); err != nil {
		t.Fatal(err)
	}
	if err := run.Get(ctx, nil); err != nil {
		t.Fatal(err)
	}
	opts.ID, opts.CronSchedule = "wf-cron", "@every 1h"
	if _, err := c.StartWorkflow(ctx, opts, waitForSignal, "cron"); err != nil {
		t.Fatal(err)
	}
	// Let wf-open's first decision finish so queries work.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := c.QueryWorkflow(ctx, "wf-open", "", "name"); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	return &fixture{t: t, env: env, ctx: ctx, ui: New(Options{NC: env.NC, JS: env.JS, Backend: env.Server.Engine(), AllowWrite: write})}
}

func (f *fixture) do(method, path string, form url.Values, htmx bool) (int, string) {
	f.t.Helper()
	var body io.Reader
	if form != nil {
		body = strings.NewReader(form.Encode())
	}
	req := httptest.NewRequest(method, path, body)
	if form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	if htmx {
		req.Header.Set("HX-Request", "true")
	}
	rec := httptest.NewRecorder()
	f.ui.ServeHTTP(rec, req)
	b, _ := io.ReadAll(rec.Result().Body)
	return rec.Code, string(b)
}

func (f *fixture) get(path string, want ...string) string {
	f.t.Helper()
	code, body := f.do(http.MethodGet, path, nil, false)
	if code != http.StatusOK {
		f.t.Fatalf("GET %s: status %d: %.300s", path, code, body)
	}
	for _, w := range want {
		if !strings.Contains(body, w) {
			f.t.Errorf("GET %s: missing %q", path, w)
		}
	}
	return body
}

func TestPages(t *testing.T) {
	f := setup(t, true)
	base := "/domains/" + domain

	code, _ := f.do(http.MethodGet, "/", nil, false)
	if code != http.StatusFound {
		t.Fatalf("root: %d", code)
	}
	f.get("/domains", domain, "Open workflows")
	f.get(base+"/workflows", "wf-open", "wf-cron", `hx-trigger="every 3s"`)
	body := f.get(base+"/workflows?status=closed", "wf-done", "COMPLETED")
	if strings.Contains(body, ">wf-open<") {
		t.Error("closed tab lists an open workflow")
	}
	f.get(base + "/workflows?status=all&type=waitForSignal")
	f.get("/partials"+base+"/workflows?status=all&id=wf-done", "wf-done", "1 matching")
	f.get(base+"/workflows?status=all&query=WorkflowType+%3D", "Query failed")
	f.get(base+"/cron", "wf-cron", "@every 1h")
	f.get(base+"/tasklists", "web-tl")
	f.get("/partials"+base+"/tasklists", `id="tasklists"`)
	f.get(base+"/tasklists/decision/web-tl", "Pollers")
	f.get("/cluster", "TAKT_HISTORY", "schema v1")
	f.get("/partials/overview", "TAKT_TIMERS")

	done := f.get(base+"/workflow?id=wf-done", "wf-done", "WorkflowExecutionCompleted", "hello done now", "Result", "Input")
	if strings.Contains(done, `hx-trigger="every 2s"`) {
		t.Error("a closed workflow should not poll")
	}
	open := f.get(base+"/workflow?id=wf-open", "RUNNING", "Pending activities", "neverRegistered", `hx-trigger="every 2s"`, "Terminate")
	if !strings.Contains(open, "ActivityTaskScheduled") {
		t.Error("history missing")
	}
	f.get("/partials"+base+"/workflow/summary?id=wf-open", `id="summary"`)
	f.get("/partials"+base+"/workflow/history?id=wf-open", `id="history"`)
	f.get("/partials"+base+"/workflow/stack?id=wf-open", "coroutine root")
	f.get(base+"/workflow?id=missing", "Could not load workflow")

	code, body = f.do(http.MethodPost, base+"/workflow/query", url.Values{"id": {"wf-open"}, "type": {"name"}}, true)
	if code != http.StatusOK || !strings.Contains(body, "&#34;web&#34;") {
		t.Fatalf("query: %d %s", code, body)
	}
	_, body = f.do(http.MethodPost, base+"/workflow/query", url.Values{"id": {"wf-open"}, "type": {"nope"}}, true)
	if !strings.Contains(body, "Failed") {
		t.Fatalf("bad query: %s", body)
	}
}

func TestActions(t *testing.T) {
	f := setup(t, true)
	base := "/domains/" + domain + "/workflow/"
	if code, _ := f.do(http.MethodPost, base+"signal", url.Values{"id": {"wf-open"}, "name": {"go"}}, false); code != http.StatusForbidden {
		t.Fatalf("post without HX-Request: %d", code)
	}
	code, body := f.do(http.MethodPost, base+"signal", url.Values{"id": {"wf-open"}, "name": {"go"}, "input": {`"from web"`}}, true)
	if code != http.StatusOK || !strings.Contains(body, "Signal sent") {
		t.Fatalf("signal: %d %s", code, body)
	}
	c := sdk.NewClient(f.env.Client, domain, nil)
	var result string
	if err := c.GetWorkflow(f.ctx, "wf-open", "").Get(f.ctx, &result); err != nil || result != "hello web from web" {
		t.Fatalf("result %q err %v", result, err)
	}
	_, body = f.do(http.MethodPost, base+"cancel", url.Values{"id": {"wf-cron"}}, true)
	if !strings.Contains(body, "Cancellation requested") {
		t.Fatalf("cancel: %s", body)
	}
	_, body = f.do(http.MethodPost, base+"terminate", url.Values{"id": {"wf-cron"}}, true)
	if !strings.Contains(body, "Workflow terminated") {
		t.Fatalf("terminate: %s", body)
	}
	_, body = f.do(http.MethodPost, base+"terminate", url.Values{"id": {"wf-cron"}}, true)
	if !strings.Contains(body, "Failed") {
		t.Fatalf("second terminate: %s", body)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, base+"cancel", strings.NewReader("id=wf-cron"))
	req.Header.Set("HX-Request", "true")
	New(Options{NC: f.env.NC, JS: f.env.JS, Backend: f.env.Server.Engine()}).ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("read-only UI allowed an action: %d", rec.Code)
	}
	req = httptest.NewRequest(http.MethodPost, base+"cancel", strings.NewReader("id=wf-cron"))
	req.Header.Set("HX-Request", "true")
	req.Header.Set("Origin", "https://evil.example")
	rec = httptest.NewRecorder()
	f.ui.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("cross-origin action allowed: %d", rec.Code)
	}
}

func TestStaticAndHealth(t *testing.T) {
	env := servertest.New(t)
	ui := New(Options{NC: env.NC, JS: env.JS, Backend: env.Server.Engine()})
	f := &fixture{t: t, env: env, ui: ui}
	f.get("/static/htmx.min.js", "htmx")
	f.get("/static/app.css")
	f.get("/healthz", "ok")
	env.NC.Close()
	if code, _ := f.do(http.MethodGet, "/healthz", nil, false); code != http.StatusServiceUnavailable {
		t.Fatalf("healthz after close: %d", code)
	}
	if code, _ := f.do(http.MethodGet, "/cluster", nil, false); code != http.StatusInternalServerError {
		t.Fatalf("cluster after close: %d", code)
	}
	if code, _ := f.do(http.MethodGet, "/partials/overview", nil, false); code != http.StatusInternalServerError {
		t.Fatalf("overview partial after close: %d", code)
	}
}

func TestListQuery(t *testing.T) {
	cases := map[string]string{
		listQuery("open", "", "", ""):          "CloseTime = missing",
		listQuery("closed", "a", "", ""):       "CloseTime != missing AND WorkflowID = 'a'",
		listQuery("all", "it's", "t", "x = 1"): `WorkflowID = "it's" AND WorkflowType = 't' AND (x = 1)`,
		listQuery("all", "", "", ""):           "",
	}
	for got, want := range cases {
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
}
