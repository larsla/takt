package web

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/larsla/takt/internal/embeddednats"
	"github.com/larsla/takt/internal/server/migrations"
)

func setup(t *testing.T) (*UI, *nats.Conn) {
	t.Helper()
	s, err := embeddednats.Start(embeddednats.Options{Port: -1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Shutdown)
	nc, err := s.Connect()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := migrations.Apply(context.Background(), js, migrations.Options{}); err != nil {
		t.Fatal(err)
	}
	return New(Options{NC: nc, JS: js}), nc
}

func get(t *testing.T, h http.Handler, path string) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	body, err := io.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatal(err)
	}
	return rec.Code, string(body)
}

func TestOverview(t *testing.T) {
	ui, _ := setup(t)

	code, body := get(t, ui, "/")
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	for _, want := range []string{"<!doctype html>", "TAKT_HISTORY", "takt_executions", "takt_payloads", "schema v1", `hx-get="/partials/overview"`} {
		if !strings.Contains(strings.ToLower(body), strings.ToLower(want)) {
			t.Errorf("page missing %q", want)
		}
	}

	code, body = get(t, ui, "/partials/overview")
	if code != http.StatusOK {
		t.Fatalf("partial status %d", code)
	}
	if strings.Contains(body, "<html") {
		t.Error("partial should not include the layout")
	}
	if !strings.Contains(body, "TAKT_TIMERS") {
		t.Error("partial missing streams")
	}
}

func TestStaticAndHealth(t *testing.T) {
	ui, nc := setup(t)

	code, body := get(t, ui, "/static/htmx.min.js")
	if code != http.StatusOK || !strings.HasPrefix(body, "var htmx") {
		t.Fatalf("htmx: status %d", code)
	}
	if code, _ := get(t, ui, "/healthz"); code != http.StatusOK {
		t.Fatalf("healthz: status %d", code)
	}

	nc.Close()
	if code, _ := get(t, ui, "/healthz"); code != http.StatusServiceUnavailable {
		t.Fatalf("healthz after close: status %d", code)
	}
	if code, _ := get(t, ui, "/"); code != http.StatusInternalServerError {
		t.Fatalf("overview after close: status %d", code)
	}
	if code, _ := get(t, ui, "/partials/overview"); code != http.StatusInternalServerError {
		t.Fatalf("partial after close: status %d", code)
	}
}

func TestFormatBytesViaPage(t *testing.T) {
	ui, _ := setup(t)
	_, body := get(t, ui, "/partials/overview")
	if !strings.Contains(body, " B<") && !strings.Contains(body, "iB<") {
		t.Error("expected formatted byte sizes")
	}
}
