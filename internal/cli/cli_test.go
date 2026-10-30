package cli

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestRunUsage(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := Run(context.Background(), nil, &out, &errOut); code != 2 {
		t.Fatalf("no args: code %d", code)
	}
	if code := Run(context.Background(), []string{"bogus"}, &out, &errOut); code != 2 {
		t.Fatalf("unknown: code %d", code)
	}
	if !strings.Contains(errOut.String(), `unknown command "bogus"`) {
		t.Errorf("stderr = %q", errOut.String())
	}
	out.Reset()
	if code := Run(context.Background(), []string{"help"}, &out, &errOut); code != 0 || !strings.Contains(out.String(), "Commands:") {
		t.Fatalf("help: code %d, out %q", code, out.String())
	}
	out.Reset()
	if code := Run(context.Background(), []string{"version"}, &out, &errOut); code != 0 || out.Len() == 0 {
		t.Fatalf("version: code %d, out %q", code, out.String())
	}
}

func TestRunFlagErrors(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := Run(context.Background(), []string{"server", "-h"}, &out, &errOut); code != 0 {
		t.Fatalf("-h: code %d", code)
	}
	if code := Run(context.Background(), []string{"server", "--no-such-flag"}, &out, &errOut); code != 1 {
		t.Fatalf("bad flag: code %d", code)
	}
	if code := Run(context.Background(), []string{"dev", "extra"}, &out, &errOut); code != 1 {
		t.Fatalf("extra arg: code %d", code)
	}
}

func TestServerConnectFailure(t *testing.T) {
	var out, errOut bytes.Buffer
	// Port 1 on localhost refuses connections.
	code := Run(context.Background(), []string{"server", "--nats-url", "nats://127.0.0.1:1", "--ui-addr", ""}, &out, &errOut)
	if code != 1 || !strings.Contains(errOut.String(), "connect to nats") {
		t.Fatalf("code %d, stderr %q", code, errOut.String())
	}
}

func TestDevServesUI(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ready := make(chan readyInfo, 1)
	done := make(chan error, 1)
	go func() {
		done <- runServer(ctx, "dev", []string{"--nats-port", "-1", "--ui-addr", "127.0.0.1:0", "--metrics-addr", "127.0.0.1:0", "--data-dir", t.TempDir()}, io.Discard, func(r readyInfo) { ready <- r })
	}()

	var info readyInfo
	select {
	case info = <-ready:
	case err := <-done:
		t.Fatalf("server exited early: %v", err)
	case <-time.After(20 * time.Second):
		t.Fatal("server not ready")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+info.UIAddr+"/domains", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), ">default<") {
		t.Fatalf("status %d, body %.200s", resp.StatusCode, body)
	}

	for path, want := range map[string]string{"/metrics": "go_goroutines", "/healthz": "ok"} {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+info.MetricsAddr+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), want) {
			t.Fatalf("%s: status %d, body %.300s", path, resp.StatusCode, body)
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("server did not stop")
	}
}

func TestServerAgainstExternalNATS(t *testing.T) {
	// Start a dev instance for its NATS, then point a plain server at it.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	devReady := make(chan readyInfo, 1)
	go func() {
		_ = runServer(ctx, "dev", []string{"--nats-port", "-1", "--ui-addr", ""}, io.Discard, func(r readyInfo) { devReady <- r })
	}()
	var dev readyInfo
	select {
	case dev = <-devReady:
	case <-time.After(20 * time.Second):
		t.Fatal("dev not ready")
	}

	srvCtx, srvCancel := context.WithCancel(ctx)
	srvReady := make(chan readyInfo, 1)
	done := make(chan error, 1)
	go func() {
		done <- runServer(srvCtx, "server", []string{"--nats-url", dev.NATSURL, "--ui-addr", ""}, io.Discard, func(r readyInfo) { srvReady <- r })
	}()
	select {
	case <-srvReady:
	case err := <-done:
		t.Fatalf("server exited: %v", err)
	case <-time.After(20 * time.Second):
		t.Fatal("server not ready")
	}
	srvCancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestFlagsFromEnv(t *testing.T) {
	t.Setenv("TAKT_REPLICAS", "3")
	t.Setenv("TAKT_UI_ADDR", ":9999")
	cfg, err := parseServerFlags("server", []string{"--ui-addr", ":1"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.replicas != 3 || cfg.uiAddr != ":1" {
		t.Fatalf("replicas %d, ui %q", cfg.replicas, cfg.uiAddr)
	}
	t.Setenv("TAKT_REPLICAS", "many")
	if _, err := parseServerFlags("server", nil, io.Discard); err == nil || !strings.Contains(err.Error(), "TAKT_REPLICAS") {
		t.Fatalf("want an error naming TAKT_REPLICAS, got %v", err)
	}
}
