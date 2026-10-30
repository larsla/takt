// Package cli implements the takt command line.
package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"runtime/debug"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/larsla/takt/api"
	"github.com/larsla/takt/internal/embeddednats"
	"github.com/larsla/takt/internal/metrics"
	"github.com/larsla/takt/internal/server"
	"github.com/larsla/takt/internal/server/engine"
	"github.com/larsla/takt/internal/server/migrations"
	"github.com/larsla/takt/web"
)

const usage = `takt is a workflow engine on NATS.

Usage:
  takt <command> [flags]

Commands:
  dev       run server, embedded NATS and web UI for local development
  server    run the takt server
  domain    register, describe and list domains
  workflow  start, list, inspect, signal, query, cancel and terminate workflows
  version   print the version

Run "takt <command> -h" for the flags of a command. Every server flag can
also be set with an environment variable: --nats-url is TAKT_NATS_URL.
`

// Run executes the command line and returns the process exit code.
func Run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		_, _ = fmt.Fprint(stderr, usage)
		return 2
	}
	var err error
	switch args[0] {
	case "dev":
		err = runServer(ctx, "dev", args[1:], stderr, nil)
	case "server":
		err = runServer(ctx, "server", args[1:], stderr, nil)
	case "domain":
		err = runDomain(ctx, args[1:], stdout, stderr)
	case "workflow", "wf":
		err = runWorkflow(ctx, args[1:], stdout, stderr)
	case "version":
		_, _ = fmt.Fprintln(stdout, version())
	case "help", "-h", "--help":
		_, _ = fmt.Fprint(stdout, usage)
	default:
		_, _ = fmt.Fprintf(stderr, "unknown command %q\n\n%s", args[0], usage)
		return 2
	}
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "error:", err)
		return 1
	}
	return 0
}

// buildVersion is set at build time with
// -ldflags "-X github.com/larsla/takt/internal/cli.buildVersion=v1.2.3".
var buildVersion string

func version() string {
	if buildVersion != "" {
		return buildVersion
	}
	if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" {
		return bi.Main.Version
	}
	return "(devel)"
}

type serverConfig struct {
	natsURL      string
	natsCreds    string
	embeddedNATS bool
	natsHost     string
	natsPort     int
	dataDir      string
	uiAddr       string
	metricsAddr  string
	replicas     int
	debug        bool
	devDomain    string
	uiWrite      bool
}

func parseServerFlags(mode string, args []string, stderr io.Writer) (serverConfig, error) {
	fs := flag.NewFlagSet(mode, flag.ContinueOnError)
	fs.SetOutput(stderr)
	cfg := serverConfig{}
	fs.StringVar(&cfg.natsURL, "nats-url", nats.DefaultURL, "NATS server to connect to (ignored with -embedded-nats)")
	fs.StringVar(&cfg.natsCreds, "nats-creds", "", "NATS credentials file (JWT and nkey seed)")
	fs.BoolVar(&cfg.embeddedNATS, "embedded-nats", mode == "dev", "run a NATS server inside this process")
	fs.StringVar(&cfg.natsHost, "nats-host", "127.0.0.1", "listen host for embedded NATS")
	fs.IntVar(&cfg.natsPort, "nats-port", 4222, "listen port for embedded NATS (-1 picks a free port)")
	fs.StringVar(&cfg.dataDir, "data-dir", "", "JetStream store dir for embedded NATS (empty: temporary dir)")
	fs.StringVar(&cfg.uiAddr, "ui-addr", "127.0.0.1:8088", "web UI listen address (empty disables the UI)")
	fs.StringVar(&cfg.metricsAddr, "metrics-addr", "", "listen address for Prometheus /metrics and /healthz (empty disables it)")
	fs.IntVar(&cfg.replicas, "replicas", 0, "replica count for streams and buckets (0: 1)")
	fs.BoolVar(&cfg.uiWrite, "ui-write", mode == "dev", "allow signal, cancel and terminate from the web UI")
	fs.BoolVar(&cfg.debug, "debug", false, "verbose logging")
	if mode == "dev" {
		fs.StringVar(&cfg.devDomain, "domain", "default", "domain to register on startup")
	}
	if err := flagsFromEnv(fs); err != nil {
		return cfg, err
	}
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if fs.NArg() > 0 {
		return cfg, fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	return cfg, nil
}

// flagsFromEnv sets flags from TAKT_<NAME> environment variables, with
// dashes as underscores. Command line flags still win.
func flagsFromEnv(fs *flag.FlagSet) error {
	var err error
	fs.VisitAll(func(f *flag.Flag) {
		name := "TAKT_" + strings.ToUpper(strings.ReplaceAll(f.Name, "-", "_"))
		if v, ok := os.LookupEnv(name); ok && err == nil {
			if serr := f.Value.Set(v); serr != nil {
				err = fmt.Errorf("%s: %w", name, serr)
			}
		}
	})
	return err
}

// readyInfo is passed to the onReady hook once everything is listening.
type readyInfo struct {
	NATSURL     string
	UIAddr      string
	MetricsAddr string
}

func runServer(ctx context.Context, mode string, args []string, stderr io.Writer, onReady func(readyInfo)) error {
	cfg, err := parseServerFlags(mode, args, stderr)
	if err != nil {
		return err
	}
	level := slog.LevelInfo
	if cfg.debug {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: level}))

	natsURL := cfg.natsURL
	if cfg.embeddedNATS {
		ns, err := embeddednats.Start(embeddednats.Options{
			Host:     cfg.natsHost,
			Port:     cfg.natsPort,
			StoreDir: cfg.dataDir,
			Debug:    cfg.debug,
		})
		if err != nil {
			return err
		}
		defer ns.Shutdown()
		natsURL = ns.ClientURL()
		log.Info("embedded nats started", "url", natsURL, "data_dir", cfg.dataDir)
	}

	natsOpts := []nats.Option{nats.Name("takt-server"), nats.MaxReconnects(-1)}
	if cfg.natsCreds != "" && !cfg.embeddedNATS {
		natsOpts = append(natsOpts, nats.UserCredentials(cfg.natsCreds))
	}
	nc, err := nats.Connect(natsURL, natsOpts...)
	if err != nil {
		return fmt.Errorf("connect to nats: %w", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}

	v, err := migrations.Apply(ctx, js, migrations.Options{Replicas: cfg.replicas})
	if err != nil {
		return err
	}
	log.Info("schema ready", "version", v)

	scfg := server.Config{NC: nc, JS: js, Logger: log}
	var prom *metrics.Prometheus
	if cfg.metricsAddr != "" {
		prom = metrics.NewPrometheus(log)
		defer func() { _ = prom.Close() }()
		scfg.Metrics = prom.Scope
	}
	takt, err := server.Start(ctx, scfg)
	if err != nil {
		return err
	}
	defer takt.Stop()
	if mode == "dev" {
		if err := ensureDomain(ctx, takt.Engine(), cfg.devDomain); err != nil {
			return err
		}
		log.Info("dev domain ready", "domain", cfg.devDomain)
	}

	info := readyInfo{NATSURL: natsURL}
	var servers []*http.Server
	errc := make(chan error, 2)
	serve := func(what, addr string, h http.Handler) (string, error) {
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			return "", fmt.Errorf("%s listen: %w", what, err)
		}
		srv := &http.Server{Handler: h, ReadHeaderTimeout: 10 * time.Second}
		servers = append(servers, srv)
		go func() { errc <- fmt.Errorf("%s server: %w", what, srv.Serve(ln)) }()
		return ln.Addr().String(), nil
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, srv := range servers {
			_ = srv.Shutdown(shutdownCtx)
		}
	}()
	if cfg.uiAddr != "" {
		ui := web.New(web.Options{NC: nc, JS: js, Backend: takt.Engine(), Logger: log, AllowWrite: cfg.uiWrite})
		if info.UIAddr, err = serve("ui", cfg.uiAddr, ui); err != nil {
			return err
		}
		log.Info("web ui listening", "url", "http://"+info.UIAddr)
	}
	if prom != nil {
		mux := http.NewServeMux()
		mux.Handle("GET /metrics", prom.Handler)
		mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
			if !nc.IsConnected() {
				http.Error(w, "nats disconnected", http.StatusServiceUnavailable)
				return
			}
			_, _ = w.Write([]byte("ok"))
		})
		if info.MetricsAddr, err = serve("metrics", cfg.metricsAddr, mux); err != nil {
			return err
		}
		log.Info("metrics listening", "url", "http://"+info.MetricsAddr+"/metrics")
	}
	if onReady != nil {
		onReady(info)
	}

	select {
	case <-ctx.Done():
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}
	log.Info("shutting down")
	return nil
}

// ensureDomain registers a domain unless it exists.
func ensureDomain(ctx context.Context, e *engine.Engine, name string) error {
	err := e.RegisterDomain(ctx, &api.RegisterDomainRequest{Name: name, Description: "registered by takt dev"})
	var exists *api.DomainAlreadyExistsError
	if errors.As(err, &exists) {
		return nil
	}
	return err
}
