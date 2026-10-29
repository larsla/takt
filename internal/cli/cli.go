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
	"runtime/debug"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/larsla/takt/internal/embeddednats"
	"github.com/larsla/takt/internal/server/migrations"
	"github.com/larsla/takt/web"
)

const usage = `takt is a workflow engine on NATS.

Usage:
  takt <command> [flags]

Commands:
  dev       run server, embedded NATS and web UI for local development
  server    run the takt server
  version   print the version

Run "takt <command> -h" for the flags of a command.
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

func version() string {
	if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" {
		return bi.Main.Version
	}
	return "(devel)"
}

type serverConfig struct {
	natsURL      string
	embeddedNATS bool
	natsHost     string
	natsPort     int
	dataDir      string
	uiAddr       string
	replicas     int
	debug        bool
}

func parseServerFlags(mode string, args []string, stderr io.Writer) (serverConfig, error) {
	fs := flag.NewFlagSet(mode, flag.ContinueOnError)
	fs.SetOutput(stderr)
	cfg := serverConfig{}
	fs.StringVar(&cfg.natsURL, "nats-url", nats.DefaultURL, "NATS server to connect to (ignored with -embedded-nats)")
	fs.BoolVar(&cfg.embeddedNATS, "embedded-nats", mode == "dev", "run a NATS server inside this process")
	fs.StringVar(&cfg.natsHost, "nats-host", "127.0.0.1", "listen host for embedded NATS")
	fs.IntVar(&cfg.natsPort, "nats-port", 4222, "listen port for embedded NATS (-1 picks a free port)")
	fs.StringVar(&cfg.dataDir, "data-dir", "", "JetStream store dir for embedded NATS (empty: temporary dir)")
	fs.StringVar(&cfg.uiAddr, "ui-addr", "127.0.0.1:8088", "web UI listen address (empty disables the UI)")
	fs.IntVar(&cfg.replicas, "replicas", 0, "replica count for streams and buckets (0: 1)")
	fs.BoolVar(&cfg.debug, "debug", false, "verbose logging")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if fs.NArg() > 0 {
		return cfg, fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	return cfg, nil
}

// readyInfo is passed to the onReady hook once everything is listening.
type readyInfo struct {
	NATSURL string
	UIAddr  string
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

	nc, err := nats.Connect(natsURL, nats.Name("takt-server"), nats.MaxReconnects(-1))
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

	info := readyInfo{NATSURL: natsURL}
	var srv *http.Server
	errc := make(chan error, 1)
	if cfg.uiAddr != "" {
		ln, err := net.Listen("tcp", cfg.uiAddr)
		if err != nil {
			return fmt.Errorf("ui listen: %w", err)
		}
		srv = &http.Server{
			Handler:           web.New(web.Options{NC: nc, JS: js, Logger: log}),
			ReadHeaderTimeout: 10 * time.Second,
		}
		info.UIAddr = ln.Addr().String()
		go func() { errc <- srv.Serve(ln) }()
		log.Info("web ui listening", "url", "http://"+info.UIAddr)
	}
	if onReady != nil {
		onReady(info)
	}

	select {
	case <-ctx.Done():
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("ui server: %w", err)
		}
	}
	log.Info("shutting down")
	if srv != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return err
		}
	}
	return nil
}
