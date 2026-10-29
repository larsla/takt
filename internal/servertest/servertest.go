// Package servertest starts a complete takt stack for tests: embedded NATS,
// migrations, server and a client.
package servertest

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/larsla/takt/api"
	"github.com/larsla/takt/internal/embeddednats"
	"github.com/larsla/takt/internal/server"
	"github.com/larsla/takt/internal/server/migrations"
	"github.com/larsla/takt/internal/transport"
)

// Domain is registered by New.
const Domain = "test-domain"

// Env is a running stack.
type Env struct {
	NATS   *embeddednats.Server
	NC     *nats.Conn
	JS     jetstream.JetStream
	Server *server.Server
	Client *transport.Client
}

// Options tunes New.
type Options struct {
	// Logger defaults to discarding output.
	Logger *slog.Logger
	// PollTimeout for the client. Defaults to 2s.
	PollTimeout time.Duration
	// SweepInterval for the server. Defaults to 1h (effectively off).
	SweepInterval time.Duration
}

// New starts a stack and registers Domain. Everything stops at test cleanup.
func New(t testing.TB, opts ...Options) *Env {
	t.Helper()
	var o Options
	if len(opts) > 0 {
		o = opts[0]
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if o.PollTimeout == 0 {
		o.PollTimeout = 2 * time.Second
	}
	if o.SweepInterval == 0 {
		o.SweepInterval = time.Hour
	}

	ns, err := embeddednats.Start(embeddednats.Options{Port: -1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ns.Shutdown)
	nc, err := ns.Connect()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := migrations.Apply(ctx, js, migrations.Options{}); err != nil {
		t.Fatal(err)
	}
	srv, err := server.Start(ctx, server.Config{NC: nc, JS: js, Logger: o.Logger, SweepInterval: o.SweepInterval})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Stop)
	client, err := transport.New(nc, transport.Options{PollTimeout: o.PollTimeout})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.RegisterDomain(ctx, &api.RegisterDomainRequest{Name: Domain}); err != nil {
		t.Fatal(err)
	}
	return &Env{NATS: ns, NC: nc, JS: js, Server: srv, Client: client}
}
