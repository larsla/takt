// Package embeddednats runs a NATS server with JetStream inside the current
// process. It is meant for development (takt dev) and tests.
package embeddednats

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// Options configures the embedded server.
type Options struct {
	// Host to listen on. Defaults to 127.0.0.1.
	Host string
	// Port to listen on. 0 picks the NATS default (4222), -1 picks a random free port.
	Port int
	// StoreDir holds JetStream data. Empty means a fresh temporary directory
	// that is removed on Shutdown.
	StoreDir string
	// ReadyTimeout bounds how long Start waits for the server. Defaults to 10s.
	ReadyTimeout time.Duration
	// Debug enables NATS server logging to stderr.
	Debug bool
}

// Server is a running embedded NATS server.
type Server struct {
	ns      *server.Server
	tempDir string
}

// Start launches a NATS server with JetStream enabled and waits until it
// accepts connections.
func Start(opts Options) (*Server, error) {
	if opts.Host == "" {
		opts.Host = "127.0.0.1"
	}
	if opts.ReadyTimeout == 0 {
		opts.ReadyTimeout = 10 * time.Second
	}

	s := &Server{}
	storeDir := opts.StoreDir
	if storeDir == "" {
		dir, err := os.MkdirTemp("", "takt-nats-*")
		if err != nil {
			return nil, fmt.Errorf("create temp store dir: %w", err)
		}
		s.tempDir = dir
		storeDir = dir
	}

	ns, err := server.NewServer(&server.Options{
		ServerName: "takt-embedded",
		Host:       opts.Host,
		Port:       opts.Port,
		JetStream:  true,
		StoreDir:   storeDir,
		NoSigs:     true,
	})
	if err != nil {
		s.cleanup()
		return nil, fmt.Errorf("create nats server: %w", err)
	}
	if opts.Debug {
		ns.ConfigureLogger()
	}
	s.ns = ns

	go ns.Start()
	if !ns.ReadyForConnections(opts.ReadyTimeout) {
		ns.Shutdown()
		s.cleanup()
		return nil, errors.New("embedded nats server not ready in time")
	}
	return s, nil
}

// ClientURL returns the URL clients connect to.
func (s *Server) ClientURL() string {
	return s.ns.ClientURL()
}

// Connect opens a client connection to the embedded server.
func (s *Server) Connect(opts ...nats.Option) (*nats.Conn, error) {
	return nats.Connect(s.ClientURL(), opts...)
}

// Shutdown stops the server and removes a temporary store dir if Start created one.
func (s *Server) Shutdown() {
	s.ns.Shutdown()
	s.ns.WaitForShutdown()
	s.cleanup()
}

func (s *Server) cleanup() {
	if s.tempDir != "" {
		_ = os.RemoveAll(s.tempDir)
		s.tempDir = ""
	}
}
