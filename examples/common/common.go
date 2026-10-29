// Package common holds the plumbing shared by the examples: flags, the NATS
// connection and logging.
package common

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/larsla/takt"
)

// Sample runs one example against service in domain and writes its output to out.
type Sample func(ctx context.Context, service takt.ServiceClient, domain string, out io.Writer) error

// Main parses flags, connects to NATS and runs the sample. Start a server
// first with `takt dev`.
func Main(sample Sample) {
	if err := run(sample); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(sample Sample) error {
	url := flag.String("nats", nats.DefaultURL, "NATS server URL")
	domain := flag.String("domain", "default", "takt domain")
	timeout := flag.Duration("timeout", 2*time.Minute, "give up after this long")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	ctx, cancelTimeout := context.WithTimeout(ctx, *timeout)
	defer cancelTimeout()

	service, closeConn, err := takt.Dial(*url, nil)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer closeConn()
	return sample(ctx, service, *domain, os.Stdout)
}
