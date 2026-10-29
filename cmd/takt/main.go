// Command takt runs the takt server, web UI and tooling.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/larsla/takt/internal/cli"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := cli.Run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}
