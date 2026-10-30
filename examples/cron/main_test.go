package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/larsla/takt/internal/servertest"
)

func TestSample(t *testing.T) {
	env := servertest.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var out bytes.Buffer
	if err := run(ctx, env.Client, servertest.Domain, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "cron workflow ran 3 times") {
		t.Fatalf("output %q", out.String())
	}
}
