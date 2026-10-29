package migrations

import (
	"context"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/larsla/takt/internal/embeddednats"
)

func newJS(t *testing.T) jetstream.JetStream {
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
	return js
}

func TestApplyIsIdempotent(t *testing.T) {
	js := newJS(t)
	ctx := context.Background()

	ms, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	want := ms[len(ms)-1].Version

	for i := range 2 {
		v, err := Apply(ctx, js, Options{})
		if err != nil {
			t.Fatalf("apply #%d: %v", i, err)
		}
		if v != want {
			t.Fatalf("apply #%d: version = %d, want %d", i, v, want)
		}
	}
	if v, err := Version(ctx, js); err != nil || v != want {
		t.Fatalf("Version() = %d, %v", v, err)
	}

	for _, name := range []string{"TAKT_HISTORY", "TAKT_TASKS", "TAKT_TIMERS"} {
		if _, err := js.Stream(ctx, name); err != nil {
			t.Errorf("stream %s: %v", name, err)
		}
	}
	for _, name := range []string{"takt_domains", "takt_executions", "takt_visibility", "takt_pollers"} {
		if _, err := js.KeyValue(ctx, name); err != nil {
			t.Errorf("kv %s: %v", name, err)
		}
	}
	if _, err := js.ObjectStore(ctx, "takt_payloads"); err != nil {
		t.Errorf("object store: %v", err)
	}
}

func TestApplyConcurrent(t *testing.T) {
	js := newJS(t)
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for range 4 {
		wg.Go(func() {
			if _, err := Apply(context.Background(), js, Options{}); err != nil {
				errs <- err
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestVersionWithoutMeta(t *testing.T) {
	js := newJS(t)
	v, err := Version(context.Background(), js)
	if err != nil || v != 0 {
		t.Fatalf("Version() = %d, %v; want 0, nil", v, err)
	}
}

// The timer stream is a work queue with schedules enabled. Check that a
// scheduled message still fires there and the fired copy is consumable.
func TestTimerStreamSchedules(t *testing.T) {
	js := newJS(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := Apply(ctx, js, Options{}); err != nil {
		t.Fatal(err)
	}

	msg := nats.NewMsg("takt.timer.sched.d.wf.run.1")
	msg.Header.Set("Nats-Schedule", "@at "+time.Now().Add(time.Second).UTC().Format(time.RFC3339Nano))
	msg.Header.Set("Nats-Schedule-Target", "takt.timer.fire.d.wf.run.1")
	msg.Data = []byte("x")
	if _, err := js.PublishMsg(ctx, msg); err != nil {
		t.Fatal(err)
	}

	cons, err := js.CreateConsumer(ctx, "TAKT_TIMERS", jetstream.ConsumerConfig{
		Durable:       "timer-test",
		FilterSubject: "takt.timer.fire.>",
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := cons.Fetch(1, jetstream.FetchMaxWait(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for m := range batch.Messages() {
		n++
		if err := m.Ack(); err != nil {
			t.Fatal(err)
		}
	}
	if n != 1 {
		t.Fatalf("got %d fired messages, want 1", n)
	}
}

func TestLoadRejectsBadFiles(t *testing.T) {
	cases := map[string]fstest.MapFS{
		"no underscore": {"m/0001.json": {Data: []byte(`{}`)}},
		"bad version":   {"m/abc_x.json": {Data: []byte(`{}`)}},
		"zero version":  {"m/0000_x.json": {Data: []byte(`{}`)}},
		"duplicate": {
			"m/0001_a.json": {Data: []byte(`{}`)},
			"m/0001_b.json": {Data: []byte(`{}`)},
		},
		"bad json": {"m/0001_a.json": {Data: []byte(`{`)}},
	}
	for name, fsys := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := load(fsys, "m"); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestLoadSortsAndSkipsOtherFiles(t *testing.T) {
	fsys := fstest.MapFS{
		"m/0002_b.json": {Data: []byte(`{"description":"b"}`)},
		"m/0001_a.json": {Data: []byte(`{"description":"a"}`)},
		"m/README.md":   {Data: []byte(`hi`)},
	}
	ms, err := load(fsys, "m")
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 2 || ms[0].Version != 1 || ms[1].Description != "b" {
		t.Fatalf("unexpected result: %+v", ms)
	}
}
