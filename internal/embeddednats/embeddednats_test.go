package embeddednats

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func startTest(t *testing.T) *Server {
	t.Helper()
	s, err := Start(Options{Port: -1})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(s.Shutdown)
	return s
}

func TestStartAndConnect(t *testing.T) {
	s := startTest(t)
	nc, err := s.Connect()
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.AccountInfo(context.Background()); err != nil {
		t.Fatalf("jetstream not enabled: %v", err)
	}
}

// TestMessageScheduleAt checks that JetStream delivers an @at scheduled
// message to its target subject. The timer service depends on this.
func TestMessageScheduleAt(t *testing.T) {
	s := startTest(t)
	nc, err := s.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:              "SPIKE",
		Subjects:          []string{"spike.sched.>", "spike.fire.>"},
		AllowMsgSchedules: true,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}

	fireAt := time.Now().Add(1500 * time.Millisecond)
	msg := nats.NewMsg("spike.sched.t1")
	msg.Header.Set("Nats-Schedule", "@at "+fireAt.UTC().Format(time.RFC3339Nano))
	msg.Header.Set("Nats-Schedule-Target", "spike.fire.t1")
	msg.Data = []byte("payload")
	if _, err := js.PublishMsg(ctx, msg); err != nil {
		t.Fatalf("publish schedule: %v", err)
	}

	cons, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		FilterSubject: "spike.fire.>",
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := cons.Fetch(1, jetstream.FetchMaxWait(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var got jetstream.Msg
	for m := range batch.Messages() {
		got = m
	}
	if got == nil {
		t.Fatal("scheduled message never fired")
	}
	if string(got.Data()) != "payload" {
		t.Fatalf("payload = %q", got.Data())
	}
	if early := time.Until(fireAt); early > 0 {
		t.Fatalf("fired %v early", early)
	}
	t.Logf("fired %v after target time", time.Since(fireAt))
}
