package engine

import (
	"testing"
	"time"

	"github.com/larsla/takt/api"
)

func TestNextCronBackoff(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name      string
		spec      string
		policy    api.CronOverlapPolicy
		scheduled time.Time
		now       time.Time
		want      int32
	}{
		// The run took 1s; the next fire is 5s after it was scheduled, not
		// 5s after it closed.
		{"every on time", "@every 5s", "", t0, t0.Add(time.Second), 4},
		{"every skip", "@every 5s", api.CronOverlapPolicySkipped, t0, t0.Add(7 * time.Second), 3},
		{"every skip many", "@every 5s", "", t0, t0.Add(23 * time.Second), 2},
		{"every buffer one", "@every 5s", api.CronOverlapPolicyBufferone, t0, t0.Add(7 * time.Second), 0},
		{"every buffer one on time", "@every 5s", api.CronOverlapPolicyBufferone, t0, t0.Add(2 * time.Second), 3},
		{"minutely skip", "* * * * *", "", t0, t0.Add(90 * time.Second), 30},
		{"minutely buffer one", "* * * * *", api.CronOverlapPolicyBufferone, t0, t0.Add(90 * time.Second), 0},
		// A late start (scheduled time rounded up) doesn't shift the ticks.
		{"late start", "@every 5s", "", t0.Add(300 * time.Millisecond), t0.Add(time.Second), 4},
		{"invalid", "nope", "", t0, t0, 0},
	}
	for _, c := range cases {
		if got := nextCronBackoff(c.spec, c.policy, c.scheduled, c.now); got != c.want {
			t.Errorf("%s: got %d, want %d", c.name, got, c.want)
		}
	}
}

func TestJitter(t *testing.T) {
	if jitter(0) != 0 || jitter(-1) != 0 {
		t.Fatal("no jitter expected")
	}
	for range 100 {
		if j := jitter(3); j < 0 || j >= 3 {
			t.Fatalf("jitter %d out of range", j)
		}
	}
}
