package engine

import (
	"math"
	"slices"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/larsla/takt/api"
)

// normalizeRetryPolicy fills in defaults. A nil policy stays nil.
func normalizeRetryPolicy(p *api.RetryPolicy) *api.RetryPolicy {
	if p == nil {
		return nil
	}
	n := *p
	if n.InitialIntervalInSeconds <= 0 {
		n.InitialIntervalInSeconds = 1
	}
	if n.BackoffCoefficient < 1 {
		n.BackoffCoefficient = 2
	}
	return &n
}

// nextBackoff returns the delay before the attempt after `attempt` (0-based),
// or false if the policy says to stop.
func nextBackoff(p *api.RetryPolicy, attempt int32, reason string, now, expiration int64) (int64, bool) {
	if p.MaximumAttempts > 0 && attempt+1 >= p.MaximumAttempts {
		return 0, false
	}
	if slices.Contains(p.NonRetriableErrorReasons, reason) {
		return 0, false
	}
	interval := float64(p.InitialIntervalInSeconds) * math.Pow(p.BackoffCoefficient, float64(attempt))
	if p.MaximumIntervalInSeconds > 0 {
		interval = math.Min(interval, float64(p.MaximumIntervalInSeconds))
	}
	backoff := int64(interval * float64(time.Second))
	if backoff < 0 || interval > float64(math.MaxInt64/int64(time.Second)) {
		backoff = math.MaxInt64 / 2
	}
	if expiration > 0 && now+backoff > expiration {
		return 0, false
	}
	return backoff, true
}

// Cron schedules use the standard five-field format plus descriptors like
// @every, the same parser options Cadence uses.
var cronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

func validateCron(spec string) error {
	if spec == "" {
		return nil
	}
	if _, err := cronParser.Parse(spec); err != nil {
		return badRequest("invalid cron schedule %q: %v", spec, err)
	}
	return nil
}

// cronBackoff returns the seconds until the next cron fire after now.
func cronBackoff(spec string, now time.Time) int32 {
	sched, err := cronParser.Parse(spec)
	if err != nil {
		return 0
	}
	next := sched.Next(now.UTC())
	return int32(math.Ceil(next.Sub(now.UTC()).Seconds()))
}

// clampInt64 converts a JetStream counter to int64 without overflowing.
func clampInt64(u uint64) int64 {
	if u > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(u)
}

// ceilSeconds converts nanoseconds to whole seconds, rounding up and
// clamping to int32.
func ceilSeconds(ns int64) int32 {
	secs := (ns + int64(time.Second) - 1) / int64(time.Second)
	if secs > math.MaxInt32 {
		return math.MaxInt32
	}
	if secs < 0 {
		return 0
	}
	return int32(secs)
}
