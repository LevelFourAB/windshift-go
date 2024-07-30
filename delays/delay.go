package delays

import (
	"math"
	"math/rand/v2"
	"time"
)

// Stop indicates that no more retries should be made.
const Stop time.Duration = -1

// DelayDecider returns the duration to wait before the next retry based on
// the current attempt and when the action being retried started.
//
// - attempt is the current attempt, starting at 1.
// - startTime is the time when the first attempt was made.
//
// May return [Stop] to indicate that no more retries should be made.
type DelayDecider func(attempt uint, startTime time.Time) time.Duration

// Never returns a delay decider that never retries.
func Never() DelayDecider {
	return func(_ uint, _ time.Time) time.Duration {
		return Stop
	}
}

// Constant returns a delay decider that always returns the same delay.
func Constant(d time.Duration) DelayDecider {
	return func(uint, time.Time) time.Duration {
		return d
	}
}

// Exponential returns a delay decider that returns an exponential delay.
// The delay is calculated as initial * multiplier^(attempt-1).
//
// Example:
//
//	decider := Exponential(1*time.Second, 2)
//	decider(1, ...) // 1s
//	decider(2, ...) // 2s
//	decider(3, ...) // 4s
func Exponential(initial time.Duration, multiplier float64) DelayDecider {
	return func(attempt uint, _ time.Time) time.Duration {
		return time.Duration(float64(initial) * math.Pow(multiplier, float64(attempt-1)))
	}
}

// WithJitter add a jitter to the delay returned by another delay decider.
// The jitter will be a random value between -jitter and +jitter.
//
// Example:
//
//	WithJitter(Exponential(1*time.Second, 2), 100*time.Millisecond)
func WithJitter(decider DelayDecider, jitter time.Duration) DelayDecider {
	return func(attempt uint, startTime time.Time) time.Duration {
		delay := decider(attempt, startTime)

		minInterval := math.Max(0, float64(delay-jitter))
		maxInterval := float64(delay + jitter)
		return time.Duration(minInterval + rand.Float64()*(maxInterval-minInterval+1))
	}
}

// WithScaledJitter takes an existing delay decider and add a jitter based on
// a scale factor.
//
// Scale should be between 0 and 1. A scale of 0 will return the delay without
// any jitter. A scale of 1 will return a random value between 0 and 2*delay.

// Example:
//
//	WithJitterFactor(Exponential(1*time.Second, 2), 0.1)
func WithJitterFactor(decider DelayDecider, jitterScale float64) DelayDecider {
	return func(attempt uint, startTime time.Time) time.Duration {
		delay := decider(attempt, startTime)
		jitter := time.Duration(jitterScale * float64(delay))

		minInterval := float64(delay - jitter)
		maxInterval := float64(delay + jitter)
		return time.Duration(minInterval + rand.Float64()*(maxInterval-minInterval+1))
	}
}

// WithMaxDelay applies a maximum delay to an existing delay decider. This
// will not limit the number of attempts, only the delay between attempts.
//
// Example:
//
//	decider := WithMaxDelay(Exponential(1*time.Second, 2), 5*time.Second)
//	decider(1, ...) // 1s
//	decider(2, ...) // 2s
//	decider(3, ...) // 4s
//	decider(4, ...) // 5s (instead of 8s)
func WithMaxDelay(decider DelayDecider, maxDelay time.Duration) DelayDecider {
	return func(attempt uint, startTime time.Time) time.Duration {
		delay := decider(attempt, startTime)
		if maxDelay == 0 {
			return delay
		}

		if delay > maxDelay {
			return maxDelay
		}

		return delay
	}
}

// StopAfterMaxAttempts creates a [DelayDecider] that stops the retrying after
// a certain number of attempts.
//
// Example:
//
//	decider := StopAfterMaxAttempts(Exponential(1*time.Second, 2), 3)
//	decider(1, ...) // 1s
//	decider(2, ...) // 2s
//	decider(3, ...) // 4s
//	decider(4, ...) // backoff.Stop
func StopAfterMaxAttempts(decider DelayDecider, maxAttempts uint) DelayDecider {
	return func(attempt uint, startTime time.Time) time.Duration {
		if maxAttempts == 0 {
			return decider(attempt, startTime)
		}

		if attempt >= maxAttempts {
			return Stop
		}

		return decider(attempt, startTime)
	}
}

// StopAfterMaxDelay creates a [DelayDecider] that stops the retrying after a
// certain delay has been reached.
//
// Example:
//
//	decider := StopAfterMaxDelay(Exponential(1*time.Second, 2), 5*time.Second)
//	decider(1) // 1s
//	decider(2) // 2s
//	decider(3) // 4s
//	decider(4) // backoff.Stop
func StopAfterMaxDelay(decider DelayDecider, maxDelay time.Duration) DelayDecider {
	return func(attempt uint, startTime time.Time) time.Duration {
		delay := decider(attempt, startTime)
		if maxDelay == 0 {
			return delay
		}

		if delay > maxDelay {
			return Stop
		}

		return delay
	}
}

// StopAfterMaxTime creates a [DelayDecider] that stops the retrying after a
// certain amount of time has passed.
//
// Example:
//
//	decider := StopAfterMaxTime(Exponential(1*time.Second, 2), 5*time.Second)
//
//	startTime := time.Now()
//	decider(1, startTime) // 1s
//
//	time.Sleep(5 * time.Second)
//	decider(2, startTime) // backoff.Stop
func StopAfterMaxTime(decider DelayDecider, maxTime time.Duration) DelayDecider {
	return func(attempt uint, startTime time.Time) time.Duration {
		if maxTime == 0 {
			return decider(attempt, startTime)
		}

		if time.Since(startTime) > maxTime {
			return Stop
		}

		return decider(attempt, startTime)
	}
}
