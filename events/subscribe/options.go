package subscribe

import (
	"time"

	"github.com/levelfourab/windshift-go/delays"
)

type Options struct {
	MaxPendingEvents uint

	// CallRetryBackoff is the backoff strategy to use when acking, rejecting
	// or pinging an event fails.
	CallRetryBackoff delays.DelayDecider

	// AutoPingInterval is the interval at which events should be pinged.
	// Defaults to zero which will determine the ping interval based on the
	// timeout of the consumer.
	AutoPingInterval time.Duration
}

func (o *Options) Apply(opts []Option) {
	for _, opt := range opts {
		opt(o)
	}
}

type Option func(*Options)

// MaxPendingEvents is the maximum number of events to keep ready for
// processing.
//
// If not set this defaults to 50.
func MaxPendingEvents(n uint) Option {
	return func(o *Options) {
		o.MaxPendingEvents = n
	}
}

// WithDefaultRetryBackoff sets the default backoff strategy to use when
// acking, rejecting or pinging an event fails.
func WithDefaultRetryBackoff(decider delays.DelayDecider) Option {
	return func(o *Options) {
		o.CallRetryBackoff = decider
	}
}

// DisableAutoPing disables automatic pinging of events.
func DisableAutoPing() Option {
	return func(o *Options) {
		o.AutoPingInterval = -1
	}
}

// WithAutoPingInterval sets the interval at which events should be pinged.
func WithAutoPingInterval(interval time.Duration) Option {
	return func(o *Options) {
		o.AutoPingInterval = interval
	}
}
