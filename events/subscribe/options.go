package subscribe

import "github.com/levelfourab/windshift-go/delays"

type Options struct {
	MaxPendingEvents uint

	// CallRetryBackoff is the backoff strategy to use when acking, rejecting
	// or pinging an event fails.
	CallRetryBackoff delays.DelayDecider
}

func ApplyOptions(opts []Option) Options {
	var options Options
	for _, o := range opts {
		o(&options)
	}
	return options
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
