package consumers

import (
	"time"

	"github.com/levelfourab/windshift-go/events/streams"
)

// Options contains resolved options for creating or updating a
// consumer.
type Options struct {
	// Name is the name of the consumer. If not set an ephemeral consumer will
	// be created.
	Name string
	// Subjects is a list of subjects to consume events from.
	Subjects []string
	// From is the pointer to start consuming events from.
	From streams.Pointer
	// ProcessingTimeout is the timeout for processing an event. If the timeout
	// is exceeded the event will be redelivered. Leave empty to use the
	// servers default timeout.
	ProcessingTimeout time.Duration
	// MaxDeliveryAttempts is the maximum number of times an event will be
	// delivered before it is considered failed.
	MaxDeliveryAttempts uint
	// InactiveThreshold is the maximum time the consumer can be inactive
	// before it is automatically removed.
	InactiveThreshold time.Duration
}

// Option is an option for creating or updating a consumer.
type Option func(*Options) error

// WithName sets the name of the consumer. This will enable durable delivery
// and shared processing of events for this consumer. Not setting a name will
// create an ephemeral consumer.
func WithName(name string) Option {
	return func(o *Options) error {
		o.Name = name
		return nil
	}
}

// WithSubjects sets the subjects to consume events from. At least one subject
// is required and wildcards are supported.
//
// Examples:
//
//	WithSubjects("user.created", "user.updated")
//	WithSubjects("user.*")
//	WithSubjects("user.>")
//
// Subjects are case-sensitive and should only contain the following
// characters:
//
//   - `a` to `z`, `A` to `Z` and `0` to `9` are allowed.
//
//   - `_` and `-` are allowed for separating words, but the use of
//     camelCase is recommended.
//
//   - `.` is allowed and used as a hierarchy separator, such as
//     `time.us.east` and `time.eu.sweden`, which share the `time`
//     prefix.
//
//   - `*` matches a single token, at any level of the subject. Such as
//     `time.*.east` will match `time.us.east` and `time.eu.east` but
//     not `time.us.west` or `time.us.central.east`. Similarly `time.us.*`
//     will match `time.us.east` but not `time.us.east.atlanta`.
//
//     The `*` wildcard can be used multiple times in a subject, such as
//     `time.*.*` will match `time.us.east` and `time.eu.west` but not
//     `time.us.east.atlanta`.
//
//   - `>` matches one or more tokens at the tail of a subject, and can
//     only be used as the last token. Such as `time.us.>` will match
//     `time.us.east` and `time.us.east.atlanta` but not `time.eu.east`.
//
// See NATS concepts: https://docs.nats.io/nats-concepts/subjects
func WithSubjects(subjects ...string) Option {
	return func(o *Options) error {
		o.Subjects = subjects
		return nil
	}
}

// WithConsumeFrom sets the pointer to start consuming events from. This
// applies only to when the consumer is created, and allows the consumer to
// consume not only new events but historical events as well.
//
// If not specified the consumer will start consuming events from the end of
// the stream.
//
// Examples:
//
//	WithConsumeFrom(streams.AtStreamStart())
//	WithConsumeFrom(streams.AtStreamEnd())
//	WithConsumeFrom(streams.AtStreamOffset(123))
//	WithConsumeFrom(streams.AtStreamTimestamp(time.Now()))
func WithConsumeFrom(pointer streams.Pointer) Option {
	return func(o *Options) error {
		o.From = pointer
		return nil
	}
}

// WithProcessingTimeout sets the timeout for processing an event. If the
// timeout is exceeded the event will be redelivered. If not specified the
// servers default timeout will be used.
func WithProcessingTimeout(timeout time.Duration) Option {
	return func(o *Options) error {
		o.ProcessingTimeout = timeout
		return nil
	}
}

// WithMaxDeliveryAttempts sets the maximum number of times an event will be
// delivered before it is considered failed.
func WithMaxDeliveryAttempts(attempts uint) Option {
	return func(o *Options) error {
		o.MaxDeliveryAttempts = attempts
		return nil
	}
}
