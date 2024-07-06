package events

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"google.golang.org/protobuf/proto"
)

// Event is a received event that should be processed. Events must be
// acknowledged using [Event.Ack] or rejected using [Event.Reject]. If the
// event is not acknowledged or rejected within the time frame set by the
// consumer configuration the event will be redelivered.
//
// During processing the event can be pinged using [Event.Ping] to indicate
// that the event is still being processed and that the event should not be
// redelivered.
//
// Events include a [Event.Context] that contains OpenTelemetry tracing
// information. This context should be used when creating new spans to ensure
// that the spans are correctly linked to the event.
//
// Information about the event, such as when it was published, the redelivery
// attempt etc, can be found in [Event.Headers].
type Event interface {
	// Context is the context that the event was created with. This context
	// will carry OpenTelemetry tracing information.
	Context() context.Context

	// ID is the identifier of the event.
	ID() uint64

	// Subject contains the subject the event was published to.
	Subject() string

	// Headers contain information about the event, such as when it was
	// published.
	Headers() Headers

	// UnmarshalNew unmarshals the data of the event into a new instance of
	// the correct type. The type must be imported into your code before
	// calling this method.
	//
	// Example:
	//
	//   import "path/to/your/proto/messages"
	//
	//   var data messages.YourMessageType
	//   if err := event.UnmarshalNew(&data); err != nil {
	//     return err
	//   }
	//
	// Use [Event.UnmarshalTo] if you want to unmarshal into an existing instance.
	UnmarshalNew() (proto.Message, error)

	// UnmarshalTo unmarshals the data of the event into the provided
	// instance. The instance must be a pointer to the correct type.
	//
	// Use [UnmarshalNew] if you want to unmarshal into a new instance.
	UnmarshalTo(v proto.Message) error

	// Ack acknowledges the event, indicating that it was processed
	// successfully.
	Ack(ctx context.Context, opts ...AckOption) error

	// Reject rejects the event, indicating that it was not processed
	// successfully. Depending on the options passed the event may be
	// redelivered.
	//
	// Options may be passed to control how the event is rejected, such as
	// how long to wait before redelivering the event or to indicate that
	// the event should not be redelivered.
	//
	// Examples:
	//
	//   event.Reject(ctx, events.WithRedeliverDelay(5 * time.Second))
	//   event.Reject(ctx, events.Permanently())
	Reject(ctx context.Context, opts ...RejectOption) error

	// Ping indicates that the event is still being processed and that the
	// event should not be redelivered. This is useful for long running
	// processes.
	Ping(ctx context.Context, opts ...PingOption) error
}

// Headers contains information about an event.
type Headers interface {
	// OccurredAt returns the time that the event occurred.
	OccurredAt() time.Time

	// IDempotencyKey returns the idempotency key of the event. Will be an
	// empty string if the event was not published with an idempotency key.
	IdempotencyKey() string

	// DeliveryAttempt returns the number of times the event has been
	// delivered. The first delivery attempt will return 1.
	DeliveryAttempt() int
}

type AckOptions struct {
	Backoff backoff.BackOff
}

func (o *AckOptions) Apply(opts []AckOption) {
	for _, opt := range opts {
		opt.applyToAck(o)
	}
}

type AckOption interface {
	applyToAck(*AckOptions)
}

type RejectOptions struct {
	Delay             time.Duration
	RejectPermanently bool
	RedeliveryDecider func(Event) time.Duration
	Backoff           backoff.BackOff
}

func (o *RejectOptions) Apply(opts []RejectOption) {
	for _, opt := range opts {
		opt.applyToReject(o)
	}
}

// RejectOption is an option that can be passed to [Reject] to control how
// the event is rejected.
type RejectOption interface {
	applyToReject(*RejectOptions)
}

type rejectPermanently struct{}

// Permanently indicates that the event should not be redelivered. Use this
// option when the event is invalid and will never be valid, meaning the
// processing of the event will never succeed.
func Permanently() RejectOption {
	return rejectPermanently{}
}

func (o rejectPermanently) applyToReject(opts *RejectOptions) {
	opts.RejectPermanently = true
}

type redeliverDelay time.Duration

// WithRedeliverDelay indicates that the event should be redelivered after
// the specified delay. This can be used to control how long to wait in case
// of a temporary error.
func WithRedeliverDelay(delay time.Duration) RejectOption {
	return redeliverDelay(delay)
}

func (o redeliverDelay) applyToReject(opts *RejectOptions) {
	opts.Delay = time.Duration(o)
}

type redeliveryDecider func(Event) time.Duration

// WithRedeliveryDecider indicates that the event might be redelivered after
// a certain time based on what the decider returns:
//
//   - A negative duration indicates a permanent rejection.
//   - A zero duration indicates that the event should be redelivered
//     according to the consumer defaults.
//   - A positive duration indicates that the event should be redelivered after
//     the specified duration.
func WithRedeliveryDecider(decider func(Event) time.Duration) RejectOption {
	return redeliveryDecider(decider)
}

func (o redeliveryDecider) applyToReject(opts *RejectOptions) {
	opts.RedeliveryDecider = o
}

type PingOptions struct {
	Backoff backoff.BackOff
}

func (o *PingOptions) Apply(opts []PingOption) {
	for _, opt := range opts {
		opt.applyToPing(o)
	}
}

type PingOption interface {
	applyToPing(*PingOptions)
}

type CallOption interface {
	AckOption
	RejectOption
	PingOption
}

type backoffOption struct {
	backoff backoff.BackOff
}

func (b backoffOption) applyToAck(o *AckOptions) {
	o.Backoff = b.backoff
}

func (b backoffOption) applyToReject(o *RejectOptions) {
	o.Backoff = b.backoff
}

func (b backoffOption) applyToPing(o *PingOptions) {
	o.Backoff = b.backoff
}

// WithExponentialBackoff sets the backoff strategy to use when retrying an
// operation.
//
// The default retry strategy for acknowledging, rejecting and pinging events
// is to retry after 10 milliseconds, with a maximum interval of 1 second.
//
// Example:
//
//	event.Ack(ctx, events.WithExponentialBackoff(
//	  backoff.WithInitialInterval(10*time.Millisecond),
//	  backoff.WithMaxInterval(1*time.Second),
//	))
func WithExponentialBackoff(opts ...backoff.ExponentialBackOffOpts) CallOption {
	return backoffOption{
		backoff: backoff.NewExponentialBackOff(opts...),
	}
}

// WithNoRetry disables retrying an operation.
func WithNoRetry() CallOption {
	return backoffOption{
		backoff: &backoff.StopBackOff{},
	}
}
