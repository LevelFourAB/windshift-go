package events

import (
	"context"
	"time"

	"github.com/levelfourab/windshift-go/delays"
	"google.golang.org/protobuf/proto"
)

// Subscription is a handle to an active subscription. The subscription owns
// its own lifecycle: the context passed to [Client.Subscribe] is only used for
// setup (consumer lookup and tracing) and does not tear the subscription down.
// Use [Subscription.Drain] for a graceful shutdown that settles in-flight
// events, or [Subscription.Stop] for an immediate one.
//
// This mirrors the vocabulary of [net/http.Server] (Shutdown / Close).
type Subscription interface {
	// Events is the channel of received events. It is closed once Drain
	// completes or Stop is called, so ranging over it terminates.
	Events() <-chan Event

	// Drain stops pulling new events and blocks until every
	// already-delivered event has been settled (Ack/Reject), bounded by
	// ctx. Returns nil when all settled; ctx's error (wrapped) if the
	// deadline elapses with events still outstanding; ErrSubscriptionStopped
	// if Stop was called. The Events channel is closed on return. Idempotent.
	//
	// Like [net/http.Server.Shutdown], a deadline does not force-kill
	// outstanding events: they simply fall back to redelivery once the
	// consumer's processing timeout (see [WithProcessingTimeout]) elapses.
	// [Event.Context] is never canceled by Drain; callers must settle
	// in-flight events with a context that outlives the drain.
	Drain(ctx context.Context) error

	// Stop immediately stops the subscription, abandoning outstanding
	// events. Abandoned events are redelivered once the consumer's
	// processing timeout (see [WithProcessingTimeout]) elapses. Idempotent;
	// preempts an in-progress Drain.
	Stop()
}

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

	// DeliveryAttempt returns the number of times the event has been
	// delivered. The first delivery attempt will return 1.
	DeliveryAttempt() uint

	// Headers contain static information about the event, such as when it was
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
	//   data, err := event.UnmarshalNew()
	//
	// Use [Event.UnmarshalTo] if you want to unmarshal into an existing instance.
	UnmarshalNew() (proto.Message, error)

	// UnmarshalTo unmarshals the data of the event into the provided
	// instance. The instance must be a pointer to the correct type.
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
	// Use [Event.UnmarshalNew] if you want to unmarshal into a new instance.
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

	// IdempotencyKey returns the idempotency key of the event. Will be an
	// empty string if the event was not published with an idempotency key.
	IdempotencyKey() string
}

type AckOptions struct {
	Backoff delays.DelayDecider
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
	Backoff delays.DelayDecider

	RejectPermanently bool
	Delay             time.Duration
	RedeliveryDecider func(Event) time.Duration
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

// WithRedeliveryDelay indicates that the event should be redelivered after
// the specified delay. This can be used to control how long to wait in case
// of a temporary error.
func WithRedeliveryDelay(delay time.Duration) RejectOption {
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
	Backoff delays.DelayDecider
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
	PublishOption
}

type backoffOption delays.DelayDecider

func (b backoffOption) applyToAck(o *AckOptions) {
	o.Backoff = delays.DelayDecider(b)
}

func (b backoffOption) applyToReject(o *RejectOptions) {
	o.Backoff = delays.DelayDecider(b)
}

func (b backoffOption) applyToPing(o *PingOptions) {
	o.Backoff = delays.DelayDecider(b)
}

func (b backoffOption) applyToPublish(o *PublishOptions) {
	o.Backoff = delays.DelayDecider(b)
}

// WithBackoff sets the backoff strategy to use when retrying an operation.
//
// The default retry strategy for acknowledging, rejecting and pinging events
// is to retry after 10 milliseconds, with a maximum total time of 5 seconds.
//
// Example:
//
//	event.Ack(ctx, events.WithBackoff(
//	  delays.StopAfterMaxTime(delays.Exponential(10*time.Millisecond, 2), 10*time.Second),
//	))
func WithBackoff(decider delays.DelayDecider) CallOption {
	return backoffOption(decider)
}

// WithNoRetry disables retrying an operation.
func WithNoRetry() CallOption {
	return backoffOption(delays.Never())
}

// SubscribeOptions contains resolved options for a subscription.
type SubscribeOptions struct {
	// Prefetch is the number of events to keep buffered locally and ready for
	// processing. Maps to the pull consumer's max in-flight message buffer.
	Prefetch uint

	// CallRetryBackoff is the backoff strategy to use when acking, rejecting
	// or pinging an event fails.
	CallRetryBackoff delays.DelayDecider

	// AutoPingInterval is the interval at which events should be pinged.
	// Defaults to zero which will determine the ping interval based on the
	// timeout of the consumer.
	AutoPingInterval time.Duration

	// DrainPrefetched controls what happens to events that have been
	// prefetched locally but not yet delivered to the Events channel when
	// [Subscription.Drain] is called. Defaults to false (abandon prefetched).
	DrainPrefetched bool
}

func (o *SubscribeOptions) Apply(opts []SubscribeOption) {
	for _, opt := range opts {
		opt.applyToSubscribe(o)
	}
}

// SubscribeOption is an option for configuring a subscription.
type SubscribeOption interface {
	applyToSubscribe(*SubscribeOptions)
}

type prefetchOption uint

func (o prefetchOption) applyToSubscribe(opts *SubscribeOptions) {
	opts.Prefetch = uint(o)
}

// WithPrefetch sets the number of events to keep buffered locally and ready
// for processing.
//
// If not set this defaults to 50.
func WithPrefetch(n uint) SubscribeOption {
	return prefetchOption(n)
}

type defaultRetryBackoffOption struct {
	decider delays.DelayDecider
}

func (o defaultRetryBackoffOption) applyToSubscribe(opts *SubscribeOptions) {
	opts.CallRetryBackoff = o.decider
}

// WithDefaultRetryBackoff sets the default backoff strategy to use when
// acking, rejecting or pinging an event fails.
func WithDefaultRetryBackoff(decider delays.DelayDecider) SubscribeOption {
	return defaultRetryBackoffOption{decider: decider}
}

type withoutAutoPingOption struct{}

func (withoutAutoPingOption) applyToSubscribe(opts *SubscribeOptions) {
	opts.AutoPingInterval = -1
}

// WithoutAutoPing disables automatic pinging of events.
func WithoutAutoPing() SubscribeOption {
	return withoutAutoPingOption{}
}

type withDrainPrefetchedOption struct{}

func (withDrainPrefetchedOption) applyToSubscribe(opts *SubscribeOptions) {
	opts.DrainPrefetched = true
}

// WithDrainPrefetched makes [Subscription.Drain] flush events that have been
// prefetched locally but not yet delivered to the Events channel through the
// channel during the drain window, instead of abandoning them.
//
// By default (without this option) prefetched-but-undelivered events are
// abandoned when draining and redelivered once the consumer's processing
// timeout (see [WithProcessingTimeout]) elapses. This option only reduces
// the number of such redeliveries.
//
// This is best-effort. The underlying NATS consumer exposes no signal for
// when its prefetch buffer has been fully processed: an immediate stop
// discards buffered messages, while a drain processes them through the
// handler with no completion notification. With this option there is
// therefore a small window where the internal count of unsettled events can
// momentarily reach zero between two buffered events, allowing Drain to
// return slightly early. No event is ever lost: any unsettled event is
// redelivered once the processing timeout elapses. Without this option the
// behavior is exact, because once intake has been stopped no further events
// are handled and the unsettled count only decreases.
func WithDrainPrefetched() SubscribeOption {
	return withDrainPrefetchedOption{}
}

type autoPingIntervalOption time.Duration

func (o autoPingIntervalOption) applyToSubscribe(opts *SubscribeOptions) {
	opts.AutoPingInterval = time.Duration(o)
}

// WithAutoPingInterval sets the interval at which events should be pinged.
func WithAutoPingInterval(interval time.Duration) SubscribeOption {
	return autoPingIntervalOption(interval)
}
