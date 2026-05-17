package events

import (
	"context"
	"fmt"
	"time"
)

// Consumer contains information about a defined consumer.
type Consumer interface {
	// Name is the unique identifier of the consumer. Will be an auto-generated
	// identifier for ephemeral consumers.
	Name() string

	// Subscribe starts consuming events from this consumer.
	//
	// The returned [Subscription] owns the lifecycle of the subscription; the
	// ctx is only used for setup (consumer lookup and tracing). Use
	// [Subscription.Drain] or [Subscription.Stop] to shut it down.
	Subscribe(ctx context.Context, opts ...SubscribeOption) (Subscription, error)
}

// ConsumerOptions contains resolved options for creating or updating a
// consumer.
type ConsumerOptions struct {
	// Name is the name of the consumer. If not set an ephemeral consumer will
	// be created.
	Name string
	// Subjects is a list of subjects to consume events from.
	Subjects []string
	// From is the pointer to start consuming events from.
	From Pointer
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
	// MaxPendingEvents is the maximum number of events that can be pending for
	// this consumer. If not specified the server's default will be used.
	MaxPendingEvents uint
}

func (o *ConsumerOptions) Apply(opts []ConsumerOption) error {
	for _, opt := range opts {
		err := opt.applyToConsumer(o)
		if err != nil {
			return fmt.Errorf("failed to apply consumer option: %w", err)
		}
	}

	return nil
}

// ConsumerOption is an option for creating or updating a consumer.
type ConsumerOption interface {
	applyToConsumer(*ConsumerOptions) error
}

type nameOption string

func (o nameOption) applyToConsumer(opts *ConsumerOptions) error {
	opts.Name = string(o)
	return nil
}

// WithName sets the name of the consumer. This will enable durable delivery
// and shared processing of events for this consumer. Not setting a name will
// create an ephemeral consumer.
func WithName(name string) ConsumerOption {
	return nameOption(name)
}

type consumeFromOption struct {
	pointer Pointer
}

func (o consumeFromOption) applyToConsumer(opts *ConsumerOptions) error {
	opts.From = o.pointer
	return nil
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
//	WithConsumeFrom(AtStreamStart())
//	WithConsumeFrom(AtStreamEnd())
//	WithConsumeFrom(AtStreamOffset(123))
//	WithConsumeFrom(AtStreamTimestamp(time.Now()))
func WithConsumeFrom(pointer Pointer) ConsumerOption {
	return consumeFromOption{pointer: pointer}
}

type processingTimeoutOption time.Duration

func (o processingTimeoutOption) applyToConsumer(opts *ConsumerOptions) error {
	opts.ProcessingTimeout = time.Duration(o)
	return nil
}

// WithProcessingTimeout sets the timeout for processing an event. If the
// timeout is exceeded the event will be redelivered. If not specified the
// servers default timeout will be used.
func WithProcessingTimeout(timeout time.Duration) ConsumerOption {
	return processingTimeoutOption(timeout)
}

type maxDeliveryAttemptsOption uint

func (o maxDeliveryAttemptsOption) applyToConsumer(opts *ConsumerOptions) error {
	opts.MaxDeliveryAttempts = uint(o)
	return nil
}

// WithMaxDeliveryAttempts sets the maximum number of times an event will be
// delivered before it is considered failed.
func WithMaxDeliveryAttempts(attempts uint) ConsumerOption {
	return maxDeliveryAttemptsOption(attempts)
}

type maxPendingEventsOption uint

func (o maxPendingEventsOption) applyToConsumer(opts *ConsumerOptions) error {
	opts.MaxPendingEvents = uint(o)
	return nil
}

// WithMaxPendingEvents sets the maximum number of events that can be pending for
// this consumer. If not specified the server's default will be used.
func WithMaxPendingEvents(maxPendingEvents uint) ConsumerOption {
	return maxPendingEventsOption(maxPendingEvents)
}
