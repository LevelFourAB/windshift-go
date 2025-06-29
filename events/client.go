package events

import (
	"context"

	"github.com/levelfourab/windshift-go/events/consumers"
	"github.com/levelfourab/windshift-go/events/streams"
	"github.com/levelfourab/windshift-go/events/subscribe"
	"google.golang.org/protobuf/proto"
)

// Client is used to interact with events.
type Client interface {
	// EnsureStream creates or updates a stream with the given name.
	//
	// Streams are collections of events that can later be consumed, they can
	// source events from subjects and other streams.
	EnsureStream(ctx context.Context, name string, opts ...streams.Option) (streams.Stream, error)

	// Publish an event to a stream. Subject and data are required.
	//
	// The subject of the event is used to route the event to the correct
	// stream and by consumers to filter events. If no stream exists that
	// can handle the subject then the publish will fail. Can not be blank.
	//
	// Data is the data of the message, can not be nil. Will be marshaled
	// into a [anypb.Any] instance. If the message is already an [anypb.Any]
	// instance then it will be used as is.
	//
	// By default the publish will not retry if it fails, use [WithBackoff]
	// to enable retries.
	Publish(ctx context.Context, subject string, data proto.Message, opts ...PublishOption) (PublishedEvent, error)

	// EnsureConsumer creates or updates a consumer for a given stream. There
	// are two types of consumers, durable and ephemeral.
	//
	// Durable consumers are created when a name is provided via
	// [consumers.WithName]. Durable consumers can be subscribed to by several
	// clients to distribute the load of processing events.
	//
	// Ephemeral consumers are created when no name is provided. An ephemeral
	// consumer will have an auto-generated name that is returned in the
	// response - this can be used when subscribing to it. If an ephemeral
	// consumer is unused for a period of time, an hour by default, it will be
	// automatically deleted.
	//
	// Filtering of what subjects a consumer should receive events from can be
	// done via [consumers.WithSubjects].
	//
	// [consumers.WithConsumeFrom] can be used to control which events the
	// consumer should receive, such as starting from the beginning of the stream
	// or from a specific event id.
	//
	// To subscribe to the events from a consumer, use [Client.Subscribe].
	EnsureConsumer(ctx context.Context, stream string, opts ...consumers.Option) (consumers.Consumer, error)

	// Subscribe starts consuming events from the given stream. The consumer
	// must have been created before calling this method, use [Client.EnsureConsumer]
	// to create a consumer.
	//
	// Subscriptions are valid until the context is canceled.
	//
	// To control the number of events that can be processed concurrently, use
	// [subscribe.MaxProcessingEvents].
	Subscribe(ctx context.Context, stream string, consumer string, opts ...subscribe.Option) (<-chan Event, error)
}
