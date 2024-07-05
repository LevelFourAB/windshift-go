package events

import (
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// OutgoingEvent describes an event that is to be published to a stream.
type OutgoingEvent struct {
	// Subject of the event, used to route the event to the correct stream and
	// by consumers to filter events. If no stream exists that can handle the
	// subject then the publish will fail.
	//
	// Can not be blank.
	Subject string
	// Data is the data of the message, can not be nil. Will be marshaled
	// into a [anypb.Any] instance. If the message is already an [anypb.Any]
	// instance then it will be used as is.
	Data proto.Message
	// Timestamp is an optional time when the event occurred, if not set the
	// current time will be used.
	Timestamp time.Time
	// IdempotencyKey is used to deduplicate events, setting this to the same
	// value as a previous event will disable the new event from being
	// published for a certain period of time.
	//
	// The time period in which deduplication occurs is controlled by
	// the stream configuration.
	//
	// Leave blank to use no deduplication.
	IdempotencyKey string
	// ExpectedLastID is the last id that the stream is expected to have
	// received, if the stream has not received this id then the event will
	// not be published. This can be used for optimistic concurrency control.
	ExpectedLastID *uint64
}

// PublishedEvent contains information about an event that has been published
// to a stream.
type PublishedEvent interface {
	// Id of the event, this is the id that the stream assigned to the event.
	ID() uint64
}

var _ = anypb.Any{}
