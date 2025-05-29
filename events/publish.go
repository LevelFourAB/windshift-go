package events

import (
	"time"

	"github.com/levelfourab/windshift-go/delays"
	"google.golang.org/protobuf/types/known/anypb"
)

type PublishOptions struct {
	Timestamp      time.Time
	IdempotencyKey string
	ExpectedLastID *uint64
	Backoff        delays.DelayDecider
}

func (o *PublishOptions) Apply(opts []PublishOption) {
	for _, opt := range opts {
		opt.applyToPublish(o)
	}
}

type PublishOption interface {
	applyToPublish(*PublishOptions)
}

type timestampOption time.Time

func (o timestampOption) applyToPublish(opts *PublishOptions) {
	opts.Timestamp = time.Time(o)
}

// Timestamp is an optional time when the event occurred, if not set the
// current time will be used.
func WithTimestamp(timestamp time.Time) PublishOption {
	return timestampOption(timestamp)
}

type idempotencyKeyOption string

func (o idempotencyKeyOption) applyToPublish(opts *PublishOptions) {
	opts.IdempotencyKey = string(o)
}

// IdempotencyKey is used to deduplicate events, setting this to the same
// value as a previous event will disable the new event from being
// published for a certain period of time.
//
// The time period in which deduplication occurs is controlled by
// the stream configuration.
//
// Leave blank to use no deduplication.
func WithIdempotencyKey(idempotencyKey string) PublishOption {
	return idempotencyKeyOption(idempotencyKey)
}

type expectedLastIDOption uint64

func (o expectedLastIDOption) applyToPublish(opts *PublishOptions) {
	v := uint64(o)
	opts.ExpectedLastID = &v
}

// ExpectedLastID is the last id that the stream is expected to have
// received, if the stream has not received this id then the event will
// not be published. This can be used for optimistic concurrency control.
func WithExpectedLastID(expectedLastID uint64) PublishOption {
	return expectedLastIDOption(expectedLastID)
}

// PublishedEvent contains information about an event that has been published
// to a stream.
type PublishedEvent interface {
	// ID of the event, this is the id that the stream assigned to the event.
	// This will also be the [Event.ID] when the event is consumed.
	//
	// See [WithExpectedLastID] to use this for optimistic concurrency
	// control.
	ID() uint64
}

var _ = anypb.Any{}
