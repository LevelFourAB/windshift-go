package streams

import "time"

// Stream contains information about a defined stream.
type Stream interface {
	// Name is the unique identifier of the stream.
	Name() string

	// RetentionPolicy defines the retention policy for the stream.
	RetentionPolicy() RetentionPolicy

	// Source defines how events are sourced into this stream.
	Source() DataSource

	// Storage defines the storage configuration for the stream.
	Storage() Storage

	// DeduplicationWindow defines the window of time in which duplicate events
	// are discarded.
	DeduplicationWindow() time.Duration

	// MaxEventSize defines the maximum size of an event in bytes.
	MaxEventSize() uint
}
