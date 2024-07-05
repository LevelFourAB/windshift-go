package consumers

// Consumer contains information about a defined consumer.
type Consumer interface {
	// ID is the unique identifier of the consumer. Will be an auto-generated
	// identifier for ephemeral consumers.
	ID() string
}
