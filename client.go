package windshift

import (
	"fmt"
	"log/slog"

	"github.com/levelfourab/windshift-go/events"
	eventsimpl "github.com/levelfourab/windshift-go/internal/events"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// NewEvents creates a new client for consuming and publishing events.
func NewEvents(nats *nats.Conn, opts ...ClientOption) (events.Client, error) {
	options := &clientOptions{}
	for _, o := range opts {
		o(options)
	}

	if options.logger == nil {
		options.logger = slog.Default()
	}

	js, err := jetstream.New(nats)
	if err != nil {
		return nil, fmt.Errorf("failed to create JetStream client: %w", err)
	}

	return eventsimpl.New(js, options.logger), nil
}

type clientOptions struct {
	logger *slog.Logger
}

// ClientOption is an option to configure the client.
type ClientOption func(*clientOptions)

// WithLogger sets the logger for the client.
func WithLogger(logger *slog.Logger) ClientOption {
	return func(o *clientOptions) {
		o.logger = logger
	}
}
