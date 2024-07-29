package events

import (
	"log/slog"

	"github.com/levelfourab/windshift-go/events"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type Client struct {
	js     jetstream.JetStream
	logger *slog.Logger
	tracer trace.Tracer
	// w3cPropagator is the W3C propagator used to propagate tracing information
	// into published and consumed events.
	w3cPropagator propagation.TextMapPropagator
}

func New(js jetstream.JetStream, logger *slog.Logger) *Client {
	tracer := otel.Tracer("windshift-go/events")

	return &Client{
		logger:        logger,
		tracer:        tracer,
		js:            js,
		w3cPropagator: propagation.TraceContext{},
	}
}

var _ events.Client = (*Client)(nil)
