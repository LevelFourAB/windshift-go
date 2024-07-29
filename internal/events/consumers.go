package events

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/levelfourab/windshift-go/events"
	"github.com/levelfourab/windshift-go/events/consumers"
	"github.com/levelfourab/windshift-go/events/streams"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

type Consumer struct {
	name string
}

func (c *Consumer) Name() string {
	return c.name
}

func (c *Client) EnsureConsumer(ctx context.Context, stream string, opts ...consumers.Option) (consumers.Consumer, error) {
	var resolvedOpts *consumers.Options = &consumers.Options{}
	for _, opt := range opts {
		if err := opt(resolvedOpts); err != nil {
			return nil, err
		}
	}

	if strings.TrimSpace(stream) == "" {
		return nil, events.ErrStreamRequired
	}

	return c.ensureConsumer(ctx, stream, resolvedOpts)
}

func (c *Client) ensureConsumer(ctx context.Context, stream string, resolvedOpts *consumers.Options) (consumers.Consumer, error) {
	ctx, span := c.tracer.Start(
		ctx,
		"windshift.events.EnsureConsumer",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.MessagingSystemKey.String("nats"),
			attribute.String("stream", stream),
		),
	)
	defer span.End()

	if !events.IsValidStreamName(stream) {
		span.SetStatus(codes.Error, "invalid stream")
		return nil, events.NewValidationError("invalid stream: " + stream)
	}

	for _, s := range resolvedOpts.Subjects {
		if !events.IsValidSubject(s, true) {
			span.SetStatus(codes.Error, "invalid subject")
			return nil, events.NewValidationError("invalid subject: " + s)
		}
	}

	var name string
	var err error
	if resolvedOpts.Name == "" {
		// If the name is not specified, we create an ephemeral consumer
		span.SetAttributes(attribute.String("type", "ephemeral"))

		name, err = c.declareEphemeralConsumer(ctx, stream, resolvedOpts)
		if err != nil {
			return nil, err
		}

		// Update the span with the generated name of the ephemeral consumer
		span.SetAttributes(attribute.String("name", name))
	} else {
		// If the name is specified, we create a durable consumer
		if !events.IsValidConsumerName(resolvedOpts.Name) {
			span.SetStatus(codes.Error, "invalid consumer name")
			return nil, events.NewValidationError("invalid consumer name: " + resolvedOpts.Name)
		}

		span.SetAttributes(
			attribute.String("type", "durable"),
			attribute.String("name", resolvedOpts.Name),
		)

		name, err = c.declareDurableConsumer(ctx, stream, resolvedOpts)
		if err != nil {
			return nil, err
		}
	}

	return &Consumer{
		name: name,
	}, nil
}

// declareEphemeralConsumer creates an ephemeral consumer. Ephemeral consumers
// are automatically deleted when they have not been used for a period of time,
// and are useful for one-off consumers.
func (c *Client) declareEphemeralConsumer(ctx context.Context, stream string, options *consumers.Options) (string, error) {
	consumerConfig := &jetstream.ConsumerConfig{
		InactiveThreshold: 1 * time.Hour,
	}

	c.logger.Info(
		"Creating ephemeral consumer",
		slog.String("stream", stream),
		getOptionsAsAttr(options),
	)

	c.setConsumerSettings(consumerConfig, options, false)
	consumer, err := c.js.CreateOrUpdateConsumer(ctx, stream, *consumerConfig)
	if err != nil {
		return "", fmt.Errorf("could not create consumer: %w", err)
	}
	return consumer.CachedInfo().Name, nil
}

// declareDurableConsumer creates a durable consumer. Durable consumers are
// useful for long-running consumers that need to be able to resume event
// processing.
func (c *Client) declareDurableConsumer(ctx context.Context, stream string, options *consumers.Options) (string, error) {
	consumer, err := c.js.Consumer(ctx, stream, options.Name)
	if err != nil {
		if errors.Is(err, jetstream.ErrConsumerNotFound) {
			c.logger.Info(
				"Creating durable consumer",
				slog.String("stream", stream),
				slog.String("name", options.Name),
				getOptionsAsAttr(options),
			)

			// Consumer does not exist, create it
			consumerConfig := &jetstream.ConsumerConfig{
				Durable:           options.Name,
				InactiveThreshold: 30 * 24 * time.Hour,
			}

			c.setConsumerSettings(consumerConfig, options, false)

			_, err = c.js.CreateOrUpdateConsumer(ctx, stream, *consumerConfig)
			if err != nil {
				return "", fmt.Errorf("could not create consumer: %w", err)
			}
			return options.Name, nil
		}

		return "", fmt.Errorf("could not get consumer: %w", err)
	}

	c.logger.Info(
		"Updating durable consumer",
		slog.String("stream", stream),
		slog.String("name", options.Name),
		getOptionsAsAttr(options),
	)

	// For updates certain fields can not be set, so we only set what we can
	consumerConfig := consumer.CachedInfo().Config
	c.setConsumerSettings(&consumerConfig, options, true)

	_, err = c.js.CreateOrUpdateConsumer(ctx, stream, consumerConfig)
	if err != nil {
		return "", fmt.Errorf("could not update consumer: %w", err)
	}

	return options.Name, nil
}

// setConsumerSettings sets the shared settings for both ephemeral and durable
// consumers.
func (c *Client) setConsumerSettings(config *jetstream.ConsumerConfig, options *consumers.Options, update bool) {
	config.AckPolicy = jetstream.AckExplicitPolicy
	if len(options.Subjects) == 1 {
		config.FilterSubjects = options.Subjects
	} else {
		config.FilterSubjects = options.Subjects
	}

	// If a timeout is specified set it or use the default
	if options.ProcessingTimeout > 0 {
		config.AckWait = options.ProcessingTimeout
	} else {
		config.AckWait = 30 * time.Second
	}

	// If the max delivery attempts is specified set it
	if options.MaxDeliveryAttempts > 0 {
		config.MaxDeliver = int(options.MaxDeliveryAttempts)
	}

	if !update {
		// When creating a consumer we can specify where to start from
		config.DeliverPolicy = jetstream.DeliverNewPolicy
		if options.From != nil {
			switch p := options.From.(type) {
			case *streams.PointerTimestamp:
				config.DeliverPolicy = jetstream.DeliverByStartTimePolicy
				config.OptStartTime = &p.Timestamp
			case *streams.PointerOffset:
				config.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
				config.OptStartSeq = p.ID
			case *streams.PointerStart:
				config.DeliverPolicy = jetstream.DeliverAllPolicy
			case *streams.PointerEnd:
				config.DeliverPolicy = jetstream.DeliverLastPolicy
			}
		}
	}
}

func getOptionsAsAttr(options *consumers.Options) slog.Attr {
	attrs := make([]any, 0, 4)

	attrs = append(attrs, slog.Any("subjects", options.Subjects))

	if options.From != nil {
		switch p := options.From.(type) {
		case *streams.PointerTimestamp:
			attrs = append(attrs, slog.Time("from", p.Timestamp))
		case *streams.PointerOffset:
			attrs = append(attrs, slog.Uint64("from", p.ID))
		case *streams.PointerStart:
			attrs = append(attrs, slog.String("from", "start"))
		case *streams.PointerEnd:
			attrs = append(attrs, slog.String("from", "end"))
		}
	}

	if options.ProcessingTimeout > 0 {
		attrs = append(attrs, slog.Duration("processingTimeout", options.ProcessingTimeout))
	}

	if options.MaxDeliveryAttempts > 0 {
		attrs = append(attrs, slog.Uint64("maxDeliveryAttempts", uint64(options.MaxDeliveryAttempts)))
	}

	return slog.Group("options", attrs...)
}
