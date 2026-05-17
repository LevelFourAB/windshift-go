package events

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/levelfourab/windshift-go/events"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

type Consumer struct {
	client *Client
	stream string
	name   string
}

func (c *Consumer) Name() string {
	return c.name
}

func (c *Consumer) Subscribe(ctx context.Context, opts ...events.SubscribeOption) (events.Subscription, error) {
	return c.client.Subscribe(ctx, c.stream, c.name, opts...)
}

func (c *Client) EnsureConsumer(ctx context.Context, stream string, opts ...events.ConsumerOption) (events.Consumer, error) {
	resolvedOpts := &events.ConsumerOptions{}
	if err := resolvedOpts.Apply(opts); err != nil {
		return nil, err
	}

	if strings.TrimSpace(stream) == "" {
		return nil, events.ErrStreamRequired
	}

	return c.ensureConsumer(ctx, stream, resolvedOpts)
}

func (c *Client) ensureConsumer(ctx context.Context, stream string, resolvedOpts *events.ConsumerOptions) (events.Consumer, error) {
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
		client: c,
		stream: stream,
		name:   name,
	}, nil
}

// declareEphemeralConsumer creates an ephemeral consumer. Ephemeral consumers
// are automatically deleted when they have not been used for a period of time,
// and are useful for one-off events.
func (c *Client) declareEphemeralConsumer(ctx context.Context, stream string, options *events.ConsumerOptions) (string, error) {
	consumerConfig := &jetstream.ConsumerConfig{}

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
func (c *Client) declareDurableConsumer(ctx context.Context, stream string, options *events.ConsumerOptions) (string, error) {
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
				Durable: options.Name,
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
// events.
func (c *Client) setConsumerSettings(config *jetstream.ConsumerConfig, options *events.ConsumerOptions, update bool) {
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
		//nolint:gosec // user-provided configuration value, not attacker-controlled
		config.MaxDeliver = int(options.MaxDeliveryAttempts)
	}

	if !update {
		// When creating a consumer we can specify where to start from
		config.DeliverPolicy = jetstream.DeliverNewPolicy
		if options.From != nil {
			switch p := options.From.(type) {
			case *events.PointerTimestamp:
				config.DeliverPolicy = jetstream.DeliverByStartTimePolicy
				config.OptStartTime = &p.Timestamp
			case *events.PointerOffset:
				config.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
				config.OptStartSeq = p.ID
			case *events.PointerStart:
				config.DeliverPolicy = jetstream.DeliverAllPolicy
			case *events.PointerEnd:
				config.DeliverPolicy = jetstream.DeliverLastPolicy
			}
		}
	}

	config.InactiveThreshold = options.InactiveThreshold
	if config.Durable == "" && config.InactiveThreshold == 0 {
		config.InactiveThreshold = 1 * time.Hour
	}

	if options.MaxPendingEvents > 0 {
		//nolint:gosec // user-provided configuration value, not attacker-controlled
		config.MaxAckPending = int(options.MaxPendingEvents)
	}
}

func getOptionsAsAttr(options *events.ConsumerOptions) slog.Attr {
	attrs := make([]any, 0, 4)

	attrs = append(attrs, slog.Any("subjects", options.Subjects))

	if options.From != nil {
		switch p := options.From.(type) {
		case *events.PointerTimestamp:
			attrs = append(attrs, slog.Time("from", p.Timestamp))
		case *events.PointerOffset:
			attrs = append(attrs, slog.Uint64("from", p.ID))
		case *events.PointerStart:
			attrs = append(attrs, slog.String("from", "start"))
		case *events.PointerEnd:
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
