package events

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/levelfourab/windshift-go/events"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/anypb"
)

func (e *Client) Publish(ctx context.Context, event *events.OutgoingEvent) (events.PublishedEvent, error) {
	subject := strings.TrimSpace(event.Subject)

	ctx, span := e.tracer.Start(
		ctx,
		subject+" publish",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystemKey.String("nats"),
			semconv.MessagingOperationTypePublish,
			semconv.MessagingDestinationName(subject),
		),
	)
	defer span.End()

	if !events.IsValidSubject(subject, false) {
		span.SetStatus(codes.Error, "invalid subject")
		return nil, events.NewValidationError("invalid subject: " + subject)
	}

	if event.Data == nil {
		// Data is required, wrap into DataInvalidError
		span.SetStatus(codes.Error, "no data specified")
		return nil, &events.DataInvalidError{Err: events.ErrDataRequired}
	}

	// Data is either anypb.Any or a message that should be converted to anypb.Any
	var data *anypb.Any
	if asAny, ok := event.Data.(*anypb.Any); ok {
		data = asAny
	} else {
		asAny, err := anypb.New(event.Data)
		if err != nil {
			// Issue with data, wrap into DataInvalidError
			span.SetStatus(codes.Error, "invalid data")
			return nil, &events.DataInvalidError{Err: err}
		}

		data = asAny
	}

	// Create the message
	msg := &nats.Msg{
		Subject: subject,
		Header:  nats.Header{},
	}

	publishOpts := []jetstream.PublishOpt{}

	// Set the published time
	publishTime := time.Now()
	if !event.Timestamp.IsZero() {
		publishTime = event.Timestamp
	}
	msg.Header.Set("WS-Published-Time", publishTime.Format(time.RFC3339Nano))

	// Set the idempotency key
	if event.IdempotencyKey != "" {
		msg.Header.Set("Nats-Msg-Id", event.IdempotencyKey)
	}

	// FIXME
	/*
		// Set the expected subject sequence
		if events.ExpectedSubjectSeq != nil {
			publishOpts = append(publishOpts, jetstream.WithExpectLastSequencePerSubject(*config.ExpectedSubjectSeq))
		}
	*/

	// Inject the tracing headers
	e.w3cPropagator.Inject(ctx, eventTracingHeaders{
		headers: &msg.Header,
	})

	msg.Header.Set("WS-Data-Type", string(data.MessageName()))
	msg.Data = data.Value

	e.logger.Debug(
		"Publishing event",
		slog.String("subject", subject),
		slog.String("dataType", string(data.MessageName())),
		slog.Any("headers", msg.Header),
	)

	// Publish the message.
	f, err := e.js.PublishMsgAsync(msg, publishOpts...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to publish message")
		return nil, fmt.Errorf("failed to publish message: %w", err)
	}

	select {
	case <-ctx.Done():
		// We don't know if the message was published or not, so the trace
		// will be marked as unset.
		span.SetStatus(codes.Unset, "context canceled")
		return nil, fmt.Errorf("failed to publish message: %w", ctx.Err())
	case ack := <-f.Ok():
		span.SetAttributes(
			semconv.MessagingMessageID(fmt.Sprintf("%d", ack.Sequence)),
		)
		span.SetStatus(codes.Ok, "")
		return &PublishedEvent{
			id: ack.Sequence,
		}, nil
	case err := <-f.Err():
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to publish message")

		if errors.Is(err, jetstream.ErrNoStreamResponse) || errors.Is(err, nats.ErrNoResponders) {
			return nil, events.ErrUnboundSubject
		} else if errors.Is(err, nats.ErrTimeout) {
			return nil, events.ErrPublishTimeout
		}

		var natsErr *jetstream.APIError
		if errors.As(err, &natsErr) {
			if natsErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence {
				return nil, events.ErrWrongSequence
			}
		}

		return nil, fmt.Errorf("failed to publish message: %w", err)
	}
}

type PublishedEvent struct {
	id uint64
}

func (e *PublishedEvent) ID() uint64 {
	return e.id
}
