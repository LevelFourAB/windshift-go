package events

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/levelfourab/windshift-go/delays"
	"github.com/levelfourab/windshift-go/events"
	"github.com/levelfourab/windshift-go/events/subscribe"
	"github.com/levelfourab/windshift-go/internal/backoff"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

var defaultEventBackoff = delays.StopAfterMaxTime(delays.Exponential(10*time.Millisecond, 2), 5*time.Second)

type subscription struct {
	client *Client

	logger *slog.Logger

	// channel is the channel used to send events to the caller.
	events chan events.Event

	// Timeout is the timeout for processing an event. Will be fetched
	// from the consumer configuration.
	Timeout time.Duration
}

func (c *Client) Subscribe(ctx context.Context, stream string, consumer string, opts ...subscribe.Option) (<-chan events.Event, error) {
	ctx, span := c.tracer.Start(
		ctx,
		stream+" subscribe",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystemKey.String("nats"),
		),
	)
	defer span.End()

	if !events.IsValidStreamName(stream) {
		return nil, events.NewValidationError("invalid stream name: " + stream)
	}

	if !events.IsValidConsumerName(consumer) {
		return nil, events.NewValidationError("invalid consumer name: " + consumer)
	}

	options := subscribe.ApplyOptions(opts)

	if options.MaxProcessingEvents == 0 {
		// Default to 50 events
		options.MaxProcessingEvents = 50
	}

	jsConsumer, err := c.js.Consumer(ctx, stream, consumer)
	if err != nil {
		return nil, fmt.Errorf("failed to get consumer info: %w", err)
	}

	maxEvents := options.MaxProcessingEvents / 2
	if maxEvents < 1 {
		maxEvents = 1
	}

	logger := c.logger.With(slog.String("stream", stream), slog.String("consumer", consumer))

	s := &subscription{
		client: c,
		logger: logger,

		events: make(chan events.Event),

		Timeout: jsConsumer.CachedInfo().Config.AckWait,
	}

	consumeCtx, err := jsConsumer.Consume(func(msg jetstream.Msg) {
		s.handleMsg(ctx, msg)
	}, jetstream.PullExpiry(1*time.Second), jetstream.PullMaxMessages(maxEvents))
	if err != nil {
		return nil, fmt.Errorf("failed to create message subscription: %w", err)
	}

	logger.Debug("Subscribed to consumer")
	go s.canceler(ctx, consumeCtx)
	return s.events, nil
}

func (s *subscription) canceler(ctx context.Context, consumeCtx jetstream.ConsumeContext) {
	<-ctx.Done()
	s.logger.Debug("Context done, stopping subscription")
	consumeCtx.Stop()
}

func (s *subscription) handleMsg(ctx context.Context, msg jetstream.Msg) {
	event, err2 := s.newEvent(ctx, msg)
	if err2 != nil {
		return
	}

	if s.logger.Enabled(ctx, slog.LevelDebug) {
		s.logger.Debug(
			"Received event",
			slog.Uint64("id", event.ID()),
			slog.String("type", msg.Headers().Get("WS-Data-Type")),
		)
	}

	select {
	case s.events <- event:
		// Event sent to channel
	case <-ctx.Done():
		// Context is done, stop trying to fetch messages
		break
	}
}

func (s *subscription) newEvent(ctx context.Context, msg jetstream.Msg) (*Event, error) {
	// We may have tracing information stored in the event headers, so we
	// extract them and create our own span indicating that we received the
	// message.
	//
	// Unlike for most tracing the span is only ended in this function if an
	// error occurs, otherwise it is passed into the event and ended when the
	// event is consumed.
	headers := msg.Headers()
	msgCtx := s.client.w3cPropagator.Extract(ctx, eventTracingHeaders{
		headers: &headers,
	})
	msgCtx, span := s.client.tracer.Start(
		msgCtx,
		msg.Subject()+" receive", trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingSystemKey.String("nats"),
			semconv.MessagingOperationTypeReceive,
			semconv.MessagingDestinationName(msg.Subject()),
		),
	)

	md, err := msg.Metadata()
	if err != nil {
		// Record the error and end the tracing as the span is not passed on
		s.logger.Error("Failed to get message metadata", slog.Any("error", err))
		span.RecordError(err)

		// If we fail to parse the metadata something is off, terminate the
		// message so it is not redelivered.
		err2 := msg.Term()
		if err2 != nil {
			s.logger.Warn("Failed to terminate message", slog.Any("error", err2))
			span.RecordError(err2)
		}

		span.SetStatus(codes.Error, "failed to get message metadata")
		span.End()
		return nil, err
	}

	// Set the message ID as an attribute
	span.SetAttributes(semconv.MessagingMessageID(fmt.Sprintf("%d", md.Sequence.Stream)))

	event := &Event{
		sub:      s,
		ctx:      msgCtx,
		span:     span,
		msg:      msg,
		metadata: md,
		headers:  parseHeaders(msg),
	}
	return event, nil
}

func parseHeaders(msg jetstream.Msg) *headers {
	h := msg.Headers()
	occurredAt, _ := time.Parse(time.RFC3339Nano, h.Get("WS-Published-Time"))
	idempotencyKey := h.Get("Nats-Msg-Id")

	return &headers{
		occurredAt:     occurredAt,
		idempotencyKey: idempotencyKey,
	}
}

type Event struct {
	sub *subscription

	ctx  context.Context
	span trace.Span

	msg      jetstream.Msg
	metadata *jetstream.MsgMetadata

	headers *headers
}

func (e *Event) Context() context.Context {
	return e.ctx
}

func (e *Event) ID() uint64 {
	return e.metadata.Sequence.Stream
}

func (e *Event) Subject() string {
	return e.msg.Subject()
}

func (e *Event) DeliveryAttempt() uint {
	return uint(e.metadata.NumDelivered)
}

func (e *Event) Headers() events.Headers {
	return e.headers
}

func (e *Event) data() *anypb.Any {
	return &anypb.Any{
		TypeUrl: "type.googleapis.com/" + e.msg.Headers().Get("WS-Data-Type"),
		Value:   e.msg.Data(),
	}
}

func (e *Event) UnmarshalNew() (proto.Message, error) {
	return e.data().UnmarshalNew()
}

func (e *Event) UnmarshalTo(v proto.Message) error {
	return e.data().UnmarshalTo(v)
}

func (e *Event) Ack(ctx context.Context, opts ...events.AckOption) error {
	options := &events.AckOptions{}
	options.Apply(opts)

	if options.Backoff == nil {
		options.Backoff = defaultEventBackoff
	}

	return backoff.Run(ctx, func() error {
		err := e.msg.Ack()
		if errors.Is(err, jetstream.ErrMsgAlreadyAckd) {
			e.span.RecordError(err)
			return backoff.Permanent(fmt.Errorf("message already acked: %w", err))
		} else if err != nil {
			e.span.RecordError(err)
			return fmt.Errorf("could not ack message: %w", err)
		}

		e.span.SetStatus(codes.Ok, "")
		return nil
	}, options.Backoff)
}

func (e *Event) Reject(ctx context.Context, opts ...events.RejectOption) error {
	options := &events.RejectOptions{}
	options.Apply(opts)

	if options.Backoff == nil {
		options.Backoff = defaultEventBackoff
	}

	var permanently bool
	var delay time.Duration
	if options.RejectPermanently {
		permanently = true
	} else if options.RedeliveryDecider != nil {
		decidedDelay := options.RedeliveryDecider(e)
		if decidedDelay < 0 {
			// Negative delay is a permanent rejection.
			permanently = true
		} else {
			delay = decidedDelay
		}
	} else if options.Delay > 0 {
		delay = options.Delay
	}

	return backoff.Run(ctx, func() error {
		var err error
		if permanently {
			err = e.msg.Term()
		} else if delay > 0 {
			err = e.msg.NakWithDelay(delay)
		} else {
			err = e.msg.Nak()
		}

		if err != nil {
			e.span.RecordError(err)
			return fmt.Errorf("could not reject message: %w", err)
		}

		e.span.SetStatus(codes.Ok, "")
		return nil
	}, options.Backoff)
}

func (e *Event) Ping(ctx context.Context, opts ...events.PingOption) error {
	options := &events.PingOptions{}
	options.Apply(opts)

	if options.Backoff == nil {
		options.Backoff = defaultEventBackoff
	}

	return backoff.Run(ctx, func() error {
		err := e.msg.InProgress()
		if err != nil {
			e.span.RecordError(err)
			return fmt.Errorf("could not ping message: %w", err)
		}

		e.span.AddEvent("pinged")
		return nil
	}, options.Backoff)
}

var _ events.Event = (*Event)(nil)

type headers struct {
	occurredAt     time.Time
	idempotencyKey string
}

func (h *headers) OccurredAt() time.Time {
	return h.occurredAt
}

func (h *headers) IdempotencyKey() string {
	return h.idempotencyKey
}

var _ events.Headers = (*headers)(nil)
