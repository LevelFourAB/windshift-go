package events

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
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

	// mu guards closed and serializes sending on events with closing it,
	// so a message delivered after the context is canceled can never send
	// on a closed channel.
	mu sync.Mutex
	// closed indicates that events has been closed and no more events
	// should be sent.
	closed bool

	// callRetryBackoff is the default backoff strategy to use when acking,
	// rejecting or pinging an event fails.
	callRetryBackoff delays.DelayDecider

	// autoPing provides automatic pinging of events.
	autoPing *AutoPing
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

	options := &subscribe.Options{
		MaxPendingEvents: 50,
		CallRetryBackoff: defaultEventBackoff,
	}
	options.Apply(opts)

	jsConsumer, err := c.js.Consumer(ctx, stream, consumer)
	if err != nil {
		return nil, fmt.Errorf("failed to get consumer info: %w", err)
	}

	logger := c.logger.With(slog.String("stream", stream), slog.String("consumer", consumer))

	autoPing := createAutoPing(ctx, logger, jsConsumer.CachedInfo().Config.AckWait, options.AutoPingInterval)

	s := &subscription{
		client: c,
		logger: logger,

		events:           make(chan events.Event),
		callRetryBackoff: options.CallRetryBackoff,
		autoPing:         autoPing,
	}

	consumeCtx, err := jsConsumer.Consume(func(msg jetstream.Msg) {
		s.handleMsg(ctx, msg)
	}, jetstream.PullMaxMessages(options.MaxPendingEvents))
	if err != nil {
		return nil, fmt.Errorf("failed to create message subscription: %w", err)
	}

	logger.Debug("Subscribed to consumer")
	go s.canceler(ctx, consumeCtx)
	return s.events, nil
}

func createAutoPing(ctx context.Context, logger *slog.Logger, ackWait time.Duration, configuredAutoPingInterval time.Duration) *AutoPing {
	pingInterval := configuredAutoPingInterval
	if pingInterval == 0 {
		pingInterval = ackWait / 3
		if pingInterval < 1*time.Second {
			pingInterval = ackWait / 2
		}
	}

	if pingInterval > 0 {
		return newAutoPing(ctx, logger, pingInterval)
	}

	return nil
}

func (s *subscription) canceler(ctx context.Context, consumeCtx jetstream.ConsumeContext) {
	<-ctx.Done()
	s.logger.Debug("Context done, stopping subscription")

	// Stop delivery first. After this point no new messages will be
	// dispatched, but a callback may still be in-flight or buffered, so we
	// synchronize via mu before closing the channel.
	consumeCtx.Stop()

	s.mu.Lock()
	s.closed = true
	close(s.events)
	s.mu.Unlock()
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

	// Hold mu for the duration of the send so the canceler cannot close
	// the channel underneath us. The send is bounded by ctx.Done(), so the
	// canceler never blocks on mu for longer than it takes this select to
	// observe the canceled context.
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		// The subscription has been canceled and the channel closed; drop
		// the event rather than sending on a closed channel. The library owns
		// the process span, so end it here.
		event.finish(codes.Error, "subscription closed before delivery")
		return
	}

	select {
	case s.events <- event:
		// Event sent to channel. Ownership of the process span passes to the
		// consumer, who must Ack/Reject to finish it.
	case <-ctx.Done():
		// Context is done, stop trying to fetch messages. End the process
		// span as the event will never be delivered.
		event.finish(codes.Error, "context canceled before delivery")
	}
}

func (s *subscription) newEvent(ctx context.Context, msg jetstream.Msg) (*Event, error) {
	// The producer may have stored its trace context in the event headers.
	//
	// Two spans are created per message:
	//
	//   - A short "<subject> receive" CLIENT span, started as a root (its own
	//     trace, detached from the long-lived subscribe span) and ended
	//     immediately in this function.
	//   - A long "<subject> process" CONSUMER span, child of the receive span,
	//     carried in Event.Context(). Its lifecycle is fully owned by the
	//     library: it is ended by Event.finish on Ack/Reject or when the event
	//     is dropped before delivery. A caller that never settles the event is
	//     an inherent, accepted leak of this design.
	//
	// Both spans link to the producer's span when a valid trace context was
	// propagated.
	headers := msg.Headers()
	extractedCtx := s.client.w3cPropagator.Extract(ctx, eventTracingHeaders{
		headers: &headers,
	})
	producerSC := trace.SpanContextFromContext(extractedCtx)
	var producerLink []trace.Link
	if producerSC.IsValid() {
		producerLink = []trace.Link{{SpanContext: producerSC}}
	}

	receiveCtx, receiveSpan := s.client.tracer.Start(
		context.Background(),
		msg.Subject()+" receive",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithLinks(producerLink...),
		trace.WithAttributes(
			semconv.MessagingSystemKey.String("nats"),
			semconv.MessagingOperationTypeReceive,
			semconv.MessagingDestinationName(msg.Subject()),
		),
	)

	md, err := msg.Metadata()
	if err != nil {
		// Record the error and end the tracing as no span is passed on. Only
		// the receive span exists at this point, so there is no leak.
		s.logger.Error("Failed to get message metadata", slog.Any("error", err))
		receiveSpan.RecordError(err)

		// If we fail to parse the metadata something is off, terminate the
		// message so it is not redelivered.
		err2 := msg.Term()
		if err2 != nil {
			s.logger.Warn("Failed to terminate message", slog.Any("error", err2))
			receiveSpan.RecordError(err2)
		}

		receiveSpan.SetStatus(codes.Error, "failed to get message metadata")
		receiveSpan.End()
		return nil, err
	}

	messageID := semconv.MessagingMessageID(fmt.Sprintf("%d", md.Sequence.Stream))
	receiveSpan.SetAttributes(messageID)

	processCtx, processSpan := s.client.tracer.Start(
		receiveCtx,
		msg.Subject()+" process",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithLinks(producerLink...),
		trace.WithAttributes(
			semconv.MessagingSystemKey.String("nats"),
			semconv.MessagingOperationTypeDeliver,
			semconv.MessagingDestinationName(msg.Subject()),
			messageID,
		),
	)

	// The receive span is short: it only covers the receive + metadata parse.
	receiveSpan.End()

	event := &Event{
		sub:      s,
		ctx:      processCtx,
		span:     processSpan,
		msg:      msg,
		metadata: md,
		headers:  parseHeaders(msg),
	}

	if s.autoPing != nil {
		id := s.autoPing.Add(event)
		event.onDone = func() {
			s.autoPing.Remove(id)
		}
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

	onDone func()

	finishOnce sync.Once
}

func (e *Event) Context() context.Context {
	return e.ctx
}

// finish runs the terminal cleanup for an event exactly once: it removes any
// autoping entry and ends the process span with the given status. It is safe
// to call from any exit path (Ack, Reject, or a drop before delivery).
func (e *Event) finish(code codes.Code, desc string) {
	e.finishOnce.Do(func() {
		if e.onDone != nil {
			e.onDone()
		}
		e.span.SetStatus(code, desc)
		e.span.End()
	})
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
		options.Backoff = e.sub.callRetryBackoff
	}

	err := backoff.Run(ctx, func() error {
		e.sub.logger.Debug("Acknowledging event", slog.Uint64("eventID", e.ID()))
		err := e.msg.Ack()
		if errors.Is(err, jetstream.ErrMsgAlreadyAckd) {
			e.span.RecordError(err)
			e.finish(codes.Ok, "already settled")
			return backoff.Permanent(fmt.Errorf("message already acked: %w", err))
		} else if err != nil {
			e.span.RecordError(err)
			return fmt.Errorf("could not ack message: %w", err)
		}

		e.finish(codes.Ok, "")
		return nil
	}, options.Backoff)
	if err != nil {
		// Ack ultimately failed (retries exhausted or context canceled).
		// finish is idempotent, so the success / already-settled paths
		// that already ended the span are unaffected; this only ends the
		// span on the genuine-failure exits, preventing a leak.
		e.finish(codes.Error, "ack failed")
	}
	return err
}

func (e *Event) Reject(ctx context.Context, opts ...events.RejectOption) error {
	options := &events.RejectOptions{}
	options.Apply(opts)

	if options.Backoff == nil {
		options.Backoff = e.sub.callRetryBackoff
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

	err := backoff.Run(ctx, func() error {
		var err error
		if permanently {
			e.sub.logger.Debug("Rejecting event", slog.Uint64("eventID", e.ID()), slog.String("type", "permanent"))
			err = e.msg.Term()
		} else if delay > 0 {
			e.sub.logger.Debug("Rejecting event", slog.Uint64("eventID", e.ID()), slog.String("type", "delayed"), slog.Duration("delay", delay))
			err = e.msg.NakWithDelay(delay)
		} else {
			e.sub.logger.Debug("Rejecting event", slog.Uint64("eventID", e.ID()), slog.String("type", "redelivery"))
			err = e.msg.Nak()
		}

		if errors.Is(err, jetstream.ErrMsgAlreadyAckd) {
			// The message was already finalized, likely because a
			// previous attempt succeeded server-side but errored
			// client-side. Treat this as a permanent, successful reject.
			e.span.RecordError(err)
			e.finish(codes.Ok, "already settled")
			return backoff.Permanent(fmt.Errorf("message already acked: %w", err))
		} else if err != nil {
			e.span.RecordError(err)
			return fmt.Errorf("could not reject message: %w", err)
		}

		e.finish(codes.Ok, "")
		return nil
	}, options.Backoff)
	if err != nil {
		// Reject ultimately failed (retries exhausted or context
		// canceled). finish is idempotent, so this only ends the span on
		// the genuine-failure exits, preventing a leak.
		e.finish(codes.Error, "reject failed")
	}
	return err
}

func (e *Event) Ping(ctx context.Context, opts ...events.PingOption) error {
	options := &events.PingOptions{}
	options.Apply(opts)

	if options.Backoff == nil {
		options.Backoff = e.sub.callRetryBackoff
	}

	return backoff.Run(ctx, func() error {
		e.sub.logger.Debug("Pinging event", slog.Uint64("eventID", e.ID()))
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
