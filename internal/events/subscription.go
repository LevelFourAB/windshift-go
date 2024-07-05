package events

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/levelfourab/windshift-go/events"
	"github.com/levelfourab/windshift-go/events/subscribe"
	eventsv1alpha1 "github.com/levelfourab/windshift-go/internal/proto/windshift/events/v1alpha1"
	"github.com/levelfourab/windshift-go/internal/rpc"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

var (
	errInvalidID        = backoff.Permanent(errors.New("invalid id"))
	errTemporaryFailure = errors.New("temporary failure")
	w3cPropagator       = propagation.TraceContext{}
)

type subscription struct {
	client eventsv1alpha1.EventsService_EventsClient

	events chan events.Event

	ackNacks *rpc.Exchange[eventsv1alpha1.EventsRequest, struct{}]
	pings    *rpc.Exchange[eventsv1alpha1.EventsRequest, struct{}]
}

func (c *Client) Subscribe(ctx context.Context, stream string, consumer string, opts ...subscribe.Option) (<-chan events.Event, error) {
	if strings.TrimSpace(stream) == "" {
		return nil, events.ErrStreamRequired
	}

	if strings.TrimSpace(consumer) == "" {
		return nil, errors.New("consumer is required")
	}

	options := &subscribe.Options{
		MaxProcessingEvents: 50,
	}

	for _, opt := range opts {
		opt(options)
	}

	s, err := c.service.Events(ctx)
	if err != nil {
		return nil, err
	}

	err = s.Send(&eventsv1alpha1.EventsRequest{
		Request: &eventsv1alpha1.EventsRequest_Subscribe_{
			Subscribe: &eventsv1alpha1.EventsRequest_Subscribe{
				Stream:   stream,
				Consumer: consumer,
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to send subscription request: %w", err)
	}

	// Wait for the initial response that should be the subscription
	// confirmation.
	msg, err := s.Recv()
	if err != nil {
		return nil, fmt.Errorf("failed to receive response: %w", err)
	}

	switch r := msg.Response.(type) {
	case *eventsv1alpha1.EventsResponse_Subscribed_:
		res := &subscription{
			client:   s,
			events:   make(chan events.Event),
			ackNacks: rpc.NewExchange[eventsv1alpha1.EventsRequest, struct{}](),
			pings:    rpc.NewExchange[eventsv1alpha1.EventsRequest, struct{}](),
		}

		// The receive and send loop use the context of the events call.
		go res.receiveLoop(s.Context()) //nolint:contextcheck
		go res.sendLoop(s.Context())    //nolint:contextcheck
		return res.events, nil
	default:
		// Received an unexpected response, close the stream and report the error.
		err := s.CloseSend()
		if err != nil {
			return nil, fmt.Errorf("unexpected response: %T, failed to close stream: %w", r, err)
		}

		return nil, fmt.Errorf("unexpected response: %T", r)
	}
}

func (s *subscription) sendLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-s.ackNacks.Requests():
			err := s.client.Send(&req.Request)
			if err != nil {
				s.ackNacks.Fail(req.ID, err)
			}
		case req := <-s.pings.Requests():
			err := s.client.Send(&req.Request)
			if err != nil {
				s.pings.Fail(req.ID, err)
			}
		}
	}
}

func (s *subscription) receiveLoop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			// Context has been canceled
			return
		}

		msg, err := s.client.Recv()
		if err != nil {
			return
		}

		switch r := msg.Response.(type) {
		case *eventsv1alpha1.EventsResponse_Event:
			// Send the response to the channel
			s.events <- s.newEvent(ctx, r.Event)
		case *eventsv1alpha1.EventsResponse_AckConfirmation_:
			s.handleAckConfirmation(r.AckConfirmation)
		case *eventsv1alpha1.EventsResponse_RejectConfirmation_:
			s.handleRejectConfirmation(r.RejectConfirmation)
		case *eventsv1alpha1.EventsResponse_PingConfirmation_:
			s.handlePingConfirmation(r.PingConfirmation)
		}
	}
}

func (s *subscription) handleAckConfirmation(r *eventsv1alpha1.EventsResponse_AckConfirmation) {
	for _, id := range r.Ids {
		s.ackNacks.Reply(id, struct{}{})
	}

	for _, id := range r.InvalidIds {
		s.ackNacks.Fail(id, errInvalidID)
	}

	for _, id := range r.TemporaryFailedIds {
		s.ackNacks.Fail(id, errTemporaryFailure)
	}
}

func (s *subscription) handleRejectConfirmation(r *eventsv1alpha1.EventsResponse_RejectConfirmation) {
	for _, id := range r.Ids {
		s.ackNacks.Reply(id, struct{}{})
	}

	for _, id := range r.InvalidIds {
		s.ackNacks.Fail(id, errInvalidID)
	}

	for _, id := range r.TemporaryFailedIds {
		s.ackNacks.Fail(id, errTemporaryFailure)
	}
}

func (s *subscription) handlePingConfirmation(r *eventsv1alpha1.EventsResponse_PingConfirmation) {
	for _, id := range r.Ids {
		s.pings.Reply(id, struct{}{})
	}

	for _, id := range r.InvalidIds {
		s.pings.Fail(id, errInvalidID)
	}

	for _, id := range r.TemporaryFailedIds {
		s.pings.Fail(id, errTemporaryFailure)
	}
}

func (s *subscription) newEvent(ctx context.Context, e *eventsv1alpha1.Event) *Event {
	ctx = w3cPropagator.Extract(ctx, eventTracingHeaders{headers: e.Headers})

	return &Event{
		ctx:   ctx,
		sub:   s,
		event: e,
	}
}

type Event struct {
	sub   *subscription
	ctx   context.Context
	event *eventsv1alpha1.Event
}

func (e *Event) Context() context.Context {
	return e.ctx
}

func (e *Event) ID() uint64 {
	return e.event.Id
}

func (e *Event) Subject() string {
	return e.event.Subject
}

func (e *Event) Headers() events.Headers {
	return &headersAdapter{e.event}
}

func (e *Event) UnmarshalNew() (proto.Message, error) {
	return e.event.Data.UnmarshalNew()
}

func (e *Event) UnmarshalTo(v proto.Message) error {
	return e.event.Data.UnmarshalTo(v)
}

func (e *Event) Ack(ctx context.Context, opts ...events.AckOption) error {
	options := &events.AckOptions{}
	options.Apply(opts)

	if options.Backoff == nil {
		options.Backoff = defaultEventBackoff()
	}

	return backoff.Retry(func() error {
		req := rpc.NewPendingRequest[eventsv1alpha1.EventsRequest, struct{}](e.ID(), eventsv1alpha1.EventsRequest{
			Request: &eventsv1alpha1.EventsRequest_Ack_{
				Ack: &eventsv1alpha1.EventsRequest_Ack{
					Ids: []uint64{e.ID()},
				},
			},
		})

		if !e.sub.ackNacks.Add(req) {
			return backoff.Permanent(errors.New("message is already being acknowledged or rejected"))
		}

		_, err := req.Wait(ctx)
		return err
	}, options.Backoff)
}

func (e *Event) Reject(ctx context.Context, opts ...events.RejectOption) error {
	options := &events.RejectOptions{}
	options.Apply(opts)

	if options.Backoff == nil {
		options.Backoff = defaultEventBackoff()
	}

	var permanently *bool
	var delay *durationpb.Duration
	if options.RejectPermanently {
		// Permanent rejects always take precedence, regardless of the delay.
		permanently = proto.Bool(true)
	} else if options.RedeliveryDecider != nil {
		// When the reject is not permanent, check if there is a delay.
		decidedDelay := options.RedeliveryDecider(e)
		if decidedDelay < 0 {
			// Negative delay is a permanent rejection.
			permanently = proto.Bool(true)
		} else if decidedDelay > 0 {
			// A delay greater than zero is used as is, 0 is treated as default.
			delay = durationpb.New(decidedDelay)
		}
	} else if options.Delay > 0 {
		// If there is a delay set, use it.
		delay = durationpb.New(options.Delay)
	}

	return backoff.Retry(func() error {
		req := rpc.NewPendingRequest[eventsv1alpha1.EventsRequest, struct{}](e.ID(), eventsv1alpha1.EventsRequest{
			Request: &eventsv1alpha1.EventsRequest_Reject_{
				Reject: &eventsv1alpha1.EventsRequest_Reject{
					Ids:         []uint64{e.ID()},
					Permanently: permanently,
					Delay:       delay,
				},
			},
		})

		if !e.sub.ackNacks.Add(req) {
			return backoff.Permanent(errors.New("message is already being acknowledged or rejected"))
		}

		_, err := req.Wait(ctx)
		return err
	}, options.Backoff)
}

func (e *Event) Ping(ctx context.Context, opts ...events.PingOption) error {
	options := &events.PingOptions{}
	options.Apply(opts)

	if options.Backoff == nil {
		options.Backoff = defaultEventBackoff()
	}

	return backoff.Retry(func() error {
		req := rpc.NewPendingRequest[eventsv1alpha1.EventsRequest, struct{}](e.ID(), eventsv1alpha1.EventsRequest{
			Request: &eventsv1alpha1.EventsRequest_Ping_{
				Ping: &eventsv1alpha1.EventsRequest_Ping{
					Ids: []uint64{e.ID()},
				},
			},
		})

		if !e.sub.pings.Add(req) {
			return backoff.Permanent(errors.New("message is already being pinged"))
		}

		_, err := req.Wait(ctx)
		return err
	}, options.Backoff)
}

var _ events.Event = (*Event)(nil)

func defaultEventBackoff() backoff.BackOff {
	return backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(10*time.Millisecond),
		backoff.WithRetryStopDuration(1*time.Second),
	)
}

type headersAdapter struct {
	event *eventsv1alpha1.Event
}

func (h *headersAdapter) OccurredAt() time.Time {
	return h.event.Headers.Timestamp.AsTime()
}

func (h *headersAdapter) IdempotencyKey() string {
	if h.event.Headers.IdempotencyKey == nil {
		return ""
	}

	return *h.event.Headers.IdempotencyKey
}

func (h *headersAdapter) DeliveryAttempt() int {
	return int(h.event.DeliveryAttempt)
}

var _ events.Headers = (*headersAdapter)(nil)

// eventTracingHeaders is a wrapper around the headers that implements the
// propagation.TextMapCarrier interface.
type eventTracingHeaders struct {
	headers *eventsv1alpha1.Headers
}

func (h eventTracingHeaders) Get(key string) string {
	switch key {
	case "traceparent":
		if h.headers.TraceParent == nil {
			return ""
		}
		return *h.headers.TraceParent
	case "tracestate":
		if h.headers.TraceState == nil {
			return ""
		}
		return *h.headers.TraceState
	}
	return ""
}

func (h eventTracingHeaders) Set(key string, value string) {
}

func (h eventTracingHeaders) Keys() []string {
	return []string{"traceparent", "tracestate"}
}
