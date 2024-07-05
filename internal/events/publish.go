package events

import (
	"context"
	"strings"

	"github.com/levelfourab/windshift-go/events"
	eventsv1alpha1 "github.com/levelfourab/windshift-go/internal/proto/windshift/events/v1alpha1"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (e *Client) Publish(ctx context.Context, event *events.OutgoingEvent) (events.PublishedEvent, error) {
	subject := strings.TrimSpace(event.Subject)
	if subject == "" {
		// Subject is required
		return nil, events.ErrSubjectRequired
	}

	if event.Data == nil {
		// Data is required, wrap into DataInvalidError
		return nil, &events.DataInvalidError{Err: events.ErrDataRequired}
	}

	data, err := anypb.New(event.Data)
	if err != nil {
		// Issue with data, wrap into DataInvalidError
		return nil, &events.DataInvalidError{Err: err}
	}

	var timestamp *timestamppb.Timestamp
	if !event.Timestamp.IsZero() {
		timestamp = timestamppb.New(event.Timestamp)
	}

	var idempotencyKey *string
	if event.IdempotencyKey != "" {
		idempotencyKey = &event.IdempotencyKey
	}

	request := &eventsv1alpha1.PublishEventRequest{
		Subject:        subject,
		Data:           data,
		Timestamp:      timestamp,
		IdempotencyKey: idempotencyKey,
		ExpectedLastId: event.ExpectedLastID,
	}
	reply, err := e.service.PublishEvent(ctx, request)
	if err != nil {
		return nil, err
	}

	return &PublishedEvent{
		id: reply.Id,
	}, nil
}

type PublishedEvent struct {
	id uint64
}

func (e *PublishedEvent) ID() uint64 {
	return e.id
}
