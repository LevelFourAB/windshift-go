package events

import (
	"context"
	"fmt"
	"strings"

	"github.com/levelfourab/windshift-go/events"
	"github.com/levelfourab/windshift-go/events/consumers"
	eventsv1alpha1 "github.com/levelfourab/windshift-go/internal/proto/windshift/events/v1alpha1"
	"google.golang.org/protobuf/types/known/durationpb"
)

func (c *Client) EnsureConsumer(ctx context.Context, stream string, opts ...consumers.Option) (consumers.Consumer, error) {
	var resolvedOpts consumers.Options
	for _, opt := range opts {
		if err := opt(&resolvedOpts); err != nil {
			return nil, err
		}
	}

	if strings.TrimSpace(stream) == "" {
		return nil, events.ErrStreamRequired
	}

	return c.ensureConsumer(ctx, stream, resolvedOpts)
}

func (c *Client) ensureConsumer(ctx context.Context, stream string, resolvedOpts consumers.Options) (consumers.Consumer, error) {
	req := eventsv1alpha1.EnsureConsumerRequest{
		Stream: stream,
	}

	if resolvedOpts.Name != "" {
		req.Name = &resolvedOpts.Name
	}

	if resolvedOpts.Subjects == nil || len(resolvedOpts.Subjects) == 0 {
		return nil, events.ErrSubjectsRequired
	}

	req.Subjects = resolvedOpts.Subjects

	if resolvedOpts.Pointer != nil {
		pointer, err := toProtobufStreamPointer(resolvedOpts.Pointer)
		if err != nil {
			return nil, fmt.Errorf("pointer is invalid: %w", err)
		}

		req.From = pointer
	}

	if resolvedOpts.ProcessingTimeout > 0 {
		req.ProcessingTimeout = durationpb.New(resolvedOpts.ProcessingTimeout)
	}

	consumer, err := c.service.EnsureConsumer(ctx, &req)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		id: consumer.Id,
	}, nil
}

type Consumer struct {
	id string
}

func (c *Consumer) ID() string {
	return c.id
}
