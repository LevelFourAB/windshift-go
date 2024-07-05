package events

import (
	"context"
	"errors"
	"fmt"

	"github.com/levelfourab/windshift-go/events/streams"
	eventsv1alpha1 "github.com/levelfourab/windshift-go/internal/proto/windshift/events/v1alpha1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (c *Client) EnsureStream(
	ctx context.Context,
	name string,
	opts ...streams.Option,
) error {
	var resolvedOptions streams.Options
	for _, opt := range opts {
		if err := opt(&resolvedOptions); err != nil {
			return err
		}
	}

	req := &eventsv1alpha1.EnsureStreamRequest{
		Name:            name,
		RetentionPolicy: &eventsv1alpha1.EnsureStreamRequest_RetentionPolicy{},
	}

	err := handleStreamRetentionPolicy(resolvedOptions, req)
	if err != nil {
		return err
	}

	err = handleStreamSource(resolvedOptions, req)
	if err != nil {
		return err
	}

	err = handleStreamStorage(resolvedOptions, req)
	if err != nil {
		return err
	}

	if resolvedOptions.DeduplicationWindow != nil {
		req.DeduplicationWindow = durationpb.New(*resolvedOptions.DeduplicationWindow)
	}

	if resolvedOptions.MaxEventSize > 0 {
		v := uint32(resolvedOptions.MaxEventSize)
		req.MaxEventSize = &v
	}

	_, err = c.service.EnsureStream(ctx, req)
	return err
}

func handleStreamRetentionPolicy(opts streams.Options, req *eventsv1alpha1.EnsureStreamRequest) error {
	policy := opts.RetentionPolicy

	if policy.MaxAge > 0 {
		req.RetentionPolicy.MaxAge = durationpb.New(policy.MaxAge)
	}

	if policy.MaxEvents > 0 {
		req.RetentionPolicy.MaxEvents = proto.Uint64(uint64(policy.MaxEvents))
	}

	if policy.MaxEventsPerSubject > 0 {
		req.RetentionPolicy.MaxEventsPerSubject = proto.Uint64(uint64(policy.MaxEventsPerSubject))
	}

	if policy.MaxBytes > 0 {
		req.RetentionPolicy.MaxBytes = proto.Uint64(uint64(policy.MaxBytes))
	}

	if policy.DiscardPolicy != nil {
		switch *policy.DiscardPolicy {
		case streams.DiscardPolicyOld:
			v := eventsv1alpha1.EnsureStreamRequest_DISCARD_POLICY_OLD
			req.RetentionPolicy.DiscardPolicy = &v
		case streams.DiscardPolicyNew:
			v := eventsv1alpha1.EnsureStreamRequest_DISCARD_POLICY_NEW
			req.RetentionPolicy.DiscardPolicy = &v
		}
	}

	if policy.DiscardNewPerSubject {
		req.RetentionPolicy.DiscardNewPerSubject = proto.Bool(policy.DiscardNewPerSubject)
	}

	return nil
}

func handleStreamSource(opts streams.Options, req *eventsv1alpha1.EnsureStreamRequest) error {
	if opts.Source == nil {
		return errors.New("source of events is required")
	}

	switch source := opts.Source.(type) {
	case streams.DataSourceSubjects:
		req.Source = &eventsv1alpha1.EnsureStreamRequest_Subjects_{
			Subjects: &eventsv1alpha1.EnsureStreamRequest_Subjects{
				Subjects: source.Subjects,
			},
		}
	case streams.DataSourceMirror:
		streamSource, err := toProtobufStreamSource(source.Source)
		if err != nil {
			return fmt.Errorf("source of mirror invalid: %s", err)
		}

		req.Source = &eventsv1alpha1.EnsureStreamRequest_Mirror{
			Mirror: streamSource,
		}
	case streams.DataSourceAggregate:
		sources := make([]*eventsv1alpha1.EnsureStreamRequest_StreamSource, len(source.Sources))
		for i, s := range source.Sources {
			streamSource, err := toProtobufStreamSource(s)
			if err != nil {
				return fmt.Errorf("source of aggregate invalid: %s", err)
			}

			sources[i] = streamSource
		}

		req.Source = &eventsv1alpha1.EnsureStreamRequest_Aggregate{
			Aggregate: &eventsv1alpha1.EnsureStreamRequest_StreamSources{
				Sources: sources,
			},
		}
	default:
		return fmt.Errorf("unknown stream source type: %T", source)
	}

	return nil
}

func toProtobufStreamSource(s *streams.StreamSource) (*eventsv1alpha1.EnsureStreamRequest_StreamSource, error) {
	if s == nil {
		return nil, errors.New("source defining stream is required")
	}

	pointer, err := toProtobufStreamPointer(s.Pointer)
	if err != nil {
		return nil, err
	}

	res := &eventsv1alpha1.EnsureStreamRequest_StreamSource{
		Name:           s.Name,
		From:           pointer,
		FilterSubjects: s.FilterSubjects,
	}

	return res, nil
}

func toProtobufStreamPointer(p streams.Pointer) (*eventsv1alpha1.StreamPointer, error) {
	if p == nil {
		return nil, nil //nolint:nilnil
	}

	switch pointer := p.(type) {
	case *streams.PointerStart:
		return &eventsv1alpha1.StreamPointer{
			Pointer: &eventsv1alpha1.StreamPointer_Start{
				Start: true,
			},
		}, nil
	case *streams.PointerEnd:
		return &eventsv1alpha1.StreamPointer{
			Pointer: &eventsv1alpha1.StreamPointer_End{
				End: true,
			},
		}, nil
	case *streams.PointerOffset:
		return &eventsv1alpha1.StreamPointer{
			Pointer: &eventsv1alpha1.StreamPointer_Offset{
				Offset: pointer.ID,
			},
		}, nil
	case *streams.PointerTimestamp:
		return &eventsv1alpha1.StreamPointer{
			Pointer: &eventsv1alpha1.StreamPointer_Time{
				Time: timestamppb.New(pointer.Timestamp),
			},
		}, nil
	}

	return nil, fmt.Errorf("unknown stream pointer type: %T", p)
}

func handleStreamStorage(opts streams.Options, req *eventsv1alpha1.EnsureStreamRequest) error {
	if opts.Storage.Type != nil {
		switch *opts.Storage.Type {
		case streams.StorageTypeFile:
			v := eventsv1alpha1.EnsureStreamRequest_STORAGE_TYPE_FILE
			req.Storage.Type = &v
		case streams.StorageTypeMemory:
			v := eventsv1alpha1.EnsureStreamRequest_STORAGE_TYPE_MEMORY
			req.Storage.Type = &v
		default:
			return fmt.Errorf("unknown stream storage type: %s", *opts.Storage.Type)
		}
	}

	if opts.Storage.Replicas > 0 {
		req.Storage.Replicas = proto.Uint32(uint32(opts.Storage.Replicas))
	}

	return nil
}
