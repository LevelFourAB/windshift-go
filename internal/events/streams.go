package events

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/levelfourab/windshift-go/events"
	"github.com/levelfourab/windshift-go/events/streams"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
)

type Stream struct{}

// EnsureStream ensures that a JetStream stream exists with the given configuration.
// If the stream already exists, it will be updated with the new configuration.
func (m *Client) EnsureStream(ctx context.Context, name string, opts ...streams.Option) (streams.Stream, error) {
	_, span := m.tracer.Start(
		ctx,
		"windshift.events.EnsureStream",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.MessagingSystemKey.String("nats"),
			attribute.String("stream", name),
		),
	)
	defer span.End()

	options := streams.Options{}
	for _, opt := range opts {
		err := opt(&options)
		if err != nil {
			return nil, err
		}
	}

	if !events.IsValidStreamName(name) {
		span.SetStatus(codes.Error, "invalid stream name")
		return nil, events.NewValidationError("invalid stream name: " + name)
	}

	streamConfig := jetstream.StreamConfig{
		Name:         name,
		MaxConsumers: -1,
	}

	// Retention policy
	if options.RetentionPolicy.MaxEvents == 0 {
		streamConfig.MaxMsgs = -1
	} else {
		streamConfig.MaxMsgs = int64(options.RetentionPolicy.MaxEvents)
	}

	if options.RetentionPolicy.MaxEventsPerSubject == 0 {
		streamConfig.MaxMsgsPerSubject = -1
	} else {
		streamConfig.MaxMsgsPerSubject = int64(options.RetentionPolicy.MaxEventsPerSubject)
	}

	if options.RetentionPolicy.MaxBytes == 0 {
		streamConfig.MaxBytes = -1
	} else {
		streamConfig.MaxBytes = int64(options.RetentionPolicy.MaxBytes)
	}

	streamConfig.MaxAge = options.RetentionPolicy.MaxAge

	switch options.RetentionPolicy.DiscardPolicy {
	case streams.DiscardPolicyOld, streams.DiscardPolicyDefault:
		streamConfig.Discard = jetstream.DiscardOld
	case streams.DiscardPolicyNew:
		streamConfig.Discard = jetstream.DiscardNew
	default:
		span.SetStatus(codes.Error, "invalid discard policy")
		return nil, events.NewValidationError("invalid discard policy")
	}

	streamConfig.DiscardNewPerSubject = options.RetentionPolicy.DiscardNewPerSubject

	// Storage
	switch options.Storage.Type {
	case streams.StorageTypeFile, streams.StorageTypeDefault:
		streamConfig.Storage = jetstream.FileStorage
	case streams.StorageTypeMemory:
		streamConfig.Storage = jetstream.MemoryStorage
	default:
		span.SetStatus(codes.Error, "invalid storage type")
		return nil, events.NewValidationError("invalid storage type")
	}

	if options.Storage.Replicas > 0 {
		streamConfig.Replicas = int(options.Storage.Replicas)
	} else {
		streamConfig.Replicas = 1
	}

	// Source of events
	switch source := options.Source.(type) {
	case *streams.DataSourceSubjects:
		streamConfig.Subjects = source.Subjects
	case *streams.DataSourceAggregate:
		sources := make([]*jetstream.StreamSource, len(source.Sources))
		for i, source := range source.Sources {
			natsSource, err := toNatsStreamSource(source)
			if err != nil {
				span.SetStatus(codes.Error, "source config invalid")
				return nil, fmt.Errorf("source config invalid: %w", err)
			}

			sources[i] = natsSource
		}

		streamConfig.Sources = sources
	default:
		span.SetStatus(codes.Error, "invalid source type")
		return nil, events.NewValidationError("invalid source type")
	}

	// Other settings
	if options.DeduplicationWindow != nil {
		streamConfig.Duplicates = *options.DeduplicationWindow
	} else {
		streamConfig.Duplicates = 2 * time.Minute
	}

	if options.MaxEventSize > 0 {
		streamConfig.MaxMsgSize = int32(options.MaxEventSize)
	} else {
		streamConfig.MaxMsgSize = -1
	}

	m.logger.Info("Ensuring stream exists", slog.String("name", name))
	res, err := m.js.CreateOrUpdateStream(ctx, streamConfig)
	if err != nil {
		span.SetStatus(codes.Error, "failed to create or update JetStream stream")
		return nil, fmt.Errorf("failed to create or update JetStream stream: %w", err)
	}

	return newStream(res), nil
}

func toNatsStreamSource(source *streams.StreamSource) (*jetstream.StreamSource, error) {
	res := &jetstream.StreamSource{
		Name: source.Name,
	}

	if source.FilterSubjects != nil && len(source.FilterSubjects) > 0 {
		if len(source.FilterSubjects) > 1 {
			return nil, errors.New("only one filter subject can be specified")
		}

		res.FilterSubject = source.FilterSubjects[0]
	}

	switch from := source.Pointer.(type) {
	case *streams.PointerEnd:
		now := time.Now()
		res.OptStartTime = &now
	case *streams.PointerOffset:
		res.OptStartSeq = from.ID
	case *streams.PointerTimestamp:
		res.OptStartTime = &from.Timestamp
	case *streams.PointerStart:
		// Start is the default
	}

	return res, nil
}

type stream struct {
	name                string
	retentionPolicy     streams.RetentionPolicy
	source              streams.DataSource
	storage             streams.Storage
	deduplicationWindow time.Duration
	maxEventSize        uint
}

func (s *stream) Name() string {
	return s.name
}

func (s *stream) RetentionPolicy() streams.RetentionPolicy {
	return s.retentionPolicy
}

func (s *stream) Source() streams.DataSource {
	return s.source
}

func (s *stream) Storage() streams.Storage {
	return s.storage
}

func (s *stream) DeduplicationWindow() time.Duration {
	return s.deduplicationWindow
}

func (s *stream) MaxEventSize() uint {
	return s.maxEventSize
}

func newStream(jsStream jetstream.Stream) *stream {
	info := jsStream.CachedInfo()
	res := &stream{
		name: info.Config.Name,
	}

	if info.Config.MaxMsgs < 0 {
		res.retentionPolicy.MaxEvents = 0
	} else {
		res.retentionPolicy.MaxEvents = uint(info.Config.MaxMsgs)
	}

	if info.Config.MaxMsgsPerSubject < 0 {
		res.retentionPolicy.MaxEventsPerSubject = 0
	} else {
		res.retentionPolicy.MaxEventsPerSubject = uint(info.Config.MaxMsgsPerSubject)
	}

	if info.Config.MaxBytes < 0 {
		res.retentionPolicy.MaxBytes = 0
	} else {
		res.retentionPolicy.MaxBytes = uint(info.Config.MaxBytes)
	}

	res.retentionPolicy.MaxAge = info.Config.MaxAge

	switch info.Config.Discard {
	case jetstream.DiscardOld:
		res.retentionPolicy.DiscardPolicy = streams.DiscardPolicyOld
	case jetstream.DiscardNew:
		res.retentionPolicy.DiscardPolicy = streams.DiscardPolicyNew
	}

	res.retentionPolicy.DiscardNewPerSubject = info.Config.DiscardNewPerSubject

	switch info.Config.Storage {
	case jetstream.FileStorage:
		res.storage.Type = streams.StorageTypeFile
	case jetstream.MemoryStorage:
		res.storage.Type = streams.StorageTypeMemory
	}

	res.storage.Replicas = uint(info.Config.Replicas)

	if len(info.Config.Sources) > 0 {
		sources := make([]*streams.StreamSource, len(info.Config.Sources))
		for i, source := range info.Config.Sources {
			sources[i] = &streams.StreamSource{
				Name: source.Name,
			}
		}

		res.source = &streams.DataSourceAggregate{
			Sources: sources,
		}
	} else {
		res.source = &streams.DataSourceSubjects{
			Subjects: info.Config.Subjects,
		}
	}

	res.deduplicationWindow = info.Config.Duplicates

	if info.Config.MaxMsgSize < 0 {
		res.maxEventSize = 0
	} else {
		res.maxEventSize = uint(info.Config.MaxMsgSize)
	}

	return res
}
