package events

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/levelfourab/windshift-go/events"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
)

type Stream struct{}

// EnsureStream ensures that a JetStream stream exists with the given configuration.
// If the stream already exists, it will be updated with the new configuration.
func (m *Client) EnsureStream(ctx context.Context, name string, opts ...events.StreamOption) (events.Stream, error) {
	ctx, span := m.tracer.Start(
		ctx,
		"windshift.events.EnsureStream",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.MessagingSystemKey.String("nats"),
			attribute.String("stream", name),
		),
	)
	defer span.End()

	options := events.StreamOptions{}
	if err := options.Apply(opts); err != nil {
		return nil, err
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
	case events.DiscardPolicyOld, events.DiscardPolicyDefault:
		streamConfig.Discard = jetstream.DiscardOld
	case events.DiscardPolicyNew:
		streamConfig.Discard = jetstream.DiscardNew
	default:
		span.SetStatus(codes.Error, "invalid discard policy")
		return nil, events.NewValidationError("invalid discard policy")
	}

	streamConfig.DiscardNewPerSubject = options.RetentionPolicy.DiscardNewPerSubject

	// Storage
	switch options.Storage.Type {
	case events.StorageTypeFile, events.StorageTypeDefault:
		streamConfig.Storage = jetstream.FileStorage
	case events.StorageTypeMemory:
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
	case *events.DataSourceSubjects:
		streamConfig.Subjects = source.Subjects
	case *events.DataSourceAggregate:
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
	case *events.DataSourceMirror:
		natsSource, err := toNatsStreamSource(source.Source)
		if err != nil {
			span.SetStatus(codes.Error, "source config invalid")
			return nil, fmt.Errorf("source config invalid: %w", err)
		}

		streamConfig.Mirror = natsSource
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

func toNatsStreamSource(source *events.StreamSource) (*jetstream.StreamSource, error) {
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
	case *events.PointerEnd:
		now := time.Now()
		res.OptStartTime = &now
	case *events.PointerOffset:
		res.OptStartSeq = from.ID
	case *events.PointerTimestamp:
		res.OptStartTime = &from.Timestamp
	case *events.PointerStart:
		// Start is the default
	}

	return res, nil
}

type stream struct {
	name                string
	retentionPolicy     events.RetentionPolicy
	source              events.DataSource
	storage             events.Storage
	deduplicationWindow time.Duration
	maxEventSize        uint
}

func (s *stream) Name() string {
	return s.name
}

func (s *stream) RetentionPolicy() events.RetentionPolicy {
	return s.retentionPolicy
}

func (s *stream) Source() events.DataSource {
	return s.source
}

func (s *stream) Storage() events.Storage {
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
		res.retentionPolicy.DiscardPolicy = events.DiscardPolicyOld
	case jetstream.DiscardNew:
		res.retentionPolicy.DiscardPolicy = events.DiscardPolicyNew
	}

	res.retentionPolicy.DiscardNewPerSubject = info.Config.DiscardNewPerSubject

	switch info.Config.Storage {
	case jetstream.FileStorage:
		res.storage.Type = events.StorageTypeFile
	case jetstream.MemoryStorage:
		res.storage.Type = events.StorageTypeMemory
	}

	res.storage.Replicas = uint(info.Config.Replicas)

	if info.Config.Mirror != nil {
		res.source = &events.DataSourceMirror{
			Source: &events.StreamSource{Name: info.Config.Mirror.Name},
		}
	} else if len(info.Config.Sources) > 0 {
		sources := make([]*events.StreamSource, len(info.Config.Sources))
		for i, source := range info.Config.Sources {
			sources[i] = &events.StreamSource{
				Name: source.Name,
			}
		}

		res.source = &events.DataSourceAggregate{
			Sources: sources,
		}
	} else {
		res.source = &events.DataSourceSubjects{
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
