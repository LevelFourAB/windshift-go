package streams

import (
	"errors"
	"time"
)

type Options struct {
	// RetentionPolicy defines the policy for retaining events in the stream.
	RetentionPolicy RetentionPolicy

	// Source defines how this stream receives events.
	Source DataSource

	// Storage defines where this stream is stored.
	Storage Storage

	// DeduplicationWindow defines the window of time in which duplicate events
	// are discarded.
	DeduplicationWindow *time.Duration

	// MaxEventSize defines the maximum size of an event in bytes.
	MaxEventSize uint
}

// Option defines an option for configuring a stream.
type Option func(*Options) error

// RetentionPolicy defines the policy for retaining events in the stream.
type RetentionPolicy struct {
	// MaxAge defines the maximum age of events in the stream.
	MaxAge time.Duration
	// MaxEvents defines the maximum number of events in the stream.
	MaxEvents uint
	// MaxEventsPerSubject defines the maximum number of events per subject in
	// the stream.
	MaxEventsPerSubject uint
	// MaxBytes defines the maximum number of bytes in the stream.
	MaxBytes uint

	// DiscardPolicy defines the behavior when a limit on events is reached.
	DiscardPolicy *DiscardPolicy
	// DiscardNewPerSubject defines whether to discard new events per subject
	// if the discard policy is set to [DiscardPolicyNew].
	DiscardNewPerSubject bool
}

// DiscardPolicy defines the behavior when a limit on events is reached. The
// default behavior is to discard old events, but some applications may prefer
// to discard new events instead.
type DiscardPolicy string

const (
	// DiscardPolicyOld discards old events when a limit is reached.
	DiscardPolicyOld DiscardPolicy = "old"
	// DiscardPolicyNew discards new events when a limit is reached.
	DiscardPolicyNew DiscardPolicy = "new"
)

// WithMaxAge sets the maximum age of events in the stream. Use [WithDiscardPolicy]
// to change the behavior when this limit is reached.
func WithMaxAge(maxAge time.Duration) Option {
	return func(o *Options) error {
		o.RetentionPolicy.MaxAge = maxAge
		return nil
	}
}

// WithMaxEvents sets the maximum number of events in the stream. Use
// [WithDiscardPolicy] to change the behavior when this limit is reached.
func WithMaxEvents(maxEvents uint) Option {
	return func(o *Options) error {
		o.RetentionPolicy.MaxEvents = maxEvents
		return nil
	}
}

// WithMaxEventsPerSubject sets the maximum number of events per subject in the
// stream. Use [WithDiscardPolicy] to change the behavior when this limit is
// reached.
func WithMaxEventsPerSubject(maxEventsPerSubject uint) Option {
	return func(o *Options) error {
		o.RetentionPolicy.MaxEventsPerSubject = maxEventsPerSubject
		return nil
	}
}

// WithMaxBytes sets the maximum number of bytes in the stream. Use
// [WithDiscardPolicy] to change the behavior when this limit is reached.
func WithMaxBytes(maxBytes uint) Option {
	return func(o *Options) error {
		o.RetentionPolicy.MaxBytes = maxBytes
		return nil
	}
}

// WithDiscardPolicy sets the discard policy to use when a limit is reached.
// The default is to discard old events.
//
// If this is set to [DiscardPolicyNew], use [WithDiscardNewPerSubject] to
// control whether to discard new events per subject.
func WithDiscardPolicy(discardPolicy DiscardPolicy) Option {
	return func(o *Options) error {
		o.RetentionPolicy.DiscardPolicy = &discardPolicy
		return nil
	}
}

// WithDiscardNewPerSubject sets whether to discard new events per subject when
// a limit is reached. The default is false.
//
// Use with [WithDiscardPolicy] set to [DiscardPolicyNew].
func WithDiscardNewPerSubject(discardNewPerSubject bool) Option {
	return func(o *Options) error {
		o.RetentionPolicy.DiscardNewPerSubject = discardNewPerSubject
		return nil
	}
}

// StreamSource is used to define how to source events from a given stream. It
// can be used to specify where events are to be received from, and optionally
// filter the subjects that are received.
type StreamSource struct {
	// Name of the stream to receive events from.
	Name string
	// Pointer is a pointer where to start receiving events from. If nil, events
	// will be received from the beginning of the stream.
	Pointer Pointer
	// FilterSubjects is a list of subjects to filter events by. If empty, all
	// subjects will be received.
	//
	// See [WithSubjects] for more information on subjects.
	FilterSubjects []string
}

// CopyFromStream creates a new [StreamSource] that will receive events from
// the given stream.
func CopyFromStream(name string) *StreamSource {
	return &StreamSource{
		Name: name,
	}
}

// CopyFromStreamAt creates a new [StreamSource] that will receive events from
// the given stream, starting at the given [Pointer].
func CopyFromStreamAt(name string, pointer Pointer) *StreamSource {
	return &StreamSource{
		Name:    name,
		Pointer: pointer,
	}
}

// CopyFromStreamWithSubjects creates a new [StreamSource] that will receive
// events from the given stream, filtering by the given subjects.
//
// See [WithSubjects] for more information on subjects.
func CopyFromStreamWithSubjects(name string, subjects ...string) *StreamSource {
	return &StreamSource{
		Name:           name,
		FilterSubjects: subjects,
	}
}

// CopyFromStreamAtWithSubjects creates a new [StreamSource] that will receive
// events from the given stream, starting at the given [Pointer] and filtering
// by the given subjects.
//
// See [WithSubjects] for more information on subjects.
func CopyFromStreamAtWithSubjects(name string, pointer Pointer, subjects ...string) *StreamSource {
	return &StreamSource{
		Name:           name,
		Pointer:        pointer,
		FilterSubjects: subjects,
	}
}

func validateStreamSource(source *StreamSource) error {
	if source == nil {
		return errors.New("source is required")
	}

	if source.Name == "" {
		return errors.New("name is required")
	}

	return nil
}

// DataSource is a source of events for a stream. It can be a list of subjects,
// a mirror of another stream, or an aggregate of multiple streams.
type DataSource interface {
	isStreamDataSource()
}

// DataSourceSubjects is a list of subjects that will be used as a source of
// events for a stream.
type DataSourceSubjects struct {
	Subjects []string
}

func (DataSourceSubjects) isStreamDataSource() {}

// DataSourceMirror is used to indicate that a stream is a mirror of another
// stream.
type DataSourceMirror struct {
	Source *StreamSource
}

func (DataSourceMirror) isStreamDataSource() {}

// DataSourceAggregate is used to indicate that a stream is an aggregate of
// multiple streams.
type DataSourceAggregate struct {
	Sources []*StreamSource
}

func (DataSourceAggregate) isStreamDataSource() {}

// WithSubjects is used to indicate that a stream should receive events
// from the given subjects. At least one subject is required and wildcards are
// supported.
//
// This option is mutually exclusive with [MirrorStream] and [AggregateStreams].
//
// Examples:
//
//	WithSubjects("orders.created", "orders.updated")
//	WithSubjects("orders.*")
//	WithSubjects("orders.>")
//
// Subjects are case-sensitive and should only contain the following
// characters:
//
//   - `a` to `z`, `A` to `Z` and `0` to `9` are allowed.
//
//   - `_` and `-` are allowed for separating words, but the use of
//     camelCase is recommended.
//
//   - `.` is allowed and used as a hierarchy separator, such as
//     `time.us.east` and `time.eu.sweden`, which share the `time`
//     prefix.
//
//   - `*` matches a single token, at any level of the subject. Such as
//     `time.*.east` will match `time.us.east` and `time.eu.east` but
//     not `time.us.west` or `time.us.central.east`. Similarly `time.us.*`
//     will match `time.us.east` but not `time.us.east.atlanta`.
//
//     The `*` wildcard can be used multiple times in a subject, such as
//     `time.*.*` will match `time.us.east` and `time.eu.west` but not
//     `time.us.east.atlanta`.
//
//   - `>` matches one or more tokens at the tail of a subject, and can
//     only be used as the last token. Such as `time.us.>` will match
//     `time.us.east` and `time.us.east.atlanta` but not `time.eu.east`.
//
// See NATS concepts: https://docs.nats.io/nats-concepts/subjects
func WithSubjects(subjects ...string) Option {
	return func(o *Options) error {
		if o.Source != nil {
			return errors.New("source is already set")
		}

		if len(subjects) == 0 {
			return errors.New("at least one subject is required")
		}

		o.Source = &DataSourceSubjects{
			Subjects: subjects,
		}
		return nil
	}
}

// MirrorStream is used to indicate that a stream is a mirror of another
// stream.
//
// This option is mutually exclusive with [WithSubjects] and [AggregateStreams].
func MirrorStream(source *StreamSource) Option {
	return func(o *Options) error {
		if o.Source != nil {
			return errors.New("source is already set")
		}

		if err := validateStreamSource(source); err != nil {
			return err
		}

		o.Source = &DataSourceMirror{
			Source: source,
		}
		return nil
	}
}

// AggregateStreams is used to indicate that a stream is an aggregate of
// multiple streams. At least one stream source is required.
//
// This option is mutually exclusive with [WithSubjects] and [MirrorStream].
func AggregateStreams(sources ...*StreamSource) Option {
	return func(o *Options) error {
		if o.Source != nil {
			return errors.New("source is already set")
		}

		for _, source := range sources {
			if err := validateStreamSource(source); err != nil {
				return err
			}
		}

		o.Source = &DataSourceAggregate{
			Sources: sources,
		}
		return nil
	}
}

// Storage is used to define how to store a stream.
type Storage struct {
	// Type indicates where the stream should be stored. If nil, the default
	// [StorageTypeFile] will be used.
	Type *StorageType
	// Replicas indicates how many replicas of the stream should be stored. If
	// zero, the default value of 1 will be used.
	Replicas uint
}

// StorageType indicates where the stream should be stored.
type StorageType string

const (
	// StorageTypeFile indicates that the stream should be stored in a file.
	// This is the default.
	StorageTypeFile StorageType = "file"
	// StorageTypeMemory indicates that the stream should be stored in memory.
	StorageTypeMemory StorageType = "memory"
)

// WithStorageType is used to indicate where a stream should be stored. If not
// set, the default [StorageTypeFile] will be used.
func WithStorageType(storageType StorageType) Option {
	return func(o *Options) error {
		o.Storage.Type = &storageType
		return nil
	}
}

// WithStorageReplicas is used to indicate how many replicas of a stream should
// be stored. If not set, the default value of 1 will be used.
func WithStorageReplicas(replicas uint) Option {
	return func(o *Options) error {
		o.Storage.Replicas = replicas
		return nil
	}
}

// WithDeduplicationWindow is used to indicate how long events should be
// deduplicated for.
//
// Defaults to 2 minutes if not set.
func WithDeduplicationWindow(deduplicationWindow time.Duration) Option {
	return func(o *Options) error {
		o.DeduplicationWindow = &deduplicationWindow
		return nil
	}
}

// WithMaxEventSize is used to indicate the maximum size of an event. Can not
// be larger than 1 MiB.
//
// Defaults to 1 MiB if not set.
func WithMaxEventSize(maxEventSize uint) Option {
	return func(o *Options) error {
		o.MaxEventSize = maxEventSize
		return nil
	}
}
