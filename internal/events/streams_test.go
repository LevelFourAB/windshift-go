package events_test

import (
	"context"
	"time"

	"github.com/levelfourab/windshift-go/events"
	"github.com/levelfourab/windshift-go/events/streams"
	"github.com/nats-io/nats.go/jetstream"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/types/known/emptypb"
)

var _ = Describe("Streams", func() {
	var manager events.Client
	var js jetstream.JetStream

	BeforeEach(func() {
		manager, js = createClientAndJetStream()
	})

	It("stream can not have invalid name", func(ctx context.Context) {
		_, err := manager.EnsureStream(ctx, "invalid name", streams.WithSubjects("test"))
		Expect(err).To(HaveOccurred())
	})

	Describe("Sources", func() {
		Context("Subjects", func() {
			It("can create stream with single subject", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				stream, err := manager.EnsureStream(ctx, "test", streams.WithSubjects("test"))
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.Name()).To(Equal("test"))
				Expect(stream.Source()).To(Equal(&streams.DataSourceSubjects{
					Subjects: []string{"test"},
				}))

				createdStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(createdStream.CachedInfo().Config.Name).To(Equal("test"))
				Expect(createdStream.CachedInfo().Config.Subjects).To(ContainElement("test"))
			})

			It("can update stream with single subject", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				stream, err := manager.EnsureStream(ctx, "test", streams.WithSubjects("test"))
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.Source()).To(Equal(&streams.DataSourceSubjects{
					Subjects: []string{"test"},
				}))

				stream, err = manager.EnsureStream(ctx, "test", streams.WithSubjects("test2"))
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.Source()).To(Equal(&streams.DataSourceSubjects{
					Subjects: []string{"test2"},
				}))

				updatedStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(updatedStream.CachedInfo().Config.Name).To(Equal("test"))
				Expect(updatedStream.CachedInfo().Config.Subjects).To(ContainElement("test2"))
				Expect(updatedStream.CachedInfo().Config.Subjects).ToNot(ContainElement("test"))
			})

			It("can create stream with multiple subjects", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				stream, err := manager.EnsureStream(ctx, "test", streams.WithSubjects("test", "test.*"))
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.Source()).To(Equal(&streams.DataSourceSubjects{
					Subjects: []string{"test", "test.*"},
				}))

				createdStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(createdStream.CachedInfo().Config.Name).To(Equal("test"))
				Expect(createdStream.CachedInfo().Config.Subjects).To(ContainElement("test"))
				Expect(createdStream.CachedInfo().Config.Subjects).To(ContainElement("test.*"))
			})

			It("can remove a subject from a stream", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				stream, err := manager.EnsureStream(ctx, "test", streams.WithSubjects("test", "test.*"))
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.Source()).To(Equal(&streams.DataSourceSubjects{
					Subjects: []string{"test", "test.*"},
				}))

				stream, err = manager.EnsureStream(ctx, "test", streams.WithSubjects("test"))
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.Source()).To(Equal(&streams.DataSourceSubjects{
					Subjects: []string{"test"},
				}))

				updatedStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(updatedStream.CachedInfo().Config.Subjects).To(ContainElement("test"))
				Expect(updatedStream.CachedInfo().Config.Subjects).ToNot(ContainElement("test.*"))
			})

			It("can add a subject to a stream", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				stream, err := manager.EnsureStream(ctx, "test", streams.WithSubjects("test"))
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.Source()).To(Equal(&streams.DataSourceSubjects{
					Subjects: []string{"test"},
				}))

				stream, err = manager.EnsureStream(ctx, "test", streams.WithSubjects("test", "test.*"))
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.Source()).To(Equal(&streams.DataSourceSubjects{
					Subjects: []string{"test", "test.*"},
				}))

				updatedStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(updatedStream.CachedInfo().Config.Subjects).To(ContainElement("test"))
				Expect(updatedStream.CachedInfo().Config.Subjects).To(ContainElement("test.*"))
			})

			It("can create stream with multiple subjects and wildcards", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				stream, err := manager.EnsureStream(ctx, "test", streams.WithSubjects("test", "test.1.*", "test.2.*"))
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.Name()).To(Equal("test"))
				Expect(stream.Source()).To(Equal(&streams.DataSourceSubjects{
					Subjects: []string{"test", "test.1.*", "test.2.*"},
				}))

				createdStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(createdStream.CachedInfo().Config.Subjects).To(ContainElement("test"))
				Expect(createdStream.CachedInfo().Config.Subjects).To(ContainElement("test.1.*"))
				Expect(createdStream.CachedInfo().Config.Subjects).To(ContainElement("test.2.*"))
			})
		})

		Context("Aggregated source", func() {
			It("can have a single stream as source", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, "test", streams.WithSubjects("test"))
				Expect(err).ToNot(HaveOccurred())

				stream, err := manager.EnsureStream(ctx, "test2", streams.AggregateStreams(streams.CopyFromStream("test")))
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.Source()).To(Equal(&streams.DataSourceAggregate{
					Sources: []*streams.StreamSource{
						streams.CopyFromStream("test"),
					},
				}))

				createdStream, err := js.Stream(ctx, "test2")
				Expect(err).ToNot(HaveOccurred())
				Expect(createdStream.CachedInfo().Config.Sources).To(HaveLen(1))
				Expect(createdStream.CachedInfo().Config.Sources[0].Name).To(Equal("test"))
			})

			It("can have multiple streams as source", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, "test1", streams.WithSubjects("test1"))
				Expect(err).ToNot(HaveOccurred())

				_, err = manager.EnsureStream(ctx, "test2", streams.WithSubjects("test2"))
				Expect(err).ToNot(HaveOccurred())

				stream, err := manager.EnsureStream(ctx, "test3", streams.AggregateStreams(
					streams.CopyFromStream("test1"),
					streams.CopyFromStream("test2"),
				))
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.Source()).To(Equal(&streams.DataSourceAggregate{
					Sources: []*streams.StreamSource{
						streams.CopyFromStream("test1"),
						streams.CopyFromStream("test2"),
					},
				}))

				createdStream, err := js.Stream(ctx, "test3")
				Expect(err).ToNot(HaveOccurred())
				Expect(createdStream.CachedInfo().Config.Sources).To(HaveLen(2))
				Expect(createdStream.CachedInfo().Config.Sources[0].Name).To(Equal("test1"))
				Expect(createdStream.CachedInfo().Config.Sources[1].Name).To(Equal("test2"))
			})

			It("copies old data by default", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, "test", streams.WithSubjects("test"))
				Expect(err).ToNot(HaveOccurred())

				_, err = manager.Publish(ctx, "test", &emptypb.Empty{})
				Expect(err).ToNot(HaveOccurred())

				_, err = manager.EnsureStream(ctx, "test2", streams.AggregateStreams(streams.CopyFromStream("test")))
				Expect(err).ToNot(HaveOccurred())

				stream, err := js.Stream(ctx, "test2")
				Expect(err).ToNot(HaveOccurred())

				for i := 0; i < 10; i++ {
					time.Sleep(100 * time.Millisecond)
					info, err := stream.Info(ctx)
					Expect(err).ToNot(HaveOccurred())
					if info.State.Msgs == 1 {
						return
					}
				}

				Fail("Did not receive message on secondary stream")
			})

			It("copies new data", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, "test", streams.WithSubjects("test"))
				Expect(err).ToNot(HaveOccurred())

				_, err = manager.EnsureStream(ctx, "test2", streams.AggregateStreams(streams.CopyFromStream("test")))
				Expect(err).ToNot(HaveOccurred())

				stream, err := js.Stream(ctx, "test2")
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.CachedInfo().State.Msgs).To(Equal(uint64(0)))

				_, err = manager.Publish(ctx, "test", Data(&emptypb.Empty{}))
				Expect(err).ToNot(HaveOccurred())

				for i := 0; i < 10; i++ {
					time.Sleep(100 * time.Millisecond)
					info, err := stream.Info(ctx)
					Expect(err).ToNot(HaveOccurred())
					if info.State.Msgs == 1 {
						return
					}
				}

				Fail("Did not receive message on secondary stream")
			})

			It("can copy from specific id of another stream", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				// Create the source stream
				_, err = manager.EnsureStream(ctx, "test", streams.WithSubjects("test"))
				Expect(err).ToNot(HaveOccurred())

				// Publish a few messages to the source stream
				_, err = manager.Publish(ctx, "test", &emptypb.Empty{})
				Expect(err).ToNot(HaveOccurred())
				e, err := manager.Publish(ctx, "test", &emptypb.Empty{})
				Expect(err).ToNot(HaveOccurred())

				// Create the stream with the source
				_, err = manager.EnsureStream(ctx, "test2", streams.AggregateStreams(
					streams.CopyFromStreamAt("test", streams.AtStreamOffset(e.ID())),
				))
				Expect(err).ToNot(HaveOccurred())

				stream, err := js.Stream(ctx, "test2")
				Expect(err).ToNot(HaveOccurred())

				for i := 0; i < 10; i++ {
					time.Sleep(100 * time.Millisecond)
					info, err := stream.Info(ctx)
					Expect(err).ToNot(HaveOccurred())
					if info.State.Msgs == 1 {
						return
					}
				}

				Fail("Did not receive message on secondary stream")
			})

			It("can copy from specific time of another stream", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				// Create the source stream
				_, err = manager.EnsureStream(ctx, "test", streams.WithSubjects("test"))
				Expect(err).ToNot(HaveOccurred())

				// Publish a few messages to the source stream
				_, err = manager.Publish(ctx, "test", &emptypb.Empty{})
				Expect(err).ToNot(HaveOccurred())

				time.Sleep(50 * time.Millisecond)
				now := time.Now()

				_, err = manager.Publish(ctx, "test", &emptypb.Empty{})
				Expect(err).ToNot(HaveOccurred())

				// Create the stream with the source
				_, err = manager.EnsureStream(ctx, "test2", streams.AggregateStreams(
					streams.CopyFromStreamAt("test", streams.AtStreamTimestamp(now)),
				))
				Expect(err).ToNot(HaveOccurred())

				stream, err := js.Stream(ctx, "test2")
				Expect(err).ToNot(HaveOccurred())

				for i := 0; i < 10; i++ {
					time.Sleep(100 * time.Millisecond)
					info, err := stream.Info(ctx)
					Expect(err).ToNot(HaveOccurred())
					if info.State.Msgs == 1 {
						return
					}
				}

				Fail("Did not receive message on secondary stream")
			})

			It("can copy from start of other stream", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				// Create the source stream
				_, err = manager.EnsureStream(ctx, "test", streams.WithSubjects("test"))
				Expect(err).ToNot(HaveOccurred())

				// Publish a message to the source stream
				_, err = manager.Publish(ctx, "test", Data(&emptypb.Empty{}))
				Expect(err).ToNot(HaveOccurred())

				// Create the stream with the source
				_, err = manager.EnsureStream(ctx, "test2", streams.AggregateStreams(
					streams.CopyFromStreamAt("test", streams.AtStreamStart()),
				))
				Expect(err).ToNot(HaveOccurred())

				stream, err := js.Stream(ctx, "test2")
				Expect(err).ToNot(HaveOccurred())

				for i := 0; i < 10; i++ {
					time.Sleep(100 * time.Millisecond)
					info, err := stream.Info(ctx)
					Expect(err).ToNot(HaveOccurred())
					if info.State.Msgs == 1 {
						return
					}
				}

				Fail("Did not receive message on secondary stream")
			})

			It("can start at end of other stream and will not receive old data", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				// Create the source stream
				_, err = manager.EnsureStream(ctx, "test", streams.WithSubjects("test"))
				Expect(err).ToNot(HaveOccurred())

				// Publish a message to the source stream
				_, err = manager.Publish(ctx, "test", Data(&emptypb.Empty{}))
				Expect(err).ToNot(HaveOccurred())

				// Create the stream with the source
				_, err = manager.EnsureStream(ctx, "test2", streams.AggregateStreams(
					streams.CopyFromStreamAt("test", streams.AtStreamEnd()),
				))
				Expect(err).ToNot(HaveOccurred())

				time.Sleep(500 * time.Millisecond)

				stream, err := js.Stream(ctx, "test2")
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.CachedInfo().State.Msgs).To(Equal(uint64(0)))
			})
		})

		Describe("Retention policies", func() {
			It("can create stream with max age", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				stream, err := manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxAge(1*time.Hour),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.RetentionPolicy().MaxAge).To(Equal(1 * time.Hour))

				createdStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(createdStream.CachedInfo().Config.Name).To(Equal("test"))
				Expect(createdStream.CachedInfo().Config.MaxAge).To(Equal(1 * time.Hour))
			})

			It("can update stream without max age and set max age", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, "test", streams.WithSubjects("test"))
				Expect(err).ToNot(HaveOccurred())

				stream, err := manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxAge(1*time.Hour),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.RetentionPolicy().MaxAge).To(Equal(1 * time.Hour))

				updatedStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(updatedStream.CachedInfo().Config.Name).To(Equal("test"))
				Expect(updatedStream.CachedInfo().Config.MaxAge).To(Equal(1 * time.Hour))
			})

			It("can update stream and change max age", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxAge(1*time.Hour),
				)
				Expect(err).ToNot(HaveOccurred())

				stream, err := manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxAge(2*time.Hour),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.RetentionPolicy().MaxAge).To(Equal(2 * time.Hour))

				updatedStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(updatedStream.CachedInfo().Config.Name).To(Equal("test"))
				Expect(updatedStream.CachedInfo().Config.MaxAge).To(Equal(2 * time.Hour))
			})

			It("can remove max age from stream", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxAge(1*time.Hour),
				)
				Expect(err).ToNot(HaveOccurred())

				stream, err := manager.EnsureStream(ctx, "test", streams.WithSubjects("test"))
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.RetentionPolicy().MaxAge).To(Equal(0 * time.Second))

				updatedStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(updatedStream.CachedInfo().Config.MaxAge).To(Equal(0 * time.Second))
			})

			It("can create stream with max messages", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				stream, err := manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxEvents(100),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.RetentionPolicy().MaxEvents).To(Equal(uint(100)))

				createdStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(createdStream.CachedInfo().Config.Name).To(Equal("test"))
				Expect(createdStream.CachedInfo().Config.MaxMsgs).To(Equal(int64(100)))
			})

			It("can update stream without max messages and set max messages", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, "test", streams.WithSubjects("test"))
				Expect(err).ToNot(HaveOccurred())

				stream, err := manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxEvents(100),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.RetentionPolicy().MaxEvents).To(Equal(uint(100)))

				updatedStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(updatedStream.CachedInfo().Config.Name).To(Equal("test"))
				Expect(updatedStream.CachedInfo().Config.MaxMsgs).To(Equal(int64(100)))
			})

			It("can update stream and change max messages", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, "test",

					streams.WithSubjects("test"),
					streams.WithMaxEvents(100),
				)
				Expect(err).ToNot(HaveOccurred())

				stream, err := manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxEvents(200),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.RetentionPolicy().MaxEvents).To(Equal(uint(200)))

				updatedStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(updatedStream.CachedInfo().Config.Name).To(Equal("test"))
				Expect(updatedStream.CachedInfo().Config.MaxMsgs).To(Equal(int64(200)))
			})

			It("can remove max messages from stream", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxEvents(100),
				)
				Expect(err).ToNot(HaveOccurred())

				stream, err := manager.EnsureStream(ctx, "test", streams.WithSubjects("test"))
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.RetentionPolicy().MaxEvents).To(Equal(uint(0)))

				updatedStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(updatedStream.CachedInfo().Config.MaxMsgs).To(Equal(int64(-1)))
			})

			It("can create stream with max bytes", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				stream, err := manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxBytes(100),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.RetentionPolicy().MaxBytes).To(Equal(uint(100)))

				createdStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(createdStream.CachedInfo().Config.Name).To(Equal("test"))
				Expect(createdStream.CachedInfo().Config.MaxBytes).To(Equal(int64(100)))
			})

			It("can update stream without max bytes and set max bytes", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, "test", streams.WithSubjects("test"))
				Expect(err).ToNot(HaveOccurred())

				stream, err := manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxBytes(100),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.RetentionPolicy().MaxBytes).To(Equal(uint(100)))

				updatedStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(updatedStream.CachedInfo().Config.Name).To(Equal("test"))
				Expect(updatedStream.CachedInfo().Config.MaxBytes).To(Equal(int64(100)))
			})

			It("can update stream and change max bytes", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxBytes(100),
				)
				Expect(err).ToNot(HaveOccurred())

				stream, err := manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxBytes(200),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.RetentionPolicy().MaxBytes).To(Equal(uint(200)))

				updatedStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(updatedStream.CachedInfo().Config.Name).To(Equal("test"))
				Expect(updatedStream.CachedInfo().Config.MaxBytes).To(Equal(int64(200)))
			})

			It("can remove max bytes from stream", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxBytes(100),
				)
				Expect(err).ToNot(HaveOccurred())

				stream, err := manager.EnsureStream(ctx, "test", streams.WithSubjects("test"))
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.RetentionPolicy().MaxBytes).To(Equal(uint(0)))

				updatedStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(updatedStream.CachedInfo().Config.MaxBytes).To(Equal(int64(-1)))
			})

			It("can create stream with max age and max messages", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				stream, err := manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxAge(1*time.Hour),
					streams.WithMaxEvents(100),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.RetentionPolicy().MaxAge).To(Equal(1 * time.Hour))
				Expect(stream.RetentionPolicy().MaxEvents).To(Equal(uint(100)))

				createdStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(createdStream.CachedInfo().Config.Name).To(Equal("test"))
				Expect(createdStream.CachedInfo().Config.MaxAge).To(Equal(1 * time.Hour))
				Expect(createdStream.CachedInfo().Config.MaxMsgs).To(Equal(int64(100)))
			})

			It("can update stream with max age and max messages", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxAge(1*time.Hour),
					streams.WithMaxEvents(100),
				)
				Expect(err).ToNot(HaveOccurred())

				stream, err := manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxAge(2*time.Hour),
					streams.WithMaxEvents(200),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.RetentionPolicy().MaxAge).To(Equal(2 * time.Hour))
				Expect(stream.RetentionPolicy().MaxEvents).To(Equal(uint(200)))

				updatedStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(updatedStream.CachedInfo().Config.MaxAge).To(Equal(2 * time.Hour))
				Expect(updatedStream.CachedInfo().Config.MaxMsgs).To(Equal(int64(200)))
			})

			It("can remove max age and max messages from stream", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxAge(1*time.Hour),
					streams.WithMaxEvents(100),
				)
				Expect(err).ToNot(HaveOccurred())

				stream, err := manager.EnsureStream(ctx, "test", streams.WithSubjects("test"))
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.RetentionPolicy().MaxAge).To(Equal(0 * time.Second))
				Expect(stream.RetentionPolicy().MaxEvents).To(Equal(uint(0)))

				updatedStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(updatedStream.CachedInfo().Config.MaxAge).To(Equal(0 * time.Second))
				Expect(updatedStream.CachedInfo().Config.MaxMsgs).To(Equal(int64(-1)))
			})

			It("can create stream with max age and add max messages", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxAge(1*time.Hour),
				)
				Expect(err).ToNot(HaveOccurred())

				stream, err := manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxAge(1*time.Hour),
					streams.WithMaxEvents(100),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.RetentionPolicy().MaxAge).To(Equal(1 * time.Hour))
				Expect(stream.RetentionPolicy().MaxEvents).To(Equal(uint(100)))

				updatedStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(updatedStream.CachedInfo().Config.MaxAge).To(Equal(1 * time.Hour))
				Expect(updatedStream.CachedInfo().Config.MaxMsgs).To(Equal(int64(100)))
			})

			It("can create stream with max age and replace with max messages", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxAge(1*time.Hour),
				)
				Expect(err).ToNot(HaveOccurred())

				stream, err := manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxEvents(100),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.RetentionPolicy().MaxAge).To(Equal(0 * time.Second))
				Expect(stream.RetentionPolicy().MaxEvents).To(Equal(uint(100)))

				updatedStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(updatedStream.CachedInfo().Config.MaxAge).To(Equal(0 * time.Second))
				Expect(updatedStream.CachedInfo().Config.MaxMsgs).To(Equal(int64(100)))
			})

			It("can create stream with max age and max bytes", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				stream, err := manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxAge(1*time.Hour),
					streams.WithMaxBytes(100),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.RetentionPolicy().MaxAge).To(Equal(1 * time.Hour))
				Expect(stream.RetentionPolicy().MaxBytes).To(Equal(uint(100)))

				createdStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(createdStream.CachedInfo().Config.MaxAge).To(Equal(1 * time.Hour))
				Expect(createdStream.CachedInfo().Config.MaxBytes).To(Equal(int64(100)))
			})

			It("can update stream with max age and max bytes", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxAge(1*time.Hour),
					streams.WithMaxBytes(100),
				)
				Expect(err).ToNot(HaveOccurred())

				stream, err := manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxAge(2*time.Hour),
					streams.WithMaxBytes(200),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.RetentionPolicy().MaxAge).To(Equal(2 * time.Hour))
				Expect(stream.RetentionPolicy().MaxBytes).To(Equal(uint(200)))

				updatedStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(updatedStream.CachedInfo().Config.MaxAge).To(Equal(2 * time.Hour))
				Expect(updatedStream.CachedInfo().Config.MaxBytes).To(Equal(int64(200)))
			})

			It("can remove max age and max bytes from stream", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxAge(1*time.Hour),
					streams.WithMaxBytes(100),
				)
				Expect(err).ToNot(HaveOccurred())

				stream, err := manager.EnsureStream(ctx, "test", streams.WithSubjects("test"))
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.RetentionPolicy().MaxAge).To(Equal(0 * time.Second))
				Expect(stream.RetentionPolicy().MaxBytes).To(Equal(uint(0)))

				updatedStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(updatedStream.CachedInfo().Config.MaxAge).To(Equal(0 * time.Second))
				Expect(updatedStream.CachedInfo().Config.MaxBytes).To(Equal(int64(-1)))
			})

			It("can create stream with max age and add max bytes", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxAge(1*time.Hour),
				)
				Expect(err).ToNot(HaveOccurred())

				stream, err := manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxAge(1*time.Hour),
					streams.WithMaxBytes(100),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.RetentionPolicy().MaxAge).To(Equal(1 * time.Hour))
				Expect(stream.RetentionPolicy().MaxBytes).To(Equal(uint(100)))

				updatedStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(updatedStream.CachedInfo().Config.MaxAge).To(Equal(1 * time.Hour))
				Expect(updatedStream.CachedInfo().Config.MaxBytes).To(Equal(int64(100)))
			})

			It("can create stream with max age and replace with max bytes", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxAge(1*time.Hour),
				)
				Expect(err).ToNot(HaveOccurred())

				stream, err := manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxBytes(100),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.RetentionPolicy().MaxAge).To(Equal(0 * time.Second))
				Expect(stream.RetentionPolicy().MaxBytes).To(Equal(uint(100)))

				updatedStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(updatedStream.CachedInfo().Config.MaxAge).To(Equal(0 * time.Second))
				Expect(updatedStream.CachedInfo().Config.MaxBytes).To(Equal(int64(100)))
			})

			It("can create stream with max messages and max bytes", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				stream, err := manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxEvents(100),
					streams.WithMaxBytes(100),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.RetentionPolicy().MaxEvents).To(Equal(uint(100)))
				Expect(stream.RetentionPolicy().MaxBytes).To(Equal(uint(100)))

				createdStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(createdStream.CachedInfo().Config.MaxMsgs).To(Equal(int64(100)))
				Expect(createdStream.CachedInfo().Config.MaxBytes).To(Equal(int64(100)))
			})

			It("can update stream with max messages and max bytes", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxEvents(100),
					streams.WithMaxBytes(100),
				)
				Expect(err).ToNot(HaveOccurred())

				stream, err := manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxEvents(200),
					streams.WithMaxBytes(200),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.RetentionPolicy().MaxEvents).To(Equal(uint(200)))
				Expect(stream.RetentionPolicy().MaxBytes).To(Equal(uint(200)))

				updatedStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(updatedStream.CachedInfo().Config.MaxMsgs).To(Equal(int64(200)))
				Expect(updatedStream.CachedInfo().Config.MaxBytes).To(Equal(int64(200)))
			})

			It("can remove max messages and max bytes from stream", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxEvents(100),
					streams.WithMaxBytes(100),
				)
				Expect(err).ToNot(HaveOccurred())

				stream, err := manager.EnsureStream(ctx, "test", streams.WithSubjects("test"))
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.RetentionPolicy().MaxEvents).To(Equal(uint(0)))
				Expect(stream.RetentionPolicy().MaxBytes).To(Equal(uint(0)))

				updatedStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(updatedStream.CachedInfo().Config.MaxMsgs).To(Equal(int64(-1)))
				Expect(updatedStream.CachedInfo().Config.MaxBytes).To(Equal(int64(-1)))
			})

			It("can create stream with max messages and add max bytes", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxEvents(100),
				)
				Expect(err).ToNot(HaveOccurred())

				stream, err := manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxEvents(100),
					streams.WithMaxBytes(100),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.RetentionPolicy().MaxEvents).To(Equal(uint(100)))
				Expect(stream.RetentionPolicy().MaxBytes).To(Equal(uint(100)))

				updatedStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(updatedStream.CachedInfo().Config.MaxMsgs).To(Equal(int64(100)))
				Expect(updatedStream.CachedInfo().Config.MaxBytes).To(Equal(int64(100)))
			})
		})

		Describe("Discard policies", func() {
			It("default discard policy is old", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				stream, err := manager.EnsureStream(ctx, "test", streams.WithSubjects("test"))
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.RetentionPolicy().DiscardPolicy).To(Equal(streams.DiscardPolicyOld))

				createdStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(createdStream.CachedInfo().Config.Discard).To(Equal(jetstream.DiscardOld))
			})

			It("can create stream with discard policy set to old", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				stream, err := manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithDiscardPolicy(streams.DiscardPolicyOld),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.RetentionPolicy().DiscardPolicy).To(Equal(streams.DiscardPolicyOld))

				createdStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(createdStream.CachedInfo().Config.Discard).To(Equal(jetstream.DiscardOld))
			})

			It("can create stream with discard policy set to new", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				stream, err := manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithDiscardPolicy(streams.DiscardPolicyNew),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.RetentionPolicy().DiscardPolicy).To(Equal(streams.DiscardPolicyNew))

				createdStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(createdStream.CachedInfo().Config.Discard).To(Equal(jetstream.DiscardNew))
			})

			It("can set discard policy to new and per subject", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				stream, err := manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithDiscardPolicy(streams.DiscardPolicyNew),
					streams.WithDiscardNewPerSubject(true),
					streams.WithMaxEventsPerSubject(100),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.RetentionPolicy().DiscardPolicy).To(Equal(streams.DiscardPolicyNew))
				Expect(stream.RetentionPolicy().DiscardNewPerSubject).To(BeTrue())

				createdStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(createdStream.CachedInfo().Config.Discard).To(Equal(jetstream.DiscardNew))
				Expect(createdStream.CachedInfo().Config.DiscardNewPerSubject).To(BeTrue())
			})

			It("can update discard policy", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithDiscardPolicy(streams.DiscardPolicyNew),
				)
				Expect(err).ToNot(HaveOccurred())

				stream, err := manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithDiscardPolicy(streams.DiscardPolicyOld),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.RetentionPolicy().DiscardPolicy).To(Equal(streams.DiscardPolicyOld))

				createdStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(createdStream.CachedInfo().Config.Discard).To(Equal(jetstream.DiscardOld))
			})

			It("setting discard policy to new per subject fails if no max messages per subject", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithDiscardPolicy(streams.DiscardPolicyNew),
					streams.WithDiscardNewPerSubject(true),
				)
				Expect(err).To(HaveOccurred())
			})
		})

		Describe("Deduplication", func() {
			It("default deduplication window is 2 minutes", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				stream, err := manager.EnsureStream(ctx, "test", streams.WithSubjects("test"))
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.DeduplicationWindow()).To(Equal(2 * time.Minute))

				createdStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(createdStream.CachedInfo().Config.Duplicates).To(Equal(2 * time.Minute))
			})

			It("deduplication window can be set to 1 minute", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				stream, err := manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithDeduplicationWindow(1*time.Minute),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.DeduplicationWindow()).To(Equal(1 * time.Minute))

				createdStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(createdStream.CachedInfo().Config.Duplicates).To(Equal(1 * time.Minute))
			})

			It("deduplication window can be updated", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithDeduplicationWindow(1*time.Minute),
				)
				Expect(err).ToNot(HaveOccurred())

				stream, err := manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithDeduplicationWindow(2*time.Minute),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.DeduplicationWindow()).To(Equal(2 * time.Minute))

				updatedStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(updatedStream.CachedInfo().Config.Duplicates).To(Equal(2 * time.Minute))
			})
		})

		Describe("Event sizes", func() {
			It("default max event size is unknown", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				stream, err := manager.EnsureStream(ctx, "test", streams.WithSubjects("test"))
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.MaxEventSize()).To(Equal(uint(0)))

				createdStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(createdStream.CachedInfo().Config.MaxMsgSize).To(BeNumerically("==", -1))
			})

			It("max event size can be set to 2MB", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				stream, err := manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxEventSize(2*1024*1024),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.MaxEventSize()).To(Equal(uint(2 * 1024 * 1024)))

				createdStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(createdStream.CachedInfo().Config.MaxMsgSize).To(BeNumerically("==", 2*1024*1024))
			})

			It("max event size can be added", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
				)
				Expect(err).ToNot(HaveOccurred())

				stream, err := manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxEventSize(2*1024*1024),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.MaxEventSize()).To(Equal(uint(2 * 1024 * 1024)))

				updatedStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(updatedStream.CachedInfo().Config.MaxMsgSize).To(BeNumerically("==", 2*1024*1024))
			})

			It("max event size can be updated", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxEventSize(2*1024*1024),
				)
				Expect(err).ToNot(HaveOccurred())

				stream, err := manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxEventSize(4*1024*1024),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.MaxEventSize()).To(Equal(uint(4 * 1024 * 1024)))

				updatedStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(updatedStream.CachedInfo().Config.MaxMsgSize).To(BeNumerically("==", 4*1024*1024))
			})

			It("max event size can be removed", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithMaxEventSize(2*1024*1024),
				)
				Expect(err).ToNot(HaveOccurred())

				stream, err := manager.EnsureStream(ctx, "test", streams.WithSubjects("test"))
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.MaxEventSize()).To(Equal(uint(0)))

				updatedStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(updatedStream.CachedInfo().Config.MaxMsgSize).To(BeNumerically("==", -1))
			})
		})

		Describe("Storage", func() {
			It("defaults to file storage", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				stream, err := manager.EnsureStream(ctx, "test", streams.WithSubjects("test"))
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.Storage().Type).To(Equal(streams.StorageTypeFile))

				createdStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(createdStream.CachedInfo().Config.Storage).To(Equal(jetstream.FileStorage))
			})

			It("can create stream with file storage", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				stream, err := manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithStorageType(streams.StorageTypeFile),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.Storage().Type).To(Equal(streams.StorageTypeFile))

				createdStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(createdStream.CachedInfo().Config.Storage).To(Equal(jetstream.FileStorage))
			})

			It("can create stream with memory storage", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				stream, err := manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithStorageType(streams.StorageTypeMemory),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.Storage().Type).To(Equal(streams.StorageTypeMemory))

				createdStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(createdStream.CachedInfo().Config.Storage).To(Equal(jetstream.MemoryStorage))
			})

			It("updating storage type of existing stream errors", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithStorageType(streams.StorageTypeMemory),
				)
				Expect(err).ToNot(HaveOccurred())

				_, err = manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithStorageType(streams.StorageTypeFile),
				)
				Expect(err).To(HaveOccurred())
			})

			It("replicas defaults to 1", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, "test", streams.WithSubjects("test"))
				Expect(err).ToNot(HaveOccurred())

				stream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.CachedInfo().Config.Replicas).To(Equal(1))
			})

			It("can create stream with 0 replicas defaults to 1 replica", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				stream, err := manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithStorageReplicas(0),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.Storage().Replicas).To(Equal(uint(1)))

				createdStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(createdStream.CachedInfo().Config.Replicas).To(Equal(1))
			})

			It("can create stream with 1 replica", func(ctx context.Context) {
				_, err := js.Stream(ctx, "test")
				Expect(err).To(MatchError(jetstream.ErrStreamNotFound))

				stream, err := manager.EnsureStream(ctx, "test",
					streams.WithSubjects("test"),
					streams.WithStorageReplicas(1),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(stream.Storage().Replicas).To(Equal(uint(1)))

				createdStream, err := js.Stream(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(createdStream.CachedInfo().Config.Replicas).To(Equal(1))
			})
		})
	})
})
