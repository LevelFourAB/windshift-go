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

var _ = Describe("Publish", func() {
	var manager events.Client
	var js jetstream.JetStream

	BeforeEach(func() {
		manager, js = createClientAndJetStream()

		_, err := manager.EnsureStream(context.Background(), "test", streams.WithSubjects("test"))
		Expect(err).ToNot(HaveOccurred())
	})

	It("can publish a message", func(ctx context.Context) {
		_, err := manager.Publish(ctx, "test", &emptypb.Empty{})
		Expect(err).ToNot(HaveOccurred())

		// Check that the message was published
		stream, err := js.Stream(ctx, "test")
		Expect(err).ToNot(HaveOccurred())
		Expect(stream.CachedInfo().State.Msgs).To(Equal(uint64(1)))
		Expect(stream.CachedInfo().State.FirstSeq).To(Equal(uint64(1)))
		Expect(stream.CachedInfo().State.LastSeq).To(Equal(uint64(1)))
	})

	It("can publish a message with a timestamp", func(ctx context.Context) {
		timestamp := time.Now().Add(-time.Second).UTC()
		_, err := manager.Publish(ctx, "test", &emptypb.Empty{},
			events.WithTimestamp(timestamp))
		Expect(err).ToNot(HaveOccurred())

		// Check that the message was published
		stream, err := js.Stream(ctx, "test")
		Expect(err).ToNot(HaveOccurred())
		Expect(stream.CachedInfo().State.Msgs).To(Equal(uint64(1)))

		// Get the message to check the timestamp
		msg, err := stream.GetMsg(ctx, 1)
		Expect(err).ToNot(HaveOccurred())
		Expect(msg.Header.Get("WS-Published-Time")).To(Equal(timestamp.Format(time.RFC3339Nano)))
	})

	It("publishing with an idempotency key will not publish the same message twice", func(ctx context.Context) {
		_, err := manager.Publish(ctx, "test", &emptypb.Empty{}, events.WithIdempotencyKey("123"))
		Expect(err).ToNot(HaveOccurred())

		_, err = manager.Publish(ctx, "test", &emptypb.Empty{}, events.WithIdempotencyKey("123"))
		Expect(err).ToNot(HaveOccurred())

		// Check that the message was published
		stream, err := js.Stream(ctx, "test")
		Expect(err).ToNot(HaveOccurred())
		Expect(stream.CachedInfo().State.Msgs).To(Equal(uint64(1)))
		Expect(stream.CachedInfo().State.FirstSeq).To(Equal(uint64(1)))
		Expect(stream.CachedInfo().State.LastSeq).To(Equal(uint64(1)))
	})
})
