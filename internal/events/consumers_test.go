package events_test

import (
	"context"
	"errors"
	"time"

	"github.com/levelfourab/windshift-go/events"
	"github.com/levelfourab/windshift-go/events/consumers"
	"github.com/levelfourab/windshift-go/events/streams"
	"github.com/nats-io/nats.go/jetstream"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Consumers", func() {
	var manager events.Client
	var js jetstream.JetStream

	BeforeEach(func() {
		manager, js = createClientAndJetStream()
	})

	Describe("Configuration issues", func() {
		It("consumer with invalid stream name fails", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, "invalid name", consumers.WithSubjects("test"))
			Expect(err).To(HaveOccurred())
		})

		It("consumer with non-existent stream fails", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, "nonexistent", consumers.WithSubjects("test"))
			Expect(err).To(HaveOccurred())
		})

		It("consumer with invalid name fails", func(ctx context.Context) {
			_, err := manager.EnsureStream(ctx, "test", streams.WithSubjects("test"))
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.EnsureConsumer(ctx, "test", consumers.WithName("invalid name"), consumers.WithSubjects("test"))
			Expect(err).To(HaveOccurred())
		})

		It("consumer with no subjects works", func(ctx context.Context) {
			_, err := manager.EnsureStream(ctx, "test", streams.WithSubjects("test"))
			Expect(err).ToNot(HaveOccurred())

			consumer, err := manager.EnsureConsumer(ctx, "test")
			Expect(err).ToNot(HaveOccurred())
			Expect(consumer.Name()).ToNot(BeEmpty())

			createdConsumer, err := js.Consumer(ctx, "test", consumer.Name())
			Expect(err).ToNot(HaveOccurred())
			Expect(createdConsumer.CachedInfo().Stream).To(Equal("test"))
			Expect(createdConsumer.CachedInfo().Config.FilterSubject).To(BeEmpty())
			Expect(createdConsumer.CachedInfo().Config.FilterSubjects).To(BeEmpty())
		})

		It("consumer with invalid subject fails", func(ctx context.Context) {
			_, err := manager.EnsureStream(ctx, "test", streams.WithSubjects("test"))
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.EnsureConsumer(ctx, "test", consumers.WithSubjects("invalid subject"))
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("Ephemeral", func() {
		It("can create consumer with no subject", func(ctx context.Context) {
			_, err := manager.EnsureStream(ctx, "test", streams.WithSubjects("test"))
			Expect(err).ToNot(HaveOccurred())

			consumer, err := manager.EnsureConsumer(ctx, "test")
			Expect(err).ToNot(HaveOccurred())

			createdConsumer, err := js.Consumer(ctx, "test", consumer.Name())
			Expect(err).ToNot(HaveOccurred())
			Expect(createdConsumer.CachedInfo().Stream).To(Equal("test"))
			Expect(createdConsumer.CachedInfo().Config.InactiveThreshold).To(Equal(1 * time.Hour))
		})

		It("can create consumer with single subject", func(ctx context.Context) {
			_, err := manager.EnsureStream(ctx, "test", streams.WithSubjects("test"))
			Expect(err).ToNot(HaveOccurred())

			consumer, err := manager.EnsureConsumer(ctx, "test", consumers.WithSubjects("test"))
			Expect(err).ToNot(HaveOccurred())

			createdConsumer, err := js.Consumer(ctx, "test", consumer.Name())
			Expect(err).ToNot(HaveOccurred())
			Expect(createdConsumer.CachedInfo().Stream).To(Equal("test"))
			Expect(createdConsumer.CachedInfo().Config.FilterSubject).To(BeEmpty())
			Expect(createdConsumer.CachedInfo().Config.FilterSubjects).To(ConsistOf("test"))
		})

		It("can create consumer with multiple subjects", func(ctx context.Context) {
			_, err := manager.EnsureStream(ctx, "test", streams.WithSubjects("test", "test.>"))
			Expect(err).ToNot(HaveOccurred())

			_, err = js.Consumer(ctx, "test", "test")
			Expect(err).To(HaveOccurred())
			Expect(errors.Is(err, jetstream.ErrConsumerNotFound)).To(BeTrue())

			consumer, err := manager.EnsureConsumer(ctx, "test", consumers.WithSubjects("test", "test.2"))
			Expect(err).ToNot(HaveOccurred())

			createdConsumer, err := js.Consumer(ctx, "test", consumer.Name())
			Expect(err).ToNot(HaveOccurred())
			Expect(createdConsumer.CachedInfo().Config.FilterSubject).To(BeEmpty())
			Expect(createdConsumer.CachedInfo().Config.FilterSubjects).To(ConsistOf("test", "test.2"))
		})
	})

	Describe("Durable", func() {
		It("can create a subscription", func(ctx context.Context) {
			_, err := manager.EnsureStream(ctx, "test", streams.WithSubjects("test"))
			Expect(err).ToNot(HaveOccurred())

			_, err = js.Consumer(ctx, "test", "test")
			Expect(err).To(HaveOccurred())
			Expect(errors.Is(err, jetstream.ErrConsumerNotFound)).To(BeTrue())

			consumer, err := manager.EnsureConsumer(ctx, "test", consumers.WithName("test"), consumers.WithSubjects("test"))
			Expect(err).ToNot(HaveOccurred())

			_, err = js.Consumer(ctx, "test", consumer.Name())
			Expect(err).ToNot(HaveOccurred())
		})

		It("can update subject of subscription", func(ctx context.Context) {
			_, err := manager.EnsureStream(ctx, "test", streams.WithSubjects("test", "test.>"))
			Expect(err).ToNot(HaveOccurred())

			consumer, err := manager.EnsureConsumer(ctx, "test", consumers.WithName("test"), consumers.WithSubjects("test"))
			Expect(err).ToNot(HaveOccurred())

			_, err = js.Consumer(ctx, "test", consumer.Name())
			Expect(err).ToNot(HaveOccurred())

			consumer, err = manager.EnsureConsumer(ctx, "test", consumers.WithName("test"), consumers.WithSubjects("test.2"))
			Expect(err).ToNot(HaveOccurred())

			c, err := js.Consumer(ctx, "test", consumer.Name())
			Expect(err).ToNot(HaveOccurred())
			Expect(c.CachedInfo().Config.FilterSubject).To(BeEmpty())
			Expect(c.CachedInfo().Config.FilterSubjects).To(ConsistOf("test.2"))
		})

		It("can create consumer with multiple subjects", func(ctx context.Context) {
			_, err := manager.EnsureStream(ctx, "test", streams.WithSubjects("test", "test.>"))
			Expect(err).ToNot(HaveOccurred())

			_, err = js.Consumer(ctx, "test", "test")
			Expect(err).To(HaveOccurred())
			Expect(errors.Is(err, jetstream.ErrConsumerNotFound)).To(BeTrue())

			consumer, err := manager.EnsureConsumer(ctx, "test", consumers.WithName("test"), consumers.WithSubjects("test", "test.2"))
			Expect(err).ToNot(HaveOccurred())

			c, err := js.Consumer(ctx, "test", consumer.Name())
			Expect(err).ToNot(HaveOccurred())
			Expect(c.CachedInfo().Config.FilterSubject).To(BeEmpty())
			Expect(c.CachedInfo().Config.FilterSubjects).To(ConsistOf("test", "test.2"))
		})

		It("can update from one subject to multiple", func(ctx context.Context) {
			_, err := manager.EnsureStream(ctx, "test", streams.WithSubjects("test", "test.>"))
			Expect(err).ToNot(HaveOccurred())

			consumer, err := manager.EnsureConsumer(ctx, "test", consumers.WithName("test"), consumers.WithSubjects("test"))
			Expect(err).ToNot(HaveOccurred())

			_, err = js.Consumer(ctx, "test", consumer.Name())
			Expect(err).ToNot(HaveOccurred())

			consumer, err = manager.EnsureConsumer(ctx, "test", consumers.WithName("test"), consumers.WithSubjects("test.2", "test.3"))
			Expect(err).ToNot(HaveOccurred())

			c, err := js.Consumer(ctx, "test", consumer.Name())
			Expect(err).ToNot(HaveOccurred())
			Expect(c.CachedInfo().Config.FilterSubject).To(BeEmpty())
			Expect(c.CachedInfo().Config.FilterSubjects).To(ConsistOf("test.2", "test.3"))
		})

		Describe("From", func() {
			It("defaults to delivering new messages", func(ctx context.Context) {
				_, err := manager.EnsureStream(ctx, "test", streams.WithSubjects("test"))
				Expect(err).ToNot(HaveOccurred())

				consumer, err := manager.EnsureConsumer(ctx, "test", consumers.WithName("test"), consumers.WithSubjects("test"))
				Expect(err).ToNot(HaveOccurred())

				c, err := js.Consumer(ctx, "test", consumer.Name())
				Expect(err).ToNot(HaveOccurred())
				Expect(c.CachedInfo().Config.DeliverPolicy).To(Equal(jetstream.DeliverNewPolicy))
			})

			It("can set to specific start time", func(ctx context.Context) {
				_, err := manager.EnsureStream(ctx, "test", streams.WithSubjects("test"))
				Expect(err).ToNot(HaveOccurred())

				t := time.Now()
				consumer, err := manager.EnsureConsumer(ctx, "test",
					consumers.WithName("test"),
					consumers.WithSubjects("test"),
					consumers.WithConsumeFrom(streams.AtStreamTimestamp(t)),
				)
				Expect(err).ToNot(HaveOccurred())

				c, err := js.Consumer(ctx, "test", consumer.Name())
				Expect(err).ToNot(HaveOccurred())
				Expect(c.CachedInfo().Config.DeliverPolicy).To(Equal(jetstream.DeliverByStartTimePolicy))
				Expect(*c.CachedInfo().Config.OptStartTime).To(BeTemporally("~", t, time.Millisecond))
			})

			It("can set to specific start ID", func(ctx context.Context) {
				_, err := manager.EnsureStream(ctx, "test", streams.WithSubjects("test"))
				Expect(err).ToNot(HaveOccurred())

				consumer, err := manager.EnsureConsumer(ctx, "test",
					consumers.WithName("test"),
					consumers.WithSubjects("test"),
					consumers.WithConsumeFrom(streams.AtStreamOffset(1)),
				)
				Expect(err).ToNot(HaveOccurred())

				c, err := js.Consumer(ctx, "test", consumer.Name())
				Expect(err).ToNot(HaveOccurred())
				Expect(c.CachedInfo().Config.DeliverPolicy).To(Equal(jetstream.DeliverByStartSequencePolicy))
				Expect(c.CachedInfo().Config.OptStartSeq).To(Equal(uint64(1)))
			})
		})
	})
})
