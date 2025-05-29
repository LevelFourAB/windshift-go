package events_test

import (
	"context"
	"time"

	"github.com/levelfourab/windshift-go/events"
	"github.com/levelfourab/windshift-go/events/consumers"
	"github.com/levelfourab/windshift-go/events/streams"
	"github.com/levelfourab/windshift-go/events/subscribe"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Event Consumption", func() {
	var manager events.Client

	BeforeEach(func() {
		manager, _ = createClientAndJetStream()

		_, err := manager.EnsureStream(context.Background(), "events", streams.WithSubjects("events.>"))
		Expect(err).ToNot(HaveOccurred())
	})

	Describe("Ephemeral consumption", func() {
		It("can create", func(ctx context.Context) {
			sub, err := manager.EnsureConsumer(ctx, "events", consumers.WithSubjects("events.>"))
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.Subscribe(ctx, "events", sub.Name())
			Expect(err).ToNot(HaveOccurred())
		})

		It("can receive events with subject specified", func(ctx context.Context) {
			sub, err := manager.EnsureConsumer(ctx, "events", consumers.WithSubjects("events.>"))
			Expect(err).ToNot(HaveOccurred())

			ec, err := manager.Subscribe(ctx, "events", sub.Name())
			Expect(err).ToNot(HaveOccurred())

			msg := structpb.NewStringValue("test")
			Expect(err).ToNot(HaveOccurred())
			_, err = manager.Publish(ctx, "events.test", msg)
			Expect(err).ToNot(HaveOccurred())

			select {
			case event := <-ec:
				Expect(event).ToNot(BeNil())
				Expect(event.Subject()).To(Equal("events.test"))

				data, err := event.UnmarshalNew()
				Expect(err).ToNot(HaveOccurred())
				if msg2, ok := data.(*structpb.Value); ok {
					Expect(msg2.GetStringValue()).To(Equal(msg.GetStringValue()))
				} else {
					Fail("unexpected data type")
				}
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}
		})

		It("can receive events without subject specified", func(ctx context.Context) {
			sub, err := manager.EnsureConsumer(ctx, "events")
			Expect(err).ToNot(HaveOccurred())

			ec, err := manager.Subscribe(ctx, "events", sub.Name())
			Expect(err).ToNot(HaveOccurred())

			msg := structpb.NewStringValue("test")
			Expect(err).ToNot(HaveOccurred())
			_, err = manager.Publish(ctx, "events.test", msg)
			Expect(err).ToNot(HaveOccurred())

			select {
			case event := <-ec:
				Expect(event).ToNot(BeNil())
				Expect(event.Subject()).To(Equal("events.test"))

				data, err := event.UnmarshalNew()
				Expect(err).ToNot(HaveOccurred())
				if msg2, ok := data.(*structpb.Value); ok {
					Expect(msg2.GetStringValue()).To(Equal(msg.GetStringValue()))
				} else {
					Fail("unexpected data type")
				}
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}
		})

		It("multiple subscribers receive same events", func(ctx context.Context) {
			sub1, err := manager.EnsureConsumer(ctx, "events")
			Expect(err).ToNot(HaveOccurred())

			sub2, err := manager.EnsureConsumer(ctx, "events")
			Expect(err).ToNot(HaveOccurred())

			ec1, err := manager.Subscribe(ctx, "events", sub1.Name())
			Expect(err).ToNot(HaveOccurred())

			ec2, err := manager.Subscribe(ctx, "events", sub2.Name())
			Expect(err).ToNot(HaveOccurred())

			Expect(err).ToNot(HaveOccurred())
			_, err = manager.Publish(ctx, "events.test", &emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())

			select {
			case event := <-ec1:
				Expect(event).ToNot(BeNil())
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}

			select {
			case event := <-ec2:
				Expect(event).ToNot(BeNil())
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}
		})

		It("will not receive events published before subscription", func(ctx context.Context) {
			_, err := manager.Publish(ctx, "events.test", &emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())

			sub, err := manager.EnsureConsumer(ctx, "events")
			Expect(err).ToNot(HaveOccurred())

			ec, err := manager.Subscribe(ctx, "events", sub.Name())
			Expect(err).ToNot(HaveOccurred())

			select {
			case <-ec:
				Fail("received event")
			case <-time.After(200 * time.Millisecond):
			}
		})

		It("can receive events published before subscription", func(ctx context.Context) {
			_, err := manager.Publish(ctx, "events.test", &emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())

			sub, err := manager.EnsureConsumer(ctx, "events", consumers.WithConsumeFrom(streams.AtStreamStart()))
			Expect(err).ToNot(HaveOccurred())

			ec, err := manager.Subscribe(ctx, "events", sub.Name())
			Expect(err).ToNot(HaveOccurred())

			select {
			case event := <-ec:
				Expect(event).ToNot(BeNil())
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}
		})
	})

	Describe("Durable consumption", func() {
		It("can create", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, "events", consumers.WithName("test"))
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.Subscribe(ctx, "events", "test")
			Expect(err).ToNot(HaveOccurred())
		})

		It("can receive events", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, "events", consumers.WithName("test"))
			Expect(err).ToNot(HaveOccurred())

			ec, err := manager.Subscribe(ctx, "events", "test")
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.Publish(ctx, "events.test", &emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())

			select {
			case event := <-ec:
				Expect(event).ToNot(BeNil())

				err = event.Ack(ctx)
				Expect(err).ToNot(HaveOccurred())

				// Check that we have the correct message
				empty := &emptypb.Empty{}
				err = event.UnmarshalTo(empty)
				Expect(err).ToNot(HaveOccurred())
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}
		})

		It("can receive events with multiple subscribers with same name", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, "events", consumers.WithName("test"))
			Expect(err).ToNot(HaveOccurred())

			ec1, err := manager.Subscribe(ctx, "events", "test")
			Expect(err).ToNot(HaveOccurred())

			ec2, err := manager.Subscribe(ctx, "events", "test")
			Expect(err).ToNot(HaveOccurred())

			for i := 0; i < 10; i++ {
				_, err2 := manager.Publish(ctx, "events.test", &emptypb.Empty{})
				Expect(err2).ToNot(HaveOccurred())
			}

			eventsReceived := 0
			ec1EventsReceived := 0
			ec2EventsReceived := 0
		_outer:
			for {
				select {
				case e := <-ec1:
					eventsReceived++
					ec1EventsReceived++
					err = e.Ack(ctx)
					Expect(err).ToNot(HaveOccurred())
				case e := <-ec2:
					eventsReceived++
					ec2EventsReceived++
					err = e.Ack(ctx)
					Expect(err).ToNot(HaveOccurred())
				case <-time.After(500 * time.Millisecond):
					break _outer
				}
			}

			// Check that the right number of events were received
			Expect(eventsReceived).To(BeNumerically("==", 10))

			// Make sure that each instance has received at least one event
			Expect(ec1EventsReceived).To(BeNumerically(">", 0))
			Expect(ec2EventsReceived).To(BeNumerically(">", 0))
		})

		It("multiple subscribers with different names receive same events", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, "events", consumers.WithName("test1"))
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.EnsureConsumer(ctx, "events", consumers.WithName("test2"))
			Expect(err).ToNot(HaveOccurred())

			ec1, err := manager.Subscribe(ctx, "events", "test1")
			Expect(err).ToNot(HaveOccurred())

			ec2, err := manager.Subscribe(ctx, "events", "test2")
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.Publish(ctx, "events.test", &emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())

			select {
			case event := <-ec1:
				Expect(event).ToNot(BeNil())
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}

			select {
			case event := <-ec2:
				Expect(event).ToNot(BeNil())
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}
		})

		It("canceling context stops receiving events", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, "events", consumers.WithName("test"))
			Expect(err).ToNot(HaveOccurred())

			ctx1, cancel1 := context.WithCancel(ctx)
			ec, err := manager.Subscribe(ctx1, "events", "test")
			Expect(err).ToNot(HaveOccurred())

			cancel1()
			time.Sleep(50 * time.Millisecond)

			_, err = manager.Publish(ctx, "events.test", &emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())

			// Check if the event can be received again
			select {
			case _, ok := <-ec:
				if ok {
					Fail("event received after close")
				}
			case <-time.After(200 * time.Millisecond):
			}
		})

		It("acknowledging event stops delivery", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, "events",
				consumers.WithName("test"),
				consumers.WithProcessingTimeout(100*time.Millisecond),
			)
			Expect(err).ToNot(HaveOccurred())

			ec, err := manager.Subscribe(ctx, "events", "test")
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.Publish(ctx, "events.test", &emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())

			select {
			case event := <-ec:
				Expect(event).ToNot(BeNil())

				err = event.Ack(ctx)
				Expect(err).ToNot(HaveOccurred())
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}

			select {
			case <-ec:
				Fail("event received again")
			case <-time.After(200 * time.Millisecond):
				// Make sure event isn't delivered for a certain period
			}
		})

		It("rejecting event redelivers it", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, "events",
				consumers.WithName("test"),
				consumers.WithProcessingTimeout(100*time.Millisecond),
			)
			Expect(err).ToNot(HaveOccurred())

			ec, err := manager.Subscribe(ctx, "events", "test")
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.Publish(ctx, "events.test", &emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())

			select {
			case event := <-ec:
				Expect(event).ToNot(BeNil())
				Expect(event.DeliveryAttempt()).To(BeNumerically("==", 1))

				err = event.Reject(ctx)
				Expect(err).ToNot(HaveOccurred())
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}

			select {
			case event := <-ec:
				Expect(event).ToNot(BeNil())
				Expect(event.DeliveryAttempt()).To(BeNumerically("==", 2))

				err = event.Ack(ctx)
				Expect(err).ToNot(HaveOccurred())

				// Check that we have the correct message
				empty := &emptypb.Empty{}
				err = event.UnmarshalTo(empty)
				Expect(err).ToNot(HaveOccurred())
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}
		})

		It("rejecting event redelivers it to another instance", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, "events",
				consumers.WithName("test"),
				consumers.WithProcessingTimeout(100*time.Millisecond),
			)
			Expect(err).ToNot(HaveOccurred())

			ctx1, cancel1 := context.WithCancel(ctx)
			ec1, err := manager.Subscribe(ctx1, "events", "test")
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.Publish(ctx, "events.test", &emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())

			var event events.Event
			select {
			case event = <-ec1:
				Expect(event).ToNot(BeNil())
				Expect(event.DeliveryAttempt()).To(BeNumerically("==", 1))
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}

			cancel1()

			ec2, err := manager.Subscribe(ctx, "events", "test")
			Expect(err).ToNot(HaveOccurred())

			err = event.Reject(ctx)
			Expect(err).ToNot(HaveOccurred())

			// Receive the event again
			select {
			case event = <-ec2:
				Expect(event).ToNot(BeNil())
				Expect(event.DeliveryAttempt()).To(BeNumerically(">=", 2))
			case <-time.After(500 * time.Millisecond):
				Fail("timeout waiting for event")
			}
		})

		It("can reject with a delay", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, "events", consumers.WithName("test"))
			Expect(err).ToNot(HaveOccurred())

			ec, err := manager.Subscribe(ctx, "events", "test")
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.Publish(ctx, "events.test", &emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())

			event := <-ec
			Expect(event).ToNot(BeNil())
			Expect(event.DeliveryAttempt()).To(BeNumerically("==", 1))

			start := time.Now()
			err = event.Reject(ctx, events.WithRedeliveryDelay(100*time.Millisecond))
			Expect(err).ToNot(HaveOccurred())

			// Receive the event again
			select {
			case event := <-ec:
				if time.Since(start) < 100*time.Millisecond {
					Fail("event received too early")
				}

				Expect(event.DeliveryAttempt()).To(BeNumerically("==", 2))
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}
		})

		It("permanently rejecting event does not redeliver it", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, "events", consumers.WithName("test"))
			Expect(err).ToNot(HaveOccurred())

			ec, err := manager.Subscribe(ctx, "events", "test")
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.Publish(ctx, "events.test", &emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())

			var event events.Event
			select {
			case event = <-ec:
				Expect(event).ToNot(BeNil())
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}

			err = event.Reject(ctx, events.Permanently())
			Expect(err).ToNot(HaveOccurred())

			// Receive the event again
			select {
			case <-ec:
				Fail("event received again")
			case <-time.After(200 * time.Millisecond):
			}
		})

		It("event gets permanently rejected after max deliveries is reached", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, "events",
				consumers.WithName("test"),
				consumers.WithMaxDeliveryAttempts(1),
			)
			Expect(err).ToNot(HaveOccurred())

			ec, err := manager.Subscribe(ctx, "events", "test")
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.Publish(ctx, "events.test", &emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())

			var event events.Event
			select {
			case event = <-ec:
				Expect(event).ToNot(BeNil())
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}

			err = event.Reject(ctx)
			Expect(err).ToNot(HaveOccurred())

			// Receive the event again
			select {
			case <-ec:
				Fail("event received again")
			case <-time.After(200 * time.Millisecond):
			}
		})

		It("events are automatically pinged", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, "events",
				consumers.WithName("test"),
				consumers.WithProcessingTimeout(200*time.Millisecond),
			)
			Expect(err).ToNot(HaveOccurred())

			ec, err := manager.Subscribe(ctx, "events", "test", subscribe.WithAutoPingInterval(50*time.Millisecond))
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.Publish(ctx, "events.test", &emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())

			select {
			case event := <-ec:
				Expect(event).ToNot(BeNil())
				time.Sleep(100 * time.Millisecond)
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}

			select {
			case <-ec:
				Fail("event received again after ping")
			case <-time.After(100 * time.Millisecond):
				// Make sure event isn't delivered for a certain period
			}
		})

		It("can manually ping events to extend their processing time", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, "events",
				consumers.WithName("test"),
				consumers.WithProcessingTimeout(200*time.Millisecond),
			)
			Expect(err).ToNot(HaveOccurred())

			ec, err := manager.Subscribe(ctx, "events", "test", subscribe.DisableAutoPing())
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.Publish(ctx, "events.test", &emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())

			select {
			case event := <-ec:
				Expect(event).ToNot(BeNil())

				time.Sleep(50 * time.Millisecond)

				err = event.Ping(ctx)
				Expect(err).ToNot(HaveOccurred())

				time.Sleep(20 * time.Millisecond)
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}

			select {
			case <-ec:
				Fail("event received again after ping")
			case <-time.After(100 * time.Millisecond):
				// Make sure event isn't delivered for a certain period
			}
		})

		It("not processing event redelivers it when auto-ping is disabled", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, "events",
				consumers.WithName("test"),
				consumers.WithProcessingTimeout(100*time.Millisecond),
			)
			Expect(err).ToNot(HaveOccurred())

			ec, err := manager.Subscribe(ctx, "events", "test", subscribe.DisableAutoPing())
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.Publish(ctx, "events.test", &emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())

			select {
			case event := <-ec:
				Expect(event).ToNot(BeNil())
				Expect(event.DeliveryAttempt()).To(BeNumerically("==", 1))
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}

			time.Sleep(200 * time.Millisecond)

			select {
			case event := <-ec:
				Expect(event).ToNot(BeNil())
				Expect(event.DeliveryAttempt()).To(BeNumerically("==", 2))

				err = event.Ack(ctx)
				Expect(err).ToNot(HaveOccurred())

				// Check that we have the correct message
				empty := &emptypb.Empty{}
				err = event.UnmarshalTo(empty)
				Expect(err).ToNot(HaveOccurred())
			case <-time.After(1000 * time.Millisecond):
				Fail("redelivered event not received")
			}
		})
	})

	Describe("OpenTelemetry", func() {
		var tracer trace.Tracer

		BeforeEach(func() {
			// Set up a trace.Tracer that will record all spans
			tracingProvider := sdktrace.NewTracerProvider()
			tracer = tracingProvider.Tracer("test")
		})

		It("receiving an event creates a span", func(ctx context.Context) {
			ctx, span := tracer.Start(ctx, "test")
			defer span.End()

			sub, err := manager.EnsureConsumer(ctx, "events")
			Expect(err).ToNot(HaveOccurred())

			ec, err := manager.Subscribe(ctx, "events", sub.Name())
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.Publish(ctx, "events.test", &emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())

			select {
			case event := <-ec:
				Expect(event).ToNot(BeNil())

				eventSpan := trace.SpanFromContext(event.Context())
				Expect(eventSpan.SpanContext().IsValid()).To(BeTrue())
				Expect(eventSpan.SpanContext().TraceID().String()).To(Equal(span.SpanContext().TraceID().String()))
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}
		})
	})
})
