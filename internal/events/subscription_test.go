package events_test

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/levelfourab/windshift-go/delays"
	"github.com/levelfourab/windshift-go/events"
	"github.com/levelfourab/windshift-go/events/consumers"
	"github.com/levelfourab/windshift-go/events/streams"
	"github.com/levelfourab/windshift-go/events/subscribe"
	internalevents "github.com/levelfourab/windshift-go/internal/events"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
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

		It("canceling context closes the channel", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, "events", consumers.WithName("test"))
			Expect(err).ToNot(HaveOccurred())

			ctx1, cancel1 := context.WithCancel(ctx)
			ec, err := manager.Subscribe(ctx1, "events", "test")
			Expect(err).ToNot(HaveOccurred())

			cancel1()

			// Ranging over the channel must terminate, so it has to be
			// closed once delivery has stopped.
			done := make(chan struct{})
			go func() {
				defer close(done)
				//nolint:revive // draining until closed is the point
				for range ec {
				}
			}()

			select {
			case <-done:
			case <-time.After(2 * time.Second):
				Fail("channel was not closed after context cancellation")
			}
		})

		It("canceling context while a send is blocked closes the channel without panicking", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, "events", consumers.WithName("test"))
			Expect(err).ToNot(HaveOccurred())

			ctx1, cancel1 := context.WithCancel(ctx)
			ec, err := manager.Subscribe(ctx1, "events", "test")
			Expect(err).ToNot(HaveOccurred())

			// Publish events but never read from the channel, so the
			// internal send blocks. Canceling must unblock it and close
			// the channel without a send-on-closed-channel panic.
			for i := 0; i < 10; i++ {
				_, err = manager.Publish(ctx, "events.test", &emptypb.Empty{})
				Expect(err).ToNot(HaveOccurred())
			}

			time.Sleep(100 * time.Millisecond)
			cancel1()

			done := make(chan struct{})
			go func() {
				defer close(done)
				//nolint:revive // draining until closed is the point
				for range ec {
				}
			}()

			select {
			case <-done:
			case <-time.After(2 * time.Second):
				Fail("channel was not closed after context cancellation")
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

		It("rejecting an already-finalized event returns promptly without retrying", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, "events",
				consumers.WithName("test"),
				consumers.WithProcessingTimeout(time.Second),
			)
			Expect(err).ToNot(HaveOccurred())

			// Use a long retry backoff so that, if Reject did not treat an
			// already-finalized message as a permanent outcome, the second
			// Reject would block on retries for several seconds before
			// returning.
			ec, err := manager.Subscribe(ctx, "events", "test",
				subscribe.WithDefaultRetryBackoff(delays.Constant(5*time.Second)))
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

			// First reject finalizes the message server-side.
			Expect(event.Reject(ctx, events.Permanently())).ToNot(HaveOccurred())

			// A second reject on the same event mirrors a retry after a
			// client-side error on an already-successful reject. It must
			// return promptly with the already-acked error rather than
			// exhausting the retry backoff. The bounded context ensures a
			// regression fails fast (returning a context error instead of
			// the already-acked error) rather than retrying indefinitely.
			rejectCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			err = event.Reject(rejectCtx)
			Expect(err).To(HaveOccurred())
			Expect(errors.Is(err, jetstream.ErrMsgAlreadyAckd)).To(BeTrue())
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

		It("auto-ping requeue/cancel path makes progress under volume of reject+redeliver", func(ctx context.Context) {
			// Regression guard for the AutoPing stable-id path. Each event is
			// held on its first delivery long enough to be auto-pinged (and so
			// internally requeued in the DelayQueue with a fresh id) several
			// times, then rejected (per-event onDone -> AutoPing.Remove), then
			// redelivered and acked. This drives the requeue-then-cancel path
			// hard and concurrently across many events. The meaningful,
			// non-flaky invariants are: every event is eventually acked, the
			// run loop keeps draining (the test completes well within the
			// timeout rather than starving), and redeliveries stay bounded
			// (no runaway redelivery storm). A single rare extra redelivery is
			// tolerated because auto-ping + immediate reject has an inherent
			// one-last-ping race that is independent of the stable-id fix.
			const eventCount = 15

			_, err := manager.EnsureConsumer(ctx, "events",
				consumers.WithName("test"),
				consumers.WithProcessingTimeout(300*time.Millisecond),
			)
			Expect(err).ToNot(HaveOccurred())

			ec, err := manager.Subscribe(ctx, "events", "test",
				subscribe.WithAutoPingInterval(25*time.Millisecond))
			Expect(err).ToNot(HaveOccurred())

			for i := 0; i < eventCount; i++ {
				_, err = manager.Publish(ctx, "events.test", &emptypb.Empty{})
				Expect(err).ToNot(HaveOccurred())
			}

			// deliveries[streamSeq] = number of times that logical event was
			// delivered (stream sequence is stable across redeliveries).
			// acked tracks which logical events have been acked so we ack each
			// exactly once even if a stray extra redelivery arrives.
			deliveries := make(map[uint64]int)
			acked := make(map[uint64]bool)
			totalDeliveries := 0
			start := time.Now()
			deadline := time.After(12 * time.Second)

			for len(acked) < eventCount {
				select {
				case event := <-ec:
					Expect(event).ToNot(BeNil())
					id := event.ID()
					deliveries[id]++
					totalDeliveries++

					if deliveries[id] == 1 {
						// First delivery: hold it so auto-ping fires (and
						// requeues with a new internal id) several times, then
						// reject to trigger AutoPing.Remove.
						time.Sleep(90 * time.Millisecond)
						Expect(event.Reject(ctx)).ToNot(HaveOccurred())
					} else if !acked[id] {
						Expect(event.Ack(ctx)).ToNot(HaveOccurred())
						acked[id] = true
					} else {
						// Stray late redelivery of an already-acked event:
						// ack again so it is not redelivered further.
						Expect(event.Ack(ctx)).ToNot(HaveOccurred())
					}
				case <-deadline:
					Fail("timed out; acked " +
						strconv.Itoa(len(acked)) + "/" + strconv.Itoa(eventCount) +
						" events - auto-ping requeue/cancel path is not draining")
				}
			}

			// Every logical event was delivered and acked.
			Expect(deliveries).To(HaveLen(eventCount))
			Expect(acked).To(HaveLen(eventCount))
			for id := range deliveries {
				Expect(deliveries[id]).To(BeNumerically(">=", 2),
					"event %d should be delivered at least twice (reject then redeliver)", id)
			}

			// Redeliveries must stay bounded. The expected total is 2 per
			// event (reject + redeliver); a leaked-pinger storm would push
			// this far higher. Allow generous slack for the inherent
			// one-last-ping race without tolerating runaway redelivery.
			Expect(totalDeliveries).To(BeNumerically("<=", eventCount*3),
				"redelivery storm: %d deliveries for %d events suggests leaked auto-pingers", totalDeliveries, eventCount)

			// The whole churn must complete promptly; a starved/leaking run
			// loop would crawl toward the 12s deadline instead.
			Expect(time.Since(start)).To(BeNumerically("<", 8*time.Second))
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
		// Spans are produced by the client's global tracer (otel.Tracer).
		// otel caches a delegating tracer that binds once to the first
		// provider set, so we install the provider exactly once and reset an
		// in-memory exporter between specs instead of swapping providers.
		var exporter *tracetest.InMemoryExporter
		var otelProviderInstalled bool

		BeforeEach(func() {
			if !otelProviderInstalled {
				exporter = tracetest.NewInMemoryExporter()
				tp := sdktrace.NewTracerProvider(
					sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(exporter)),
				)
				otel.SetTracerProvider(tp)
				otelProviderInstalled = true
			}
			exporter.Reset()
		})

		findSpan := func(spans tracetest.SpanStubs, name string) *tracetest.SpanStub {
			for i := range spans {
				if spans[i].Name == name {
					return &spans[i]
				}
			}
			return nil
		}

		It("receive + process spans are created and linked to the publisher", func(ctx context.Context) {
			sub, err := manager.EnsureConsumer(ctx, "events")
			Expect(err).ToNot(HaveOccurred())

			ec, err := manager.Subscribe(ctx, "events", sub.Name())
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.Publish(ctx, "events.test", &emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())

			select {
			case event := <-ec:
				Expect(event).ToNot(BeNil())

				processCtxSpan := trace.SpanFromContext(event.Context())
				Expect(processCtxSpan.SpanContext().IsValid()).To(BeTrue())

				err = event.Ack(ctx)
				Expect(err).ToNot(HaveOccurred())
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}

			Eventually(func() tracetest.SpanStubs {
				return exporter.GetSpans()
			}).Should(ContainElements(
				HaveField("Name", "events.test receive"),
				HaveField("Name", "events.test process"),
				HaveField("Name", "events.test publish"),
			))

			spans := exporter.GetSpans()
			receiveSpan := findSpan(spans, "events.test receive")
			processSpan := findSpan(spans, "events.test process")
			publishSpan := findSpan(spans, "events.test publish")

			Expect(receiveSpan).ToNot(BeNil())
			Expect(processSpan).ToNot(BeNil())
			Expect(publishSpan).ToNot(BeNil())

			Expect(receiveSpan.SpanKind).To(Equal(trace.SpanKindClient))
			Expect(processSpan.SpanKind).To(Equal(trace.SpanKindConsumer))
			Expect(publishSpan.SpanKind).To(Equal(trace.SpanKindProducer))

			// process is a child of receive.
			Expect(processSpan.Parent.SpanID()).To(Equal(receiveSpan.SpanContext.SpanID()))

			// Both receive and process link to the publisher's trace.
			Expect(receiveSpan.Links).To(HaveLen(1))
			Expect(receiveSpan.Links[0].SpanContext.TraceID()).To(Equal(publishSpan.SpanContext.TraceID()))
			Expect(processSpan.Links).To(HaveLen(1))
			Expect(processSpan.Links[0].SpanContext.TraceID()).To(Equal(publishSpan.SpanContext.TraceID()))

			// Linking model: producer and consumer are separate traces.
			Expect(processSpan.SpanContext.TraceID()).ToNot(Equal(publishSpan.SpanContext.TraceID()))
		})

		It("process span is ended on drop", func(ctx context.Context) {
			subCtx, cancel := context.WithCancel(ctx)

			sub, err := manager.EnsureConsumer(subCtx, "events")
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.Subscribe(subCtx, "events", sub.Name())
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.Publish(ctx, "events.test", &emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())

			// Give the message time to be delivered to the subscription's
			// handler, where it blocks on the channel send because nothing
			// consumes the channel. Then cancel so the event is dropped
			// before delivery (context-canceled drop path).
			time.Sleep(150 * time.Millisecond)
			cancel()

			Eventually(func() tracetest.SpanStubs {
				return exporter.GetSpans()
			}).Should(ContainElement(HaveField("Name", "events.test process")))

			processSpan := findSpan(exporter.GetSpans(), "events.test process")
			Expect(processSpan).ToNot(BeNil())
			Expect(processSpan.Status.Code).To(Equal(codes.Error))
		})

		It("process span is ended when Ack ultimately fails", func(ctx context.Context) {
			// Build a client whose NATS connection we control so we can
			// break it and force msg.Ack() to fail deterministically.
			natsConn := GetNATS()
			js, err := jetstream.New(natsConn)
			Expect(err).ToNot(HaveOccurred())
			logger := slog.New(slog.NewTextHandler(GinkgoWriter, &slog.HandlerOptions{Level: slog.LevelError}))
			client := internalevents.New(js, logger)

			_, err = client.EnsureStream(ctx, "events", streams.WithSubjects("events.>"))
			Expect(err).ToNot(HaveOccurred())

			sub, err := client.EnsureConsumer(ctx, "events")
			Expect(err).ToNot(HaveOccurred())

			ec, err := client.Subscribe(ctx, "events", sub.Name())
			Expect(err).ToNot(HaveOccurred())

			_, err = client.Publish(ctx, "events.test", &emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())

			select {
			case event := <-ec:
				Expect(event).ToNot(BeNil())

				// Break the connection so the ack round-trip fails, then
				// Ack with no retry: backoff.Run returns the error and
				// Ack must still end the process span (the failure path).
				natsConn.Close()
				err = event.Ack(ctx, events.WithNoRetry())
				Expect(err).To(HaveOccurred())
			case <-time.After(500 * time.Millisecond):
				Fail("no event received")
			}

			Eventually(func() tracetest.SpanStubs {
				return exporter.GetSpans()
			}).Should(ContainElement(HaveField("Name", "events.test process")))

			processSpan := findSpan(exporter.GetSpans(), "events.test process")
			Expect(processSpan).ToNot(BeNil())
			Expect(processSpan.Status.Code).To(Equal(codes.Error))
		})
	})
})
