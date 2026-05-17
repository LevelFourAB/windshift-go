package events_test

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/levelfourab/windshift-go/delays"
	"github.com/levelfourab/windshift-go/events"
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

// subscribe wraps Client.Subscribe and returns just the event channel, for
// the many call sites that only consume events and never drive the
// subscription's lifecycle. Tests that need to Drain or Stop call
// Client.Subscribe directly to get the Subscription handle.
func subscribe(c events.Client, ctx context.Context, consumer string, opts ...events.SubscribeOption) (<-chan events.Event, error) {
	sub, err := c.Subscribe(ctx, "events", consumer, opts...)
	if err != nil {
		return nil, err
	}
	return sub.Events(), nil
}

var _ = Describe("Event Consumption", func() {
	var manager events.Client

	BeforeEach(func() {
		manager, _ = createClientAndJetStream()

		_, err := manager.EnsureStream(context.Background(), "events", events.WithSubjects("events.>"))
		Expect(err).ToNot(HaveOccurred())
	})

	Describe("Ephemeral consumption", func() {
		It("can create", func(ctx context.Context) {
			sub, err := manager.EnsureConsumer(ctx, "events", events.WithSubjects("events.>"))
			Expect(err).ToNot(HaveOccurred())

			_, err = subscribe(manager, ctx, sub.Name())
			Expect(err).ToNot(HaveOccurred())
		})

		It("can receive events with subject specified", func(ctx context.Context) {
			sub, err := manager.EnsureConsumer(ctx, "events", events.WithSubjects("events.>"))
			Expect(err).ToNot(HaveOccurred())

			ec, err := subscribe(manager, ctx, sub.Name())
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

			ec, err := subscribe(manager, ctx, sub.Name())
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

			ec1, err := subscribe(manager, ctx, sub1.Name())
			Expect(err).ToNot(HaveOccurred())

			ec2, err := subscribe(manager, ctx, sub2.Name())
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

			ec, err := subscribe(manager, ctx, sub.Name())
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

			sub, err := manager.EnsureConsumer(ctx, "events", events.WithConsumeFrom(events.AtStreamStart()))
			Expect(err).ToNot(HaveOccurred())

			ec, err := subscribe(manager, ctx, sub.Name())
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
			_, err := manager.EnsureConsumer(ctx, "events", events.WithName("test"))
			Expect(err).ToNot(HaveOccurred())

			_, err = subscribe(manager, ctx, "test")
			Expect(err).ToNot(HaveOccurred())
		})

		It("can receive events", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, "events", events.WithName("test"))
			Expect(err).ToNot(HaveOccurred())

			ec, err := subscribe(manager, ctx, "test")
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
			_, err := manager.EnsureConsumer(ctx, "events", events.WithName("test"))
			Expect(err).ToNot(HaveOccurred())

			ec1, err := subscribe(manager, ctx, "test")
			Expect(err).ToNot(HaveOccurred())

			ec2, err := subscribe(manager, ctx, "test")
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
			_, err := manager.EnsureConsumer(ctx, "events", events.WithName("test1"))
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.EnsureConsumer(ctx, "events", events.WithName("test2"))
			Expect(err).ToNot(HaveOccurred())

			ec1, err := subscribe(manager, ctx, "test1")
			Expect(err).ToNot(HaveOccurred())

			ec2, err := subscribe(manager, ctx, "test2")
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

		It("Stop stops receiving events", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, "events", events.WithName("test"))
			Expect(err).ToNot(HaveOccurred())

			sub, err := manager.Subscribe(ctx, "events", "test")
			Expect(err).ToNot(HaveOccurred())
			ec := sub.Events()

			sub.Stop()
			time.Sleep(50 * time.Millisecond)

			_, err = manager.Publish(ctx, "events.test", &emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())

			// The channel is closed and no event is delivered.
			select {
			case _, ok := <-ec:
				if ok {
					Fail("event received after stop")
				}
			case <-time.After(200 * time.Millisecond):
			}
		})

		It("Stop closes the channel", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, "events", events.WithName("test"))
			Expect(err).ToNot(HaveOccurred())

			sub, err := manager.Subscribe(ctx, "events", "test")
			Expect(err).ToNot(HaveOccurred())
			ec := sub.Events()

			sub.Stop()

			// Ranging over the channel must terminate, so it has to be
			// closed once the subscription is stopped.
			done := make(chan struct{})
			go func() {
				defer close(done)
				for range ec {
				}
			}()

			select {
			case <-done:
			case <-time.After(2 * time.Second):
				Fail("channel was not closed after Stop")
			}
		})

		It("Drain closes the channel", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, "events", events.WithName("test"))
			Expect(err).ToNot(HaveOccurred())

			sub, err := manager.Subscribe(ctx, "events", "test")
			Expect(err).ToNot(HaveOccurred())
			ec := sub.Events()

			drainCtx, drainCancel := context.WithTimeout(ctx, 2*time.Second)
			defer drainCancel()
			Expect(sub.Drain(drainCtx)).ToNot(HaveOccurred())

			done := make(chan struct{})
			go func() {
				defer close(done)
				for range ec {
				}
			}()

			select {
			case <-done:
			case <-time.After(2 * time.Second):
				Fail("channel was not closed after Drain")
			}
		})

		It("Stop unblocks a blocked send and closes the channel without panicking", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, "events", events.WithName("test"))
			Expect(err).ToNot(HaveOccurred())

			sub, err := manager.Subscribe(ctx, "events", "test")
			Expect(err).ToNot(HaveOccurred())
			ec := sub.Events()

			// Publish events but never read from the channel, so the
			// internal send blocks. Stop must unblock it and close the
			// channel without a send-on-closed-channel panic.
			for i := 0; i < 10; i++ {
				_, err = manager.Publish(ctx, "events.test", &emptypb.Empty{})
				Expect(err).ToNot(HaveOccurred())
			}

			time.Sleep(100 * time.Millisecond)
			sub.Stop()

			done := make(chan struct{})
			go func() {
				defer close(done)
				for range ec {
				}
			}()

			select {
			case <-done:
			case <-time.After(2 * time.Second):
				Fail("channel was not closed after Stop")
			}
		})

		It("acknowledging event stops delivery", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, "events",
				events.WithName("test"),
				events.WithProcessingTimeout(100*time.Millisecond),
			)
			Expect(err).ToNot(HaveOccurred())

			ec, err := subscribe(manager, ctx, "test")
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
				events.WithName("test"),
				events.WithProcessingTimeout(100*time.Millisecond),
			)
			Expect(err).ToNot(HaveOccurred())

			ec, err := subscribe(manager, ctx, "test")
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
				events.WithName("test"),
				events.WithProcessingTimeout(time.Second),
			)
			Expect(err).ToNot(HaveOccurred())

			// Use a long retry backoff so that, if Reject did not treat an
			// already-finalized message as a permanent outcome, the second
			// Reject would block on retries for several seconds before
			// returning.
			ec, err := subscribe(manager, ctx, "test",
				events.WithDefaultRetryBackoff(delays.Constant(5*time.Second)))
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
				events.WithName("test"),
				events.WithProcessingTimeout(100*time.Millisecond),
			)
			Expect(err).ToNot(HaveOccurred())

			sub1, err := manager.Subscribe(ctx, "events", "test")
			Expect(err).ToNot(HaveOccurred())
			ec1 := sub1.Events()

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

			// Stop the first subscription so the rejected event is
			// redelivered to the second instance rather than back here.
			sub1.Stop()

			ec2, err := subscribe(manager, ctx, "test")
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
			_, err := manager.EnsureConsumer(ctx, "events", events.WithName("test"))
			Expect(err).ToNot(HaveOccurred())

			ec, err := subscribe(manager, ctx, "test")
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
			_, err := manager.EnsureConsumer(ctx, "events", events.WithName("test"))
			Expect(err).ToNot(HaveOccurred())

			ec, err := subscribe(manager, ctx, "test")
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
				events.WithName("test"),
				events.WithMaxDeliveryAttempts(1),
			)
			Expect(err).ToNot(HaveOccurred())

			ec, err := subscribe(manager, ctx, "test")
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
				events.WithName("test"),
				events.WithProcessingTimeout(200*time.Millisecond),
			)
			Expect(err).ToNot(HaveOccurred())

			ec, err := subscribe(manager, ctx, "test", events.WithAutoPingInterval(50*time.Millisecond))
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
				events.WithName("test"),
				events.WithProcessingTimeout(300*time.Millisecond),
			)
			Expect(err).ToNot(HaveOccurred())

			ec, err := subscribe(manager, ctx, "test",
				events.WithAutoPingInterval(25*time.Millisecond))
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
				events.WithName("test"),
				events.WithProcessingTimeout(200*time.Millisecond),
			)
			Expect(err).ToNot(HaveOccurred())

			ec, err := subscribe(manager, ctx, "test", events.WithoutAutoPing())
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
				events.WithName("test"),
				events.WithProcessingTimeout(100*time.Millisecond),
			)
			Expect(err).ToNot(HaveOccurred())

			ec, err := subscribe(manager, ctx, "test", events.WithoutAutoPing())
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

	Describe("Draining", func() {
		It("waits for an in-flight event then closes the channel and returns nil once it is acked", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, "events",
				events.WithName("test"),
				events.WithProcessingTimeout(5*time.Second),
			)
			Expect(err).ToNot(HaveOccurred())

			sub, err := manager.Subscribe(ctx, "events", "test")
			Expect(err).ToNot(HaveOccurred())
			ec := sub.Events()

			_, err = manager.Publish(ctx, "events.test", &emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())

			var event events.Event
			select {
			case event = <-ec:
				Expect(event).ToNot(BeNil())
			case <-time.After(500 * time.Millisecond):
				Fail("no event received")
			}

			drainResult := make(chan error, 1)
			go func() {
				drainCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				drainResult <- sub.Drain(drainCtx)
			}()

			// Drain must block while the event is still in-flight.
			select {
			case <-drainResult:
				Fail("Drain returned before the in-flight event was settled")
			case <-time.After(200 * time.Millisecond):
			}

			// Settle with a context that outlives the drain.
			Expect(event.Ack(context.Background())).ToNot(HaveOccurred())

			select {
			case err := <-drainResult:
				Expect(err).ToNot(HaveOccurred())
			case <-time.After(2 * time.Second):
				Fail("Drain did not return after the event was acked")
			}

			// The channel must be closed once Drain completes.
			_, ok := <-ec
			Expect(ok).To(BeFalse())
		})

		It("returns a wrapped context error when an event is never settled before the deadline", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, "events",
				events.WithName("test"),
				events.WithProcessingTimeout(500*time.Millisecond),
			)
			Expect(err).ToNot(HaveOccurred())

			sub, err := manager.Subscribe(ctx, "events", "test", events.WithoutAutoPing())
			Expect(err).ToNot(HaveOccurred())
			ec := sub.Events()

			_, err = manager.Publish(ctx, "events.test", &emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())

			select {
			case event := <-ec:
				Expect(event).ToNot(BeNil())
			case <-time.After(500 * time.Millisecond):
				Fail("no event received")
			}

			// Never settle the event; the drain must time out.
			drainCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
			defer cancel()
			err = sub.Drain(drainCtx)
			Expect(err).To(HaveOccurred())
			Expect(errors.Is(err, context.DeadlineExceeded)).To(BeTrue())

			// The channel must still be closed on a timed-out drain.
			_, ok := <-ec
			Expect(ok).To(BeFalse())

			// The unsettled event is redelivered to a fresh subscription.
			sub2, err := manager.Subscribe(ctx, "events", "test", events.WithoutAutoPing())
			Expect(err).ToNot(HaveOccurred())
			select {
			case event := <-sub2.Events():
				Expect(event).ToNot(BeNil())
				Expect(event.DeliveryAttempt()).To(BeNumerically(">=", 2))
				Expect(event.Ack(context.Background())).ToNot(HaveOccurred())
			case <-time.After(3 * time.Second):
				Fail("unsettled event was not redelivered")
			}
		})

		It("Stop closes the channel immediately, abandons the in-flight event, and is idempotent", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, "events",
				events.WithName("test"),
				events.WithProcessingTimeout(300*time.Millisecond),
			)
			Expect(err).ToNot(HaveOccurred())

			sub, err := manager.Subscribe(ctx, "events", "test", events.WithoutAutoPing())
			Expect(err).ToNot(HaveOccurred())
			ec := sub.Events()

			_, err = manager.Publish(ctx, "events.test", &emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())

			select {
			case event := <-ec:
				Expect(event).ToNot(BeNil())
			case <-time.After(500 * time.Millisecond):
				Fail("no event received")
			}

			sub.Stop()
			// Idempotent: a second Stop must not panic.
			sub.Stop()

			Eventually(func() bool {
				_, ok := <-ec
				return ok
			}).Should(BeFalse())

			// The abandoned event is redelivered to a fresh subscription.
			sub2, err := manager.Subscribe(ctx, "events", "test", events.WithoutAutoPing())
			Expect(err).ToNot(HaveOccurred())
			select {
			case event := <-sub2.Events():
				Expect(event).ToNot(BeNil())
				Expect(event.DeliveryAttempt()).To(BeNumerically(">=", 2))
				Expect(event.Ack(context.Background())).ToNot(HaveOccurred())
			case <-time.After(3 * time.Second):
				Fail("abandoned event was not redelivered")
			}
		})

		It("Stop during an in-progress Drain makes Drain return ErrSubscriptionStopped", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, "events",
				events.WithName("test"),
				events.WithProcessingTimeout(5*time.Second),
			)
			Expect(err).ToNot(HaveOccurred())

			sub, err := manager.Subscribe(ctx, "events", "test")
			Expect(err).ToNot(HaveOccurred())
			ec := sub.Events()

			_, err = manager.Publish(ctx, "events.test", &emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())

			select {
			case event := <-ec:
				Expect(event).ToNot(BeNil())
			case <-time.After(500 * time.Millisecond):
				Fail("no event received")
			}

			drainResult := make(chan error, 1)
			go func() {
				drainCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				drainResult <- sub.Drain(drainCtx)
			}()

			// Let Drain start and block on the in-flight event.
			select {
			case <-drainResult:
				Fail("Drain returned before being preempted")
			case <-time.After(200 * time.Millisecond):
			}

			sub.Stop()

			select {
			case err := <-drainResult:
				Expect(errors.Is(err, events.ErrSubscriptionStopped)).To(BeTrue())
			case <-time.After(2 * time.Second):
				Fail("Drain did not return after Stop")
			}
		})

		It("is idempotent and safe under concurrent callers", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, "events",
				events.WithName("test"),
				events.WithProcessingTimeout(5*time.Second),
			)
			Expect(err).ToNot(HaveOccurred())

			sub, err := manager.Subscribe(ctx, "events", "test")
			Expect(err).ToNot(HaveOccurred())
			ec := sub.Events()

			_, err = manager.Publish(ctx, "events.test", &emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())

			select {
			case event := <-ec:
				Expect(event).ToNot(BeNil())
				Expect(event.Ack(context.Background())).ToNot(HaveOccurred())
			case <-time.After(500 * time.Millisecond):
				Fail("no event received")
			}

			const callers = 5
			results := make(chan error, callers)
			for i := 0; i < callers; i++ {
				go func() {
					drainCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					results <- sub.Drain(drainCtx)
				}()
			}

			for i := 0; i < callers; i++ {
				select {
				case err := <-results:
					Expect(err).ToNot(HaveOccurred())
				case <-time.After(3 * time.Second):
					Fail("concurrent Drain caller did not return")
				}
			}

			// A subsequent Drain returns the same stored result.
			Expect(sub.Drain(ctx)).ToNot(HaveOccurred())
		})

		It("auto-ping keeps an outstanding event alive during a slow drain", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, "events",
				events.WithName("test"),
				events.WithProcessingTimeout(200*time.Millisecond),
			)
			Expect(err).ToNot(HaveOccurred())

			sub, err := manager.Subscribe(ctx, "events", "test",
				events.WithAutoPingInterval(50*time.Millisecond))
			Expect(err).ToNot(HaveOccurred())
			ec := sub.Events()

			// A second subscription on the same durable consumer would
			// receive the event if it were redelivered during the drain.
			sub2, err := manager.Subscribe(ctx, "events", "test", events.WithoutAutoPing())
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.Publish(ctx, "events.test", &emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())

			var event events.Event
			select {
			case event = <-ec:
				Expect(event).ToNot(BeNil())
			case <-time.After(500 * time.Millisecond):
				Fail("no event received")
			}

			drainResult := make(chan error, 1)
			go func() {
				drainCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				drainResult <- sub.Drain(drainCtx)
			}()

			// Hold the event well past the processing timeout. Auto-ping
			// must keep it alive so it is not redelivered to sub2.
			select {
			case e := <-sub2.Events():
				Fail("event was redelivered during the drain (auto-ping did not keep it alive): " +
					strconv.FormatUint(e.ID(), 10))
			case <-time.After(600 * time.Millisecond):
			}

			Expect(event.Ack(context.Background())).ToNot(HaveOccurred())

			select {
			case err := <-drainResult:
				Expect(err).ToNot(HaveOccurred())
			case <-time.After(2 * time.Second):
				Fail("Drain did not return after the event was acked")
			}
		})

		It("abandons prefetched events by default but flushes them with WithDrainPrefetched", func(ctx context.Context) {
			By("abandoning prefetched events by default")
			_, err := manager.EnsureConsumer(ctx, "events",
				events.WithName("abandon"),
				events.WithProcessingTimeout(500*time.Millisecond),
			)
			Expect(err).ToNot(HaveOccurred())

			sub, err := manager.Subscribe(ctx, "events", "abandon",
				events.WithPrefetch(10), events.WithoutAutoPing())
			Expect(err).ToNot(HaveOccurred())
			ec := sub.Events()

			for i := 0; i < 3; i++ {
				_, err = manager.Publish(ctx, "events.test", &emptypb.Empty{})
				Expect(err).ToNot(HaveOccurred())
			}

			// Receive and settle the first event; the rest are prefetched
			// but not yet delivered (the handler is blocked on the send).
			var first events.Event
			select {
			case first = <-ec:
				Expect(first).ToNot(BeNil())
			case <-time.After(500 * time.Millisecond):
				Fail("no event received")
			}
			Expect(first.Ack(context.Background())).ToNot(HaveOccurred())

			drainCtx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			Expect(sub.Drain(drainCtx)).ToNot(HaveOccurred())

			// The prefetched, undelivered events were abandoned and are
			// redelivered to a fresh subscription.
			sub2, err := manager.Subscribe(ctx, "events", "abandon", events.WithoutAutoPing())
			Expect(err).ToNot(HaveOccurred())
			received := 0
			for received < 2 {
				select {
				case event := <-sub2.Events():
					Expect(event).ToNot(BeNil())
					Expect(event.Ack(context.Background())).ToNot(HaveOccurred())
					received++
				case <-time.After(3 * time.Second):
					Fail("abandoned prefetched events were not redelivered")
				}
			}

			By("flushing prefetched events with WithDrainPrefetched")
			_, err = manager.EnsureConsumer(ctx, "events",
				events.WithName("flush"),
				events.WithProcessingTimeout(2*time.Second),
			)
			Expect(err).ToNot(HaveOccurred())

			flushSub, err := manager.Subscribe(ctx, "events", "flush",
				events.WithPrefetch(10), events.WithoutAutoPing(),
				events.WithDrainPrefetched())
			Expect(err).ToNot(HaveOccurred())
			fec := flushSub.Events()

			for i := 0; i < 3; i++ {
				_, err = manager.Publish(ctx, "events.flush", &emptypb.Empty{})
				Expect(err).ToNot(HaveOccurred())
			}

			var firstFlush events.Event
			select {
			case firstFlush = <-fec:
				Expect(firstFlush).ToNot(BeNil())
			case <-time.After(500 * time.Millisecond):
				Fail("no event received")
			}

			drainResult := make(chan error, 1)
			go func() {
				dctx, c := context.WithTimeout(context.Background(), 5*time.Second)
				defer c()
				drainResult <- flushSub.Drain(dctx)
			}()

			// Receive the first event plus the two prefetched events
			// through the channel during the drain window before settling
			// any of them. Keeping at least one delivered-but-unsettled
			// event outstanding at all times ensures the internal pending
			// count never momentarily reaches zero between buffered
			// callbacks (the documented best-effort window), so all three
			// are reliably flushed here.
			flushed := []events.Event{firstFlush}
			for len(flushed) < 3 {
				select {
				case event := <-fec:
					if event == nil {
						Fail("channel closed before all prefetched events were flushed")
					}
					flushed = append(flushed, event)
				case <-time.After(3 * time.Second):
					Fail("prefetched events were not flushed during the drain")
				}
			}
			for _, event := range flushed {
				Expect(event.Ack(context.Background())).ToNot(HaveOccurred())
			}

			select {
			case err := <-drainResult:
				Expect(err).ToNot(HaveOccurred())
			case <-time.After(2 * time.Second):
				Fail("Drain did not return after prefetched events were flushed")
			}
		})

		It("canceling the Subscribe context does not stop the subscription or close the channel", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, "events", events.WithName("test"))
			Expect(err).ToNot(HaveOccurred())

			subCtx, cancel := context.WithCancel(ctx)
			sub, err := manager.Subscribe(subCtx, "events", "test")
			Expect(err).ToNot(HaveOccurred())
			ec := sub.Events()

			// Canceling the setup context must not tear down the
			// subscription nor close the channel.
			cancel()

			_, err = manager.Publish(ctx, "events.test", &emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())

			select {
			case event, ok := <-ec:
				Expect(ok).To(BeTrue(), "channel must not be closed by Subscribe ctx cancellation")
				Expect(event).ToNot(BeNil())
				Expect(event.Ack(context.Background())).ToNot(HaveOccurred())
			case <-time.After(2 * time.Second):
				Fail("event was not delivered after Subscribe ctx cancellation")
			}

			sub.Stop()
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

			ec, err := subscribe(manager, ctx, sub.Name())
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
			consumer, err := manager.EnsureConsumer(ctx, "events")
			Expect(err).ToNot(HaveOccurred())

			sub, err := manager.Subscribe(ctx, "events", consumer.Name())
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.Publish(ctx, "events.test", &emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())

			// Give the message time to be delivered to the subscription's
			// handler, where it blocks on the channel send because nothing
			// consumes the channel. Then Stop so the event is dropped
			// before delivery (closed-before-delivery drop path).
			time.Sleep(150 * time.Millisecond)
			sub.Stop()

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

			_, err = client.EnsureStream(ctx, "events", events.WithSubjects("events.>"))
			Expect(err).ToNot(HaveOccurred())

			sub, err := client.EnsureConsumer(ctx, "events")
			Expect(err).ToNot(HaveOccurred())

			ec, err := subscribe(client, ctx, sub.Name())
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
