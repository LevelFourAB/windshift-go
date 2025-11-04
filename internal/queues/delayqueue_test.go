package queues_test

import (
	"context"
	"strconv"
	"time"

	"github.com/levelfourab/windshift-go/internal/queues"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("DelayQueue", func() {
	It("should deliver items after the specified delay", func(ctx context.Context) {
		dq := queues.NewDelayQueue[string](ctx)
		dq.Add("item1", 100*time.Millisecond)
		dq.Add("item2", 100*time.Millisecond)

		Expect(<-dq.Items).To(Equal("item1"))
		Expect(<-dq.Items).To(Equal("item2"))
	})

	It("should deliver items in order of their delay, regardless of add order", func(ctx context.Context) {
		dq := queues.NewDelayQueue[string](ctx)
		start := time.Now()

		dq.Add("Item 1", 300*time.Millisecond)
		time.Sleep(10 * time.Millisecond)
		dq.Add("Item 2", 100*time.Millisecond)
		time.Sleep(10 * time.Millisecond)

		item1 := <-dq.Items
		item1Delay := time.Since(start)
		Expect(item1).To(Equal("Item 2"))
		Expect(item1Delay).To(BeNumerically("~", 100*time.Millisecond, 50*time.Millisecond))

		item2 := <-dq.Items
		item2Delay := time.Since(start)
		Expect(item2).To(Equal("Item 1"))
		Expect(item2Delay).To(BeNumerically("~", 300*time.Millisecond, 50*time.Millisecond))
	})

	It("should handle multiple items with varied delays correctly", func(ctx context.Context) {
		dq := queues.NewDelayQueue[string](ctx)
		start := time.Now()

		dq.Add("Item 3", 300*time.Millisecond)
		time.Sleep(10 * time.Millisecond)
		dq.Add("Item 1", 100*time.Millisecond)
		time.Sleep(10 * time.Millisecond)
		dq.Add("Item 4", 400*time.Millisecond)
		time.Sleep(10 * time.Millisecond)
		dq.Add("Item 2", 200*time.Millisecond)

		expectedItems := []string{"Item 1", "Item 2", "Item 3", "Item 4"}
		expectedDelays := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 300 * time.Millisecond, 400 * time.Millisecond}

		for i := 0; i < 4; i++ {
			item := <-dq.Items
			itemDelay := time.Since(start)
			Expect(item).To(Equal(expectedItems[i]))
			Expect(itemDelay).To(BeNumerically("~", expectedDelays[i], 50*time.Millisecond))
		}
	})

	It("should stop delivering items when context is canceled", func(ctx context.Context) {
		ctx, cancel := context.WithCancel(ctx)
		dq := queues.NewDelayQueue[string](ctx)
		dq.Add("item1", 100*time.Millisecond)
		dq.Add("item2", 1*time.Hour) // This item should not be delivered

		Expect(<-dq.Items).To(Equal("item1"))

		cancel()

		_, ok := <-dq.Items
		Expect(ok).To(BeFalse())
	})

	It("should handle many items efficiently", func(ctx context.Context) {
		dq := queues.NewDelayQueue[string](ctx)

		itemCount := 1000
		for i := 0; i < itemCount; i++ {
			dq.Add(strconv.Itoa(i), time.Duration(i)*time.Millisecond)
		}

		receivedCount := 0
		for range dq.Items {
			receivedCount++
			if receivedCount == itemCount {
				break
			}
		}

		Expect(receivedCount).To(Equal(itemCount))
	})

	It("should remove items correctly", func(ctx context.Context) {
		dq := queues.NewDelayQueue[string](ctx)

		dq.Add("item1", 200*time.Millisecond)
		id2 := dq.Add("item2", 100*time.Millisecond)
		dq.Add("item3", 300*time.Millisecond)

		Expect(dq.Remove(id2)).To(BeTrue())
		Expect(dq.Remove(id2)).To(BeFalse()) // Already removed

		Expect(<-dq.Items).To(Equal("item1"))
		Expect(<-dq.Items).To(Equal("item3"))
	})

	It("should not leak timers when context is canceled", func(ctx context.Context) {
		ctx, cancel := context.WithCancel(ctx)
		dq := queues.NewDelayQueue[string](ctx)

		// Add items with long delays to ensure timers are created
		dq.Add("item1", 10*time.Second)
		dq.Add("item2", 20*time.Second)

		// Give the queue time to create the timer
		time.Sleep(50 * time.Millisecond)

		// Cancel the context - the timer should be cleaned up
		cancel()

		// Wait a bit and verify the channel is closed
		time.Sleep(100 * time.Millisecond)
		_, ok := <-dq.Items
		Expect(ok).To(BeFalse(), "Items channel should be closed")
	})

	It("should handle removal of the earliest item correctly", func(ctx context.Context) {
		dq := queues.NewDelayQueue[string](ctx)
		start := time.Now()

		// Add item with 1 second delay - it will be the earliest
		idEarliest := dq.Add("earliest", 1*time.Second)
		// Add item with 2 second delay - it will be second
		dq.Add("second", 2*time.Second)

		// Remove the earliest item almost immediately
		time.Sleep(10 * time.Millisecond)
		Expect(dq.Remove(idEarliest)).To(BeTrue())

		// Try to receive the second item
		item := <-dq.Items
		Expect(item).To(Equal("second"))

		// The queue should have waited for around 2 seconds for the second item
		elapsed := time.Since(start)
		Expect(elapsed).To(BeNumerically("~", 2*time.Second, 200*time.Millisecond))
	})

	It("should handle context cancellation during item delivery gracefully", func(ctx context.Context) {
		ctx, cancel := context.WithCancel(ctx)
		dq := queues.NewDelayQueue[string](ctx)

		// Add items with very short delay
		dq.Add("item1", 10*time.Millisecond)
		dq.Add("item2", 20*time.Millisecond)

		// Receive first item
		Expect(<-dq.Items).To(Equal("item1"))

		// Cancel context before receiving the second item
		cancel()

		// The channel should close after the cancel
		time.Sleep(100 * time.Millisecond)
		_, ok := <-dq.Items
		Expect(ok).To(BeFalse(), "Items channel should be closed")
	})

	It("should handle concurrent adds and removes safely", func(ctx context.Context) {
		dq := queues.NewDelayQueue[string](ctx)

		// Start goroutines that add items concurrently
		ids := make([]uint, 100)
		for i := 0; i < 100; i++ {
			i := i // capture
			go func() {
				ids[i] = dq.Add(strconv.Itoa(i), time.Duration(i)*time.Millisecond)
			}()
		}

		// Give time for adds to complete
		time.Sleep(50 * time.Millisecond)

		// Remove some items concurrently
		for i := 0; i < 50; i++ {
			i := i // capture
			go func() {
				dq.Remove(ids[i*2])
			}()
		}

		// Count how many items we receive
		receivedCount := 0
		timeout := time.After(2 * time.Second)
		for {
			select {
			case _, ok := <-dq.Items:
				if !ok {
					return
				}
				receivedCount++
				if receivedCount >= 50 { // We should get at least 50 items
					return
				}
			case <-timeout:
				Expect(receivedCount).To(BeNumerically(">=", 50))
				return
			}
		}
	})

	It("should handle rapid queue wake-ups correctly", func(ctx context.Context) {
		dq := queues.NewDelayQueue[string](ctx)
		start := time.Now()

		// Add item with long delay
		dq.Add("long", 1*time.Second)

		// Rapidly add items with shorter delays to trigger wake-ups and timer resets
		for i := 0; i < 10; i++ {
			time.Sleep(10 * time.Millisecond)
			dq.Add(strconv.Itoa(i), 100*time.Millisecond)
		}

		// Should receive the short-delay items first
		for i := 0; i < 10; i++ {
			item := <-dq.Items
			elapsed := time.Since(start)
			// These should arrive around 100-200ms, not after 1 second
			Expect(elapsed).To(BeNumerically("<", 500*time.Millisecond))
			Expect(item).To(Equal(strconv.Itoa(i)))
		}

		// Finally get the long delay item
		item := <-dq.Items
		elapsed := time.Since(start)
		Expect(item).To(Equal("long"))
		Expect(elapsed).To(BeNumerically("~", 1*time.Second, 200*time.Millisecond))
	})

	It("should handle ID generation correctly even with many items", func(ctx context.Context) {
		dq := queues.NewDelayQueue[string](ctx)

		// Add and immediately remove many items to increase nextID
		ids := make(map[uint]bool)
		for i := 0; i < 10000; i++ {
			id := dq.Add(strconv.Itoa(i), 1*time.Hour) // Long delay so they don't get delivered
			Expect(ids[id]).To(BeFalse(), "IDs should be unique")
			ids[id] = true
			dq.Remove(id)
		}

		// Verify all IDs were unique
		Expect(len(ids)).To(Equal(10000))
	})
})
