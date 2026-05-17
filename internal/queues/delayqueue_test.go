package queues_test

import (
	"context"
	"strconv"
	"sync"
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

		// Start goroutines that add items concurrently. Each goroutine writes
		// only its own slot, and the WaitGroup establishes a happens-before
		// edge so the removers below can read the ids without a data race.
		ids := make([]uint, 100)
		var addWg sync.WaitGroup
		addWg.Add(100)
		for i := 0; i < 100; i++ {
			i := i // capture
			go func() {
				defer addWg.Done()
				ids[i] = dq.Add(strconv.Itoa(i), time.Duration(i)*time.Millisecond)
			}()
		}
		addWg.Wait()

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

	It("should not lose the wakeup when an item is added right after creation", func(ctx context.Context) {
		// Targets the lost-wakeup race in the empty-queue window: run() does
		// lock/len==0/unlock and then selects on wakeChan. An Add that lands
		// between the unlock and the select must not be lost, otherwise the
		// item is never delivered. Many iterations with a goroutine racing the
		// freshly started run() maximize the chance of hitting the window.
		for i := 0; i < 300; i++ {
			iterCtx, cancel := context.WithCancel(ctx)
			dq := queues.NewDelayQueue[string](iterCtx)

			go dq.Add("item", 1*time.Millisecond)

			select {
			case v := <-dq.Items:
				Expect(v).To(Equal("item"))
			case <-time.After(2 * time.Second):
				cancel()
				Fail("item was never delivered - wakeup was lost on iteration " + strconv.Itoa(i))
			}

			cancel()
		}
	})

	It("should not lose the wakeup when a new earliest item races the timer window", func(ctx context.Context) {
		// Targets the lost-wakeup race in the timer window: run() computes a
		// timer for the current head, unlocks, then selects. An Add of a much
		// earlier item that lands in that gap must not be lost, otherwise the
		// earlier item is delivered as late as the original head's delay.
		for i := 0; i < 300; i++ {
			iterCtx, cancel := context.WithCancel(ctx)
			dq := queues.NewDelayQueue[string](iterCtx)

			// Establish a far-away head and let run() park on its timer.
			dq.Add("late", 10*time.Second)
			time.Sleep(2 * time.Millisecond)

			// Repeatedly churn the head so run() loops through the
			// unlock->select window while a goroutine slips in an earlier item.
			start := time.Now()
			go dq.Add("early", 20*time.Millisecond)
			for j := 0; j < 5; j++ {
				id := dq.Add("churn"+strconv.Itoa(j), 5*time.Second)
				dq.Remove(id)
			}

			select {
			case v := <-dq.Items:
				Expect(v).To(Equal("early"))
				Expect(time.Since(start)).To(BeNumerically("<", 1*time.Second),
					"earliest item delivered late - wakeup was lost on iteration "+strconv.Itoa(i))
			case <-time.After(2 * time.Second):
				cancel()
				Fail("earliest item was never delivered on iteration " + strconv.Itoa(i))
			}

			cancel()
		}
	})

	It("should not spin on a stale timer tick after a wakeup", func(ctx context.Context) {
		// After the select exits via wakeChan, the old timer may fire and leave
		// a stale value in timer.C. A correct implementation drains it before
		// Reset so the next item still waits roughly its full delay rather than
		// being woken immediately by the stale tick.
		dq := queues.NewDelayQueue[string](ctx)

		dq.Add("head", 300*time.Millisecond)
		time.Sleep(50 * time.Millisecond)

		// New earliest item; run() wakes via wakeChan while the head timer is
		// still pending. The stale head tick (if not drained) must not cause
		// "earlier" to be delivered before its own 100ms delay elapses.
		start := time.Now()
		dq.Add("earlier", 100*time.Millisecond)

		Expect(<-dq.Items).To(Equal("earlier"))
		Expect(time.Since(start)).To(BeNumerically("~", 100*time.Millisecond, 50*time.Millisecond))
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
