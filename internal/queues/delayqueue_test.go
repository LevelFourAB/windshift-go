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
})
