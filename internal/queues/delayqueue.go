package queues

import (
	"container/heap"
	"context"
	"sync"
	"time"
)

type delayedItem[T any] struct {
	value   T
	readyAt time.Time
	index   int
	id      uint
}

type timePriorityQueue[T any] []*delayedItem[T]

func (pq timePriorityQueue[T]) Len() int { return len(pq) }

func (pq timePriorityQueue[T]) Less(i, j int) bool {
	return pq[i].readyAt.Before(pq[j].readyAt)
}

func (pq timePriorityQueue[T]) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *timePriorityQueue[T]) Push(x interface{}) {
	n := len(*pq)
	item := x.(*delayedItem[T])
	item.index = n
	*pq = append(*pq, item)
}

func (pq *timePriorityQueue[T]) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

// DelayQueue is a queue where items can only be pulled after a certain delay.
type DelayQueue[T any] struct {
	pq       timePriorityQueue[T]
	mu       sync.Mutex
	wakeChan chan struct{}
	nextID   uint

	Items <-chan T
}

func NewDelayQueue[T any](ctx context.Context) *DelayQueue[T] {
	itemsChan := make(chan T)

	res := &DelayQueue[T]{
		pq:       make(timePriorityQueue[T], 0),
		wakeChan: make(chan struct{}),
		Items:    itemsChan,
	}
	heap.Init(&res.pq)

	go res.run(ctx, itemsChan)
	return res
}

func (dq *DelayQueue[T]) Add(value T, delay time.Duration) uint {
	readyAt := time.Now().Add(delay)

	dq.mu.Lock()
	defer dq.mu.Unlock()

	item := &delayedItem[T]{
		value:   value,
		readyAt: readyAt,
		id:      dq.nextID,
	}
	dq.nextID++

	heap.Push(&dq.pq, item)
	if dq.pq[0] == item {
		// If this is the new earliest item, wake up the queue if we can
		select {
		case dq.wakeChan <- struct{}{}:
		default:
		}
	}

	return item.id
}

func (dq *DelayQueue[T]) Remove(id uint) bool {
	dq.mu.Lock()
	defer dq.mu.Unlock()

	for i, item := range dq.pq {
		if item.id == id {
			wasFirst := i == 0
			heap.Remove(&dq.pq, i)

			// If we removed the earliest item and there are still items left,
			// wake the queue so it recalculates the timer for the new earliest item
			if wasFirst && dq.pq.Len() > 0 {
				select {
				case dq.wakeChan <- struct{}{}:
				default:
				}
			}

			return true
		}
	}

	return false
}

func (dq *DelayQueue[T]) run(ctx context.Context, itemsChan chan<- T) {
	defer close(itemsChan)

	var timer *time.Timer
	defer func() {
		// Clean up timer on exit to prevent resource leak
		if timer != nil {
			timer.Stop()
		}
	}()

	for {
		dq.mu.Lock()
		if dq.pq.Len() == 0 {
			// There are no items in the queue at all, so we have to stop and
			// wait for items.
			dq.mu.Unlock()

			select {
			case <-dq.wakeChan:
				// There's a new item in the queue, wake up and re-check
				continue
			case <-ctx.Done():
				// Context has been canceled, stop the queue
				return
			}
		}

		item := dq.pq[0]
		now := time.Now()
		if now.After(item.readyAt) {
			heap.Pop(&dq.pq)
			dq.mu.Unlock()

			select {
			case itemsChan <- item.value:
				// Item has been delivered, nothing more to do
			case <-ctx.Done():
				// Context has been canceled before delivery, stop the queue
				return
			}
		} else {
			delay := item.readyAt.Sub(now)
			dq.mu.Unlock()

			if timer == nil {
				timer = time.NewTimer(delay)
			} else {
				timer.Reset(delay)
			}

			select {
			case <-timer.C:
				// Timer has expired, proceed to deliver the item
			case <-dq.wakeChan:
				// There's a new item that is the new earliest, don't wait for
				// the timer, just go look at the next item.
			case <-ctx.Done():
				// Context has been canceled, stop the queue
				return
			}
		}
	}
}
