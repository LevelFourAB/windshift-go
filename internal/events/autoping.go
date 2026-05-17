package events

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/levelfourab/windshift-go/internal/queues"
	"github.com/nats-io/nats.go/jetstream"
)

// pingItem is what is actually stored in the DelayQueue. It carries a stable
// AutoPing-level id so that run() can correlate a delivered item back to the
// entry that external callers hold, even though the underlying DelayQueue id
// changes every time the event is requeued for the next ping.
type pingItem struct {
	id    uint
	event *Event
}

// AutoPing is a helper for automatically pinging events as they are processed.
type AutoPing struct {
	logger *slog.Logger
	queue  *queues.DelayQueue[pingItem]

	// pingInterval is the interval at which events should be pinged.
	pingInterval time.Duration

	// mu guards nextID and entries.
	mu sync.Mutex
	// nextID is the next stable AutoPing id to hand out.
	nextID uint
	// entries maps a stable AutoPing id to the current DelayQueue id. An
	// entry is present exactly while the event should keep being pinged;
	// Remove deletes it to cancel, and run() treats a missing entry as
	// "canceled" and stops requeuing.
	entries map[uint]uint64
}

func newAutoPing(ctx context.Context, logger *slog.Logger, pingInterval time.Duration) *AutoPing {
	res := &AutoPing{
		logger:       logger,
		queue:        queues.NewDelayQueue[pingItem](ctx),
		pingInterval: pingInterval,
		entries:      make(map[uint]uint64),
	}

	go res.run(ctx)
	return res
}

func (a *AutoPing) Add(event *Event) uint {
	a.mu.Lock()
	defer a.mu.Unlock()

	id := a.nextID
	a.nextID++

	qid := a.queue.Add(pingItem{id: id, event: event}, a.pingInterval)
	a.entries[id] = qid
	return id
}

func (a *AutoPing) Remove(id uint) {
	a.mu.Lock()
	qid, ok := a.entries[id]
	delete(a.entries, id)
	a.mu.Unlock()

	if ok {
		// The item may already have been delivered (and so no longer in the
		// queue under qid); deleting the entry above is what actually cancels
		// it, by preventing run() from requeuing.
		a.queue.Remove(qid)
	}
}

func (a *AutoPing) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case item, ok := <-a.queue.Items:
			if !ok {
				return
			}

			// If the entry was removed while the item was in flight, drop it
			// without pinging or requeuing.
			a.mu.Lock()
			_, active := a.entries[item.id]
			a.mu.Unlock()
			if !active {
				continue
			}

			err := item.event.Ping(ctx)
			if errors.Is(err, jetstream.ErrMsgAlreadyAckd) {
				a.logger.Debug("Event already acknowledged, removing from queue", slog.Uint64("eventID", item.event.ID()))
				a.mu.Lock()
				delete(a.entries, item.id)
				a.mu.Unlock()
				continue
			} else if err != nil {
				a.logger.Warn("Failed to ping event", slog.Uint64("eventID", item.event.ID()), slog.String("error", err.Error()))
			}

			// Requeue the event for the next ping, unless it was canceled
			// (Remove called) while we were pinging.
			a.mu.Lock()
			if _, active := a.entries[item.id]; active {
				qid := a.queue.Add(pingItem{id: item.id, event: item.event}, a.pingInterval)
				a.entries[item.id] = qid
			}
			a.mu.Unlock()
		}
	}
}
