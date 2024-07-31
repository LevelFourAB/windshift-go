package events

import (
	"context"
	"log/slog"
	"time"

	"github.com/levelfourab/windshift-go/internal/queues"
)

// AutoPing is a helper for automatically pinging events as they are processed.
type AutoPing struct {
	logger *slog.Logger
	queue  *queues.DelayQueue[*Event]

	// pingInterval is the interval at which events should be pinged.
	pingInterval time.Duration
}

func newAutoPing(ctx context.Context, logger *slog.Logger, pingInterval time.Duration) *AutoPing {
	res := &AutoPing{
		logger:       logger,
		queue:        queues.NewDelayQueue[*Event](ctx),
		pingInterval: pingInterval,
	}

	go res.run(ctx)
	return res
}

func (a *AutoPing) Add(event *Event) uint {
	return a.queue.Add(event, a.pingInterval)
}

func (a *AutoPing) Remove(id uint) {
	a.queue.Remove(id)
}

func (a *AutoPing) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-a.queue.Items:
			if !ok {
				return
			}

			err := event.Ping(ctx)
			if err != nil {
				a.logger.Warn("Failed to ping event", slog.Uint64("eventID", event.ID()), slog.String("error", err.Error()))
			}

			// Requeue the event for the next ping
			a.queue.Add(event, a.pingInterval)
		}
	}
}
