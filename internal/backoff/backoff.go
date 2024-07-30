package backoff

import (
	"context"
	"errors"
	"time"

	"github.com/levelfourab/windshift-go/delays"
)

type PermanentError struct {
	Err error
}

func Permanent(err error) error {
	return &PermanentError{Err: err}
}

func (e *PermanentError) Error() string {
	return e.Err.Error()
}

func (e *PermanentError) Unwrap() error {
	return e.Err
}

func isPermanent(err error) bool {
	var pe *PermanentError
	return errors.As(err, &pe)
}

func Run(ctx context.Context, action func() error, decider delays.DelayDecider) error {
	attempt := uint(1)
	startTime := time.Now()
	for {
		err := action()
		if err == nil {
			// The action did not return any error, stop the retry loop.
			return nil
		} else if isPermanent(err) {
			return err
		}

		delay := decider(attempt, startTime)
		if delay == delays.Stop {
			return err
		}

		attempt++

		select {
		case <-time.After(delay):
			// Have waited the delay, continue to next attempt.
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
