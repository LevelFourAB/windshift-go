package rpc

import (
	"context"
	"sync"
)

type PendingRequest[Req any, Res any] struct {
	ID      uint64
	Request Req
	success chan Res
	failure chan error
}

// NewPendingRequest creates a new pending request, but will not send it.
func NewPendingRequest[Req any, Res any](id uint64, request Req) *PendingRequest[Req, Res] {
	return &PendingRequest[Req, Res]{
		ID:      id,
		Request: request,
		success: make(chan Res, 1),
		failure: make(chan error, 1),
	}
}

// Wait for the result to be available, or the context to be canceled.
func (p *PendingRequest[Req, Res]) Wait(ctx context.Context) (Res, error) {
	select {
	case res := <-p.success:
		return res, nil
	case err := <-p.failure:
		return *new(Res), err
	case <-ctx.Done():
		return *new(Res), ctx.Err()
	}
}

type Tracker[Req any, Res any] struct {
	mu       sync.Mutex
	requests map[uint64]*PendingRequest[Req, Res]
}

func NewTracker[Req any, Res any]() *Tracker[Req, Res] {
	return &Tracker[Req, Res]{
		requests: make(map[uint64]*PendingRequest[Req, Res]),
	}
}

func (t *Tracker[Req, Res]) Has(id uint64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	_, ok := t.requests[id]
	return ok
}

func (t *Tracker[Req, Res]) Get(id uint64) *PendingRequest[Req, Res] {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.requests[id]
}

func (t *Tracker[Req, Res]) Add(request *PendingRequest[Req, Res]) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.requests[request.ID]; ok {
		return false
	}

	t.requests[request.ID] = request
	return true
}

func (t *Tracker[Req, Res]) Reply(id uint64, response Res) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if request, ok := t.requests[id]; ok {
		request.success <- response
		delete(t.requests, id)
	}
}

func (t *Tracker[Req, Res]) Fail(id uint64, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if request, ok := t.requests[id]; ok {
		request.failure <- err
		delete(t.requests, id)
	}
}

type Exchange[Req any, Res any] struct {
	Tracker[Req, Res]
	outbound chan *PendingRequest[Req, Res]
}

func NewExchange[Req any, Res any]() *Exchange[Req, Res] {
	return &Exchange[Req, Res]{
		Tracker:  *NewTracker[Req, Res](),
		outbound: make(chan *PendingRequest[Req, Res], 10),
	}
}

func (e *Exchange[Req, Res]) Requests() <-chan *PendingRequest[Req, Res] {
	return e.outbound
}

func (e *Exchange[Req, Res]) Add(request *PendingRequest[Req, Res]) bool {
	added := e.Tracker.Add(request)
	if added {
		e.outbound <- request
	}

	return added
}
