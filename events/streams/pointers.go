package streams

import (
	"time"
)

// Pointer describes where to start receiving events from. See the functions
// [AtStreamStart], [AtStreamEnd], [AtStreamOffset], and [AtStreamTimestamp] to
// create a pointer.
type Pointer interface {
	isStreamPointer()
}

// PointerStart is a pointer that starts at the beginning of the stream.
type PointerStart struct{}

// AtStreamStart returns a pointer that points at the beginning of the stream.
func AtStreamStart() Pointer {
	return &PointerStart{}
}

func (p *PointerStart) isStreamPointer() {}

// PointerEnd is a pointer that starts at the end of the stream.
type PointerEnd struct{}

// AtStreamEnd returns a pointer that points at the end of the stream. Using
// this pointer means only new events are received.
func AtStreamEnd() Pointer {
	return &PointerEnd{}
}

func (p *PointerEnd) isStreamPointer() {}

// PointerOffset is a pointer that starts at the given offset.
type PointerOffset struct {
	ID uint64
}

// AtStreamOffset returns a pointer that points at the given offset. The ID
// should be the ID of an event in the stream.
func AtStreamOffset(id uint64) Pointer {
	return &PointerOffset{ID: id}
}

func (p *PointerOffset) isStreamPointer() {}

// PointerTimestamp is a pointer that starts at the given timestamp.
type PointerTimestamp struct {
	Timestamp time.Time
}

// AtStreamTimestamp returns a pointer that points at the given timestamp.
func AtStreamTimestamp(timestamp time.Time) Pointer {
	return &PointerTimestamp{Timestamp: timestamp}
}

func (p *PointerTimestamp) isStreamPointer() {}
