package events

import "errors"

// ErrPublishTimeout is used when publishing of an event does not meet the deadline.
var ErrPublishTimeout = errors.New("publish timeout")

// ErrUnboundSubject is used when a subject is not bound to a stream.
var ErrUnboundSubject = errors.New("unbound subject, no stream found for subject")

// ErrWrongSequence is used when the expected sequence number does not match the
// actual sequence number.
var ErrWrongSequence = errors.New("wrong sequence")

// ErrStreamRequired is used when a stream is required but not provided.
var ErrStreamRequired = NewValidationError("stream is required")

// ErrSubjectRequired is used when a name is required but not provided.
var ErrSubjectRequired = NewValidationError("subject is required")

// ErrSubjectsRequired is used when subjects are required but not provided.
var ErrSubjectsRequired = NewValidationError("one or more subjects are required")

// ErrDataRequired is used when data is required but not provided.
var ErrDataRequired = errors.New("data is required")

// ErrConsumerRequired is used when a consumer is required but not provided.
var ErrConsumerRequired = NewValidationError("consumer is required")

type validationError struct {
	err string
}

func (e *validationError) Error() string {
	return e.err
}

// NewValidationError creates a new validation error.
func NewValidationError(err string) error {
	return &validationError{err: err}
}

func IsValidationError(err error) bool {
	_, ok := err.(*validationError)
	return ok
}

// DataInvalidError represents an error where the data is invalid.
type DataInvalidError struct {
	Err error
}

func (e *DataInvalidError) Error() string {
	return "data is invalid: " + e.Err.Error()
}

func (e *DataInvalidError) Unwrap() error {
	return e.Err
}

func (e *DataInvalidError) Is(target error) bool {
	_, ok := target.(*DataInvalidError)
	return ok
}
