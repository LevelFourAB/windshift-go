package subscribe

type Options struct {
	MaxProcessingEvents int
}

type Option func(*Options)

// MaxProcessingEvents is the maximum number of events to process concurrently.
//
// If not set this defaults to 50.
func MaxProcessingEvents(n int) Option {
	return func(o *Options) {
		o.MaxProcessingEvents = n
	}
}
