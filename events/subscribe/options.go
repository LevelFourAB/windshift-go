package subscribe

type Options struct {
	MaxProcessingEvents uint
}

func ApplyOptions(opts []Option) Options {
	var options Options
	for _, o := range opts {
		o(&options)
	}
	return options
}

type Option func(*Options)

// MaxProcessingEvents is the maximum number of events to process concurrently.
//
// If not set this defaults to 50.
func MaxProcessingEvents(n uint) Option {
	return func(o *Options) {
		o.MaxProcessingEvents = n
	}
}
