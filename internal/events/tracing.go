package events

import (
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/propagation"
)

// eventTracingHeaders is a wrapper around NATS headers that implements the
// propagation.TextMapCarrier interface.
type eventTracingHeaders struct {
	headers *nats.Header
}

func (h eventTracingHeaders) Get(key string) string {
	switch key {
	case "traceparent":
		return h.headers.Get("WS-Trace-Parent")
	case "tracestate":
		return h.headers.Get("WS-Trace-State")
	}
	return ""
}

func (h eventTracingHeaders) Set(key string, value string) {
	switch key {
	case "traceparent":
		h.headers.Set("WS-Trace-Parent", value)
	case "tracestate":
		h.headers.Set("WS-Trace-State", value)
	}
}

func (h eventTracingHeaders) Keys() []string {
	res := make([]string, 0, 2)
	if h.headers.Get("WS-Trace-Parent") != "" {
		res = append(res, "traceparent")
	}
	if h.headers.Get("WS-Trace-State") != "" {
		res = append(res, "tracestate")
	}
	return res
}

var _ propagation.TextMapCarrier = eventTracingHeaders{}
