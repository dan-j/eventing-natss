package nats

import (
	"github.com/nats-io/nats.go"
	"go.opencensus.io/plugin/ochttp/propagation/tracecontext"
	"go.opencensus.io/trace"
)

var (
	traceparentHeader = "traceparent"
	tracestateHeader  = "tracestate"

	format = tracecontext.HTTPFormat{}
)

func ParseSpanContext(headers nats.Header) (trace.SpanContext, bool) {
	tp := headers.Get(traceparentHeader)
	ts := headers.Get(tracestateHeader)

	return format.SpanContextFromHeaders(tp, ts)
}

func InjectSpanContext(msg *nats.Msg, sc trace.SpanContext) {
	tp, ts := format.SpanContextToHeaders(sc)

	msg.Header.Add(traceparentHeader, tp)
	msg.Header.Add(tracestateHeader, ts)
}
