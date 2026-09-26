package runtime

import (
	"context"
	"crypto/rand"
	"encoding/hex"
)

// Trace is the W3C Trace Context a frame carries: the two members verbatim,
// neither of them read by the runtime beyond the form the decoder holds a
// traceparent to. The runtime imports no tracing library; one binds to it as a
// Propagator and nowhere else.
type Trace struct{ Parent, State string }

// Propagator moves a trace between a context and a frame. Extract places an
// incoming frame's trace in the context its handler runs under; Inject says
// what an outgoing frame carries — a child of the trace the context holds, or
// a new trace where it holds none. Both members reach it verbatim, and a peer
// sends what it returns as it returns it.
type Propagator interface {
	Extract(ctx context.Context, trace Trace) context.Context
	Inject(ctx context.Context) Trace
}

// DefaultPropagator is what a peer with no Options.Propagator propagates by: it
// keeps an incoming trace verbatim and mints an outgoing one's ids itself, so
// that frames correlate across hops with no tracing library installed.
var DefaultPropagator Propagator = w3cPropagator{}

type traceKey struct{}

// TraceOf is the trace DefaultPropagator placed in ctx, and whether one is
// there. A propagator of another making keeps its trace where it chooses.
func TraceOf(ctx context.Context) (Trace, bool) {
	trace, ok := ctx.Value(traceKey{}).(Trace)
	return trace, ok
}

// w3cPropagator carries the trace under a key of its own and mints ids in the
// one form W3C Trace Context gives them: version 00, sampled, a tracestate
// forwarded exactly as it arrived.
type w3cPropagator struct{}

// Extract keeps a trace whose traceparent the decoder has already held to its
// form. A frame carrying none leaves the context as it was, so that what is
// sent from there begins a trace rather than continuing one that is not there.
func (w3cPropagator) Extract(ctx context.Context, trace Trace) context.Context {
	if trace.Parent == "" {
		return ctx
	}
	return context.WithValue(ctx, traceKey{}, trace)
}

// Inject mints a child of the context's trace — its trace id and its flags, a
// span id of this frame's own — or a new trace where the context carries none.
func (w3cPropagator) Inject(ctx context.Context) Trace {
	parent, ok := TraceOf(ctx)
	if !ok || !validTraceparent(parent.Parent) {
		return Trace{Parent: "00-" + randomID(16) + "-" + randomID(8) + "-01"}
	}
	return Trace{Parent: parent.Parent[:36] + randomID(8) + parent.Parent[52:], State: parent.State}
}

// randomID is n random bytes in lower-case hexadecimal. crypto/rand fills the
// buffer or does not return, so a trace id is never the zero one by accident.
func randomID(n int) string {
	id := make([]byte, n)
	rand.Read(id)
	return hex.EncodeToString(id)
}
