// Package delivery is the received context bitruntime's own carriers establish
// and its own helpers recognize. bitwire 0.3 lets Go keep that association
// private to the runtime; this package is internal so that nothing outside
// bitruntime can construct or claim it. A foreign return capability therefore
// carries no recognized context, and a message's visible fields never do.
package delivery

import (
	"context"
	"maps"
	"strings"
	"sync"

	wire "github.com/Bitspark/bitwire/wire/go"
)

// Context is what a carrier established for one admitted request: the base
// context its handler runs under, the frame limit its reply must fit, where a
// handler panic is reported, the trace members it arrived with, and how a
// handler's own cancellation cause reaches a forwarded reply.
type Context struct {
	Ctx           context.Context
	MaxFrameBytes int64
	Panic         func(any)
	Traceparent   string
	Tracestate    string
	Completion    *Completion
}

// Completion carries the cause of a local handler's cancellation. Only a
// local handler can supply it; a serialized public error, even one named
// cancelled, keeps its ordinary error outcome.
type Completion struct {
	mu           sync.Mutex
	cancellation error
}

// Set records the cause of a handler's cancellation.
func (c *Completion) Set(err error) {
	c.mu.Lock()
	c.cancellation = err
	c.mu.Unlock()
}

// Get is the recorded cause, or nil.
func (c *Completion) Get() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cancellation
}

// Source is implemented by return capabilities bitruntime minted. The method
// returns a type only this module can name, so no other package can claim it.
type Source interface {
	BitruntimeDelivery() *Context
}

// Of is the context a bitruntime carrier established for message, if its
// return capability is one of bitruntime's own.
func Of(message wire.Message) *Context {
	if message.Return == nil || message.Return.Wire == nil {
		return nil
	}
	if source, ok := message.Return.Wire.(Source); ok {
		return source.BitruntimeDelivery()
	}
	return nil
}

// Event is the local capability an event carries so that the context a
// receiving runtime established survives queued local composition. It is not
// a return address: an event has no reply and no request lifetime.
type Event struct{ Ctx context.Context }

// Send refuses: an event context is not a return address.
func (*Event) Send([]string, wire.Message) error {
	return errNotReturn
}

type notReturn struct{}

func (notReturn) Error() string { return "an event context is not a return address" }

var errNotReturn error = notReturn{}

// EventContext is the context a bitruntime carrier established for an event.
func EventContext(message wire.Message) (context.Context, bool) {
	if message.Return != nil {
		if held, ok := message.Return.Wire.(*Event); ok {
			return held.Ctx, true
		}
	}
	return nil, false
}

// WithEventContext associates ctx with an event's message.
func WithEventContext(message wire.Message, ctx context.Context) wire.Message {
	message.Return = &wire.ReturnAddress{Wire: &Event{Ctx: ctx}}
	return message
}

// A carriage travels one way at a time. The meta a frame arrived with and the
// meta the next frame sent from this context will carry are separate values
// under separate keys, so that a handler's outgoing call carries the caller's
// credential only where the handler said to.
type outgoingMetaKey struct{}
type incomingMetaKey struct{}

// MetaReserved prefixes the keys bitwire/1 keeps for itself.
const MetaReserved = "nightseam."

// WithOutgoingMeta says what the requests and events sent from ctx carry.
func WithOutgoingMeta(ctx context.Context, meta map[string]string) context.Context {
	carried := make(map[string]string, len(meta))
	for key, value := range meta {
		if !strings.HasPrefix(key, MetaReserved) {
			carried[key] = value
		}
	}
	if len(carried) == 0 {
		return context.WithValue(ctx, outgoingMetaKey{}, map[string]string(nil))
	}
	return context.WithValue(ctx, outgoingMetaKey{}, carried)
}

// OutgoingMeta is what a frame sent from ctx carries, and nil where nothing
// said. It is read once per frame.
func OutgoingMeta(ctx context.Context) map[string]string {
	meta, _ := ctx.Value(outgoingMetaKey{}).(map[string]string)
	if len(meta) == 0 {
		return nil
	}
	return meta
}

// WithIncomingMeta places what a frame carried on its handler's context.
func WithIncomingMeta(ctx context.Context, meta map[string]string) context.Context {
	if len(meta) == 0 {
		return ctx
	}
	return context.WithValue(ctx, incomingMetaKey{}, meta)
}

// IncomingMeta is a copy of the meta of the frame whose handler ctx runs
// under, and nil where the frame carried none.
func IncomingMeta(ctx context.Context) map[string]string {
	meta, _ := ctx.Value(incomingMetaKey{}).(map[string]string)
	if len(meta) == 0 {
		return nil
	}
	return maps.Clone(meta)
}
