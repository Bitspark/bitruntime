// Package request is the request primitive shared by the public call helper
// and the protocol engine's inbound bridge: one request sent through addressed
// access with a fresh return capability that carries its invocation lifecycle,
// and the wait for its one response.
package request

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	core "github.com/Bitspark/bitruntime/core/go"
	"github.com/Bitspark/bitruntime/internal/delivery/go"
	"github.com/Bitspark/bitruntime/internal/profile/go"
	transports "github.com/Bitspark/bitruntime/transports/go"
	wire "github.com/Bitspark/bitwire/wire/go"
)

// DefaultTimeout is how long a call waits for its response when it states
// no deadline of its own.
const DefaultTimeout = 30 * time.Second

// Options configures one call. A zero Timeout uses DefaultTimeout; a nil
// Propagator uses core.DefaultPropagator.
type Options struct {
	Propagator core.Propagator
	Timeout    time.Duration
}

// Result is one response as a waiter receives it.
type Result struct {
	Value json.RawMessage
	Err   error
}

// Reply is the return capability of one call. Its empty path takes the
// response; every other path is its invocation's lifecycle vocabulary.
type Reply struct {
	id         string
	reply      chan Result
	done       chan struct{}
	once       sync.Once
	dispatch   *delivery.Context
	invocation *core.Invocation
}

// BitruntimeDelivery is the context the carrier established, if any.
func (w *Reply) BitruntimeDelivery() *delivery.Context { return w.dispatch }

// Invocation exposes this return capability's lifecycle to its owner.
func (w *Reply) Invocation() *core.Invocation { return w.invocation }

func (w *Reply) Send(path []string, message wire.Message) error {
	if len(path) != 0 {
		return w.invocation.Deliver(path, message)
	}
	if message.Frame.Kind != wire.ProfileResponse || message.Frame.ID != w.id {
		return errors.New("bitruntime: invalid wire response")
	}
	var limit int64
	if w.dispatch != nil {
		limit = w.dispatch.MaxFrameBytes
	}
	if err := profile.Validate("", message.Frame, limit); err != nil {
		return err
	}
	r := Result{Value: message.Frame.Result}
	if f := message.Frame.Error; f != nil {
		r.Err = &core.PublicError{Code: f.Code, Message: f.Message, Data: f.Data}
		if f.Code == "cancelled" && w.dispatch != nil && w.dispatch.Ctx.Err() != nil && w.dispatch.Completion != nil {
			if cause := w.dispatch.Completion.Get(); cause != nil {
				r.Err = cause
			}
		}
	}
	select {
	case <-w.done:
		return transports.ErrClosed
	default:
	}
	select {
	case w.reply <- r:
		w.invocation.Settle()
		return nil
	default:
		return errors.New("bitruntime: duplicate wire response")
	}
}

func (w *Reply) finish() {
	w.once.Do(func() {
		close(w.done)
		w.invocation.Settle()
		w.invocation.DispatchDone()
	})
}

// Await is the request primitive shared by carriers and addressed access: it
// waits for the response, the context's end or the carrier's end. The first
// result says whether the caller withdrew, so a cancellation is owed.
func Await(ctx context.Context, reply <-chan Result, done <-chan struct{}, ended func() error, result any) (bool, error) {
	select {
	case r := <-reply:
		if r.Err != nil {
			return false, r.Err
		}
		if result == nil {
			return false, nil
		}
		if err := json.Unmarshal(r.Value, result); err != nil {
			return false, fmt.Errorf("bitruntime: decode wire result: %w", err)
		}
		return false, nil
	case <-ctx.Done():
		return true, ctx.Err()
	case <-done:
		return false, ended()
	}
}

// Call sends one request at path through access and waits for its response.
// dispatch is the context a carrier established when this call forwards a
// request it admitted; it is nil for an application's own call.
func Call(ctx context.Context, access wire.AddressedWire, path []string, params, result any, dispatch *delivery.Context, options Options) error {
	if ctx == nil || access == nil {
		return core.Unpublished(errors.New("bitruntime: a call requires a context and access"))
	}
	if err := ctx.Err(); err != nil {
		return core.Unpublished(err)
	}
	if !profile.ValidPath(path) {
		return core.Unpublished(errors.New("bitruntime: a call requires a valid operation path"))
	}
	encoded, err := profile.MarshalJSON(params)
	if err != nil {
		return core.Unpublished(err)
	}
	if options.Timeout < 0 {
		return core.Unpublished(errors.New("bitruntime: request timeout must not be negative"))
	}
	// A forwarded request already has its carrier's admitted deadline.
	if dispatch == nil {
		timeout := options.Timeout
		if timeout == 0 {
			timeout = DefaultTimeout
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	if dispatch != nil {
		copied := *dispatch
		copied.Completion = &delivery.Completion{}
		dispatch = &copied
	}
	returning := &Reply{id: "c:1", reply: make(chan Result, 1), done: make(chan struct{}), dispatch: dispatch, invocation: core.NewInvocation(core.DefaultInvocationLimits(), nil)}
	defer returning.finish()
	address := &wire.ReturnAddress{Wire: returning}
	var trace core.Trace
	if dispatch != nil {
		trace = core.Trace{Parent: dispatch.Traceparent, State: dispatch.Tracestate}
	} else {
		propagator := options.Propagator
		if propagator == nil {
			propagator = core.DefaultPropagator
		}
		trace = propagator.Inject(ctx)
	}
	request := wire.Message{Frame: wire.ProfileFrame{Version: 1, Kind: wire.ProfileRequest, ID: returning.id, Params: encoded, Traceparent: trace.Parent, Tracestate: trace.State, Meta: delivery.OutgoingMeta(ctx)}, Return: address}
	if err := access.Send(path, request); err != nil {
		return core.Unpublished(err)
	}
	cancelRemote, err := Await(ctx, returning.reply, returning.done, func() error { return transports.ErrClosed }, result)
	if cancelRemote {
		_ = access.Send(path, wire.Message{Frame: wire.ProfileFrame{Version: 1, Kind: wire.ProfileCancel, ID: returning.id, Traceparent: trace.Parent, Tracestate: trace.State}, Return: address})
		if dispatch != nil {
			// This waiter is the carrier's admitted handler, not the outgoing
			// caller. Cancellation reaches the body immediately, but its slot
			// remains occupied until the receiver actually finishes its work.
			_, err = Await(context.WithoutCancel(ctx), returning.reply, returning.done, func() error { return transports.ErrClosed }, result)
		}
	}
	return err
}
