// Package dispatch routes addressed deliveries to handlers and provides the
// request, response and event helpers adapters use. A Dispatcher owns one
// endpoint attachment and an explicit exact/longest-prefix routing policy; Call
// and Emit send through any addressed access; Handle and Register run a body
// per request or event with the context its carrier established.
package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	core "github.com/Bitspark/bitruntime/core/go"
	"github.com/Bitspark/bitruntime/internal/delivery/go"
	"github.com/Bitspark/bitruntime/internal/profile/go"
	"github.com/Bitspark/bitruntime/internal/request/go"
	transports "github.com/Bitspark/bitruntime/transports/go"
	wire "github.com/Bitspark/bitwire/wire/go"
)

// CallOptions configures one call, independently of its carrier. A zero
// Timeout waits 30 seconds; a nil Propagator uses core.DefaultPropagator.
type CallOptions struct {
	Propagator core.Propagator
	Timeout    time.Duration
}

// Call sends one request at a relative path and waits for its response,
// decoding a success into result when it is not nil. Its return capability is
// fresh for this call and carries the invocation lifecycle, so it is
// independent of every other call's identifier. A caller that withdraws —
// ctx ends first — sends a best-effort cancellation. It never retries.
//
// A refusal before anything was sent is an *core.UnpublishedError; a response
// error is a *core.PublicError.
func Call(ctx context.Context, access wire.AddressedWire, path []string, params, result any, options ...CallOptions) error {
	var o request.Options
	if len(options) > 0 {
		o = request.Options{Propagator: options[0].Propagator, Timeout: options[0].Timeout}
	}
	return request.Call(ctx, access, path, params, result, nil, o)
}

// EmitOptions configures one event emission; a nil Propagator uses
// core.DefaultPropagator.
type EmitOptions struct {
	Propagator core.Propagator
}

// Emit admits one event at a relative path. Success says only that the
// destination accepted it; processing and transport remain asynchronous.
func Emit(ctx context.Context, access wire.AddressedWire, path []string, data any, options ...EmitOptions) error {
	if ctx == nil || access == nil {
		return core.Unpublished(errors.New("bitruntime: an event requires a context and access"))
	}
	if err := ctx.Err(); err != nil {
		return core.Unpublished(err)
	}
	if !profile.ValidPath(path) {
		return core.Unpublished(errors.New("bitruntime: an event requires a valid operation path"))
	}
	encoded, err := profile.MarshalJSON(data)
	if err != nil {
		return core.Unpublished(err)
	}
	propagator := core.DefaultPropagator
	if len(options) > 0 && options[0].Propagator != nil {
		propagator = options[0].Propagator
	}
	trace := propagator.Inject(ctx)
	return core.Unpublished(access.Send(path, wire.Message{Frame: wire.ProfileFrame{Version: 1, Kind: wire.ProfileEvent, Data: encoded, Traceparent: trace.Parent, Tracestate: trace.State, Meta: delivery.OutgoingMeta(ctx)}}))
}

// Handler is a request body, independent of the carrier. It takes the params
// as they arrived and returns the result, or an error: a *core.PublicError
// crosses with its code, a cancellation as cancelled, and any other error as
// internal. Its context carries the trace and meta the request brought, and
// ends when the caller withdraws or the deadline passes.
type Handler func(context.Context, json.RawMessage) (any, error)

// EventHandler receives an event body beside the context it was carried with.
// An error ends the registry's endpoint as a protocol error.
type EventHandler func(context.Context, json.RawMessage) error

// Handlers groups a request body and an event body that share one path.
type Handlers struct {
	Request Handler
	Event   EventHandler
}

// Registry is the registration capability Handle and Register need: send
// access plus explicit route registration. Closing it releases its
// registrations, not a borrowed carrier.
type Registry interface {
	wire.AddressedWire
	Register([]string, wire.Receiver) (func(), error)
	Close(wire.Code, string) error
}

// Handle registers one request body at a relative path. The receiver returns
// before running application code; the body runs asynchronously, and the
// request's cancellation reaches it through its return capability.
func Handle(registry Registry, path []string, handler Handler) (func(), error) {
	if registry == nil || handler == nil {
		return nil, errors.New("bitruntime: a handler requires a registry and body")
	}
	return Register(registry, path, Handlers{Request: handler})
}

// Register installs one receiver for a request body, an event body, or both,
// at a relative path. The one detach removes the group; an event-only path
// refuses requests with method_not_found.
func Register(registry Registry, path []string, handlers Handlers) (func(), error) {
	if registry == nil || (handlers.Request == nil && handlers.Event == nil) {
		return nil, errors.New("bitruntime: registration requires a registry and at least one handler")
	}
	if !profile.ValidPath(path) {
		return nil, core.ErrInvalidPath
	}
	type returnKey struct {
		address *wire.ReturnAddress
		id      string
	}
	var mu sync.Mutex
	incoming := map[returnKey]context.CancelFunc{}
	return registry.Register(path, wire.Receiver{
		Closed: func(wire.Code, string) {
			mu.Lock()
			defer mu.Unlock()
			for _, cancel := range incoming {
				cancel()
			}
		},
		Message: func(_ []string, message wire.Message) {
			if message.Frame.Kind == wire.ProfileEvent {
				if handlers.Event != nil {
					ctx, associated := delivery.EventContext(message)
					if !associated {
						ctx = core.DefaultPropagator.Extract(context.Background(), core.Trace{Parent: message.Frame.Traceparent, State: message.Frame.Tracestate})
					}
					if err := invokeEvent(delivery.WithIncomingMeta(ctx, message.Frame.Meta), handlers.Event, message.Frame.Data); err != nil {
						_ = registry.Close(transports.CodeProtocolError, "wire event rejected")
					}
				}
				return
			}
			key := returnKey{message.Return, message.Frame.ID}
			if message.Frame.Kind == wire.ProfileCancel {
				mu.Lock()
				cancel := incoming[key]
				mu.Unlock()
				if cancel != nil {
					cancel()
				}
				return
			}
			if message.Frame.Kind != wire.ProfileRequest {
				return
			}
			if handlers.Request == nil {
				core.Respond(message, nil, &core.PublicError{Code: "method_not_found", Message: "Unknown method"})
				return
			}
			dispatch := delivery.Of(message)
			base := context.Background()
			if dispatch != nil {
				base = dispatch.Ctx
			}
			ctx := core.DefaultPropagator.Extract(base, core.Trace{Parent: message.Frame.Traceparent, State: message.Frame.Tracestate})
			ctx, cancel := context.WithCancel(delivery.WithIncomingMeta(ctx, message.Frame.Meta))
			mu.Lock()
			if incoming[key] != nil {
				mu.Unlock()
				cancel()
				core.Respond(message, nil, &core.PublicError{Code: "invalid_message", Message: "Duplicate active request identifier"})
				return
			}
			incoming[key] = cancel
			mu.Unlock()
			// The body runs after this receiver returns, so returning is not
			// completion. The lease says so to whoever admitted the request: an
			// early answer to the caller cannot retire an invocation whose body
			// is still running. A bound reached is a refusal; any other refusal
			// means this return capability carries no lifecycle, and ordinary
			// addressed delivery goes on without one.
			body, leaseErr := core.BeginInvocationBody(message)
			if errors.Is(leaseErr, core.ErrInvocationLimit) {
				mu.Lock()
				delete(incoming, key)
				mu.Unlock()
				cancel()
				core.Respond(message, nil, &core.PublicError{Code: "busy", Message: "Invocation participation limit reached"})
				return
			}
			go func() {
				defer func() { body.Done(); cancel(); mu.Lock(); delete(incoming, key); mu.Unlock() }()
				result, err := invokeHandler(ctx, handlers.Request, message.Frame.Params, dispatch)
				if err == nil {
					err = ctx.Err()
				}
				data, marshalErr := profile.MarshalJSON(result)
				if err == nil {
					err = marshalErr
				}
				if dispatch != nil && dispatch.Completion != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
					dispatch.Completion.Set(err)
				}
				core.Respond(message, data, core.WithoutUnpublishedProof(err))
			}()
		},
	})
}

func invokeEvent(ctx context.Context, handler EventHandler, data json.RawMessage) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("bitruntime: event handler panic")
		}
	}()
	return handler(ctx, data)
}

func invokeHandler(ctx context.Context, handler Handler, params json.RawMessage, dispatch *delivery.Context) (result any, err error) {
	defer func() {
		if value := recover(); value != nil {
			if dispatch != nil && dispatch.Panic != nil {
				dispatch.Panic(value)
			}
			err = errors.New("bitruntime: handler panic")
		}
	}()
	return handler(ctx, params)
}
