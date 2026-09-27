package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"sync"

	core "github.com/Bitspark/bitruntime/core/go"
	"github.com/Bitspark/bitruntime/internal/delivery/go"
	"github.com/Bitspark/bitruntime/internal/profile/go"
	"github.com/Bitspark/bitruntime/internal/request/go"
	transports "github.com/Bitspark/bitruntime/transports/go"
	wire "github.com/Bitspark/bitwire/wire/go"
)

type routedFrame struct {
	path    []string
	message wire.Message
	call    *routedCall
	refusal error
}

// A request reserves one cancellation at admission. Completed requests retain
// their reservation until an already queued control has drained, so repeated
// completion and admission cannot turn the control queue into an unbounded
// buffer.
type routedCall struct {
	cancel       context.CancelFunc
	completed    bool
	cancelQueued bool
	cancelled    bool
}

type returnKey struct {
	address *wire.ReturnAddress
	id      string
}

// rootWire is the peer's addressed origin. Outgoing requests, events and
// cancellations queue here in admission order and are handed to the peer one
// at a time; its return associations stay beside the peer's pending and
// incoming tables. What the remote side sends is delivered to its one
// receiver with the path the frame's name encodes.
type rootWire struct {
	peer       *Peer
	queue      []routedFrame
	dataQueued int
	wake       chan struct{}
	mu         sync.Mutex
	incoming   map[returnKey]*routedCall
	receiver   *wire.Receiver
}

func (w *rootWire) Send(path []string, message wire.Message) error {
	// Copy, then validate the copy (research 0001, row 28): a caller mutating
	// its message during Send cannot publish bytes that were never validated.
	path = append([]string(nil), path...)
	message.Frame.Params = append(json.RawMessage(nil), message.Frame.Params...)
	message.Frame.Data = append(json.RawMessage(nil), message.Frame.Data...)
	message.Frame.Meta = maps.Clone(message.Frame.Meta)
	name, err := profile.EncodePath(path)
	if err != nil {
		return core.Unpublished(core.ErrInvalidPath)
	}
	if name == "" && (message.Frame.Kind == wire.ProfileRequest || message.Frame.Kind == wire.ProfileEvent) {
		return core.Unpublished(errors.New("bitruntime: a root operation needs a nonempty path"))
	}
	if err := w.peer.Err(); err != nil {
		return core.Unpublished(err)
	}
	if (message.Frame.Kind == wire.ProfileRequest || message.Frame.Kind == wire.ProfileCancel) && (message.Return == nil || message.Return.Wire == nil) {
		return core.Unpublished(errors.New("bitruntime: a request or cancellation requires a return address"))
	}
	if message.Frame.Kind != wire.ProfileRequest && message.Frame.Kind != wire.ProfileEvent && message.Frame.Kind != wire.ProfileCancel {
		return core.Unpublished(errors.New("bitruntime: a response is sent to its request's return address"))
	}
	if err := profile.Validate(name, message.Frame, w.peer.options.MaxFrameBytes); err != nil {
		return core.Unpublished(err)
	}
	delivered := routedFrame{path: path, message: message}
	key := returnKey{message.Return, message.Frame.ID}
	w.mu.Lock()
	if err := w.peer.Err(); err != nil {
		w.mu.Unlock()
		return core.Unpublished(err)
	}
	if message.Frame.Kind == wire.ProfileCancel {
		call := w.incoming[key]
		if call == nil || call.completed || call.cancelQueued || call.cancelled {
			w.mu.Unlock()
			return nil
		}
		call.cancelQueued = true
		delivered.call = call
	} else {
		if w.dataQueued >= w.peer.options.QueueCapacity {
			w.mu.Unlock()
			// bitwire/1: a full root queue ends the carrier.
			w.peer.fail(core.ErrBackpressure)
			return core.Unpublished(core.Ended(core.ErrBackpressure))
		}
		w.dataQueued++
		if message.Frame.Kind == wire.ProfileRequest {
			if w.incoming[key] != nil {
				delivered.refusal = &core.PublicError{Code: "invalid_message", Message: "Duplicate active request identifier"}
			} else if len(w.incoming) >= w.peer.options.MaxPendingRequests {
				delivered.refusal = &core.PublicError{Code: "busy", Message: "Outstanding call limit reached"}
			} else {
				delivered.call = &routedCall{}
				w.incoming[key] = delivered.call
			}
		}
	}
	w.queue = append(w.queue, delivered)
	w.mu.Unlock()
	select {
	case w.wake <- struct{}{}:
	default:
	}
	return nil
}

// retireLocked never removes a newer admission that reused the same local
// return identity. It is called with w.mu held on completion and control drain.
func (w *rootWire) retireLocked(key returnKey, call *routedCall) {
	if call.completed && !call.cancelQueued && w.incoming[key] == call {
		delete(w.incoming, key)
	}
}

func (w *rootWire) complete(key returnKey, call *routedCall) {
	w.mu.Lock()
	call.completed = true
	w.retireLocked(key, call)
	w.mu.Unlock()
}

func (w *rootWire) next() (routedFrame, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.queue) == 0 {
		return routedFrame{}, false
	}
	delivered := w.queue[0]
	w.queue[0] = routedFrame{}
	w.queue = w.queue[1:]
	if delivered.message.Frame.Kind != wire.ProfileCancel {
		w.dataQueued--
	}
	return delivered, true
}

// Close ends the peer. An observe-only code aborts the connection instead of
// transmitting a code that may only be observed.
func (w *rootWire) Close(code wire.Code, reason string) error {
	w.peer.end(transports.ErrClosed, code, reason)
	return nil
}

func (w *rootWire) run() {
	defer func() {
		w.mu.Lock()
		receiver := w.receiver
		w.receiver = nil
		var cancels []context.CancelFunc
		for _, call := range w.incoming {
			if call.cancel != nil {
				cancels = append(cancels, call.cancel)
			}
		}
		// Every request still queued is owed an answer: a queued refusal its
		// refusal, an admitted request that never reached the peer
		// disconnected. A request already handed to the peer is answered by its
		// own waiter, which the peer's end releases.
		var answers []routedFrame
		for _, queued := range w.queue {
			if queued.message.Frame.Kind == wire.ProfileRequest {
				answers = append(answers, queued)
			}
		}
		w.incoming = map[returnKey]*routedCall{}
		w.queue = nil
		w.dataQueued = 0
		w.mu.Unlock()
		for _, cancel := range cancels {
			cancel()
		}
		for _, queued := range answers {
			refusal := queued.refusal
			if refusal == nil {
				refusal = w.peer.Err()
			}
			core.Respond(queued.message, nil, core.WithoutUnpublishedProof(refusal))
		}
		if receiver != nil && receiver.Closed != nil {
			receiver.Closed(transports.CodeGoingAway, "peer ended")
		}
	}()
	for {
		select {
		case <-w.peer.Done():
			return
		default:
		}
		delivered, exists := w.next()
		if !exists {
			select {
			case <-w.peer.Done():
				return
			case <-w.wake:
			}
			continue
		}
		f := delivered.message.Frame
		key := returnKey{delivered.message.Return, f.ID}
		switch f.Kind {
		case wire.ProfileCancel:
			w.mu.Lock()
			state := delivered.call
			var cancel context.CancelFunc
			if w.incoming[key] == state {
				state.cancelQueued = false
				state.cancelled = true
				if !state.completed {
					cancel = state.cancel
				}
				w.retireLocked(key, state)
			}
			w.mu.Unlock()
			if cancel != nil {
				cancel()
			}
		case wire.ProfileRequest:
			if delivered.refusal != nil {
				core.Respond(delivered.message, nil, delivered.refusal)
				continue
			}
			state := delivered.call
			ctx := w.peer.options.Propagator.Extract(w.peer.Context(), core.Trace{Parent: f.Traceparent, State: f.Tracestate})
			ctx = delivery.WithOutgoingMeta(ctx, f.Meta)
			ctx, cancel := context.WithCancel(ctx)
			name, err := profile.EncodePath(delivered.path)
			var call *admittedCall
			if err == nil {
				call, err = w.peer.beginCall(ctx, name, f.Params, core.Trace{Parent: f.Traceparent, State: f.Tracestate})
			}
			if err != nil {
				cancel()
				w.complete(key, state)
				core.Respond(delivered.message, nil, core.WithoutUnpublishedProof(err))
				continue
			}
			w.mu.Lock()
			state.cancel = func() { call.withdraw(); cancel() }
			w.mu.Unlock()
			go func() {
				var result json.RawMessage
				err := call.await(&result)
				cancel()
				// Retire before delivering the response: its callback can admit
				// another request, but a queued cancellation still owns budget.
				w.complete(key, state)
				// The peer's deadline is its caller's (bitwire/1): an
				// application's own call fails locally with the deadline that
				// passed, as its own deadline would fail it, and a cancel has
				// gone to the remote. A return that may cross a wire is answered
				// cancelled, since a request_timeout is never a frame.
				if errors.Is(err, context.DeadlineExceeded) {
					if reply, ok := request.Own(delivered.message); ok {
						_ = reply.Expire(fmt.Errorf("bitruntime: the call's deadline passed; its outcome may be unknown: %w", context.DeadlineExceeded))
						return
					}
				}
				core.Respond(delivered.message, result, core.WithoutUnpublishedProof(err))
			}()
		case wire.ProfileEvent:
			name, err := profile.EncodePath(delivered.path)
			if err == nil {
				ctx := w.peer.options.Propagator.Extract(w.peer.Context(), core.Trace{Parent: f.Traceparent, State: f.Tracestate})
				err = w.peer.emit(delivery.WithOutgoingMeta(ctx, f.Meta), name, f.Data, core.Trace{Parent: f.Traceparent, State: f.Tracestate})
			}
			if err != nil {
				w.peer.fail(err)
			}
		}
	}
}

func (w *rootWire) Receive(receiver wire.Receiver) (func(), error) {
	registration := &receiver
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.peer.Err(); err != nil {
		return nil, err
	}
	if w.receiver != nil {
		return nil, core.ErrReceiverExists
	}
	w.receiver = registration
	return func() {
		w.mu.Lock()
		if w.receiver == registration {
			w.receiver = nil
		}
		w.mu.Unlock()
	}, nil
}

// attached is the path a frame's name encodes and the receiver attached now,
// or nothing when the name encodes no path or nothing is attached.
func (w *rootWire) attached(name string) ([]string, *wire.Receiver) {
	path, err := profile.DecodePath(name)
	if err != nil {
		return nil, nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return path, w.receiver
}

// requestHandler is the body the peer runs for an incoming request: it hands
// the request to the attached receiver with a fresh return capability that
// carries its invocation lifecycle and the context this peer established, and
// waits for its response. The receiver chosen now stays with this request, so
// a later detach or replacement cannot redirect its cancellation.
func (w *rootWire) requestHandler(name string) func(context.Context, json.RawMessage) (json.RawMessage, error) {
	path, receiver := w.attached(name)
	if receiver == nil {
		return nil
	}
	chosen := *receiver
	return func(ctx context.Context, params json.RawMessage) (json.RawMessage, error) {
		if chosen.Message == nil {
			return nil, &core.PublicError{Code: "method_not_found", Message: "Unknown method"}
		}
		incoming, _ := ctx.Value(frameKey{}).(profile.Frame)
		dispatch := &delivery.Context{Ctx: ctx, MaxFrameBytes: w.peer.options.MaxFrameBytes, Traceparent: incoming.Traceparent, Tracestate: incoming.Tracestate}
		var result json.RawMessage
		err := request.Call(delivery.WithOutgoingMeta(ctx, delivery.IncomingMeta(ctx)), receiverWire{chosen}, append([]string{}, path...), params, &result, dispatch, request.Options{})
		return result, err
	}
}

// deliverEvent hands an incoming event to the attached receiver, with the
// context this peer established held beside it.
func (w *rootWire) deliverEvent(ctx context.Context, queued queuedEvent) {
	path, receiver := w.attached(queued.name)
	if receiver == nil || receiver.Message == nil {
		return
	}
	message := wire.Message{Frame: wire.ProfileFrame{Version: 1, Kind: wire.ProfileEvent, Data: queued.data, Traceparent: queued.trace.Parent, Tracestate: queued.trace.State, Meta: delivery.IncomingMeta(ctx)}}
	receiver.Message(path, delivery.WithEventContext(message, ctx))
}

// receiverWire hands a request, already inside the peer's asynchronous
// dispatch, to the receiver chosen for it.
type receiverWire struct{ receiver wire.Receiver }

func (w receiverWire) Send(path []string, message wire.Message) error {
	w.receiver.Message(path, message)
	return nil
}
