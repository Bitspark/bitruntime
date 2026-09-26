package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"sync"
	"time"

	"github.com/Bitspark/nightseam/duplex/go"
)

type routedFrame struct {
	path    []string
	message duplex.Message
	call    *routedCall
	refusal error
}

// A request reserves one cancellation at admission. Completed requests retain
// their reservation until an already queued control has drained, so repeated
// completion/admission cannot turn the control queue into an unbounded buffer.
type routedCall struct {
	cancel       context.CancelFunc
	completed    bool
	cancelQueued bool
	cancelled    bool
}

type returnKey struct {
	address *duplex.ReturnAddress
	id      string
}

// peerWire is the existing peer's relative dispatch surface. Its return
// associations stay beside the peer's carrier pending/incoming tables; views
// in duplex only choose a path and never correlate an id.
type peerWire struct {
	peer       *Peer
	queue      []routedFrame
	dataQueued int
	wake       chan struct{}
	mu         sync.Mutex
	incoming   map[returnKey]*routedCall
	receiver   *wireRegistration
}

type wireRegistration struct {
	receiver duplex.Receiver
}

type wireFrameKey struct{}
type wireDispatchContext struct {
	ctx           context.Context
	peer          *Peer
	frame         frame
	panic         func(any)
	maxFrameBytes int64
	completion    *wireCompletion
}

// Only a local handler can supply the cause of its cancellation. Serialized
// public errors, even one named cancelled, retain their ordinary error outcome.
type wireCompletion struct {
	mu           sync.Mutex
	cancellation error
}

// An event has no reply or request lifetime. Its local capability only retains
// the context already established by the receiving runtime across queued local
// composition; it is never reconstructed from event data or metadata.
type wireEventContext struct{ ctx context.Context }

func (*wireEventContext) Send([]string, duplex.Message) error {
	return errors.New("an event context is not a return address")
}
func eventContextOf(message duplex.Message) (context.Context, bool) {
	if message.Return != nil {
		if held, ok := message.Return.Wire.(*wireEventContext); ok {
			return held.ctx, true
		}
	}
	return nil, false
}
func withWireEventContext(message duplex.Message, ctx context.Context) duplex.Message {
	message.Return = &duplex.ReturnAddress{Wire: &wireEventContext{ctx: ctx}}
	return message
}

// Wire selects this peer's root origin. Repeated selection shares the peer,
// its queues and its carrier lifetime.
func (p *Peer) Wire() duplex.Endpoint {
	p.wireOnce.Do(func() {
		p.wire = &peerWire{peer: p, wake: make(chan struct{}, 1), incoming: map[returnKey]*routedCall{}}
		p.mu.Lock()
		p.requestFallback = p.wire.namespaceHandler
		p.eventFallback = p.wire.namespaceEvent
		p.mu.Unlock()
		go p.wire.run()
	})
	return p.wire
}

func (w *peerWire) Send(path []string, message duplex.Message) error {
	name, err := duplex.EncodePath(path)
	if err != nil {
		return err
	}
	if name == "" && (message.Frame.Kind == duplex.ProfileRequest || message.Frame.Kind == duplex.ProfileEvent) {
		return errors.New("a root wire operation needs a nonempty path")
	}
	if err := w.peer.Err(); err != nil {
		return err
	}
	if (message.Frame.Kind == duplex.ProfileRequest || message.Frame.Kind == duplex.ProfileCancel) && (message.Return == nil || message.Return.Wire == nil) {
		return errors.New("a wire request or cancellation requires a return address")
	}
	if message.Frame.Kind != duplex.ProfileRequest && message.Frame.Kind != duplex.ProfileEvent && message.Frame.Kind != duplex.ProfileCancel {
		return errors.New("a response is sent to its request's return address")
	}
	if err := validateWireFrame(name, message.Frame, w.peer.options.MaxFrameBytes); err != nil {
		return err
	}
	message.Frame.Params = append(json.RawMessage(nil), message.Frame.Params...)
	message.Frame.Data = append(json.RawMessage(nil), message.Frame.Data...)
	message.Frame.Meta = maps.Clone(message.Frame.Meta)
	delivered := routedFrame{path: append([]string(nil), path...), message: message}
	key := returnKey{message.Return, message.Frame.ID}
	w.mu.Lock()
	if err := w.peer.Err(); err != nil {
		w.mu.Unlock()
		return err
	}
	if message.Frame.Kind == duplex.ProfileCancel {
		call := w.incoming[key]
		if call == nil || call.completed || call.cancelQueued || call.cancelled {
			w.mu.Unlock()
			return nil
		}
		call.cancelQueued = true
		delivered.call = call
	} else {
		if w.dataQueued >= w.peer.options.QueueCapacity {
			depth := w.dataQueued
			w.mu.Unlock()
			w.peer.observeBackpressure(depth, true, w.peer.options.WriteTimeout)
			w.peer.fail(ErrBackpressure)
			return ErrBackpressure
		}
		w.dataQueued++
		if message.Frame.Kind == duplex.ProfileRequest {
			if w.incoming[key] != nil {
				delivered.refusal = &PublicError{Code: "invalid_message", Message: "Duplicate active request identifier"}
			} else if len(w.incoming) >= w.peer.options.MaxPendingRequests {
				delivered.refusal = &PublicError{Code: "busy", Message: "Outstanding call limit reached"}
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
func (w *peerWire) retireLocked(key returnKey, call *routedCall) {
	if call.completed && !call.cancelQueued && w.incoming[key] == call {
		delete(w.incoming, key)
	}
}

func (w *peerWire) complete(key returnKey, call *routedCall) {
	w.mu.Lock()
	call.completed = true
	w.retireLocked(key, call)
	w.mu.Unlock()
}

func (w *peerWire) next() (routedFrame, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.queue) == 0 {
		return routedFrame{}, false
	}
	delivered := w.queue[0]
	w.queue[0] = routedFrame{}
	w.queue = w.queue[1:]
	if delivered.message.Frame.Kind != duplex.ProfileCancel {
		w.dataQueued--
	}
	return delivered, true
}

func (w *peerWire) Close(code duplex.Code, reason string) error {
	w.peer.end(ErrClosed, code, reason)
	return nil
}

func (w *peerWire) run() {
	defer func() {
		w.mu.Lock()
		var receivers []duplex.Receiver
		var cancels []context.CancelFunc
		if w.receiver != nil {
			receivers = append(receivers, w.receiver.receiver)
		}
		w.receiver = nil
		for _, call := range w.incoming {
			if call.cancel != nil {
				cancels = append(cancels, call.cancel)
			}
		}
		w.incoming = map[returnKey]*routedCall{}
		w.queue = nil
		w.dataQueued = 0
		w.mu.Unlock()
		for _, cancel := range cancels {
			cancel()
		}
		for _, receiver := range receivers {
			if receiver.Closed != nil {
				receiver.Closed(duplex.CodeGoingAway, "peer ended")
			}
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
		case duplex.ProfileCancel:
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
		case duplex.ProfileRequest:
			if delivered.refusal != nil {
				sendWireResponse(delivered.message, nil, delivered.refusal)
				continue
			}
			state := delivered.call
			ctx := w.peer.options.Propagator.Extract(w.peer.Context(), Trace{Parent: f.Traceparent, State: f.Tracestate})
			ctx = WithMeta(ctx, f.Meta)
			ctx, cancel := context.WithCancel(ctx)
			name, err := duplex.EncodePath(delivered.path)
			var call *admittedCall
			if err == nil {
				call, err = w.peer.beginCallTrace(ctx, name, f.Params, &Trace{Parent: f.Traceparent, State: f.Tracestate}, true)
			}
			if err != nil {
				cancel()
				w.complete(key, state)
				sendWireResponse(delivered.message, nil, WithoutUnpublishedProof(err))
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
				sendWireResponse(delivered.message, result, WithoutUnpublishedProof(err))
			}()
		case duplex.ProfileEvent:
			name, err := duplex.EncodePath(delivered.path)
			ctx := w.peer.options.Propagator.Extract(w.peer.Context(), Trace{Parent: f.Traceparent, State: f.Tracestate})
			if err == nil {
				err = w.peer.emitTrace(WithMeta(ctx, f.Meta), name, f.Data, &Trace{Parent: f.Traceparent, State: f.Tracestate}, true)
			}
			if err != nil {
				w.peer.fail(err)
			}
		}
	}
}

func (w *peerWire) Receive(receiver duplex.Receiver) (func(), error) {
	registration := &wireRegistration{receiver: receiver}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.peer.Err(); err != nil {
		return nil, err
	}
	if w.receiver != nil {
		return nil, duplex.ErrReceiverExists
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

// The profile presents canonical addressed operations to its one attachment.
// Registration and path precedence belong to an explicit Dispatcher.
func (w *peerWire) namespace(name string) ([]string, *wireRegistration) {
	path, err := duplex.DecodePath(name)
	if err != nil {
		return nil, nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	registration := w.receiver
	if registration == nil {
		return nil, nil
	}
	return path, registration
}

func (w *peerWire) namespaceHandler(name string) Handler {
	path, registration := w.namespace(name)
	if registration == nil {
		return nil
	}
	return w.requestReceiver(path, registration.receiver)
}

func (w *peerWire) namespaceEvent(name string) EventHandler {
	path, registration := w.namespace(name)
	if registration == nil {
		return nil
	}
	return w.eventReceiver(path, registration.receiver)
}

func (w *peerWire) requestReceiver(path []string, receiver duplex.Receiver) Handler {
	return func(ctx context.Context, _ *Peer, params json.RawMessage) (any, error) {
		if receiver.Message == nil {
			return nil, &PublicError{Code: "method_not_found", Message: "Unknown method"}
		}
		var result json.RawMessage
		incoming, _ := ctx.Value(wireFrameKey{}).(frame)
		dispatch := &wireDispatchContext{ctx: ctx, peer: w.peer, frame: incoming}
		// The chosen registration stays with this request. Later detach or
		// replacement cannot redirect its correlated cancellation.
		err := callWire(WithMeta(ctx, MetaFrom(ctx)), &receiverWire{receiver: receiver}, append([]string{}, path...), params, &result, dispatch)
		return result, err
	}
}

func (w *peerWire) eventReceiver(path []string, receiver duplex.Receiver) EventHandler {
	return func(ctx context.Context, _ *Peer, data json.RawMessage) {
		if receiver.Message == nil {
			return
		}
		incoming, _ := ctx.Value(wireFrameKey{}).(frame)
		receiver.Message(append([]string{}, path...), withWireEventContext(duplex.Message{Frame: duplex.ProfileFrame{Version: 1, Kind: duplex.ProfileEvent, Data: data, Traceparent: incoming.Traceparent, Tracestate: incoming.Tracestate, Meta: MetaFrom(ctx)}}, ctx))
	}
}

// ForwardWire joins two existing origins without allocating a peer or channel.
// Detach removes only the forwarding registrations; both wires remain owned by
// their callers. Each root remains responsible for ending its failed carrier.
func ForwardWire(inbound, outbound duplex.Endpoint) (func(), error) {
	if inbound == nil || outbound == nil {
		return nil, errors.New("wire forwarding requires two origins")
	}
	var mu sync.Mutex
	var detaches []func()
	ended := false
	stop := func() {
		mu.Lock()
		if ended {
			mu.Unlock()
			return
		}
		ended = true
		owned := detaches
		detaches = nil
		mu.Unlock()
		for _, detach := range owned {
			detach()
		}
	}
	receiver := func(destination duplex.Wire) duplex.Receiver {
		return duplex.Receiver{Closed: func(duplex.Code, string) { stop() }, Message: func(path []string, message duplex.Message) {
			if err := destination.Send(path, message); err != nil {
				stop()
				if message.Frame.Kind == duplex.ProfileRequest {
					sendWireResponse(message, nil, WithoutUnpublishedProof(err))
				}
			}
		}}
	}
	for _, direction := range []struct{ source, destination duplex.Endpoint }{{inbound, outbound}, {outbound, inbound}} {
		detach, err := direction.source.Receive(receiver(direction.destination))
		if err != nil {
			stop()
			return nil, err
		}
		mu.Lock()
		active := !ended
		if active {
			detaches = append(detaches, detach)
		}
		mu.Unlock()
		if !active {
			detach()
			return nil, ErrClosed
		}
	}
	return stop, nil
}

// receiverWire is used only inside the peer's already asynchronous request
// dispatch. It reuses the same completion primitive when handing a decoded
// request to a generated wire receiver.
type receiverWire struct{ receiver duplex.Receiver }

func (w *receiverWire) Send(path []string, message duplex.Message) error {
	w.receiver.Message(path, message)
	return nil
}

type replyWire struct {
	id         string
	reply      chan pendingResult
	done       chan struct{}
	once       sync.Once
	dispatch   *wireDispatchContext
	invocation *Invocation
}

// Invocation exposes this return capability's lifecycle to the runtime that
// owns it. The vocabulary reaches it through Send like any participant's.
func (w *replyWire) Invocation() *Invocation { return w.invocation }

func (w *replyWire) wireDispatch() *wireDispatchContext { return w.dispatch }

func (w *replyWire) Send(path []string, message duplex.Message) error {
	if len(path) != 0 {
		return w.invocation.Deliver(path, message)
	}
	if message.Frame.Kind != duplex.ProfileResponse || message.Frame.ID != w.id {
		return errors.New("invalid wire response")
	}
	var limit int64
	if w.dispatch != nil {
		limit = w.dispatch.maxFrameBytes
		if limit == 0 && w.dispatch.peer != nil {
			limit = w.dispatch.peer.options.MaxFrameBytes
		}
	}
	if err := validateWireFrame("", message.Frame, limit); err != nil {
		return err
	}
	r := pendingResult{result: message.Frame.Result}
	if f := message.Frame.Error; f != nil {
		r.err = &PublicError{Code: f.Code, Message: f.Message, Data: f.Data}
		if f.Code == "cancelled" && w.dispatch != nil && w.dispatch.ctx.Err() != nil && w.dispatch.completion != nil {
			completion := w.dispatch.completion
			completion.mu.Lock()
			if completion.cancellation != nil {
				r.err = completion.cancellation
			}
			completion.mu.Unlock()
		}
	}
	select {
	case <-w.done:
		return ErrClosed
	default:
	}
	select {
	case w.reply <- r:
		w.invocation.Settle()
		return nil
	default:
		return errors.New("duplicate wire response")
	}
}
func (w *replyWire) finish() error {
	w.once.Do(func() {
		close(w.done)
		w.invocation.Settle()
		w.invocation.DispatchDone()
	})
	return nil
}

// CallWire calls a relative operation through the peer's request primitive.
// Its local return address is independent of every other call's identifier.
func CallWire(ctx context.Context, wire duplex.Wire, path []string, params, result any, options ...WireCallOptions) error {
	return callWire(ctx, wire, path, params, result, nil, options...)
}
func callWire(ctx context.Context, wire duplex.Wire, path []string, params, result any, dispatch *wireDispatchContext, options ...WireCallOptions) (err error) {
	if ctx == nil || wire == nil {
		return Unpublished(errors.New("a wire call requires a context and wire"))
	}
	if err := ctx.Err(); err != nil {
		return Unpublished(err)
	}
	name, err := duplex.EncodePath(path)
	if err != nil {
		return Unpublished(errors.New("a wire call requires a valid operation path"))
	}
	encoded, err := MarshalJSON(params)
	if err != nil {
		return Unpublished(err)
	}
	var observation WireCallOptions
	if len(options) > 0 {
		observation = options[0]
	}
	if observation.RequestTimeout < 0 {
		return Unpublished(errors.New("wire request timeout must not be negative"))
	}
	// A forwarded request already has its carrier's admitted deadline.
	if dispatch == nil {
		timeout := observation.RequestTimeout
		if timeout == 0 {
			timeout = 30 * time.Second
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	if dispatch != nil {
		copied := *dispatch
		copied.completion = &wireCompletion{}
		dispatch = &copied
	}
	returning := &replyWire{id: "c:1", reply: make(chan pendingResult, 1), done: make(chan struct{}), dispatch: dispatch, invocation: NewInvocation(DefaultInvocationLimits(), nil)}
	defer returning.finish()
	address := &duplex.ReturnAddress{Wire: returning}
	var trace Trace
	if dispatch != nil {
		trace = Trace{Parent: dispatch.frame.Traceparent, State: dispatch.frame.Tracestate}
	} else {
		propagator := observation.Propagator
		if propagator == nil {
			propagator = DefaultPropagator
		}
		trace = propagator.Inject(ctx)
	}
	finish := observeWireRequest(observation.Observer, observation.Family, name, false, trace)
	defer func() { finish(err) }()
	request := duplex.Message{Frame: duplex.ProfileFrame{Version: 1, Kind: duplex.ProfileRequest, ID: returning.id, Params: encoded, Traceparent: trace.Parent, Tracestate: trace.State, Meta: outgoingMeta(ctx)}, Return: address}
	if err := wire.Send(path, request); err != nil {
		return Unpublished(err)
	}
	cancelRemote, err := awaitReply(ctx, returning.reply, returning.done, func() error { return ErrClosed }, result)
	finish(err)
	if cancelRemote {
		_ = wire.Send(path, duplex.Message{Frame: duplex.ProfileFrame{Version: 1, Kind: duplex.ProfileCancel, ID: returning.id, Traceparent: trace.Parent, Tracestate: trace.State}, Return: address})
		if dispatch != nil {
			// This waiter is the carrier's admitted handler, not the outgoing
			// caller. Cancellation reaches the body immediately, but its slot
			// remains occupied until the receiver actually finishes its work.
			_, err = awaitReply(context.WithoutCancel(ctx), returning.reply, returning.done, func() error { return ErrClosed }, result)
		}
	}
	return err
}

// WireHandler is a typed adapter's decoded request body, independent of the
// concrete carrier. The runtime supplies cancellation and response routing.
type WireHandler func(context.Context, json.RawMessage) (any, error)

// WireEventHandler receives an event body beside its carried context.
type WireEventHandler func(context.Context, json.RawMessage) error

// WireHandlers groups a method and event that share one declared name.
type WireHandlers struct {
	Request  WireHandler
	Event    WireEventHandler
	Observer Observer
	Family   string
}

// AdapterContext carries runtime options used when constructing model wires.
type AdapterContext struct {
	Options          Options
	ValueEnvironment ValueEnvironment
}

// EmitWire admits one event at a relative path. The return says only that the
// destination accepted it; processing and transport remain asynchronous.
func EmitWire(ctx context.Context, wire duplex.Wire, path []string, data any, options ...WireEmitOptions) error {
	if ctx == nil || wire == nil {
		return Unpublished(errors.New("a wire event requires a context and wire"))
	}
	if err := ctx.Err(); err != nil {
		return Unpublished(err)
	}
	name, err := duplex.EncodePath(path)
	if err != nil {
		return Unpublished(errors.New("a wire event requires a valid operation path"))
	}
	encoded, err := MarshalJSON(data)
	if err != nil {
		return Unpublished(err)
	}
	propagator := DefaultPropagator
	if len(options) > 0 && options[0].Propagator != nil {
		propagator = options[0].Propagator
	}
	trace := propagator.Inject(ctx)
	if len(options) > 0 && options[0].Observer != nil {
		observeWire(options[0].Observer, EventEmitted{At: time.Now(), Name: name, Bytes: len(encoded), Trace: trace, Family: options[0].Family})
	}
	return Unpublished(wire.Send(path, duplex.Message{Frame: duplex.ProfileFrame{Version: 1, Kind: duplex.ProfileEvent, Data: encoded, Traceparent: trace.Parent, Tracestate: trace.State, Meta: outgoingMeta(ctx)}}))
}

// HandleWire registers one relative operation. The receiver returns before
// running application code, and request cancellation uses its return address.
func HandleWire(wire HandlerRegistry, path []string, handler WireHandler) (func(), error) {
	if wire == nil || handler == nil {
		return nil, errors.New("a wire handler requires a wire and body")
	}
	return RegisterWire(wire, path, WireHandlers{Request: handler})
}

// RegisterWire installs a single receiver for a declared method, event, or both.
// The one detach removes the group; an event-only path refuses requests.
func RegisterWire(wire HandlerRegistry, path []string, handlers WireHandlers) (func(), error) {
	if wire == nil || (handlers.Request == nil && handlers.Event == nil) {
		return nil, errors.New("wire registration requires a wire and at least one handler")
	}
	name, err := duplex.EncodePath(path)
	if err != nil {
		return nil, err
	}
	var mu sync.Mutex
	incoming := map[returnKey]context.CancelFunc{}
	return wire.Register(path, duplex.Receiver{
		Closed: func(duplex.Code, string) {
			mu.Lock()
			defer mu.Unlock()
			for _, cancel := range incoming {
				cancel()
			}
		},
		Message: func(_ []string, message duplex.Message) {
			if message.Frame.Kind == duplex.ProfileEvent {
				if handlers.Event != nil {
					if handlers.Observer != nil {
						observeWire(handlers.Observer, EventDelivered{At: time.Now(), Name: name, Bytes: len(message.Frame.Data),
							Trace: Trace{Parent: message.Frame.Traceparent, State: message.Frame.Tracestate}, Family: handlers.Family})
					}
					ctx, associated := eventContextOf(message)
					if !associated {
						ctx = DefaultPropagator.Extract(context.Background(), Trace{Parent: message.Frame.Traceparent, State: message.Frame.Tracestate})
					}
					if err := invokeWireEvent(withIncomingMeta(ctx, message.Frame.Meta), handlers.Event, message.Frame.Data); err != nil {
						_ = wire.Close(duplex.CodeProtocolError, "wire event rejected")
					}
				}
				return
			}
			key := returnKey{message.Return, message.Frame.ID}
			if message.Frame.Kind == duplex.ProfileCancel {
				mu.Lock()
				cancel := incoming[key]
				mu.Unlock()
				if cancel != nil {
					cancel()
				}
				return
			}
			if message.Frame.Kind != duplex.ProfileRequest {
				return
			}
			finish := observeWireRequest(handlers.Observer, handlers.Family, name, true,
				Trace{Parent: message.Frame.Traceparent, State: message.Frame.Tracestate})
			if handlers.Request == nil {
				err := &PublicError{Code: "method_not_found", Message: "Unknown method"}
				finish(sendWireResponse(message, nil, err))
				return
			}
			var dispatch *wireDispatchContext
			base := context.Background()
			if message.Return != nil {
				if returning, ok := message.Return.Wire.(interface{ wireDispatch() *wireDispatchContext }); ok {
					dispatch = returning.wireDispatch()
				}
			}
			if dispatch != nil {
				base = dispatch.ctx
			}
			ctx := DefaultPropagator.Extract(base, Trace{Parent: message.Frame.Traceparent, State: message.Frame.Tracestate})
			ctx, cancel := context.WithCancel(withIncomingMeta(ctx, message.Frame.Meta))
			mu.Lock()
			if incoming[key] != nil {
				mu.Unlock()
				cancel()
				err := &PublicError{Code: "invalid_message", Message: "Duplicate active request identifier"}
				finish(sendWireResponse(message, nil, err))
				return
			}
			incoming[key] = cancel
			mu.Unlock()
			// The body runs after this receiver returns, so returning is not
			// completion. The lease says so to whoever admitted the request:
			// an early answer to the caller cannot retire an invocation whose
			// body is still running. A return capability that carries no
			// lifecycle still gets ordinary addressed delivery.
			// A bound reached is a refusal; any other refusal means this
			// return capability carries no lifecycle, and ordinary addressed
			// delivery goes on without one.
			body, leaseErr := BeginInvocationBody(message)
			if errors.Is(leaseErr, ErrInvocationLimit) {
				mu.Lock()
				delete(incoming, key)
				mu.Unlock()
				cancel()
				err := &PublicError{Code: "busy", Message: "Invocation participation limit reached"}
				finish(sendWireResponse(message, nil, err))
				return
			}
			go func() {
				defer func() { body.Done(); cancel(); mu.Lock(); delete(incoming, key); mu.Unlock() }()
				result, err := invokeWireHandler(ctx, handlers.Request, message.Frame.Params, dispatch)
				if err == nil {
					err = ctx.Err()
				}
				data, marshalErr := MarshalJSON(result)
				if err == nil {
					err = marshalErr
				}
				if dispatch != nil && dispatch.completion != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
					dispatch.completion.mu.Lock()
					dispatch.completion.cancellation = err
					dispatch.completion.mu.Unlock()
				}
				finish(sendWireResponse(message, data, WithoutUnpublishedProof(err)))
			}()
		},
	})
}

func invokeWireEvent(ctx context.Context, handler WireEventHandler, data json.RawMessage) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("wire event handler panic")
		}
	}()
	return handler(ctx, data)
}

func invokeWireHandler(ctx context.Context, handler WireHandler, params json.RawMessage, dispatch *wireDispatchContext) (result any, err error) {
	defer func() {
		if value := recover(); value != nil {
			if dispatch != nil {
				if dispatch.panic != nil {
					dispatch.panic(value)
				} else if dispatch.peer != nil {
					dispatch.peer.observePanic(dispatch.frame, value)
				}
			}
			err = errors.New("wire handler panic")
		}
	}()
	return handler(ctx, params)
}

func sendWireResponse(request duplex.Message, result json.RawMessage, err error) error {
	if request.Return == nil || request.Return.Wire == nil {
		return ErrClosed
	}
	f := duplex.ProfileFrame{Version: 1, Kind: duplex.ProfileResponse, ID: request.Frame.ID, Result: result, Traceparent: request.Frame.Traceparent, Tracestate: request.Frame.Tracestate}
	if err != nil {
		var public *PublicError
		switch {
		case errors.As(err, &public) && public != nil && public.Code != "" && public.Message != "":
			f.Error = &duplex.ProfileError{Code: public.Code, Message: public.Message, Data: public.Data}
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			f.Error = &duplex.ProfileError{Code: "cancelled", Message: "Request cancelled"}
		case errors.Is(err, ErrClosed):
			f.Error = &duplex.ProfileError{Code: "disconnected", Message: "Connection ended; outcome may be unknown"}
		default:
			f.Error = &duplex.ProfileError{Code: "internal", Message: "Internal error"}
		}
		f.Result = nil
		// Preserve the local cancellation cause, but otherwise observe exactly
		// the normalized public error selected for this response.
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			err = &PublicError{Code: f.Error.Code, Message: f.Error.Message, Data: f.Error.Data}
		}
	}
	if sendErr := request.Return.Wire.Send(nil, duplex.Message{Frame: f}); sendErr != nil {
		// A malformed or oversized public result must settle as a bounded
		// refusal, just as the carrier peer's respond does.
		f.Result = nil
		f.Error = &duplex.ProfileError{Code: "internal", Message: "Response could not be encoded"}
		if fallbackErr := request.Return.Wire.Send(nil, duplex.Message{Frame: f}); fallbackErr == nil {
			return &PublicError{Code: f.Error.Code, Message: f.Error.Message}
		}
		// A caller that already withdrew cannot receive either response. Its
		// selected refusal remains that refusal; a failed success is no success.
		if err == nil {
			return WithoutUnpublishedProof(sendErr)
		}
	}
	return err
}

// The structured boundary uses the profile's existing validator. A logical
// return address identifies an origin independently of the carrier role, so
// either profile identifier prefix is valid before the peer remaps it.
func validateWireFrame(name string, value duplex.ProfileFrame, limit int64) error {
	f := frame{Version: value.Version, Kind: string(value.Kind), ID: value.ID,
		Params: value.Params, Result: value.Result, Data: value.Data,
		Traceparent: value.Traceparent, Tracestate: value.Tracestate, Meta: value.Meta}
	if value.Error != nil {
		f.Error = &PublicError{Code: value.Error.Code, Message: value.Error.Message, Data: value.Error.Data}
	}
	switch value.Kind {
	case duplex.ProfileRequest:
		f.Method = name
	case duplex.ProfileEvent:
		f.Event = name
	}
	data, err := MarshalJSON(f)
	if err != nil {
		return err
	}
	if limit > 0 && int64(len(data)) > limit {
		return errors.New("wire frame exceeds the carrier limit")
	}
	if _, err := decodeFrame(data); err != nil {
		return err
	}
	if f.ID != "" && !validID(f.ID, "c:") && !validID(f.ID, "s:") {
		return errors.New("invalid wire request identifier")
	}
	return nil
}
