package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/Bitspark/nightseam/duplex/go"
)

// NewWirePair constructs a bounded local carrier with two relative origins.
// Sending on either endpoint delivers to receivers on the other. It allocates
// no Peer and preserves structured frames and verified local request context.
// The limits, propagator and observer in options apply to both directions.
func NewWirePair(options Options) (left, right duplex.Endpoint, err error) {
	o, err := options.normalized()
	if err != nil {
		return nil, nil, err
	}
	if len(o.Handlers) != 0 || len(o.Events) != 0 || o.Prepare != nil {
		return nil, nil, errors.New("local wires install receivers through Receive")
	}
	p := &localWirePair{options: o, done: make(chan struct{})}
	for i := range p.ends {
		p.ends[i] = &localWire{pair: p, wake: make(chan struct{}, 1), calls: map[returnKey]*localWireCall{}}
	}
	p.ends[0].other, p.ends[1].other = p.ends[1], p.ends[0]
	for _, end := range p.ends {
		go end.run()
	}
	return p.ends[0], p.ends[1], nil
}

type localWirePair struct {
	mu      sync.Mutex
	options Options
	ends    [2]*localWire
	done    chan struct{}
	closed  bool
}

type localRegistration struct {
	receiver duplex.Receiver
	active   bool
}
type localDelivery struct {
	path    []string
	message duplex.Message
	call    *localWireCall
	refusal error
}
type localWire struct {
	pair       *localWirePair
	other      *localWire
	wake       chan struct{}
	queue      []localDelivery
	dataQueued int
	active     int
	eventTimer *time.Timer
	calls      map[returnKey]*localWireCall
	receiver   *localRegistration
}
type localWireCall struct {
	key          returnKey
	path         []string
	message      duplex.Message
	returning    *duplex.ReturnAddress
	registration *localRegistration
	dispatch     *wireDispatchContext
	invocation   *Invocation
	cancel       context.CancelFunc
	timer        *time.Timer
	completed    bool
	responded    bool
	active       bool
	cancelQueued bool
	cancelled    bool
}

func (w *localWire) Send(path []string, message duplex.Message) error {
	name, err := duplex.EncodePath(path)
	if err != nil {
		return err
	}
	if err := validateWireFrame(name, message.Frame, w.pair.options.MaxFrameBytes); err != nil {
		return err
	}
	if message.Frame.Kind != duplex.ProfileRequest && message.Frame.Kind != duplex.ProfileEvent && message.Frame.Kind != duplex.ProfileCancel {
		return errors.New("a response is sent to its request's return address")
	}
	if message.Frame.Kind != duplex.ProfileEvent && (message.Return == nil || message.Return.Wire == nil) {
		return errors.New("a wire request or cancellation requires a return address")
	}
	message.Frame.Params = append(json.RawMessage(nil), message.Frame.Params...)
	message.Frame.Data = append(json.RawMessage(nil), message.Frame.Data...)
	message.Frame.Meta = maps.Clone(message.Frame.Meta)
	return w.other.admit(append([]string(nil), path...), message)
}

func (w *localWire) admit(path []string, message duplex.Message) error {
	p := w.pair
	key := returnKey{message.Return, message.Frame.ID}
	delivery := localDelivery{path: path, message: message}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrClosed
	}
	if message.Frame.Kind == duplex.ProfileCancel {
		call := w.calls[key]
		if call == nil || call.completed || call.cancelQueued || call.cancelled {
			p.mu.Unlock()
			return nil
		}
		call.cancelQueued = true
		delivery.call = call
		delivery.message.Return = call.returning
	} else {
		if w.dataQueued >= p.options.QueueCapacity {
			depth := w.dataQueued
			p.mu.Unlock()
			p.observe(Backpressure{At: time.Now(), Queued: depth, Stalled: true, Deadline: p.options.WriteTimeout})
			p.end(duplex.CodeDuplex, "local wire queue limit reached")
			return ErrBackpressure
		}
		w.dataQueued++
		if message.Frame.Kind == duplex.ProfileRequest {
			if w.calls[key] != nil {
				delivery.refusal = &PublicError{Code: "invalid_message", Message: "Duplicate active request identifier"}
			} else if len(w.calls) >= p.options.MaxPendingRequests {
				delivery.refusal = &PublicError{Code: "busy", Message: "Outstanding call limit reached"}
			} else {
				call := &localWireCall{key: key, path: path, message: message, invocation: NewInvocation(DefaultInvocationLimits(), nil)}
				call.returning = &duplex.ReturnAddress{Wire: &localReturn{wire: w, call: call}}
				w.calls[key] = call
				delivery.call, delivery.message.Return = call, call.returning
			}
		}
	}
	w.queue = append(w.queue, delivery)
	p.mu.Unlock()
	w.signal()
	return nil
}

func (w *localWire) signal() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}
func (w *localWire) retireLocked(call *localWireCall) {
	if !call.completed || call.cancelQueued || w.calls[call.key] != call {
		return
	}
	delete(w.calls, call.key)
	call.invocation.Settle()
	call.invocation.DispatchDone()
}
func (w *localWire) complete(call *localWireCall) {
	w.pair.mu.Lock()
	if !call.completed {
		call.completed = true
		if call.active {
			w.active--
			call.active = false
		}
		if call.timer != nil {
			call.timer.Stop()
		}
		if call.cancel != nil {
			call.cancel()
		}
	}
	w.retireLocked(call)
	w.pair.mu.Unlock()
}

func (w *localWire) next() (localDelivery, bool) {
	w.pair.mu.Lock()
	defer w.pair.mu.Unlock()
	if w.pair.closed || len(w.queue) == 0 {
		return localDelivery{}, false
	}
	delivery := w.queue[0]
	w.queue[0] = localDelivery{}
	w.queue = w.queue[1:]
	if delivery.message.Frame.Kind != duplex.ProfileCancel {
		w.dataQueued--
	}
	return delivery, true
}
func (w *localWire) run() {
	for {
		delivery, ok := w.next()
		if !ok {
			select {
			case <-w.pair.done:
				return
			case <-w.wake:
				continue
			}
		}
		if delivery.refusal != nil {
			sendWireResponse(delivery.message, nil, delivery.refusal)
			continue
		}
		if delivery.message.Frame.Kind == duplex.ProfileCancel {
			w.deliverCancel(delivery.call, delivery.message)
			continue
		}
		w.pair.mu.Lock()
		registration := w.receiver
		if registration != nil && registration.receiver.Message == nil {
			registration = nil
		}
		if w.pair.closed {
			w.pair.mu.Unlock()
			return
		}
		if delivery.call != nil {
			if registration == nil || w.active >= w.pair.options.MaxConcurrentHandlers {
				w.pair.mu.Unlock()
				code, message := "method_not_found", "Unknown method"
				if registration != nil {
					code, message = "busy", "Too many concurrent requests"
				}
				sendWireResponse(delivery.message, nil, &PublicError{Code: code, Message: message})
				continue
			}
			call := delivery.call
			call.registration, call.active = registration, true
			w.active++
			base := context.Background()
			if source, ok := call.key.address.Wire.(interface{ wireDispatch() *wireDispatchContext }); ok {
				call.dispatch = source.wireDispatch()
				if call.dispatch != nil {
					base = call.dispatch.ctx
				}
			}
			if call.dispatch == nil {
				base = w.pair.options.Propagator.Extract(base, Trace{Parent: delivery.message.Frame.Traceparent, State: delivery.message.Frame.Tracestate})
			}
			ctx, cancel := context.WithCancel(base)
			call.cancel = cancel
			if call.dispatch != nil {
				copied := *call.dispatch
				copied.ctx = ctx
				call.dispatch = &copied
			} else {
				name, _ := duplex.EncodePath(delivery.path)
				call.dispatch = &wireDispatchContext{ctx: ctx, maxFrameBytes: w.pair.options.MaxFrameBytes, panic: func(value any) {
					w.pair.observe(HandlerPanic{At: time.Now(), Method: name, Value: fmt.Sprint(value), Family: w.pair.options.Families[name]})
				}}
			}
			call.timer = time.AfterFunc(w.pair.options.RequestTimeout, func() { w.timeout(call) })
		}
		w.pair.mu.Unlock()
		if registration == nil {
			continue
		}
		if delivery.message.Frame.Kind == duplex.ProfileEvent {
			if _, associated := eventContextOf(delivery.message); !associated {
				ctx := w.pair.options.Propagator.Extract(context.Background(), Trace{Parent: delivery.message.Frame.Traceparent, State: delivery.message.Frame.Tracestate})
				delivery.message = withWireEventContext(delivery.message, ctx)
			}
			w.pair.mu.Lock()
			w.eventTimer = time.AfterFunc(w.pair.options.WriteTimeout, func() {
				w.pair.mu.Lock()
				closed, depth := w.pair.closed, w.dataQueued
				w.pair.mu.Unlock()
				if !closed {
					w.pair.observe(Backpressure{At: time.Now(), Queued: depth, Stalled: true, Deadline: w.pair.options.WriteTimeout})
					w.pair.end(duplex.CodeDuplex, "local wire event consumer stalled")
				}
			})
			w.pair.mu.Unlock()
		}
		w.deliver(registration, delivery.path, delivery.message)
		if delivery.message.Frame.Kind == duplex.ProfileEvent {
			w.pair.mu.Lock()
			w.eventTimer.Stop()
			w.eventTimer = nil
			w.pair.mu.Unlock()
		}
	}
}

func (w *localWire) deliver(registration *localRegistration, path []string, message duplex.Message) {
	defer func() {
		if value := recover(); value != nil {
			name, _ := duplex.EncodePath(path)
			w.pair.observe(HandlerPanic{At: time.Now(), Method: name, Value: fmt.Sprint(value), Family: w.pair.options.Families[name]})
			if message.Frame.Kind == duplex.ProfileRequest {
				sendWireResponse(message, nil, errors.New("wire receiver panic"))
			} else {
				w.pair.end(duplex.CodeDuplex, "wire event receiver failed")
			}
		}
	}()
	registration.receiver.Message(path, message)
}

func (w *localWire) deliverCancel(call *localWireCall, message duplex.Message) {
	w.pair.mu.Lock()
	call.cancelQueued, call.cancelled = false, true
	registration, completed := call.registration, call.completed
	if call.cancel != nil {
		call.cancel()
	}
	w.retireLocked(call)
	w.pair.mu.Unlock()
	if !completed && registration != nil {
		w.deliver(registration, call.path, message)
	}
}

func (w *localWire) timeout(call *localWireCall) {
	w.pair.mu.Lock()
	if w.pair.closed || call.completed {
		w.pair.mu.Unlock()
		return
	}
	if call.cancel != nil {
		call.cancel()
	}
	if !call.cancelQueued && !call.cancelled {
		call.cancelQueued = true
		w.queue = append(w.queue, localDelivery{path: call.path, message: duplex.Message{Frame: duplex.ProfileFrame{Version: 1, Kind: duplex.ProfileCancel, ID: call.message.Frame.ID}, Return: call.returning}, call: call})
	}
	respond := !call.responded
	call.responded = true
	w.pair.mu.Unlock()
	w.signal()
	// A timeout answers the caller but retains the handler's budget until its
	// actual response. An application ignoring cancellation cannot spawn an
	// unbounded number of replacement handlers by repeatedly timing out.
	if respond {
		sendWireResponse(call.message, nil, context.DeadlineExceeded)
	}
}

func (w *localWire) Receive(receiver duplex.Receiver) (func(), error) {
	registration := &localRegistration{receiver: receiver, active: true}
	w.pair.mu.Lock()
	defer w.pair.mu.Unlock()
	if w.pair.closed {
		return nil, ErrClosed
	}
	if w.receiver != nil {
		return nil, duplex.ErrReceiverExists
	}
	w.receiver = registration
	return func() {
		w.pair.mu.Lock()
		if w.receiver == registration {
			w.receiver = nil
			registration.active = false
		}
		w.pair.mu.Unlock()
	}, nil
}
func (w *localWire) Close(code duplex.Code, reason string) error {
	w.pair.end(code, reason)
	return nil
}
func (p *localWirePair) observe(event ObserverEvent) {
	if p.options.Observer != nil {
		func() { defer func() { _ = recover() }(); p.options.Observer.Observe(event) }()
	}
}
func (p *localWirePair) end(code duplex.Code, reason string) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.done)
	var receivers []duplex.Receiver
	var requests []duplex.Message
	for _, end := range p.ends {
		if end.eventTimer != nil {
			end.eventTimer.Stop()
		}
		if end.receiver != nil {
			end.receiver.active = false
			receivers = append(receivers, end.receiver.receiver)
		}
		for _, call := range end.calls {
			if call.timer != nil {
				call.timer.Stop()
			}
			if call.cancel != nil {
				call.cancel()
			}
			if !call.responded {
				requests = append(requests, call.message)
				call.responded = true
			}
			call.completed = true
			call.invocation.Settle()
			call.invocation.DispatchDone()
		}
		end.receiver = nil
		end.calls = map[returnKey]*localWireCall{}
		end.queue, end.dataQueued = nil, 0
	}
	p.mu.Unlock()
	p.observe(ConnectionClosed{At: time.Now(), Code: int(code), Reason: reason, Local: true})
	go func() {
		for _, receiver := range receivers {
			if receiver.Closed != nil {
				func() { defer func() { _ = recover() }(); receiver.Closed(code, reason) }()
			}
		}
		for _, request := range requests {
			sendWireResponse(request, nil, ErrClosed)
		}
	}()
}

type localReturn struct {
	wire *localWire
	call *localWireCall
}

func (r *localReturn) wireDispatch() *wireDispatchContext { return r.call.dispatch }

// Invocation exposes this return capability's lifecycle to the pair that owns
// it. Participants reach the same state through the vocabulary on Send.
func (r *localReturn) Invocation() *Invocation { return r.call.invocation }

func (r *localReturn) Send(path []string, message duplex.Message) (err error) {
	if len(path) != 0 {
		return r.call.invocation.Deliver(path, message)
	}
	if message.Frame.Kind != duplex.ProfileResponse || message.Frame.ID != r.call.message.Frame.ID {
		return errors.New("invalid wire response")
	}
	if err := validateWireFrame("", message.Frame, r.wire.pair.options.MaxFrameBytes); err != nil {
		return err
	}
	// Validation refuses an attempt before completion, allowing the shared
	// response helper to substitute its bounded internal-error fallback.
	defer r.wire.complete(r.call)
	defer func() {
		if value := recover(); value != nil {
			err = fmt.Errorf("wire return failed: %v", value)
		}
	}()
	r.wire.pair.mu.Lock()
	if r.call.responded || r.call.completed {
		r.wire.pair.mu.Unlock()
		return ErrClosed
	}
	r.call.responded = true
	r.wire.pair.mu.Unlock()
	r.call.invocation.Settle()
	message.Frame.Result = append(json.RawMessage(nil), message.Frame.Result...)
	if message.Frame.Error != nil {
		copied := *message.Frame.Error
		copied.Data = append(json.RawMessage(nil), copied.Data...)
		message.Frame.Error = &copied
	}
	return WithoutUnpublishedProof(r.call.key.address.Wire.Send(path, message))
}
