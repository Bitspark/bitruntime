package core

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"sync"
	"time"

	"github.com/Bitspark/bitruntime/internal/delivery/go"
	"github.com/Bitspark/bitruntime/internal/profile/go"
	transports "github.com/Bitspark/bitruntime/transports/go"
	wire "github.com/Bitspark/bitwire/wire/go"
)

// PairOptions bounds a local pair. Zero values select the defaults, which are
// the protocol engine's: 64 concurrent handlers and 128 pending requests and
// queued messages per direction, frames of at most 1 MiB, a 30-second request
// deadline and a 10-second stall deadline for event consumers.
type PairOptions struct {
	// MaxConcurrentHandlers bounds the requests a direction runs at once; the
	// one past it is refused busy.
	MaxConcurrentHandlers int
	// MaxPendingRequests bounds the requests a direction holds admitted and
	// unanswered; the one past it is refused busy.
	MaxPendingRequests int
	// QueueCapacity bounds the requests and events a direction holds queued.
	// A full queue ends the pair with ErrBackpressure.
	QueueCapacity int
	// MaxFrameBytes bounds the envelope each message would travel as.
	MaxFrameBytes int64
	// RequestTimeout answers a request whose handler has not answered by then.
	RequestTimeout time.Duration
	// WriteTimeout bounds how long an event consumer may take before the pair
	// ends as stalled.
	WriteTimeout time.Duration
	// Propagator moves a trace between a frame and a handler's context.
	Propagator Propagator
}

func (o PairOptions) normalized() (PairOptions, error) {
	if o.MaxConcurrentHandlers < 0 || o.MaxPendingRequests < 0 || o.QueueCapacity < 0 || o.MaxFrameBytes < 0 || o.RequestTimeout < 0 || o.WriteTimeout < 0 {
		return o, errors.New("bitruntime: pair limits must not be negative")
	}
	if o.MaxConcurrentHandlers == 0 {
		o.MaxConcurrentHandlers = 64
	}
	if o.MaxPendingRequests == 0 {
		o.MaxPendingRequests = 128
	}
	if o.QueueCapacity == 0 {
		o.QueueCapacity = 128
	}
	if o.MaxFrameBytes == 0 {
		o.MaxFrameBytes = 1 << 20
	}
	if o.RequestTimeout == 0 {
		o.RequestTimeout = 30 * time.Second
	}
	if o.WriteTimeout == 0 {
		o.WriteTimeout = 10 * time.Second
	}
	if o.Propagator == nil {
		o.Propagator = DefaultPropagator
	}
	return o, nil
}

// NewPair constructs a bounded local carrier with two relative origins.
// Sending on either endpoint delivers to the receiver on the other. It
// allocates no peer, serializes nothing, and preserves structured frames, the
// original return capability's identity and received context. Each admitted
// request gets a fresh return capability that carries its invocation lifecycle.
func NewPair(options PairOptions) (left, right wire.Endpoint, err error) {
	o, err := options.normalized()
	if err != nil {
		return nil, nil, err
	}
	p := &localPair{options: o, done: make(chan struct{})}
	for i := range p.ends {
		p.ends[i] = &localEnd{pair: p, wake: make(chan struct{}, 1), calls: map[returnKey]*localCall{}}
	}
	p.ends[0].other, p.ends[1].other = p.ends[1], p.ends[0]
	for _, end := range p.ends {
		go end.run()
	}
	return p.ends[0], p.ends[1], nil
}

// returnKey correlates a request by its return capability's identity and its
// identifier, as the contract requires.
type returnKey struct {
	address *wire.ReturnAddress
	id      string
}

type localPair struct {
	mu      sync.Mutex
	options PairOptions
	ends    [2]*localEnd
	done    chan struct{}
	closed  bool
}

type localRegistration struct {
	receiver wire.Receiver
	active   bool
}
type localDelivery struct {
	path    []string
	message wire.Message
	call    *localCall
	refusal error
}
type localEnd struct {
	pair       *localPair
	other      *localEnd
	wake       chan struct{}
	queue      []localDelivery
	dataQueued int
	active     int
	eventTimer *time.Timer
	calls      map[returnKey]*localCall
	receiver   *localRegistration
}
type localCall struct {
	key          returnKey
	path         []string
	message      wire.Message
	returning    *wire.ReturnAddress
	registration *localRegistration
	dispatch     *delivery.Context
	invocation   *Invocation
	cancel       context.CancelFunc
	timer        *time.Timer
	completed    bool
	responded    bool
	active       bool
	cancelQueued bool
	cancelled    bool
}

func (w *localEnd) Send(path []string, message wire.Message) error {
	name, err := profile.EncodePath(path)
	if err != nil {
		return Unpublished(err)
	}
	if err := profile.Validate(name, message.Frame, w.pair.options.MaxFrameBytes); err != nil {
		return Unpublished(err)
	}
	if message.Frame.Kind != wire.ProfileRequest && message.Frame.Kind != wire.ProfileEvent && message.Frame.Kind != wire.ProfileCancel {
		return Unpublished(errors.New("bitruntime: a response is sent to its request's return address"))
	}
	if message.Frame.Kind != wire.ProfileEvent && (message.Return == nil || message.Return.Wire == nil) {
		return Unpublished(errors.New("bitruntime: a request or cancellation requires a return address"))
	}
	// Copy, then keep: what the receiver sees is what was validated.
	message.Frame.Params = append(json.RawMessage(nil), message.Frame.Params...)
	message.Frame.Data = append(json.RawMessage(nil), message.Frame.Data...)
	message.Frame.Meta = maps.Clone(message.Frame.Meta)
	return w.other.admit(append([]string(nil), path...), message)
}

func (w *localEnd) admit(path []string, message wire.Message) error {
	p := w.pair
	key := returnKey{message.Return, message.Frame.ID}
	delivery := localDelivery{path: path, message: message}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return Unpublished(transports.ErrClosed)
	}
	if message.Frame.Kind == wire.ProfileCancel {
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
			p.mu.Unlock()
			p.end(transports.CodeProtocol, "local wire queue limit reached")
			return Unpublished(Ended(ErrBackpressure))
		}
		w.dataQueued++
		if message.Frame.Kind == wire.ProfileRequest {
			if w.calls[key] != nil {
				delivery.refusal = &PublicError{Code: "invalid_message", Message: "Duplicate active request identifier"}
			} else if len(w.calls) >= p.options.MaxPendingRequests {
				delivery.refusal = &PublicError{Code: "busy", Message: "Outstanding call limit reached"}
			} else {
				call := &localCall{key: key, path: path, message: message, invocation: NewInvocation(DefaultInvocationLimits(), nil)}
				call.returning = &wire.ReturnAddress{Wire: &localReturn{end: w, call: call}}
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

func (w *localEnd) signal() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// retireLocked frees a call's pending slot once it is answered and no
// already queued cancellation still owns its reservation. It never removes a
// newer admission that reused the same return identity and identifier.
func (w *localEnd) retireLocked(call *localCall) {
	if !call.completed || call.cancelQueued || w.calls[call.key] != call {
		return
	}
	delete(w.calls, call.key)
	call.invocation.Settle()
	call.invocation.DispatchDone()
}

func (w *localEnd) completeLocked(call *localCall) {
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
}

func (w *localEnd) next() (localDelivery, bool) {
	w.pair.mu.Lock()
	defer w.pair.mu.Unlock()
	if w.pair.closed || len(w.queue) == 0 {
		return localDelivery{}, false
	}
	delivery := w.queue[0]
	w.queue[0] = localDelivery{}
	w.queue = w.queue[1:]
	if delivery.message.Frame.Kind != wire.ProfileCancel {
		w.dataQueued--
	}
	return delivery, true
}

func (w *localEnd) run() {
	for {
		next, ok := w.next()
		if !ok {
			select {
			case <-w.pair.done:
				return
			case <-w.wake:
				continue
			}
		}
		if next.refusal != nil {
			Respond(next.message, nil, next.refusal)
			continue
		}
		if next.message.Frame.Kind == wire.ProfileCancel {
			w.deliverCancel(next.call, next.message)
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
		if next.call != nil {
			if registration == nil || w.active >= w.pair.options.MaxConcurrentHandlers {
				w.pair.mu.Unlock()
				code, message := "method_not_found", "Unknown method"
				if registration != nil {
					code, message = "busy", "Too many concurrent requests"
				}
				Respond(next.message, nil, &PublicError{Code: code, Message: message})
				continue
			}
			call := next.call
			call.registration, call.active = registration, true
			w.active++
			base := context.Background()
			if source, ok := call.key.address.Wire.(delivery.Source); ok {
				call.dispatch = source.BitruntimeDelivery()
				if call.dispatch != nil {
					base = call.dispatch.Ctx
				}
			}
			if call.dispatch == nil {
				base = w.pair.options.Propagator.Extract(base, Trace{Parent: next.message.Frame.Traceparent, State: next.message.Frame.Tracestate})
			}
			ctx, cancel := context.WithCancel(base)
			call.cancel = cancel
			if call.dispatch != nil {
				copied := *call.dispatch
				copied.Ctx = ctx
				call.dispatch = &copied
			} else {
				call.dispatch = &delivery.Context{Ctx: ctx, MaxFrameBytes: w.pair.options.MaxFrameBytes}
			}
			call.timer = time.AfterFunc(w.pair.options.RequestTimeout, func() { w.timeout(call) })
		}
		w.pair.mu.Unlock()
		if registration == nil {
			continue
		}
		if next.message.Frame.Kind == wire.ProfileEvent {
			if _, associated := delivery.EventContext(next.message); !associated {
				ctx := w.pair.options.Propagator.Extract(context.Background(), Trace{Parent: next.message.Frame.Traceparent, State: next.message.Frame.Tracestate})
				next.message = delivery.WithEventContext(next.message, ctx)
			}
			w.pair.mu.Lock()
			w.eventTimer = time.AfterFunc(w.pair.options.WriteTimeout, func() {
				w.pair.mu.Lock()
				closed := w.pair.closed
				w.pair.mu.Unlock()
				if !closed {
					w.pair.end(transports.CodeProtocol, "local wire event consumer stalled")
				}
			})
			w.pair.mu.Unlock()
		}
		w.deliver(registration, next.path, next.message)
		if next.message.Frame.Kind == wire.ProfileEvent {
			w.pair.mu.Lock()
			if w.eventTimer != nil {
				w.eventTimer.Stop()
				w.eventTimer = nil
			}
			w.pair.mu.Unlock()
		}
	}
}

func (w *localEnd) deliver(registration *localRegistration, path []string, message wire.Message) {
	defer func() {
		if value := recover(); value != nil {
			if message.Frame.Kind == wire.ProfileRequest {
				Respond(message, nil, errors.New("wire receiver panic"))
			} else {
				w.pair.end(transports.CodeProtocol, "wire event receiver failed")
			}
		}
	}()
	registration.receiver.Message(path, message)
}

func (w *localEnd) deliverCancel(call *localCall, message wire.Message) {
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

func (w *localEnd) timeout(call *localCall) {
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
		w.queue = append(w.queue, localDelivery{path: call.path, message: wire.Message{Frame: wire.ProfileFrame{Version: 1, Kind: wire.ProfileCancel, ID: call.message.Frame.ID}, Return: call.returning}, call: call})
	}
	respond := !call.responded
	call.responded = true
	w.pair.mu.Unlock()
	w.signal()
	// A timeout answers the caller but retains the handler's budget until its
	// actual response. An application ignoring cancellation cannot spawn an
	// unbounded number of replacement handlers by repeatedly timing out.
	if respond {
		Respond(call.message, nil, context.DeadlineExceeded)
	}
}

func (w *localEnd) Receive(receiver wire.Receiver) (func(), error) {
	registration := &localRegistration{receiver: receiver, active: true}
	w.pair.mu.Lock()
	defer w.pair.mu.Unlock()
	if w.pair.closed {
		return nil, transports.ErrClosed
	}
	if w.receiver != nil {
		return nil, ErrReceiverExists
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

// Close ends the pair for both sides. An observe-only code closes it as an
// abort would: nothing is transmitted in-process either way.
func (w *localEnd) Close(code wire.Code, reason string) error {
	w.pair.end(code, reason)
	return nil
}

// end closes the pair once. Every request admitted and not yet answered is
// answered with disconnected, and every refusal still queued is answered with
// the refusal it was admitted with, so no caller is left to its own deadline.
func (p *localPair) end(code wire.Code, reason string) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.done)
	var receivers []wire.Receiver
	var requests []wire.Message
	var refusals []localDelivery
	for _, end := range p.ends {
		if end.eventTimer != nil {
			end.eventTimer.Stop()
		}
		if end.receiver != nil {
			end.receiver.active = false
			receivers = append(receivers, end.receiver.receiver)
		}
		for _, queued := range end.queue {
			if queued.refusal != nil {
				refusals = append(refusals, queued)
			}
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
		end.calls = map[returnKey]*localCall{}
		end.queue, end.dataQueued = nil, 0
	}
	p.mu.Unlock()
	go func() {
		for _, receiver := range receivers {
			if receiver.Closed != nil {
				func() { defer func() { _ = recover() }(); receiver.Closed(code, reason) }()
			}
		}
		for _, refused := range refusals {
			Respond(refused.message, nil, refused.refusal)
		}
		for _, request := range requests {
			Respond(request, nil, transports.ErrClosed)
		}
	}()
}

type localReturn struct {
	end  *localEnd
	call *localCall
}

// BitruntimeDelivery is the context the pair established for this request.
func (r *localReturn) BitruntimeDelivery() *delivery.Context { return r.call.dispatch }

// Invocation exposes this return capability's lifecycle to the pair that owns
// it. Participants reach the same state through the vocabulary on Send.
func (r *localReturn) Invocation() *Invocation { return r.call.invocation }

func (r *localReturn) Send(path []string, message wire.Message) (err error) {
	if len(path) != 0 {
		return r.call.invocation.Deliver(path, message)
	}
	if message.Frame.Kind != wire.ProfileResponse || message.Frame.ID != r.call.message.Frame.ID {
		return errors.New("bitruntime: invalid wire response")
	}
	if err := profile.Validate("", message.Frame, r.end.pair.options.MaxFrameBytes); err != nil {
		// Refused before completion, so the response helper can still send
		// its bounded internal-error fallback.
		return err
	}
	p := r.end.pair
	p.mu.Lock()
	if r.call.responded || r.call.completed {
		// A deadline already answered the caller. This is the handler's actual
		// response, which is what releases its budget.
		r.end.completeLocked(r.call)
		p.mu.Unlock()
		return transports.ErrClosed
	}
	r.call.responded = true
	// Retire before the caller can hold its answer: a caller that issues its
	// next call as soon as this one returns must find the slot free. A queued
	// cancellation keeps the reservation until it drains.
	r.end.completeLocked(r.call)
	p.mu.Unlock()
	r.call.invocation.Settle()
	defer func() {
		if value := recover(); value != nil {
			err = errors.New("bitruntime: wire return failed")
		}
	}()
	message.Frame.Result = append(json.RawMessage(nil), message.Frame.Result...)
	if message.Frame.Error != nil {
		copied := *message.Frame.Error
		copied.Data = append(json.RawMessage(nil), copied.Data...)
		message.Frame.Error = &copied
	}
	return WithoutUnpublishedProof(r.call.key.address.Wire.Send(path, message))
}
