// Package runtime implements the nightseam.duplex/1 profile over a frames
// duplex connection (duplex): JSON text frames carrying requests,
// responses, events and cancellations. It never touches a WebSocket; Dial
// and Accept open one and hand it over as a connection, and a Peer over any
// other transport speaks the same profile byte for byte. It has no
// application authorization, replay, retries, or persistence policy.
package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Bitspark/nightseam/duplex/go"
	"github.com/Bitspark/nightseam/internal/scalarjson"
)

// Profile names what this package speaks: JSON text frames carrying requests,
// responses, events and cancellations both ways over a connection of the seam.
// docs/wire/profile.md is its specification.
const Profile = "nightseam.duplex/1"

// Role is the side of a connection a peer takes. It decides the prefix of the
// request ids the peer mints — c: for a client, s: for a server — so the two
// sides never mint the same id, and a tunnel over the peer chooses its channel
// ids by it.
type Role string

const (
	// ClientRole dials and mints request ids under the c: prefix.
	ClientRole Role = "client"
	// ServerRole accepts and mints request ids under the s: prefix.
	ServerRole Role = "server"
)

// The errors a peer ends with: ErrClosed when Close was called or the
// connection ended, ErrBackpressure when a queue stayed full past its write
// deadline — a consumer that does not drain is disconnected rather than
// allowed to hold the connection up. Both reach Err and every pending call.
var (
	ErrClosed       = errors.New("duplex connection closed")
	ErrBackpressure = errors.New("duplex consumer is stalled")
)

// PublicError is safe to send to the remote caller. Other handler errors are
// replaced by a generic internal error; their messages are not disclosed.
type PublicError struct {
	Code    string          `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// Error is the code and the message, as a log line shows them.
func (e *PublicError) Error() string { return e.Code + ": " + e.Message }

// Handler answers one request: it takes the params as they arrived and
// returns the result, or an error — a *PublicError crosses the wire with its
// code; any other error reaches the caller as internal. The context is
// cancelled when the caller withdraws the request or its deadline passes, and
// carries the trace and the meta the frame brought.
type Handler func(context.Context, *Peer, json.RawMessage) (any, error)

// EventHandler takes one event's data; events have no answer. Handlers run
// one at a time in the order the events arrived.
type EventHandler func(context.Context, *Peer, json.RawMessage)

// Event is one event of the profile as a handler or an emitter sees it: its
// name and its data.
type Event struct {
	Name string          `json:"event"`
	Data json.RawMessage `json:"data"`
}

// Options limits are per connection. Zero values select the documented defaults.
// Handlers may run concurrently; event callbacks run serially in receive order.
type Options struct {
	Handlers map[string]Handler
	Events   map[string]EventHandler
	// Prepare runs on the peer once it is built and before it reads its
	// first frame: what it installs — Handle, HandleEvent, a tunnel over the
	// peer — is there before anything can arrive, so the other side's first
	// request cannot be refused method_not_found by a peer whose handlers
	// are still on their way. Install in Prepare, use in OnConnect, which
	// runs on a peer that is already live. An error fails the construction:
	// the peer never runs and the constructor answers with it.
	Prepare               func(*Peer) error
	MaxConcurrentHandlers int
	// MaxPendingRequests bounds the calls this peer may have outstanding at
	// once; the one past it is refused busy without reaching the wire. It is
	// the caller's own bound, as MaxConcurrentHandlers is the receiver's.
	MaxPendingRequests int
	QueueCapacity      int
	MaxFrameBytes      int64
	RequestTimeout     time.Duration
	WriteTimeout       time.Duration
	Propagator         Propagator
	// Observer is told what the peer does with the traffic it carries; nil
	// observes nothing and costs nothing. Families labels a method or event
	// name with the family it belongs to, which the generated install fills:
	// an unlabelled name has no family, and the runtime parses none.
	Observer Observer
	Families map[string]string
}

func (o Options) normalized() (Options, error) {
	if o.MaxConcurrentHandlers < 0 || o.MaxPendingRequests < 0 || o.QueueCapacity < 0 || o.MaxFrameBytes < 0 || o.RequestTimeout < 0 || o.WriteTimeout < 0 {
		return o, errors.New("duplex limits must not be negative")
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
	for name, h := range o.Handlers {
		if name == "" || h == nil {
			return o, errors.New("invalid duplex method handler")
		}
	}
	for name, h := range o.Events {
		if name == "" || h == nil {
			return o, errors.New("invalid duplex event handler")
		}
	}
	return o, nil
}

type frame struct {
	Version int             `json:"version"`
	Kind    string          `json:"kind"`
	ID      string          `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *PublicError    `json:"error,omitempty"`
	Event   string          `json:"event,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`
	// W3C Trace Context, which every kind may carry and none requires.
	Traceparent string `json:"traceparent,omitempty"`
	Tracestate  string `json:"tracestate,omitempty"`
	// What is about a call rather than the call, which a request and an event
	// may carry. The peer keeps it for what reads it above and emits none.
	Meta map[string]string `json:"meta,omitempty"`
}

// queuedFrame is one frame waiting for the writer: the bytes it will write and
// the frame they were rendered from. The two travel together so that the send
// is observed by the one goroutine that writes, immediately before the bytes
// leave — one serialization point per peer, which is what makes the observer's
// events one order (docs/runtime/observer.md).
type queuedFrame struct {
	data  []byte
	frame frame
}

// queuedEvent keeps an event's trace beside it across the bounded queue: the
// handler runs under the context the trace was extracted into, not under one
// the reader has already left behind.
type queuedEvent struct {
	event Event
	trace Trace
	meta  Meta
}

type pendingResult struct {
	result json.RawMessage
	err    error
}

// Peer owns a frames duplex connection until Close or transport failure.
// Never send on or receive from the connection after handing it to NewPeer.
// The connection's receive limit is its maker's to set to MaxFrameBytes;
// the peer refuses a larger frame it is nonetheless handed.
type Peer struct {
	wireOnce        sync.Once
	wire            *peerWire
	conn            duplex.Conn
	ctx             context.Context
	cancel          context.CancelFunc
	options         Options
	prefix          string
	remotePrefix    string
	subprotocol     string
	next            uint64
	publish         sync.Mutex
	admitted        uint64
	done            chan struct{}
	once            sync.Once
	mu              sync.Mutex
	err             error
	pending         map[string]chan pendingResult
	incoming        map[string]context.CancelFunc
	handlers        map[string]Handler
	eventHandlers   map[string]EventHandler
	requestFallback func(string) Handler
	eventFallback   func(string) EventHandler
	listeners       map[uint64]func(context.Context, Event)
	listenerID      uint64
	outputs         chan queuedFrame
	events          chan queuedEvent
	slots           chan struct{}
}

// NewPeer speaks the profile over any connection of the seam — a pipe, a
// tunnel channel, a socket already accepted — as the given role. The peer
// owns the connection from here and closes it when it ends; ctx ending ends
// the peer. Dial and Accept are this over a WebSocket.
func NewPeer(ctx context.Context, conn duplex.Conn, role Role, options Options) (*Peer, error) {
	return newPeer(ctx, conn, role, options, "")
}

// newPeer is NewPeer carrying what the handshake beneath selected, which only
// Accept and Dial are in a position to know; every other connection has none.
// It is set before the loops start, so Subprotocol is read without a lock.
func newPeer(ctx context.Context, conn duplex.Conn, role Role, options Options, subprotocol string) (*Peer, error) {
	if ctx == nil || conn == nil {
		return nil, errors.New("duplex requires a context and connection")
	}
	if role != ClientRole && role != ServerRole {
		return nil, errors.New("invalid duplex role")
	}
	o, err := options.normalized()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	p := &Peer{conn: conn, ctx: ctx, cancel: cancel, options: o, prefix: "c:", remotePrefix: "s:", subprotocol: subprotocol, done: make(chan struct{}),
		pending: make(map[string]chan pendingResult), incoming: make(map[string]context.CancelFunc), handlers: make(map[string]Handler),
		eventHandlers: make(map[string]EventHandler), listeners: make(map[uint64]func(context.Context, Event)),
		outputs: make(chan queuedFrame, o.QueueCapacity), events: make(chan queuedEvent, o.QueueCapacity), slots: make(chan struct{}, o.MaxConcurrentHandlers)}
	if role == ServerRole {
		p.prefix, p.remotePrefix = "s:", "c:"
	}
	for k, v := range o.Handlers {
		p.handlers[k] = v
	}
	for k, v := range o.Events {
		p.eventHandlers[k] = v
	}
	if o.Prepare != nil {
		if err := o.Prepare(p); err != nil {
			p.abandon(err)
			return nil, err
		}
	}
	p.observeOpened(role)
	go p.readLoop()
	go p.writeLoop()
	go p.eventLoop()
	go func() { <-ctx.Done(); p.fail(ctx.Err()) }()
	return p, nil
}

// Done is closed when the peer has ended, for whatever reason; Err says which.
func (p *Peer) Done() <-chan struct{} { return p.done }

// Context is the peer's own, cancelled when it ends: what a handler or a
// caller derives its own from to be released with the connection.
func (p *Peer) Context() context.Context { return p.ctx }

// Role is the side of the connection this peer is: it prefixes the request
// ids it mints, and a tunnel over it chooses channel ids by it.
func (p *Peer) Role() Role {
	if p.prefix == "s:" {
		return ServerRole
	}
	return ClientRole
}

// Subprotocol is what the WebSocket handshake beneath this peer selected, and
// "" when it selected none or the peer does not run over a WebSocket. It is
// fixed for the peer's life; the profile reads nothing into it.
func (p *Peer) Subprotocol() string { return p.subprotocol }

// MaxFrameBytes is the largest frame this peer sends or receives.
func (p *Peer) MaxFrameBytes() int64 { return p.options.MaxFrameBytes }

// Err is why the peer ended, or nil while it runs: ErrClosed, ErrBackpressure,
// the context's error, or the connection's own.
func (p *Peer) Err() error { p.mu.Lock(); defer p.mu.Unlock(); return p.err }

// Close ends the peer with ErrClosed, closing the connection beneath it with
// 1000 and failing every pending call. It is safe to call more than once.
func (p *Peer) Close() error { p.end(ErrClosed, duplex.CodeNormal, ""); return nil }

// fail ends the peer on a transport there is nothing to say over: a write that
// failed, the context ending, a consumer that stalled past its deadline. The
// connection is aborted rather than closed with a handshake nobody is left to
// answer, and the far side reads an abnormal closure.
func (p *Peer) fail(err error) { p.end(err, codeAborted, "") }

// refuse ends the peer on a frame the profile does not admit — a malformed
// envelope, an id that correlates with nothing, a frame of the wrong kind. The
// other side broke the profile and is told so, with 4011 and a reason, because
// a gateway or a proxy between the two can act on a code and can act on
// nothing at all (docs/wire/profile.md).
func (p *Peer) refuse(err error) { p.end(err, duplex.CodeDuplex, err.Error()) }

// abandon releases a peer that never ran: Prepare failed, the loops were
// never started and nothing of the profile reached the wire, so there is
// nothing to close with a code here — the connection is disposed of by the
// constructor that opened it. Whatever Prepare started before it failed sees
// the context cancelled and Done closed, as it would on any other end.
func (p *Peer) abandon(err error) {
	p.once.Do(func() {
		p.mu.Lock()
		p.err = err
		p.mu.Unlock()
		p.cancel()
		close(p.done)
	})
}

// codeAborted stands for no close at all: the connection is aborted, nothing
// is sent, and the far side reads 1006.
const codeAborted duplex.Code = 0

// end ends the peer once, whatever ended it: every pending call is released,
// the connection is closed with the code this side decided on or aborted where
// there is none, and the observer is told what the wire carried.
func (p *Peer) end(err error, code duplex.Code, reason string) {
	p.once.Do(func() {
		if err == nil {
			err = ErrClosed
		}
		// This outcome settles every pending request, including ones already
		// delivered. Another send's proof cannot be broadcast as their proof.
		err = WithoutUnpublishedProof(err)
		p.mu.Lock()
		p.err = err
		p.mu.Unlock()
		p.cancel()
		close(p.done)
		if code == codeAborted {
			_ = p.conn.Abort()
		} else {
			// The peer's own context is already cancelled, so the handshake
			// waits on one of its own: a far side that answers is told the
			// code, and one that does not holds nothing up past the deadline.
			reason = closeReason(reason)
			ctx, cancel := context.WithTimeout(context.Background(), p.options.WriteTimeout)
			_ = p.conn.Close(ctx, code, reason)
			cancel()
		}
		p.observeClosed(err, code, reason)
	})
}

// closeReason is what a close frame admits: the registry bounds a reason at
// 123 bytes and requires valid UTF-8, and a transport handed a longer one
// would close with no code at all — which is the one thing a refusal must not
// do. A refused frame's own text may reach it, so it is cut on a rune.
func closeReason(reason string) string {
	const limit = 123
	if len(reason) <= limit {
		return reason
	}
	reason = reason[:limit]
	for len(reason) > 0 && !utf8.ValidString(reason) {
		reason = reason[:len(reason)-1]
	}
	return reason
}

// Handle registers a method. Duplicate registrations are rejected.
func (p *Peer) Handle(method string, handler Handler) error {
	if method == "" || handler == nil {
		return errors.New("invalid duplex method handler")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	if _, exists := p.handlers[method]; exists {
		return fmt.Errorf("method %q already registered", method)
	}
	p.handlers[method] = handler
	return nil
}

// HandleEvent registers the handler for the event of that name, replacing
// any before it; an event with no handler is dropped.
func (p *Peer) HandleEvent(name string, handler EventHandler) error {
	if name == "" || handler == nil {
		return errors.New("invalid duplex event handler")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	if _, exists := p.eventHandlers[name]; exists {
		return fmt.Errorf("event %q already registered", name)
	}
	p.eventHandlers[name] = handler
	return nil
}

// OnEvent observes every event and returns an idempotent unsubscribe function.
// Slow callbacks consume the bounded event queue and can disconnect the peer.
func (p *Peer) OnEvent(listener func(context.Context, Event)) func() {
	if listener == nil {
		return func() {}
	}
	p.mu.Lock()
	p.listenerID++
	id := p.listenerID
	p.listeners[id] = listener
	p.mu.Unlock()
	return func() { p.mu.Lock(); delete(p.listeners, id); p.mu.Unlock() }
}

// Call sends one request and waits for its response. It never retries. A caller
// cancellation also sends best-effort cancellation to the remote handler.
func (p *Peer) Call(ctx context.Context, method string, params, result any) error {
	call, err := p.beginCall(ctx, method, params)
	if err != nil {
		return err
	}
	return call.await(result)
}

type admittedCall struct {
	await    func(any) error
	withdraw func()
}

// beginCall performs the bounded admission synchronously. A wire dispatcher
// admits frames in its delivery order, then waits for each result separately;
// starting goroutines before admission would reorder requests and events.
func (p *Peer) beginCall(ctx context.Context, method string, params any) (*admittedCall, error) {
	return p.beginCallTrace(ctx, method, params, nil, false)
}

// A structured wire frame already carries its trace. Public Call still injects
// a child; forwarding the existing frame uses these exact members instead.
func (p *Peer) beginCallTrace(ctx context.Context, method string, params any, carried *Trace, immediate bool) (*admittedCall, error) {
	if ctx == nil || method == "" {
		return nil, Unpublished(errors.New("duplex call requires context and method"))
	}
	ctx, cancel := context.WithTimeout(ctx, p.options.RequestTimeout)
	if err := ctx.Err(); err != nil {
		cancel()
		return nil, Unpublished(err)
	}
	data, err := MarshalJSON(params)
	if err != nil {
		cancel()
		return nil, Unpublished(err)
	}
	// One trace serves the request and the cancellation that may follow it: a
	// cancel carries its request's members, not a sibling span of them.
	var trace Trace
	if carried != nil {
		trace = *carried
	} else {
		trace = p.options.Propagator.Inject(ctx)
	}
	reply := make(chan pendingResult, 1)
	// Reservation and publication happen under the outgoing queue's one
	// ordering gate, so serials reach the other side in the order they were
	// taken. A reservation that never publishes has still spent its serial,
	// and the receiver allows the gap it leaves.
	p.publish.Lock()
	if p.next == maxRequestSerial {
		p.publish.Unlock()
		cancel()
		p.fail(errors.New("duplex request serials exhausted"))
		return nil, Unpublished(&PublicError{Code: "identifier_exhausted", Message: "Create a new peer before issuing further calls"})
	}
	p.next++
	id := p.prefix + strconv.FormatUint(p.next, 10)
	p.mu.Lock()
	if p.err != nil {
		err = p.err
		p.mu.Unlock()
		p.publish.Unlock()
		cancel()
		return nil, Unpublished(err)
	}
	// The caller's own bound. A call past it never reaches the wire and never
	// becomes an observer's request: nothing started, so nothing ended.
	if len(p.pending) >= p.options.MaxPendingRequests {
		p.mu.Unlock()
		p.publish.Unlock()
		cancel()
		return nil, Unpublished(&PublicError{Code: "busy", Message: "Outstanding call limit reached"})
	}
	p.pending[id] = reply
	p.mu.Unlock()
	finish := func() { cancel(); p.mu.Lock(); delete(p.pending, id); p.mu.Unlock() }
	started := p.requestStarted(id, method, false, trace)
	err = p.enqueueFrame(ctx, frame{Version: 1, Kind: "request", ID: id, Method: method, Params: data,
		Traceparent: trace.Parent, Tracestate: trace.State, Meta: outgoingMeta(ctx)}, immediate)
	p.publish.Unlock()
	if err != nil {
		finish()
		err = Unpublished(err)
		p.requestEnded(started, id, method, false, trace, err)
		return nil, err
	}
	var completed sync.Once
	complete := func(err error, withdrawn bool) {
		completed.Do(func() {
			p.requestEnded(started, id, method, false, trace, err)
			if withdrawn {
				p.cancelRequest(id, trace)
			}
		})
	}
	return &admittedCall{await: func(result any) error {
		defer finish()
		cancelRemote, err := awaitReply(ctx, reply, p.done, p.Err, result)
		complete(err, cancelRemote)
		return err
	}, withdraw: func() {
		cancel()
		// A routed cancel occupies a position in this wire's send order. Its
		// observation and best-effort admission finish before the next frame.
		complete(context.Canceled, true)
	}}, nil
}

// awaitReply is the request primitive shared by carrier calls and relative
// wires. A wire changes where a request is dispatched, not how it completes.
func awaitReply(ctx context.Context, reply <-chan pendingResult, done <-chan struct{}, ended func() error, result any) (bool, error) {
	select {
	case r := <-reply:
		if r.err != nil {
			return false, r.err
		}
		if result == nil {
			return false, nil
		}
		if err := json.Unmarshal(r.result, result); err != nil {
			return false, fmt.Errorf("decode duplex result: %w", err)
		}
		return false, nil
	case <-ctx.Done():
		return true, ctx.Err()
	case <-done:
		return false, ended()
	}
}

// Cancellation is best effort; a congested transport must not extend the
// caller's already-expired deadline while waiting to send its cancellation.
func (p *Peer) cancelRequest(id string, trace Trace) {
	f := frame{Version: 1, Kind: "cancel", ID: id, Traceparent: trace.Parent, Tracestate: trace.State}
	data, err := MarshalJSON(f)
	if err != nil || int64(len(data)) > p.options.MaxFrameBytes {
		return
	}
	select {
	case <-p.done:
		return
	default:
	}
	select {
	case p.outputs <- queuedFrame{data: data, frame: f}:
	default:
	}
}

// Emit queues an event. Success means queued for this connection, not persisted
// or processed by the remote application.
func (p *Peer) Emit(ctx context.Context, event string, data any) error {
	return p.emitTrace(ctx, event, data, nil, false)
}

func (p *Peer) emitTrace(ctx context.Context, event string, data any, carried *Trace, immediate bool) error {
	if ctx == nil || event == "" {
		return Unpublished(errors.New("duplex event requires context and name"))
	}
	encoded, err := MarshalJSON(data)
	if err != nil {
		return Unpublished(err)
	}
	var trace Trace
	if carried != nil {
		trace = *carried
	} else {
		trace = p.options.Propagator.Inject(ctx)
	}
	f := frame{Version: 1, Kind: "event", Event: event, Data: encoded,
		Traceparent: trace.Parent, Tracestate: trace.State, Meta: outgoingMeta(ctx)}
	// The application emitted it here; the frame carrying it is sent when the
	// queue takes it, which is one event of its own and may not happen at all.
	p.observeEmitted(f)
	return Unpublished(p.enqueueFrame(ctx, f, immediate))
}

func (p *Peer) enqueue(ctx context.Context, f frame) error {
	return p.enqueueFrame(ctx, f, false)
}

func (p *Peer) enqueueFrame(ctx context.Context, f frame, immediate bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := MarshalJSON(f)
	if err != nil {
		return err
	}
	if int64(len(data)) > p.options.MaxFrameBytes {
		return errors.New("duplex frame exceeds size limit")
	}
	select {
	case <-p.done:
		return p.Err()
	default:
	}
	select {
	case p.outputs <- queuedFrame{data: data, frame: f}:
		return nil
	default:
	}
	if !immediate {
		// Public Peer producers pace a transient burst for one write deadline.
		// Their own cancellation withdraws only this unadmitted frame. Wire
		// dispatch instead requires an immediate handoff so a composition does
		// not run at its slowest destination's pace.
		p.observeBackpressure(len(p.outputs), false, p.options.WriteTimeout)
		timer := time.NewTimer(p.options.WriteTimeout)
		defer timer.Stop()
		select {
		case p.outputs <- queuedFrame{data: data, frame: f}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-p.done:
			return p.Err()
		case <-timer.C:
		}
	}
	p.observeBackpressure(len(p.outputs), true, p.options.WriteTimeout)
	p.fail(ErrBackpressure)
	return ErrBackpressure
}

func (p *Peer) writeLoop() {
	for {
		select {
		case <-p.done:
			return
		case queued := <-p.outputs:
			// The one place a send is observed, and before the bytes leave: a
			// reply cannot be read, let alone observed, ahead of the frame.sent
			// of the request that drew it.
			p.observeSent(queued.frame, len(queued.data))
			ctx, cancel := context.WithTimeout(p.ctx, p.options.WriteTimeout)
			err := p.conn.Send(ctx, duplex.Frame{Kind: duplex.Text, Data: queued.data})
			cancel()
			if err != nil {
				p.fail(err)
				return
			}
		}
	}
}

func (p *Peer) readLoop() {
	for {
		received, err := p.conn.Receive(p.ctx)
		if err != nil {
			p.fail(err)
			return
		}
		if received.Kind != duplex.Text {
			p.refuse(errors.New("duplex requires JSON text frames"))
			return
		}
		if int64(len(received.Data)) > p.options.MaxFrameBytes {
			p.refuse(errors.New("duplex frame exceeds size limit"))
			return
		}
		f, err := decodeFrame(received.Data)
		if err != nil {
			p.refuse(err)
			return
		}
		if f.ID != "" {
			prefix := p.remotePrefix
			if f.Kind == "response" {
				prefix = p.prefix
			}
			if !validID(f.ID, prefix) {
				p.refuse(errors.New("invalid duplex request identifier"))
				return
			}
			// Only a request advances the mark: a response or a control names
			// a serial that was taken before it.
			if f.Kind == "request" && !p.admit(f.ID, prefix) {
				p.refuse(errors.New("duplex request serial did not increase"))
				return
			}
		}
		p.observeReceived(f, len(received.Data))
		switch f.Kind {
		case "response":
			p.mu.Lock()
			reply := p.pending[f.ID]
			delete(p.pending, f.ID)
			p.mu.Unlock()
			if reply != nil {
				r := pendingResult{result: f.Result}
				if f.Error != nil {
					r.err = f.Error
				}
				reply <- r
			}
		case "cancel":
			p.mu.Lock()
			cancel := p.incoming[f.ID]
			p.mu.Unlock()
			if cancel != nil {
				cancel()
			}
		case "request":
			p.startRequest(f)
		case "event":
			if !p.enqueueEvent(queuedEvent{event: Event{Name: f.Event, Data: f.Data}, trace: Trace{Parent: f.Traceparent, State: f.Tracestate}, meta: f.Meta}) {
				return
			}
		}
	}
}

func (p *Peer) enqueueEvent(event queuedEvent) bool {
	select {
	case p.events <- event:
		return true
	default:
	}
	// A full queue can be a healthy transient burst — a tight decoder loop
	// outrunning a ready consumer — so the producer is paced for one write
	// deadline before the consumer is declared stalled, as the outgoing queue
	// does. The producer here is the remote, and the only way to pace it is to
	// stop reading: while this waits, responses and cancellations on this
	// connection wait with it. That is the cost of not ending a connection
	// that would drain in a second, and the deadline is what bounds it.
	p.observeBackpressure(len(p.events), false, p.options.WriteTimeout)
	timer := time.NewTimer(p.options.WriteTimeout)
	defer timer.Stop()
	select {
	case p.events <- event:
		return true
	case <-p.done:
		return false
	case <-timer.C:
		p.observeBackpressure(len(p.events), true, p.options.WriteTimeout)
		p.fail(ErrBackpressure)
		return false
	}
}

// maxRequestSerial is the largest serial a sender publishes. A sender that
// would wrap refuses and ends the connection instead, since a wrapped serial
// would name an invocation the receiver has already seen.
const maxRequestSerial = uint64(1)<<63 - 1

// admit holds an incoming request to the profile's publication order: within
// one connection instance and one direction, each request's serial is greater
// than every request's published before it. Gaps are allowed.
func (p *Peer) admit(id, prefix string) bool {
	serial, err := strconv.ParseUint(strings.TrimPrefix(id, prefix), 10, 64)
	if err != nil || serial == 0 {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if serial <= p.admitted {
		return false
	}
	p.admitted = serial
	return true
}

func validID(id, prefix string) bool {
	if !strings.HasPrefix(id, prefix) {
		return false
	}
	n := strings.TrimPrefix(id, prefix)
	if n == "" || n[0] == '0' {
		return false
	}
	for _, r := range n {
		if r < '0' || r > '9' {
			return false
		}
	}
	return len(n) <= 20
}

// validTraceparent holds a traceparent to the one form W3C Trace Context gives
// it: version, trace id, parent id and flags, lower-case hexadecimal, dashed.
func validTraceparent(value string) bool {
	if len(value) != 55 || value[2] != '-' || value[35] != '-' || value[52] != '-' {
		return false
	}
	for i := 0; i < len(value); i++ {
		if i == 2 || i == 35 || i == 52 {
			continue
		}
		if c := value[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func (p *Peer) startRequest(f frame) {
	p.mu.Lock()
	if _, exists := p.incoming[f.ID]; exists {
		p.mu.Unlock()
		p.refuse(errors.New("duplicate active duplex request identifier"))
		return
	}
	handler := p.handlers[f.Method]
	fallback := p.requestFallback
	p.mu.Unlock()
	if fallback != nil {
		if attached := fallback(f.Method); attached != nil {
			handler = attached
		}
	}
	// A response carries its request's trace, whether a handler ran or not.
	trace := Trace{Parent: f.Traceparent, State: f.Tracestate}
	// A request this peer refuses for want of a method or of a slot is still a
	// request it began: what starts is what ends, and the refusal is the outcome.
	started := p.requestStarted(f.ID, f.Method, true, trace)
	if handler == nil {
		refusal := &PublicError{Code: "method_not_found", Message: "Unknown method"}
		p.requestEnded(started, f.ID, f.Method, true, trace, refusal)
		p.rejectRequest(f.ID, trace, refusal)
		return
	}
	select {
	case p.slots <- struct{}{}:
	default:
		refusal := &PublicError{Code: "busy", Message: "Too many concurrent requests"}
		p.requestEnded(started, f.ID, f.Method, true, trace, refusal)
		p.rejectRequest(f.ID, trace, refusal)
		return
	}
	// What the handler sends is a child of the request that ran it, and carries
	// the request's meta only where the handler says so: a trace is the peer's
	// to propagate, a carriage the consumer's.
	handling := withIncomingMeta(p.options.Propagator.Extract(p.ctx, trace), f.Meta)
	ctx, cancel := context.WithTimeout(handling, p.options.RequestTimeout)
	ctx = context.WithValue(ctx, wireFrameKey{}, f)
	p.mu.Lock()
	p.incoming[f.ID] = cancel
	p.mu.Unlock()
	var answered sync.Once
	respond := func(result any, err error) {
		answered.Do(func() {
			// The deadline has already won when it releases the body, even if
			// that body beats the asynchronous deadline callback to this once.
			// Explicit withdrawal still preserves a later public refusal.
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				result, err = nil, context.DeadlineExceeded
			}
			p.requestEnded(started, f.ID, f.Method, true, trace, err)
			p.respond(f.ID, trace, result, err)
		})
	}
	// A receiver deadline settles the response, but cannot retire work that
	// ignores cancellation. Explicit withdrawal still waits for the body.
	stopDeadline := context.AfterFunc(ctx, func() {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			respond(nil, ctx.Err())
		}
	})
	go func() {
		defer func() { cancel(); p.mu.Lock(); delete(p.incoming, f.ID); p.mu.Unlock(); <-p.slots }()
		defer stopDeadline()
		result, err := invokeHandler(ctx, p, handler, f)
		if err == nil && ctx.Err() != nil {
			err = ctx.Err()
		}
		// Completion answers once, unless the receiver deadline already did.
		respond(result, err)
	}()
}

// Rejections run on the reader because they do not consume handler slots. They
// must never wait for outbound capacity: that could hold up a response needed by
// an already active reverse call. A flood exhausting the rejection capacity
// closes the overloaded connection after giving the writer a scheduling turn.
func (p *Peer) rejectRequest(id string, trace Trace, public *PublicError) {
	f := frame{Version: 1, Kind: "response", ID: id, Error: public,
		Traceparent: trace.Parent, Tracestate: trace.State}
	data, err := MarshalJSON(f)
	if err != nil || int64(len(data)) > p.options.MaxFrameBytes {
		p.fail(errors.New("duplex rejection exceeds frame limit"))
		return
	}
	select {
	case p.outputs <- queuedFrame{data: data, frame: f}:
		return
	case <-p.done:
		return
	default:
	}
	runtime.Gosched()
	select {
	case p.outputs <- queuedFrame{data: data, frame: f}:
	case <-p.done:
	default:
		p.fail(ErrBackpressure)
	}
}

// invokeHandler takes the whole frame so that a panic is reported as what the
// handler was called for, never as what it was called with.
func invokeHandler(ctx context.Context, p *Peer, h Handler, f frame) (result any, err error) {
	defer func() {
		if value := recover(); value != nil {
			p.observePanic(f, value)
			err = errors.New("duplex handler panic")
		}
	}()
	return h(ctx, p, f.Params)
}

func (p *Peer) respond(id string, trace Trace, result any, err error) {
	f := frame{Version: 1, Kind: "response", ID: id, Traceparent: trace.Parent, Tracestate: trace.State}
	if err != nil {
		var public *PublicError
		switch {
		case errors.As(err, &public) && public != nil && public.Code != "" && public.Message != "":
			f.Error = public
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			f.Error = &PublicError{Code: "cancelled", Message: "Request cancelled"}
		default:
			f.Error = &PublicError{Code: "internal", Message: "Internal error"}
		}
	} else {
		f.Result, err = MarshalJSON(result)
		if err != nil {
			f.Error = &PublicError{Code: "internal", Message: "Internal error"}
			f.Result = nil
		}
	}
	if err := p.enqueue(p.ctx, f); err != nil && p.ctx.Err() == nil {
		// An oversized/unencodable result cannot leave the remote call hanging.
		fallback := frame{Version: 1, Kind: "response", ID: id, Error: &PublicError{Code: "internal", Message: "Response could not be encoded"},
			Traceparent: trace.Parent, Tracestate: trace.State}
		if retryErr := p.enqueue(p.ctx, fallback); retryErr != nil {
			p.fail(retryErr)
		}
	}
}

func (p *Peer) eventLoop() {
	for {
		select {
		case <-p.done:
			return
		case queued := <-p.events:
			event := queued.event
			ctx := withIncomingMeta(p.options.Propagator.Extract(p.ctx, queued.trace), queued.meta)
			ctx = context.WithValue(ctx, wireFrameKey{}, frame{Version: 1, Kind: "event", Event: event.Name, Data: event.Data,
				Traceparent: queued.trace.Parent, Tracestate: queued.trace.State, Meta: queued.meta})
			p.observeDelivered(queued)
			p.mu.Lock()
			handler := p.eventHandlers[event.Name]
			fallback := p.eventFallback
			listeners := make([]func(context.Context, Event), 0, len(p.listeners))
			for _, l := range p.listeners {
				listeners = append(listeners, l)
			}
			p.mu.Unlock()
			if fallback != nil {
				if attached := fallback(event.Name); attached != nil {
					handler = attached
				}
			}
			func() {
				defer func() {
					if recover() != nil {
						p.fail(errors.New("duplex event handler panic"))
					}
				}()
				if handler != nil {
					handler(ctx, p, event.Data)
				}
				for _, listener := range listeners {
					listener(ctx, event)
				}
			}()
		}
	}
}

func decodeFrame(data []byte) (frame, error) {
	var f frame
	if err := scalarjson.Raw(data); err != nil {
		return f, err
	}
	// Validate members separately so duplicate fields and explicit members from
	// another frame kind cannot disappear into Go zero values while decoding.
	fields, err := frameMembers(data)
	if err != nil {
		return f, err
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&f); err != nil {
		return f, fmt.Errorf("invalid duplex frame: %w", err)
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return f, errors.New("invalid trailing duplex frame content")
	}
	if f.Version != 1 {
		return f, errors.New("unsupported duplex frame version")
	}
	valid := false
	allowed := map[string]bool{"version": true, "kind": true, "traceparent": true, "tracestate": true}
	switch f.Kind {
	case "request":
		allowed["id"], allowed["method"], allowed["params"], allowed["meta"] = true, true, true, true
		valid = f.ID != "" && f.Method != "" && len(f.Params) > 0 && len(f.Result) == 0 && f.Error == nil && f.Event == "" && len(f.Data) == 0
	case "response":
		allowed["id"], allowed["result"], allowed["error"] = true, true, true
		valid = f.ID != "" && f.Method == "" && len(f.Params) == 0 && (len(f.Result) > 0) != (f.Error != nil) && f.Event == "" && len(f.Data) == 0 && f.Meta == nil
		_, hasResult := fields["result"]
		_, hasError := fields["error"]
		valid = valid && hasResult != hasError
	case "event":
		allowed["event"], allowed["data"], allowed["meta"] = true, true, true
		valid = f.ID == "" && f.Method == "" && len(f.Params) == 0 && len(f.Result) == 0 && f.Error == nil && f.Event != "" && len(f.Data) > 0
	case "cancel":
		allowed["id"] = true
		valid = f.ID != "" && f.Method == "" && len(f.Params) == 0 && len(f.Result) == 0 && f.Error == nil && f.Event == "" && len(f.Data) == 0 && f.Meta == nil
	}
	for name := range fields {
		if !allowed[name] {
			valid = false
		}
	}
	if f.Error != nil && (f.Error.Code == "" || f.Error.Message == "") {
		valid = false
	}
	// A trace the peer cannot read is a trace it would carry wrongly; tracestate
	// has no form of its own and travels alone when an intermediary strips one.
	if _, traced := fields["traceparent"]; traced && !validTraceparent(f.Traceparent) {
		valid = false
	}
	// Meta maps names to strings and may be empty; keys under the reserved
	// prefix are the profile's to define and it defines none in this version,
	// so a frame carrying one is refused rather than read as a consumer's.
	if raw, carried := fields["meta"]; carried && !validMeta(raw) {
		valid = false
	}
	if !valid {
		return f, errors.New("invalid duplex frame shape")
	}
	return f, nil
}

func frameMembers(data []byte) (map[string]json.RawMessage, error) {
	d := json.NewDecoder(bytes.NewReader(data))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return nil, errors.New("duplex frame must be an object")
	}
	members := make(map[string]json.RawMessage)
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return nil, err
		}
		name, ok := key.(string)
		if !ok {
			return nil, errors.New("invalid duplex field name")
		}
		if _, exists := members[name]; exists {
			return nil, fmt.Errorf("duplicate duplex field %q", name)
		}
		var value json.RawMessage
		if err := d.Decode(&value); err != nil {
			return nil, err
		}
		members[name] = value
	}
	if _, err := d.Token(); err != nil {
		return nil, err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return nil, errors.New("invalid trailing duplex frame content")
	}
	return members, nil
}
