// Package engine implements the bitwire/1 protocol over a frames duplex
// connection: JSON text frames carrying requests, responses, events and
// cancellations, correlated by serial request identifiers. A Peer realizes an
// Endpoint over any transport of the seam; the addressed path of a request or
// event travels as its canonical encoding in the frame's method or event
// name. It has no application authorization, replay, retries or persistence
// policy.
//
// bitwire/1 is the behavior of nightseam v0.6.0's nightseam.duplex/1 profile
// (bitwire decision 0008). This engine accepts, refuses and sends what that
// release does; it presents the protocol only through its root Endpoint.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	core "github.com/Bitspark/bitruntime/core/go"
	"github.com/Bitspark/bitruntime/internal/delivery/go"
	"github.com/Bitspark/bitruntime/internal/profile/go"
	"github.com/Bitspark/bitruntime/internal/request/go"
	transports "github.com/Bitspark/bitruntime/transports/go"
	wire "github.com/Bitspark/bitwire/wire/go"
)

// Protocol names the protocol revision this engine speaks. The name never
// travels on a connection: bitwire/1 agrees on its revision out of band.
const Protocol = "bitwire/1"

// Role is the side of a connection a peer takes. It decides the prefix of the
// request identifiers the peer mints — c: for a client, s: for a server — so
// the two sides never mint the same one.
type Role string

const (
	// ClientRole dials and mints request identifiers under the c: prefix.
	ClientRole Role = "client"
	// ServerRole accepts and mints request identifiers under the s: prefix.
	ServerRole Role = "server"
)

// Options limits are per connection. Zero values select the defaults.
// Handlers may run concurrently; event deliveries run serially in receive
// order.
type Options struct {
	// Prepare runs on the peer once it is built and before it reads its
	// first frame: a receiver it attaches to the peer's Wire is there before
	// anything can arrive, so the other side's first request cannot be
	// refused method_not_found while the receiver is on its way. An error
	// fails the construction: the peer never runs and the constructor answers
	// with it.
	Prepare func(*Peer) error
	// MaxConcurrentHandlers bounds the incoming requests running at once; the
	// one past it is refused busy. Default 64.
	MaxConcurrentHandlers int
	// MaxPendingRequests bounds the calls this peer may have outstanding at
	// once; the one past it is refused busy without reaching the wire. It
	// also bounds the outgoing requests its root holds admitted. Default 128.
	MaxPendingRequests int
	// QueueCapacity bounds the outgoing frame queue, the incoming event queue
	// and the root's own queue, in frames. Default 128.
	QueueCapacity int
	// MaxFrameBytes is the largest frame the peer sends or receives. Default
	// 1 MiB.
	MaxFrameBytes int64
	// RequestTimeout bounds an outgoing call and an incoming request's
	// handler. Default 30 seconds.
	RequestTimeout time.Duration
	// WriteTimeout bounds a stalled write and a stalled consumer before the
	// connection ends. Default 10 seconds.
	WriteTimeout time.Duration
	// Propagator moves a trace between a frame and a handler's context.
	Propagator core.Propagator
}

// Normalized returns the options with every default applied, or an error for
// a negative limit.
func (o Options) Normalized() (Options, error) {
	if o.MaxConcurrentHandlers < 0 || o.MaxPendingRequests < 0 || o.QueueCapacity < 0 || o.MaxFrameBytes < 0 || o.RequestTimeout < 0 || o.WriteTimeout < 0 {
		return o, errors.New("bitruntime: engine limits must not be negative")
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
		o.Propagator = core.DefaultPropagator
	}
	return o, nil
}

// queuedFrame is one frame waiting for the writer.
type queuedFrame struct {
	data  []byte
	frame profile.Frame
}

// queuedEvent keeps an event's trace beside it across the bounded queue: its
// delivery runs under the context the trace was extracted into.
type queuedEvent struct {
	name  string
	data  json.RawMessage
	trace core.Trace
	meta  core.Meta
}

// Peer owns a frames duplex connection until Close or transport failure.
// Never send on or receive from the connection after handing it to NewPeer.
// The connection's receive limit is its maker's to set to MaxFrameBytes; the
// peer refuses a larger frame it is nonetheless handed.
type Peer struct {
	root         *rootWire
	conn         transports.Conn
	ctx          context.Context
	cancel       context.CancelFunc
	options      Options
	prefix       string
	remotePrefix string
	subprotocol  string
	next         uint64
	publish      sync.Mutex
	admitted     uint64
	done         chan struct{}
	once         sync.Once
	mu           sync.Mutex
	err          error
	pending      map[string]chan request.Result
	incoming     map[string]context.CancelFunc
	outputs      chan queuedFrame
	events       chan queuedEvent
	slots        chan struct{}
}

// NewPeer speaks bitwire/1 over any connection of the seam — a pipe, a
// socket already accepted, a tunnel channel — as the given role. The peer owns
// the connection from here and closes it when it ends; ctx ending ends the
// peer. If the connection reports a Subprotocol, the peer reports it too.
func NewPeer(ctx context.Context, conn transports.Conn, role Role, options Options) (*Peer, error) {
	if ctx == nil || conn == nil {
		return nil, errors.New("bitruntime: a peer requires a context and connection")
	}
	if role != ClientRole && role != ServerRole {
		return nil, errors.New("bitruntime: invalid peer role")
	}
	o, err := options.Normalized()
	if err != nil {
		return nil, err
	}
	var subprotocol string
	if negotiated, ok := conn.(interface{ Subprotocol() string }); ok {
		subprotocol = negotiated.Subprotocol()
	}
	ctx, cancel := context.WithCancel(ctx)
	p := &Peer{conn: conn, ctx: ctx, cancel: cancel, options: o, prefix: "c:", remotePrefix: "s:", subprotocol: subprotocol, done: make(chan struct{}),
		pending: make(map[string]chan request.Result), incoming: make(map[string]context.CancelFunc),
		outputs: make(chan queuedFrame, o.QueueCapacity), events: make(chan queuedEvent, o.QueueCapacity), slots: make(chan struct{}, o.MaxConcurrentHandlers)}
	if role == ServerRole {
		p.prefix, p.remotePrefix = "s:", "c:"
	}
	p.root = &rootWire{peer: p, wake: make(chan struct{}, 1), incoming: map[returnKey]*routedCall{}}
	// The root runs before Prepare, so what Prepare attaches or sends through
	// it is released and answered even when Prepare fails.
	go p.root.run()
	if o.Prepare != nil {
		if err := o.Prepare(p); err != nil {
			p.abandon(err)
			return nil, err
		}
	}
	go p.readLoop()
	go p.writeLoop()
	go p.eventLoop()
	go func() { <-ctx.Done(); p.fail(ctx.Err()) }()
	return p, nil
}

// Done is closed when the peer has ended, for whatever reason; Err says which.
func (p *Peer) Done() <-chan struct{} { return p.done }

// Context is the peer's own, cancelled when it ends: what a handler or a
// caller derives its own from to be released with the connection. It carries
// the values of the context the peer was made with, such as an authenticated
// identity.
func (p *Peer) Context() context.Context { return p.ctx }

// Role is the side of the connection this peer is.
func (p *Peer) Role() Role {
	if p.prefix == "s:" {
		return ServerRole
	}
	return ClientRole
}

// Subprotocol is what the handshake beneath this peer selected, and "" when it
// selected none. bitwire/1 reads nothing into it.
func (p *Peer) Subprotocol() string { return p.subprotocol }

// MaxFrameBytes is the largest frame this peer sends or receives.
func (p *Peer) MaxFrameBytes() int64 { return p.options.MaxFrameBytes }

// Err is why the peer ended, or nil while it runs. An ended peer's error is a
// closed carrier — errors.Is(err, transports.ErrClosed) — and also matches its
// cause: core.ErrBackpressure, the context's error, or the connection's own.
func (p *Peer) Err() error { p.mu.Lock(); defer p.mu.Unlock(); return p.err }

// Close ends the peer, closing the connection with 1000 and failing every
// pending call. It is safe to call more than once.
func (p *Peer) Close() error { p.end(transports.ErrClosed, transports.CodeNormal, ""); return nil }

// Wire is the peer's root origin: send access to the remote side's paths,
// receive attachment for the requests and events the remote side sends, and
// the connection's closure. Repeated calls return the same endpoint.
func (p *Peer) Wire() wire.Endpoint { return p.root }

// fail ends the peer on a transport there is nothing to say over: a write that
// failed, the context ending, a consumer that stalled past its deadline. The
// connection is aborted and the far side reads an abnormal closure.
func (p *Peer) fail(err error) { p.end(err, codeAborted, "") }

// refuse ends the peer on a frame the protocol does not admit — a malformed
// envelope, an identifier that correlates with nothing, a frame of the wrong
// kind. The other side broke the protocol and is told so, with 4011 and a
// reason, because an intermediary can act on a code.
func (p *Peer) refuse(err error) { p.end(err, transports.CodeProtocol, err.Error()) }

// abandon releases a peer that never ran: Prepare failed, the loops were never
// started and nothing reached the wire. The connection is disposed of by the
// constructor that opened it.
func (p *Peer) abandon(err error) {
	p.once.Do(func() {
		p.mu.Lock()
		p.err = core.Ended(err)
		p.mu.Unlock()
		p.cancel()
		close(p.done)
	})
}

// codeAborted stands for no close at all: the connection is aborted, nothing
// is sent, and the far side reads 1006.
const codeAborted transports.Code = 0

// end ends the peer once, whatever ended it: every pending call is released
// and the connection is closed with the code this side decided on, or aborted
// where there is none or the code may only be observed.
func (p *Peer) end(err error, code transports.Code, reason string) {
	p.once.Do(func() {
		// This outcome settles every pending request, including ones already
		// delivered. Another send's proof cannot be broadcast as their proof.
		err = core.WithoutUnpublishedProof(core.Ended(err))
		p.mu.Lock()
		p.err = err
		p.mu.Unlock()
		close(p.done)
		// The connection is closed before the peer's context is cancelled. The
		// reader receives under that context, and a WebSocket whose pending read
		// is cancelled drops the socket, so the far side would observe 1006
		// instead of the code chosen here.
		if code == codeAborted || !transports.Sendable(code) {
			_ = p.conn.Abort()
		} else {
			// The handshake waits on a context of its own: a far side that
			// answers is told the code, and one that does not holds nothing up
			// past the deadline.
			ctx, cancel := context.WithTimeout(context.Background(), p.options.WriteTimeout)
			_ = p.conn.Close(ctx, code, closeReason(reason))
			cancel()
		}
		p.cancel()
	})
}

// closeReason is what a close frame admits: a reason of at most 123 bytes of
// valid UTF-8, cut on a rune, since a transport handed a longer one would
// close with no code at all.
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

type admittedCall struct {
	await    func(any) error
	withdraw func()
}

// beginCall performs the bounded admission of one outgoing request
// synchronously. The root admits frames in its delivery order, then waits for
// each result separately; starting goroutines before admission would reorder
// requests and events. A structured frame already carries its trace, which the
// request keeps verbatim.
func (p *Peer) beginCall(ctx context.Context, method string, params json.RawMessage, trace core.Trace) (*admittedCall, error) {
	if ctx == nil || method == "" {
		return nil, core.Unpublished(errors.New("bitruntime: a call requires a context and method"))
	}
	ctx, cancel := context.WithTimeout(ctx, p.options.RequestTimeout)
	if err := ctx.Err(); err != nil {
		cancel()
		return nil, core.Unpublished(err)
	}
	reply := make(chan request.Result, 1)
	// Reservation and publication happen under the outgoing queue's one
	// ordering gate, so serials reach the other side in the order they were
	// taken. A reservation that never publishes has still spent its serial,
	// and the receiver allows the gap it leaves.
	p.publish.Lock()
	if p.next == maxRequestSerial {
		p.publish.Unlock()
		cancel()
		p.fail(errors.New("bitruntime: request serials exhausted"))
		return nil, core.Unpublished(core.Ended(&core.PublicError{Code: "identifier_exhausted", Message: "Create a new peer before issuing further calls"}))
	}
	p.next++
	id := p.prefix + strconv.FormatUint(p.next, 10)
	p.mu.Lock()
	if p.err != nil {
		err := p.err
		p.mu.Unlock()
		p.publish.Unlock()
		cancel()
		return nil, core.Unpublished(err)
	}
	// The caller's own bound. A call past it never reaches the wire.
	if len(p.pending) >= p.options.MaxPendingRequests {
		p.mu.Unlock()
		p.publish.Unlock()
		cancel()
		return nil, core.Unpublished(&core.PublicError{Code: "busy", Message: "Outstanding call limit reached"})
	}
	p.pending[id] = reply
	p.mu.Unlock()
	finish := func() { cancel(); p.mu.Lock(); delete(p.pending, id); p.mu.Unlock() }
	err := p.enqueueFrame(ctx, profile.Frame{Version: 1, Kind: "request", ID: id, Method: method, Params: params,
		Traceparent: trace.Parent, Tracestate: trace.State, Meta: delivery.OutgoingMeta(ctx)}, true)
	p.publish.Unlock()
	if err != nil {
		finish()
		return nil, core.Unpublished(err)
	}
	// The call completes once, by its response, its deadline or the caller's
	// withdrawal; only a withdrawal still owes the remote side a cancellation.
	var completed sync.Once
	complete := func(withdrawn bool) {
		completed.Do(func() {
			if withdrawn {
				p.cancelRequest(id, trace)
			}
		})
	}
	return &admittedCall{await: func(result any) error {
		defer finish()
		cancelRemote, err := request.Await(ctx, reply, p.done, p.Err, result)
		if cancelRemote && p.ctx.Err() != nil {
			// The call's context derives from the peer's, which ends with the
			// peer, including when the context the peer was made with ends: a
			// call cut off that way is disconnected, not withdrawn, and owes
			// the far side no cancellation.
			<-p.done
			complete(false)
			return p.Err()
		}
		complete(cancelRemote)
		return err
	}, withdraw: func() {
		cancel()
		// A routed cancel occupies a position in this wire's send order. Its
		// best-effort admission finishes before the next frame.
		complete(true)
	}}, nil
}

// cancelRequest is best effort: a congested transport must not extend the
// caller's already-expired deadline while waiting to send its cancellation.
func (p *Peer) cancelRequest(id string, trace core.Trace) {
	f := profile.Frame{Version: 1, Kind: "cancel", ID: id, Traceparent: trace.Parent, Tracestate: trace.State}
	data, err := profile.MarshalJSON(f)
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

// emit queues an outgoing event. Success means queued for this connection,
// not persisted or processed by the remote application.
func (p *Peer) emit(ctx context.Context, event string, data json.RawMessage, trace core.Trace) error {
	if ctx == nil || event == "" {
		return core.Unpublished(errors.New("bitruntime: an event requires a context and name"))
	}
	f := profile.Frame{Version: 1, Kind: "event", Event: event, Data: data,
		Traceparent: trace.Parent, Tracestate: trace.State, Meta: delivery.OutgoingMeta(ctx)}
	return core.Unpublished(p.enqueueFrame(ctx, f, true))
}

func (p *Peer) enqueue(ctx context.Context, f profile.Frame) error {
	return p.enqueueFrame(ctx, f, false)
}

func (p *Peer) enqueueFrame(ctx context.Context, f profile.Frame, immediate bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := profile.MarshalJSON(f)
	if err != nil {
		return err
	}
	if int64(len(data)) > p.options.MaxFrameBytes {
		return errors.New("bitruntime: frame exceeds size limit")
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
		// A response paces a transient burst for one write deadline. Root
		// traffic instead requires an immediate handoff so a composition does
		// not run at its slowest destination's pace.
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
	p.fail(core.ErrBackpressure)
	return core.Ended(core.ErrBackpressure)
}

func (p *Peer) writeLoop() {
	for {
		select {
		case <-p.done:
			return
		case queued := <-p.outputs:
			ctx, cancel := context.WithTimeout(p.ctx, p.options.WriteTimeout)
			err := p.conn.Send(ctx, transports.Frame{Kind: transports.Text, Data: queued.data})
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
		if received.Kind != transports.Text {
			p.refuse(errors.New("duplex requires JSON text frames"))
			return
		}
		if int64(len(received.Data)) > p.options.MaxFrameBytes {
			p.refuse(errors.New("duplex frame exceeds size limit"))
			return
		}
		f, err := profile.Decode(received.Data)
		if err != nil {
			p.refuse(err)
			return
		}
		if f.ID != "" {
			prefix := p.remotePrefix
			if f.Kind == "response" {
				prefix = p.prefix
			}
			if !profile.ValidID(f.ID, prefix) {
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
		switch f.Kind {
		case "response":
			p.mu.Lock()
			reply := p.pending[f.ID]
			delete(p.pending, f.ID)
			p.mu.Unlock()
			if reply != nil {
				r := request.Result{Value: f.Result}
				if f.Error != nil {
					r.Err = &core.PublicError{Code: f.Error.Code, Message: f.Error.Message, Data: f.Error.Data}
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
			if !p.enqueueEvent(queuedEvent{name: f.Event, data: f.Data, trace: core.Trace{Parent: f.Traceparent, State: f.Tracestate}, meta: f.Meta}) {
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
	// A full queue can be a healthy transient burst, so the producer is paced
	// for one write deadline before the consumer is declared stalled. The
	// producer here is the remote, and the only way to pace it is to stop
	// reading: while this waits, responses and cancellations on this
	// connection wait with it. The deadline is what bounds that cost.
	timer := time.NewTimer(p.options.WriteTimeout)
	defer timer.Stop()
	select {
	case p.events <- event:
		return true
	case <-p.done:
		return false
	case <-timer.C:
		p.fail(core.ErrBackpressure)
		return false
	}
}

// maxRequestSerial is the largest serial a sender publishes. A sender that
// would wrap refuses and ends the connection instead, since a wrapped serial
// would name an invocation the receiver has already seen.
const maxRequestSerial = uint64(1)<<63 - 1

// admit holds an incoming request to publication order: within one
// connection and one direction, each request's serial is greater than every
// request's published before it. Gaps are allowed.
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

type frameKey struct{}

func (p *Peer) startRequest(f profile.Frame) {
	p.mu.Lock()
	if _, exists := p.incoming[f.ID]; exists {
		p.mu.Unlock()
		p.refuse(errors.New("duplicate active duplex request identifier"))
		return
	}
	p.mu.Unlock()
	handler := p.root.requestHandler(f.Method)
	// A response carries its request's trace, whether a handler ran or not.
	trace := core.Trace{Parent: f.Traceparent, State: f.Tracestate}
	if handler == nil {
		p.rejectRequest(f.ID, trace, &core.PublicError{Code: "method_not_found", Message: "Unknown method"})
		return
	}
	select {
	case p.slots <- struct{}{}:
	default:
		p.rejectRequest(f.ID, trace, &core.PublicError{Code: "busy", Message: "Too many concurrent requests"})
		return
	}
	// What the handler sends is a child of the request that ran it, and
	// carries the request's meta only where the handler says so.
	handling := delivery.WithIncomingMeta(p.options.Propagator.Extract(p.ctx, trace), f.Meta)
	ctx, cancel := context.WithTimeout(handling, p.options.RequestTimeout)
	ctx = context.WithValue(ctx, frameKey{}, f)
	p.mu.Lock()
	p.incoming[f.ID] = cancel
	p.mu.Unlock()
	var answered sync.Once
	respond := func(result json.RawMessage, err error) {
		answered.Do(func() {
			// The deadline has already won when it releases the body, even if
			// that body beats the asynchronous deadline callback to this once.
			// Explicit withdrawal still preserves a later public refusal.
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				result, err = nil, context.DeadlineExceeded
			}
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
		result, err := invoke(ctx, handler, f.Params)
		if err == nil && ctx.Err() != nil {
			err = ctx.Err()
		}
		// Completion answers once, unless the receiver deadline already did.
		respond(result, err)
	}()
}

func invoke(ctx context.Context, handler func(context.Context, json.RawMessage) (json.RawMessage, error), params json.RawMessage) (result json.RawMessage, err error) {
	defer func() {
		if value := recover(); value != nil {
			err = errors.New("bitruntime: handler panic")
		}
	}()
	return handler(ctx, params)
}

// Rejections run on the reader because they do not consume handler slots. They
// must never wait for outbound capacity: that could hold up a response needed
// by an already active reverse call. A flood exhausting the rejection capacity
// closes the overloaded connection after giving the writer a scheduling turn.
func (p *Peer) rejectRequest(id string, trace core.Trace, public *core.PublicError) {
	f := profile.Frame{Version: 1, Kind: "response", ID: id, Error: &wire.ProfileError{Code: public.Code, Message: public.Message, Data: public.Data},
		Traceparent: trace.Parent, Tracestate: trace.State}
	data, err := profile.MarshalJSON(f)
	if err != nil || int64(len(data)) > p.options.MaxFrameBytes {
		p.fail(errors.New("bitruntime: rejection exceeds frame limit"))
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
		p.fail(core.ErrBackpressure)
	}
}

func (p *Peer) respond(id string, trace core.Trace, result json.RawMessage, err error) {
	f := profile.Frame{Version: 1, Kind: "response", ID: id, Traceparent: trace.Parent, Tracestate: trace.State}
	if err != nil {
		var public *core.PublicError
		switch {
		case errors.As(err, &public) && public != nil && public.Code != "" && public.Message != "":
			f.Error = &wire.ProfileError{Code: public.Code, Message: public.Message, Data: public.Data}
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			f.Error = &wire.ProfileError{Code: "cancelled", Message: "Request cancelled"}
		default:
			f.Error = &wire.ProfileError{Code: "internal", Message: "Internal error"}
		}
	} else {
		f.Result = result
		if len(f.Result) == 0 {
			f.Result = json.RawMessage("null")
		}
	}
	if err := p.enqueue(p.ctx, f); err != nil && p.ctx.Err() == nil {
		// An oversized or unencodable result cannot leave the remote call
		// hanging.
		fallback := profile.Frame{Version: 1, Kind: "response", ID: id, Error: &wire.ProfileError{Code: "internal", Message: "Response could not be encoded"},
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
			ctx := delivery.WithIncomingMeta(p.options.Propagator.Extract(p.ctx, queued.trace), queued.meta)
			failed := func() (failed bool) {
				defer func() {
					if recover() != nil {
						failed = true
					}
				}()
				p.root.deliverEvent(ctx, queued)
				return false
			}()
			if failed {
				p.fail(errors.New("bitruntime: event receiver panic"))
			}
		}
	}
}
