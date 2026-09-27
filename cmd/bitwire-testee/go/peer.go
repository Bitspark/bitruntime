// Ported from nightseam v0.6.0 conformance/go/testee/peer.go (5cc9723).

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"

	core "github.com/Bitspark/bitruntime/core/go"
	dispatch "github.com/Bitspark/bitruntime/dispatch/go"
	engine "github.com/Bitspark/bitruntime/engine/go"
	transports "github.com/Bitspark/bitruntime/transports/go"
	ws "github.com/Bitspark/bitruntime/transports/websocket/go"
	wire "github.com/Bitspark/bitwire/wire/go"
)

// A peer under control: bitruntime's engine Peer, the dispatcher that serves
// its root, what it received, what its canned handlers saw, and how its
// connection ended.
//
// A method or an event the runner names is one path segment at the peer's
// root: "echo" is called and served at the path ["echo"], which bitwire/1
// carries as the canonical name "4:echo". bitruntime serves only canonical
// names, so a raw frame naming a plain "echo" is answered method_not_found.
type peer struct {
	*engine.Peer
	conn     *watched
	routes   *dispatch.Dispatcher
	timeout  time.Duration
	events   *inbox[delivered]
	requests *inbox[lifecycle]
	cancel   context.CancelFunc

	mu      sync.Mutex
	served  map[string]*served
	waiters map[*waiter]struct{}
}

// lifecycle is one phase of one request a canned handler served.
type lifecycle struct {
	ID      string            `json:"id"`
	Method  string            `json:"method"`
	Phase   string            `json:"phase"`
	Outcome string            `json:"outcome,omitempty"`
	Meta    map[string]string `json:"meta,omitempty"`
}

// delivered is an event as peer.await_event reports it: its name, its data
// and the carriage its frame took.
type delivered struct {
	name string
	data json.RawMessage
	meta map[string]string
}

// served is what the peer serves at one name: a canned request handler, an
// event behaviour, or both, registered as one route.
type served struct {
	handlers dispatch.Handlers
	detach   func()
}

// waiter is a canned handler waiting for the remote to emit an event.
type waiter struct {
	name     string
	released chan struct{}
}

// shutdown ends the peer at once: its context ending aborts the connection,
// with no close handshake to wait for.
func (p *peer) shutdown() {
	p.cancel()
	select {
	case <-p.Done():
	case <-time.After(time.Second):
	}
}

// peerOptions is a peer's options as the driver spells them: the engine's,
// the WebSocket read limit that matches its frame limit, and a call's
// deadline.
//
// request_timeout_ms is a call's deadline, which bitruntime's call helper
// takes per call (dispatch.CallOptions.Timeout). It is not the engine's
// RequestTimeout, which also bounds every incoming handler, a deadline the
// contract's option does not name; that stays the runtime's.
type peerOptions struct {
	engine  engine.Options
	limit   int64
	timeout time.Duration
}

func (r request) peerOptions() (peerOptions, error) {
	raw, err := r.object("options")
	if err != nil {
		return peerOptions{}, err
	}
	var o engine.Options
	var timeout time.Duration
	for key, value := range raw {
		var n int64
		switch key {
		case "max_frame_bytes", "max_pending_requests", "queue_capacity", "request_timeout_ms", "write_timeout_ms":
			if err := json.Unmarshal(value, &n); err != nil {
				return peerOptions{}, invalid("options.%s is an integer", key)
			}
		}
		switch key {
		case "max_frame_bytes":
			o.MaxFrameBytes = n
		case "max_pending_requests":
			o.MaxPendingRequests = int(n)
		case "queue_capacity":
			o.QueueCapacity = int(n)
		case "request_timeout_ms":
			if n < 0 {
				return peerOptions{}, invalid("options.request_timeout_ms must not be negative")
			}
			timeout = time.Duration(n) * time.Millisecond
		case "write_timeout_ms":
			o.WriteTimeout = time.Duration(n) * time.Millisecond
		case "propagate":
			var on bool
			if err := json.Unmarshal(value, &on); err != nil {
				return peerOptions{}, invalid("options.propagate is a boolean")
			}
			// The engine and the call helper use the default propagator unless
			// given another; the option says a scenario relies on it.
			if on {
				o.Propagator = core.DefaultPropagator
			}
		case "observe":
			var on bool
			if err := json.Unmarshal(value, &on); err != nil {
				return peerOptions{}, invalid("options.observe is a boolean")
			}
			if on {
				return peerOptions{}, unsupported("the observer: bitruntime has none")
			}
		case "families":
			var families map[string]string
			if err := json.Unmarshal(value, &families); err != nil {
				return peerOptions{}, invalid("options.families maps names to families")
			}
			if len(families) > 0 {
				return peerOptions{}, unsupported("families, which only an observer reports")
			}
		default:
			return peerOptions{}, unsupported("option " + key)
		}
	}
	normalized, err := o.Normalized()
	if err != nil {
		return peerOptions{}, invalid("%v", err)
	}
	if timeout == 0 {
		// The runtime's: the call helper's default, which is the engine's.
		timeout = normalized.RequestTimeout
	}
	return peerOptions{engine: o, limit: normalized.MaxFrameBytes, timeout: timeout}, nil
}

// meta is the carriage a step gave a call or an event, and nil where it gave
// none: an object of strings, as the profile's member is.
func (r request) meta() (map[string]string, error) {
	raw, ok := r.args["meta"]
	if !ok {
		return nil, nil
	}
	var meta map[string]string
	if err := json.Unmarshal(raw, &meta); err != nil {
		return nil, invalid("meta is an object of strings")
	}
	return meta, nil
}

// subprotocols is what a peer.listen selects from or a peer.dial offers at
// the handshake; absent, none is offered and none selected, as the runtime
// defaults.
func (r request) subprotocols() ([]string, error) {
	raw, ok := r.args["subprotocols"]
	if !ok {
		return nil, nil
	}
	var tokens []string
	if err := json.Unmarshal(raw, &tokens); err != nil {
		return nil, invalid("subprotocols is an array of strings")
	}
	return tokens, nil
}

// start speaks bitwire/1 over c as role. The dispatcher that serves the
// peer's root is attached before the peer reads its first frame, so what the
// remote sends first is recorded rather than lost.
func start(c transports.Conn, role engine.Role, o peerOptions) (*peer, error) {
	p := &peer{conn: watch(c), timeout: o.timeout, events: newInbox[delivered](), requests: newInbox[lifecycle](),
		served: map[string]*served{}, waiters: map[*waiter]struct{}{}}
	options := o.engine
	options.Prepare = func(root *engine.Peer) error {
		// The dispatcher owns the root, so an event body's error ends the peer
		// as the protocol error dispatch.EventHandler says it is.
		routes, err := dispatch.NewDispatcher(root.Wire(), dispatch.DispatcherOptions{OwnEndpoint: true})
		if err != nil {
			return err
		}
		if _, err := routes.RegisterPrefix(nil, p.unrouted()); err != nil {
			return err
		}
		p.routes = routes
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	running, err := engine.NewPeer(ctx, p.conn, role, options)
	if err != nil {
		cancel()
		return nil, err
	}
	p.Peer, p.cancel = running, cancel
	go func() {
		<-running.Done()
		p.events.close()
	}()
	return p, nil
}

// refuse ends a connection a peer was not made over, with the code a policy
// refusal carries (1008), as bitruntime's WebSocket adapter does.
func refuse(c transports.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = c.Close(ctx, transports.CodePolicyViolation, "the connection was refused before the profile began")
}

// unrouted receives what no canned handler is registered for: every event,
// held for peer.await_event, and requests, answered method_not_found as a
// peer without that handler answers.
func (p *peer) unrouted() wire.Receiver {
	return wire.Receiver{Message: func(path []string, message wire.Message) {
		switch message.Frame.Kind {
		case wire.ProfileEvent:
			// An event at a path of other than one segment has no name the
			// runner can ask for, and is dropped as one nothing handles.
			if len(path) != 1 {
				return
			}
			p.notify(path[0])
			p.events.put(delivered{name: path[0], data: bytes.Clone(message.Frame.Data), meta: message.Frame.Meta})
		case wire.ProfileRequest:
			_ = core.Respond(message, nil, &core.PublicError{Code: "method_not_found", Message: "Unknown method"})
		}
	}}
}

// serve changes what the peer serves at name. A route is replaced by
// detaching it and registering its successor.
func (p *peer) serve(name string, change func(*dispatch.Handlers) error) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var handlers dispatch.Handlers
	current := p.served[name]
	if current != nil {
		handlers = current.handlers
	}
	if err := change(&handlers); err != nil {
		return err
	}
	if current != nil {
		current.detach()
		delete(p.served, name)
	}
	detach, err := dispatch.Register(p.routes, []string{name}, handlers)
	if err != nil {
		return invalid("%v", err)
	}
	p.served[name] = &served{handlers: handlers, detach: detach}
	return nil
}

// subscribe is released when the remote emits name.
func (p *peer) subscribe(name string) (<-chan struct{}, func()) {
	w := &waiter{name: name, released: make(chan struct{}, 1)}
	p.mu.Lock()
	p.waiters[w] = struct{}{}
	p.mu.Unlock()
	return w.released, func() {
		p.mu.Lock()
		delete(p.waiters, w)
		p.mu.Unlock()
	}
}

func (p *peer) notify(name string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for w := range p.waiters {
		if w.name == name {
			select {
			case w.released <- struct{}{}:
			default:
			}
		}
	}
}

// peerListener accepts one peer at a URL, as the server. It upgrades the
// WebSocket itself rather than through bitruntime's engine/websocket, so the
// peer's connection is one the testee watches.
type peerListener struct {
	server   *http.Server
	url      string
	mu       sync.Mutex
	closed   bool
	accepted chan *peer
}

// take hands an accepted peer to peer.accept, or ends it when one was already
// accepted or the listener is gone.
func (l *peerListener) take(p *peer) {
	l.mu.Lock()
	taken := false
	if !l.closed {
		select {
		case l.accepted <- p:
			taken = true
		default:
		}
	}
	l.mu.Unlock()
	if !taken {
		p.shutdown()
	}
}

func (l *peerListener) shutdown() {
	_ = l.server.Close()
	l.mu.Lock()
	l.closed = true
	l.mu.Unlock()
	select {
	case p := <-l.accepted:
		p.shutdown()
	default:
	}
}

// call is one call in flight, and how it ended.
type call struct {
	cancel context.CancelFunc
	done   chan struct{}
	result json.RawMessage
	err    error
}

func (c *call) shutdown() { c.cancel() }

// callError maps how a call ended onto the driver's codes: the remote's
// public error, or what the caller's side made of it. A call the peer's end
// cut off is disconnected, whatever it was waiting for.
func callError(err error) *failure {
	var public *core.PublicError
	switch {
	case errors.As(err, &public):
		f := fail(public.Code, "%s", public.Message)
		if len(public.Data) > 0 {
			f.Members = map[string]any{"data": public.Data}
		}
		return f
	case errors.Is(err, transports.ErrClosed):
		return fail("disconnected", "%v", err)
	case errors.Is(err, context.Canceled):
		return fail("cancelled", "the caller gave up")
	case errors.Is(err, context.DeadlineExceeded):
		return fail("request_timeout", "the call's deadline passed")
	}
	return fail("failed", "%v", err)
}

// deadline is a call's: its own timeout_ms where it has one shorter than its
// peer's request_timeout_ms, and otherwise the peer's.
func (p *peer) deadline(ms int64) time.Duration {
	if own := time.Duration(ms) * time.Millisecond; own > 0 && own < p.timeout {
		return own
	}
	return p.timeout
}

func (t *testee) peerOps() map[string]func(request) (any, error) {
	return map[string]func(request) (any, error){
		"peer.listen": func(r request) (any, error) {
			options, err := r.peerOptions()
			if err != nil {
				return nil, err
			}
			subprotocols, err := r.subprotocols()
			if err != nil {
				return nil, err
			}
			socket, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				return nil, err
			}
			l := &peerListener{accepted: make(chan *peer, 1), url: "ws://" + socket.Addr().String()}
			l.server = &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				s, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true, Subprotocols: subprotocols})
				if err != nil {
					return
				}
				c := ws.New(s, options.limit)
				p, err := start(c, engine.ServerRole, options)
				if err != nil {
					refuse(c)
					return
				}
				l.take(p)
			})}
			go func() { _ = l.server.Serve(socket) }()
			return map[string]any{"handle": t.mint("pl", l), "url": l.url}, nil
		},
		"peer.accept": func(r request) (any, error) {
			l, err := object[*peerListener](t, r, "peer listener")
			if err != nil {
				return nil, err
			}
			within, err := r.within()
			if err != nil {
				return nil, err
			}
			select {
			case p := <-l.accepted:
				return map[string]any{"handle": t.mint("p", p), "subprotocol": p.Subprotocol()}, nil
			case <-time.After(within):
				return nil, fail("timeout", "nobody connected within %s", within)
			}
		},
		"peer.dial": func(r request) (any, error) {
			url, err := r.mustString("url")
			if err != nil {
				return nil, err
			}
			options, err := r.peerOptions()
			if err != nil {
				return nil, err
			}
			subprotocols, err := r.subprotocols()
			if err != nil {
				return nil, err
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			s, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{Subprotocols: subprotocols})
			if err != nil {
				return nil, fail("failed", "%v", err)
			}
			c := ws.New(s, options.limit)
			p, err := start(c, engine.ClientRole, options)
			if err != nil {
				refuse(c)
				return nil, fail("failed", "%v", err)
			}
			return map[string]any{"handle": t.mint("p", p), "subprotocol": p.Subprotocol()}, nil
		},
		"peer.over": func(r request) (any, error) {
			c, err := object[*conn](t, r, "connection")
			if err != nil {
				return nil, err
			}
			role, err := r.mustString("role")
			if err != nil {
				return nil, err
			}
			if role != "client" && role != "server" {
				return nil, invalid("role is client or server")
			}
			options, err := r.peerOptions()
			if err != nil {
				return nil, err
			}
			// The peer reads the connection from here on; the wrapper's reader,
			// if any, must not. A lazy connection has none.
			if !c.lazy {
				return nil, invalid("a peer takes a lazily consumed connection, since it reads it itself")
			}
			p, err := start(c.Conn, engine.Role(role), options)
			if err != nil {
				return nil, fail("failed", "%v", err)
			}
			return map[string]any{"handle": t.mint("p", p)}, nil
		},
		"peer.handle": func(r request) (any, error) {
			p, err := object[*peer](t, r, "peer")
			if err != nil {
				return nil, err
			}
			method, err := r.mustString("method")
			if err != nil {
				return nil, err
			}
			b, err := parseBehavior(r.raw("behavior"))
			if err != nil {
				return nil, err
			}
			switch b.Kind {
			case "echo", "return", "fail", "wait", "hold", "panic", "reverse", "emit":
			case "through":
				return nil, unsupported("the through behaviour, which is the live layer's")
			default:
				return nil, invalid("no such behaviour: %q", b.Kind)
			}
			return nil, p.serve(method, func(h *dispatch.Handlers) error {
				if h.Request != nil {
					return invalid("%s already has a handler", method)
				}
				h.Request = canned(p, method, b)
				return nil
			})
		},
		"peer.on_event": func(r request) (any, error) {
			p, err := object[*peer](t, r, "peer")
			if err != nil {
				return nil, err
			}
			name, err := r.mustString("name")
			if err != nil {
				return nil, err
			}
			b, err := parseBehavior(r.raw("behavior"))
			if err != nil {
				return nil, err
			}
			var handler dispatch.EventHandler
			switch b.Kind {
			case "", "record":
				// Every event nothing else handles is held for peer.await_event.
				return nil, nil
			case "block":
				handler = func(ctx context.Context, _ json.RawMessage) error {
					p.notify(name)
					<-ctx.Done()
					return nil
				}
			case "panic":
				value := panicValue(b)
				handler = func(context.Context, json.RawMessage) error {
					p.notify(name)
					panic(value)
				}
			default:
				return nil, invalid("an event handler records, blocks or panics")
			}
			return nil, p.serve(name, func(h *dispatch.Handlers) error {
				if h.Event != nil {
					return invalid("%s already has an event handler", name)
				}
				h.Event = handler
				return nil
			})
		},
		"peer.call": func(r request) (any, error) {
			p, err := object[*peer](t, r, "peer")
			if err != nil {
				return nil, err
			}
			method, err := r.mustString("method")
			if err != nil {
				return nil, err
			}
			ms, err := r.int("timeout_ms", 0)
			if err != nil {
				return nil, err
			}
			meta, err := r.meta()
			if err != nil {
				return nil, err
			}
			params := payload(r.raw("params"))
			// A call is made from no peer's context: the peer's end reaches it
			// as its connection's end, which is disconnected.
			ctx, cancel := context.WithCancel(core.WithMeta(context.Background(), meta))
			c := &call{cancel: cancel, done: make(chan struct{})}
			go func() {
				defer close(c.done)
				c.err = dispatch.Call(ctx, p.routes, []string{method}, params, &c.result, dispatch.CallOptions{Timeout: p.deadline(ms)})
			}()
			return map[string]any{"handle": t.mint("call", c)}, nil
		},
		"call.await": func(r request) (any, error) {
			c, err := object[*call](t, r, "call")
			if err != nil {
				return nil, err
			}
			within, err := r.within()
			if err != nil {
				return nil, err
			}
			select {
			case <-c.done:
			case <-time.After(within):
				return nil, fail("timeout", "no response within %s", within)
			}
			if c.err != nil {
				return map[string]any{"error": callError(c.err)}, nil
			}
			return map[string]any{"result": payload(c.result)}, nil
		},
		"call.cancel": func(r request) (any, error) {
			c, err := object[*call](t, r, "call")
			if err != nil {
				return nil, err
			}
			c.cancel()
			return nil, nil
		},
		"peer.emit": func(r request) (any, error) {
			p, err := object[*peer](t, r, "peer")
			if err != nil {
				return nil, err
			}
			event, err := r.mustString("event")
			if err != nil {
				return nil, err
			}
			within, err := r.within()
			if err != nil {
				return nil, err
			}
			meta, err := r.meta()
			if err != nil {
				return nil, err
			}
			ctx, cancel := context.WithTimeout(core.WithMeta(context.Background(), meta), within)
			defer cancel()
			if err := dispatch.Emit(ctx, p.routes, []string{event}, payload(r.raw("data"))); err != nil {
				switch {
				case errors.Is(err, context.DeadlineExceeded):
					return nil, fail("timeout", "the event was not sent within %s", within)
				case errors.Is(err, transports.ErrClosed):
					return nil, fail("disconnected", "%v", err)
				}
				return nil, invalid("the event was refused: %v", err)
			}
			return nil, nil
		},
		"peer.await_event": func(r request) (any, error) {
			p, err := object[*peer](t, r, "peer")
			if err != nil {
				return nil, err
			}
			name, err := r.mustString("name")
			if err != nil {
				return nil, err
			}
			within, err := r.within()
			if err != nil {
				return nil, err
			}
			e, ok, ended := p.events.await(within, func(d delivered) bool { return d.name == name })
			if ended {
				return nil, fail("disconnected", "the peer ended before %s arrived", name)
			}
			if !ok {
				return nil, fail("timeout", "no %s within %s", name, within)
			}
			answer := map[string]any{"data": payload(e.data)}
			if len(e.meta) > 0 {
				answer["meta"] = e.meta
			}
			return answer, nil
		},
		"peer.await_request": func(r request) (any, error) {
			p, err := object[*peer](t, r, "peer")
			if err != nil {
				return nil, err
			}
			method, err := r.mustString("method")
			if err != nil {
				return nil, err
			}
			phase, err := r.mustString("phase")
			if err != nil {
				return nil, err
			}
			within, err := r.within()
			if err != nil {
				return nil, err
			}
			// The lifecycle inbox stays open when the peer ends: a handler the
			// end released records its end after it.
			l, ok, _ := p.requests.await(within, func(l lifecycle) bool { return l.Method == method && l.Phase == phase })
			if !ok {
				return nil, fail("timeout", "no %s %s within %s", method, phase, within)
			}
			return l, nil
		},
		"peer.observed": func(request) (any, error) {
			return nil, unsupported("the observer: bitruntime has none")
		},
		"peer.close": func(r request) (any, error) {
			p, err := object[*peer](t, r, "peer")
			if err != nil {
				return nil, err
			}
			_ = p.Close()
			return nil, nil
		},
		"peer.await_close": func(r request) (any, error) {
			p, err := object[*peer](t, r, "peer")
			if err != nil {
				return nil, err
			}
			within, err := r.within()
			if err != nil {
				return nil, err
			}
			deadline := time.After(within)
			select {
			case <-p.Done():
			case <-deadline:
				return nil, fail("timeout", "the peer did not end within %s", within)
			}
			code, ok := p.conn.endedBy(deadline)
			if !ok {
				return nil, fail("timeout", "the peer's connection did not end within %s", within)
			}
			// Clean is a close somebody chose, whichever side: a peer closes
			// with 1000 by choice and with a code of its own when it refuses a
			// frame, and 1006 is what a side that aborted leaves behind.
			return map[string]any{"clean": code == transports.CodeNormal, "code": int(code)}, nil
		},
	}
}

// behavior is what a canned handler does.
type behavior struct {
	Kind    string          `json:"kind"`
	Value   json.RawMessage `json:"value"`
	Code    string          `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	Event   string          `json:"event"`
	Then    json.RawMessage `json:"then"`
	Until   string          `json:"until"`
}

func parseBehavior(raw json.RawMessage) (behavior, error) {
	var b behavior
	if raw == nil {
		return b, nil
	}
	if err := json.Unmarshal(raw, &b); err != nil {
		return b, invalid("behavior: %v", err)
	}
	return b, nil
}

// panicValue is what a panicking behaviour gives up with: its value's text.
func panicValue(b behavior) string {
	var value any = "the handler gave up"
	if b.Value != nil {
		_ = json.Unmarshal(b.Value, &value)
	}
	return fmt.Sprint(value)
}

// canned is a handler that does what its behaviour says and records its
// lifecycle for peer.await_request.
func canned(p *peer, method string, b behavior) dispatch.Handler {
	return func(ctx context.Context, params json.RawMessage) (result any, err error) {
		// bitruntime does not hand a handler its request id, so the lifecycle
		// carries none; a scenario holds the method and the phase.
		p.requests.put(lifecycle{Method: method, Phase: "started", Meta: core.MetaFrom(ctx)})
		defer func() {
			if recovered := recover(); recovered != nil {
				p.requests.put(lifecycle{Method: method, Phase: "ended", Outcome: "panic"})
				panic(recovered)
			}
			outcome := "ok"
			switch {
			case err == nil:
			case errors.Is(err, context.Canceled), errors.Is(ctx.Err(), context.Canceled):
				outcome = "cancelled"
			default:
				outcome = "error"
			}
			p.requests.put(lifecycle{Method: method, Phase: "ended", Outcome: outcome})
		}()
		switch b.Kind {
		case "echo":
			return payload(params), nil
		case "return":
			return payload(b.Value), nil
		case "fail":
			return nil, &core.PublicError{Code: b.Code, Message: b.Message, Data: b.Data}
		case "wait":
			<-ctx.Done()
			return nil, ctx.Err()
		case "hold":
			// The one handler that does not stop when it is told to: it holds
			// the request until the remote emits what releases it, cancelled
			// or not, which is how a scenario holds when a withdrawn request
			// is answered.
			released, stop := p.subscribe(b.Until)
			defer stop()
			select {
			case <-released:
			case <-p.Done():
			}
			return payload(b.Value), nil
		case "panic":
			panic(panicValue(b))
		case "reverse":
			with := b.Params
			if with == nil {
				with = params
			}
			var back json.RawMessage
			if err := dispatch.Call(ctx, p.routes, []string{b.Method}, payload(with), &back, dispatch.CallOptions{Timeout: p.timeout}); err != nil {
				return nil, err
			}
			return payload(back), nil
		case "emit":
			if err := dispatch.Emit(ctx, p.routes, []string{b.Event}, payload(b.Data)); err != nil {
				return nil, err
			}
			return payload(b.Then), nil
		}
		return nil, &core.PublicError{Code: "internal", Message: "no such behaviour: " + b.Kind}
	}
}
