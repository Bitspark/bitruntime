package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/Bitspark/nightseam/duplex/go"
	"github.com/Bitspark/nightseam/runtime/go"
)

// A peer under control: the runtime's Peer, what it received, what its
// canned handlers saw, and what its observer was told. Every peer is observed,
// since how it ended is read off the close its observer was told about;
// observe: true is what makes those events reportable to peer.observed.
type peer struct {
	*runtime.Peer
	events     *inbox[delivered]
	requests   *inbox[lifecycle]
	observer   *recorder
	observable bool
	cancel     context.CancelFunc
}

// lifecycle is one phase of one request a canned handler served.
type lifecycle struct {
	ID      string            `json:"id"`
	Method  string            `json:"method"`
	Phase   string            `json:"phase"`
	Outcome string            `json:"outcome,omitempty"`
	Meta    map[string]string `json:"meta,omitempty"`
}

// delivered is an event beside the carriage its frame took, which the Event
// itself does not carry: peer.await_event reports both.
type delivered struct {
	event runtime.Event
	meta  map[string]string
}

func (p *peer) shutdown() {
	p.cancel()
	_ = p.Close()
}

// options reads a peer's options as the driver spells them.
// subprotocols is what a peer.listen selects from or a peer.dial offers at
// the handshake; absent, none is offered and none selected, as the runtimes
// default.
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

func (t *testee) options(r request) (runtime.Options, *recorder, error) {
	raw, err := r.object("options")
	if err != nil {
		return runtime.Options{}, nil, err
	}
	// Every peer takes an observer, whether or not a scenario asks for its
	// events: how the connection ended is read off the close it is told of.
	rec := newRecorder()
	o := runtime.Options{Observer: rec}
	for key, value := range raw {
		var n int64
		switch key {
		case "max_frame_bytes", "max_pending_requests", "queue_capacity", "request_timeout_ms", "write_timeout_ms":
			if err := json.Unmarshal(value, &n); err != nil {
				return o, nil, invalid("options.%s is an integer", key)
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
			o.RequestTimeout = time.Duration(n) * time.Millisecond
		case "write_timeout_ms":
			o.WriteTimeout = time.Duration(n) * time.Millisecond
		case "families":
			if err := json.Unmarshal(value, &o.Families); err != nil {
				return o, nil, invalid("options.families maps names to families")
			}
		case "observe":
			var on bool
			if err := json.Unmarshal(value, &on); err != nil {
				return o, nil, invalid("options.observe is a boolean")
			}
			rec.keep = on
		case "propagate":
			// Go's peer always propagates; the option says a scenario relies on it.
		default:
			return o, nil, unsupported("option " + key)
		}
	}
	return o, rec, nil
}

// adopt takes a runtime peer under control: its events into an inbox.
func (t *testee) adopt(p *runtime.Peer, rec *recorder, cancel context.CancelFunc) *peer {
	w := &peer{Peer: p, events: newInbox[delivered](), requests: newInbox[lifecycle](), observer: rec, observable: rec.keep, cancel: cancel}
	p.OnEvent(func(ctx context.Context, e runtime.Event) {
		w.events.put(delivered{event: e, meta: runtime.MetaFrom(ctx)})
	})
	go func() {
		<-p.Done()
		w.events.close()
		w.requests.close()
	}()
	return w
}

// peerListener accepts one peer at a URL, as the server.
type peerListener struct {
	server   *http.Server
	url      string
	accepted chan *runtime.Peer
	rec      *recorder
}

func (l *peerListener) shutdown() { _ = l.server.Close() }

func (t *testee) peerOf(r request, name string) (*peer, error) {
	handle, err := r.mustString(name)
	if err != nil {
		return nil, err
	}
	object, ok := t.lookup(handle)
	if !ok {
		return nil, fail("unknown_handle", "%s", handle)
	}
	p, ok := object.(*peer)
	if !ok {
		return nil, invalid("%s is not a peer", handle)
	}
	return p, nil
}

// call is one call in flight, and how it ended.
type call struct {
	cancel context.CancelFunc
	done   chan struct{}
	result json.RawMessage
	err    error
}

// callError maps how a call ended onto the driver's codes.
func callError(err error, p *peer) *failure {
	var public *runtime.PublicError
	switch {
	case errors.As(err, &public):
		f := fail(public.Code, "%s", public.Message)
		if len(public.Data) > 0 {
			var data any
			_ = json.Unmarshal(public.Data, &data)
			f.Members = map[string]any{"data": data}
		}
		return f
	case errors.Is(err, context.Canceled):
		return fail("cancelled", "the caller gave up")
	case errors.Is(err, context.DeadlineExceeded):
		return fail("request_timeout", "the peer's deadline passed")
	case p.Err() != nil:
		return fail("disconnected", "%v", err)
	}
	return fail("failed", "%v", err)
}

func (t *testee) peerOps() map[string]func(request) (any, error) {
	return map[string]func(request) (any, error){
		"peer.listen": func(r request) (any, error) {
			options, rec, err := t.options(r)
			if err != nil {
				return nil, err
			}
			subprotocols, err := r.subprotocols()
			if err != nil {
				return nil, err
			}
			l := &peerListener{accepted: make(chan *runtime.Peer, 1), rec: rec}
			handler, err := runtime.NewHandler(runtime.ServerOptions{
				Options:      options,
				Subprotocols: subprotocols,
				Authenticate: func(r *http.Request) (context.Context, error) { return r.Context(), nil },
				CheckOrigin:  func(*http.Request) bool { return true },
				OnConnect: func(p *runtime.Peer) {
					select {
					case l.accepted <- p:
					default:
						_ = p.Close()
					}
				},
			})
			if err != nil {
				return nil, invalid("%v", err)
			}
			socket, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				return nil, err
			}
			l.url = "ws://" + socket.Addr().String()
			l.server = &http.Server{Handler: handler}
			go func() { _ = l.server.Serve(socket) }()
			return map[string]any{"handle": t.mint("pl", l), "url": l.url}, nil
		},
		"peer.accept": func(r request) (any, error) {
			handle, err := r.mustString("on")
			if err != nil {
				return nil, err
			}
			object, ok := t.lookup(handle)
			l, isListener := object.(*peerListener)
			if !ok || !isListener {
				return nil, fail("unknown_handle", "%s is not a peer listener", handle)
			}
			within, err := r.within()
			if err != nil {
				return nil, err
			}
			select {
			case p := <-l.accepted:
				return map[string]any{"handle": t.mint("p", t.adopt(p, l.rec, func() {})), "subprotocol": p.Subprotocol()}, nil
			case <-time.After(within):
				return nil, fail("timeout", "nobody connected within %s", within)
			}
		},
		"peer.dial": func(r request) (any, error) {
			url, err := r.mustString("url")
			if err != nil {
				return nil, err
			}
			options, rec, err := t.options(r)
			if err != nil {
				return nil, err
			}
			subprotocols, err := r.subprotocols()
			if err != nil {
				return nil, err
			}
			ctx, cancel := context.WithCancel(context.Background())
			p, _, err := runtime.Dial(ctx, url, runtime.DialOptions{Options: options, Subprotocols: subprotocols})
			if err != nil {
				cancel()
				return nil, fail("failed", "%v", err)
			}
			return map[string]any{"handle": t.mint("p", t.adopt(p, rec, cancel)), "subprotocol": p.Subprotocol()}, nil
		},
		"peer.over": func(r request) (any, error) {
			c, err := t.connOf(r)
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
			options, rec, err := t.options(r)
			if err != nil {
				return nil, err
			}
			// The peer reads the connection from here on; the wrapper's reader,
			// if any, must not. A lazy connection has none.
			if !c.lazy {
				return nil, invalid("a peer takes a lazily consumed connection, since it reads it itself")
			}
			ctx, cancel := context.WithCancel(context.Background())
			p, err := runtime.NewPeer(ctx, c.Conn, runtime.Role(role), options)
			if err != nil {
				cancel()
				return nil, fail("failed", "%v", err)
			}
			return map[string]any{"handle": t.mint("p", t.adopt(p, rec, cancel))}, nil
		},
		"peer.identity": func(r request) (any, error) {
			p, err := t.peerOf(r, "on")
			if err != nil {
				return nil, err
			}
			path, err := r.mustString("path")
			if err != nil {
				return nil, err
			}
			digest, err := r.string("digest")
			if err != nil {
				return nil, err
			}
			handler, err := runtime.IdentityHandler(runtime.DeclarationIdentity{Path: path, Digest: digest})
			if err != nil {
				return nil, callError(err, p)
			}
			if err := p.Handle(runtime.IdentityMethod, handler); err != nil {
				return nil, invalid("%v", err)
			}
			return nil, nil
		},
		"peer.check_identity": func(r request) (any, error) {
			p, err := t.peerOf(r, "on")
			if err != nil {
				return nil, err
			}
			path, err := r.mustString("path")
			if err != nil {
				return nil, err
			}
			digest, err := r.string("digest")
			if err != nil {
				return nil, err
			}
			within, err := r.within()
			if err != nil {
				return nil, err
			}
			ctx, cancel := context.WithTimeout(p.Context(), within)
			defer cancel()
			if err := runtime.CheckIdentity(ctx, p.Call, runtime.DeclarationIdentity{Path: path, Digest: digest}); err != nil {
				return nil, callError(err, p)
			}
			return nil, nil
		},
		"peer.handle": func(r request) (any, error) {
			p, err := t.peerOf(r, "on")
			if err != nil {
				return nil, err
			}
			method, err := r.mustString("method")
			if err != nil {
				return nil, err
			}
			behavior, err := parseBehavior(r.raw("behavior"))
			if err != nil {
				return nil, err
			}
			if err := p.Handle(method, canned(p, method, behavior)); err != nil {
				return nil, invalid("%v", err)
			}
			return nil, nil
		},
		"peer.on_event": func(r request) (any, error) {
			p, err := t.peerOf(r, "on")
			if err != nil {
				return nil, err
			}
			name, err := r.mustString("name")
			if err != nil {
				return nil, err
			}
			behavior, err := parseBehavior(r.raw("behavior"))
			if err != nil {
				return nil, err
			}
			switch behavior.Kind {
			case "", "record":
				return nil, nil
			case "block":
				err = p.HandleEvent(name, func(ctx context.Context, _ *runtime.Peer, _ json.RawMessage) { <-ctx.Done() })
			case "panic":
				value := behavior.Value
				err = p.HandleEvent(name, func(context.Context, *runtime.Peer, json.RawMessage) { panic(value) })
			default:
				return nil, invalid("an event handler records, blocks or panics")
			}
			if err != nil {
				return nil, invalid("%v", err)
			}
			return nil, nil
		},
		"peer.call": func(r request) (any, error) {
			p, err := t.peerOf(r, "on")
			if err != nil {
				return nil, err
			}
			method, err := r.mustString("method")
			if err != nil {
				return nil, err
			}
			timeout, err := r.int("timeout_ms", 0)
			if err != nil {
				return nil, err
			}
			params := r.raw("params")
			if params == nil {
				params = json.RawMessage("null")
			}
			meta, err := r.meta()
			if err != nil {
				return nil, err
			}
			ctx, cancel := context.WithCancel(runtime.WithMeta(p.Context(), meta))
			if timeout > 0 {
				ctx, cancel = context.WithTimeout(ctx, time.Duration(timeout)*time.Millisecond)
			}
			c := &call{cancel: cancel, done: make(chan struct{})}
			go func() {
				defer close(c.done)
				c.err = p.Call(ctx, method, params, &c.result)
			}()
			return map[string]any{"handle": t.mint("call", &callOn{call: c, peer: p})}, nil
		},
		"call.await": func(r request) (any, error) {
			c, p, err := t.callOf(r)
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
				return map[string]any{"error": callError(c.err, p)}, nil
			}
			var result any
			if len(c.result) > 0 {
				if err := json.Unmarshal(c.result, &result); err != nil {
					return nil, fail("failed", "the result is not JSON: %v", err)
				}
			}
			return map[string]any{"result": result}, nil
		},
		"call.cancel": func(r request) (any, error) {
			c, _, err := t.callOf(r)
			if err != nil {
				return nil, err
			}
			c.cancel()
			return nil, nil
		},
		"peer.emit": func(r request) (any, error) {
			p, err := t.peerOf(r, "on")
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
			data := r.raw("data")
			if data == nil {
				data = json.RawMessage("null")
			}
			ctx, cancel := context.WithTimeout(runtime.WithMeta(p.Context(), meta), within)
			defer cancel()
			if err := p.Emit(ctx, event, data); err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					return nil, fail("timeout", "the event was not sent within %s", within)
				}
				return nil, fail("disconnected", "%v", err)
			}
			return nil, nil
		},
		"peer.await_event": func(r request) (any, error) {
			p, err := t.peerOf(r, "on")
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
			e, ok, ended := p.events.await(within, func(d delivered) bool { return d.event.Name == name })
			if ended {
				return nil, fail("disconnected", "the peer ended before %s arrived", name)
			}
			if !ok {
				return nil, fail("timeout", "no %s within %s", name, within)
			}
			var data any
			_ = json.Unmarshal(e.event.Data, &data)
			answer := map[string]any{"data": data}
			if len(e.meta) > 0 {
				answer["meta"] = e.meta
			}
			return answer, nil
		},
		"peer.await_request": func(r request) (any, error) {
			p, err := t.peerOf(r, "on")
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
			l, ok, _ := p.requests.await(within, func(l lifecycle) bool { return l.Method == method && l.Phase == phase })
			if !ok {
				return nil, fail("timeout", "no %s %s within %s", method, phase, within)
			}
			return l, nil
		},
		"peer.observed": func(r request) (any, error) {
			p, err := t.peerOf(r, "on")
			if err != nil {
				return nil, err
			}
			if !p.observable {
				return nil, invalid("the peer was made without observe")
			}
			withTrace, err := r.bool("trace")
			if err != nil {
				return nil, err
			}
			drain := true
			if raw, present := r.args["drain"]; present {
				if err := json.Unmarshal(raw, &drain); err != nil {
					return nil, invalid("drain is a boolean")
				}
			}
			return p.observer.report(withTrace, drain), nil
		},
		"peer.close": func(r request) (any, error) {
			p, err := t.peerOf(r, "on")
			if err != nil {
				return nil, err
			}
			_ = p.Close()
			return nil, nil
		},
		"peer.await_close": func(r request) (any, error) {
			p, err := t.peerOf(r, "on")
			if err != nil {
				return nil, err
			}
			within, err := r.within()
			if err != nil {
				return nil, err
			}
			closed, ok := p.observer.whenClosed(within)
			if !ok {
				return nil, fail("timeout", "the peer did not end within %s", within)
			}
			// Clean is a close somebody chose, whichever side: a peer closes
			// with 1000 by choice and with a code of its own when it refuses a
			// frame, and 1006 is what a side that aborted leaves behind.
			return map[string]any{"clean": closed.Code == int(duplex.CodeNormal), "code": closed.Code}, nil
		},
	}
}

func (t *testee) callOf(r request) (*call, *peer, error) {
	handle, err := r.mustString("on")
	if err != nil {
		return nil, nil, err
	}
	object, ok := t.lookup(handle)
	if !ok {
		return nil, nil, fail("unknown_handle", "%s", handle)
	}
	c, ok := object.(*callOn)
	if !ok {
		return nil, nil, invalid("%s is not a call", handle)
	}
	return c.call, c.peer, nil
}

// callOn is a call with the peer it was made on, for the error mapping.
type callOn struct {
	*call
	peer *peer
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
	// Attachment is the imported binding a live "through" binding calls, which
	// is how a callable that was returned reaches a callable that was supplied.
	Attachment string `json:"attachment"`
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

// canned is a handler that does what its behaviour says and records its
// lifecycle for peer.await_request.
func canned(p *peer, method string, b behavior) runtime.Handler {
	return func(ctx context.Context, remote *runtime.Peer, params json.RawMessage) (result any, err error) {
		id := requestID(ctx)
		p.requests.put(lifecycle{ID: id, Method: method, Phase: "started", Meta: runtime.MetaFrom(ctx)})
		defer func() {
			if recovered := recover(); recovered != nil {
				p.requests.put(lifecycle{ID: id, Method: method, Phase: "ended", Outcome: "panic"})
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
			p.requests.put(lifecycle{ID: id, Method: method, Phase: "ended", Outcome: outcome})
		}()
		switch b.Kind {
		case "echo":
			return params, nil
		case "return":
			return b.Value, nil
		case "fail":
			return nil, &runtime.PublicError{Code: b.Code, Message: b.Message, Data: b.Data}
		case "wait":
			<-ctx.Done()
			return nil, ctx.Err()
		case "hold":
			// The one handler that does not stop when it is told to: it holds
			// the request until the remote emits what releases it, cancelled
			// or not, which is how a scenario holds when a withdrawn request
			// is answered.
			released := make(chan struct{}, 1)
			unsubscribe := remote.OnEvent(func(_ context.Context, e runtime.Event) {
				if e.Name == b.Until {
					select {
					case released <- struct{}{}:
					default:
					}
				}
			})
			defer unsubscribe()
			select {
			case <-released:
			case <-remote.Done():
			}
			return b.Value, nil
		case "panic":
			var value any = "the handler gave up"
			if b.Value != nil {
				_ = json.Unmarshal(b.Value, &value)
			}
			panic(fmt.Sprint(value))
		case "reverse":
			with := b.Params
			if with == nil {
				with = params
			}
			var back json.RawMessage
			if err := remote.Call(ctx, b.Method, with, &back); err != nil {
				return nil, err
			}
			return back, nil
		case "emit":
			if err := remote.Emit(ctx, b.Event, b.Data); err != nil {
				return nil, err
			}
			return b.Then, nil
		}
		return nil, &runtime.PublicError{Code: "internal", Message: "no such behaviour: " + b.Kind}
	}
}

// requestID is the id of the request a handler serves, from the observer's
// point of view: the Go runtime does not hand a handler its request id, so
// the lifecycle carries none. A scenario holds the method and the phase.
func requestID(context.Context) string { return "" }
