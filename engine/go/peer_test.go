package engine_test

// Ported from nightseam v0.6.0 runtime/go/peer_test.go
// (5cc9723a24646c40ed1861f892b2b23eb6d785d7). The peer's raw method-name API
// is removed, so every handler is a dispatcher route at the one-segment path
// [name], installed in Prepare, and every call and event goes through the
// peer's root with the dispatch helpers. The tests about connection setup —
// authentication, origin, subprotocols and the dial deadline — live beside the
// WebSocket constructors in engine/websocket/go.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	core "github.com/Bitspark/bitruntime/core/go"
	dispatch "github.com/Bitspark/bitruntime/dispatch/go"
	engine "github.com/Bitspark/bitruntime/engine/go"
	transports "github.com/Bitspark/bitruntime/transports/go"
	wire "github.com/Bitspark/bitwire/wire/go"
)

func receive[T any](t *testing.T, channel <-chan T) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for peer activity")
		var zero T
		return zero
	}
}

// handler and eventHandler are a request body and an event body as v0.6.0's
// tests wrote them, with the peer they run on.
type handler func(ctx context.Context, peer *engine.Peer, params json.RawMessage) (any, error)
type eventHandler func(ctx context.Context, peer *engine.Peer, data json.RawMessage)

// serving is what a peer serves at its root: each name is the path [name],
// which travels as its canonical encoding, "4:echo" for "echo".
type serving struct {
	Handlers map[string]handler
	Events   map[string]eventHandler
}

// with is options whose Prepare attaches a dispatcher serving s to the peer's
// root before the peer reads its first frame, after any Prepare of its own.
func (s serving) with(options engine.Options) engine.Options {
	prepare := options.Prepare
	options.Prepare = func(peer *engine.Peer) error {
		if prepare != nil {
			if err := prepare(peer); err != nil {
				return err
			}
		}
		return s.attach(peer)
	}
	return options
}

func (s serving) attach(peer *engine.Peer) error {
	if len(s.Handlers) == 0 && len(s.Events) == 0 {
		return nil
	}
	d, err := dispatch.NewDispatcher(peer.Wire())
	if err != nil {
		return err
	}
	names := map[string]dispatch.Handlers{}
	for name, h := range s.Handlers {
		group := names[name]
		group.Request = func(ctx context.Context, params json.RawMessage) (any, error) { return h(ctx, peer, params) }
		names[name] = group
	}
	for name, e := range s.Events {
		group := names[name]
		group.Event = func(ctx context.Context, data json.RawMessage) error { e(ctx, peer, data); return nil }
		names[name] = group
	}
	for name, group := range names {
		if _, err := dispatch.Register(d, []string{name}, group); err != nil {
			return err
		}
	}
	return nil
}

// call and emit address the path [name] through the peer's root.
func call(ctx context.Context, peer *engine.Peer, method string, params, result any) error {
	return dispatch.Call(ctx, peer.Wire(), []string{method}, params, result)
}

func emit(ctx context.Context, peer *engine.Peer, name string, data any) error {
	return dispatch.Emit(ctx, peer.Wire(), []string{name}, data)
}

// newPair is a server and a client peer over an in-memory pipe.
func newPair(t *testing.T, serverOptions, clientOptions engine.Options) (client, server *engine.Peer) {
	t.Helper()
	return connectPair(t, serverOptions, clientOptions, nil, nil)
}

// connectPair is newPair with each end of the pipe optionally wrapped.
func connectPair(t *testing.T, serverOptions, clientOptions engine.Options, wrapServer, wrapClient func(transports.Conn) transports.Conn) (client, server *engine.Peer) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	limit := int64(1 << 20)
	for _, o := range []engine.Options{serverOptions, clientOptions} {
		if o.MaxFrameBytes > limit {
			limit = o.MaxFrameBytes
		}
	}
	clientConn, serverConn := transports.Pipe(limit)
	if wrapServer != nil {
		serverConn = wrapServer(serverConn)
	}
	if wrapClient != nil {
		clientConn = wrapClient(clientConn)
	}
	server, err := engine.NewPeer(ctx, serverConn, engine.ServerRole, serverOptions)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	client, err = engine.NewPeer(ctx, clientConn, engine.ClientRole, clientOptions)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client, server
}

// rawSide is the far end of a pipe a peer owns: the test is the remote peer,
// and spells the frames it sends byte for byte.
type rawSide struct {
	t    *testing.T
	ctx  context.Context
	conn transports.Conn
}

// rawPeer is one peer of the given role over a pipe whose other end the test
// holds.
func rawPeer(t *testing.T, role engine.Role, options engine.Options) (*engine.Peer, *rawSide) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	near, far := transports.Pipe(1 << 20)
	peer, err := engine.NewPeer(ctx, near, role, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peer.Close(); _ = far.Abort() })
	return peer, &rawSide{t: t, ctx: ctx, conn: far}
}

func (r *rawSide) write(frame string) {
	r.t.Helper()
	if err := r.conn.Send(r.ctx, transports.Frame{Kind: transports.Text, Data: []byte(frame)}); err != nil {
		r.t.Fatalf("write %s: %v", frame, err)
	}
}

// read is the next frame the peer sent, by member.
func (r *rawSide) read() map[string]json.RawMessage {
	r.t.Helper()
	received, err := r.conn.Receive(r.ctx)
	if err != nil || received.Kind != transports.Text {
		r.t.Fatalf("read frame: kind=%v error=%v", received.Kind, err)
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal(received.Data, &members); err != nil {
		r.t.Fatalf("decode frame %s: %v", received.Data, err)
	}
	return members
}

// readUntil is the first frame the peer sent that matches, passing over the
// frames before it.
func (r *rawSide) readUntil(match func(map[string]json.RawMessage) bool) map[string]json.RawMessage {
	r.t.Helper()
	for {
		if members := r.read(); match(members) {
			return members
		}
	}
}

// closed is how the connection ended as this side reads it, passing over any
// frame the peer sent before it ended.
func (r *rawSide) closed() *transports.CloseError {
	r.t.Helper()
	for {
		_, err := r.conn.Receive(r.ctx)
		if err == nil {
			continue
		}
		var closed *transports.CloseError
		if !errors.As(err, &closed) {
			r.t.Fatalf("the far side read %v, not a close", err)
		}
		return closed
	}
}

func member(value string) func(map[string]json.RawMessage) bool {
	name, want, _ := strings.Cut(value, "=")
	return func(members map[string]json.RawMessage) bool { return string(members[name]) == want }
}

func TestReverseCallCompletesWhileOriginalRequestIsOutstanding(t *testing.T) {
	client, _ := newPair(t, serving{Handlers: map[string]handler{
		"outer": func(ctx context.Context, peer *engine.Peer, _ json.RawMessage) (any, error) {
			var response int
			if err := call(ctx, peer, "reverse", 6, &response); err != nil {
				return nil, err
			}
			return response + 1, nil
		},
		"inner": func(_ context.Context, _ *engine.Peer, data json.RawMessage) (any, error) {
			var value int
			if err := json.Unmarshal(data, &value); err != nil {
				return nil, err
			}
			return value * 7, nil
		},
	}}.with(engine.Options{}), serving{Handlers: map[string]handler{
		"reverse": func(ctx context.Context, peer *engine.Peer, data json.RawMessage) (any, error) {
			var response int
			err := call(ctx, peer, "inner", data, &response)
			return response, err
		},
	}}.with(engine.Options{}))
	var result int
	if err := call(context.Background(), client, "outer", nil, &result); err != nil {
		t.Fatal(err)
	}
	if result != 43 {
		t.Fatalf("nested duplex result = %d, want 43", result)
	}
}

// v0.6.0 also held Peer.OnEvent here, a listener beside the handlers, and that
// its unsubscribe was idempotent. OnEvent is the removed raw API; the root has
// one receiver, and those assertions are dropped.
func TestEventsTravelInBothDirections(t *testing.T) {
	clientEvents, serverEvents := make(chan string, 2), make(chan string, 2)
	into := func(output chan<- string) eventHandler {
		return func(_ context.Context, _ *engine.Peer, data json.RawMessage) { output <- string(data) }
	}
	client, server := newPair(t,
		serving{Events: map[string]eventHandler{"progress": into(serverEvents)}}.with(engine.Options{}),
		serving{Events: map[string]eventHandler{"progress": into(clientEvents)}}.with(engine.Options{}))
	for _, value := range []int{1, 2} {
		if err := emit(context.Background(), client, "progress", value); err != nil {
			t.Fatal(err)
		}
	}
	if err := emit(context.Background(), server, "progress", "done"); err != nil {
		t.Fatal(err)
	}
	if got := receive(t, serverEvents); got != "1" {
		t.Fatalf("first server event = %s", got)
	}
	if got := receive(t, serverEvents); got != "2" {
		t.Fatalf("second server event = %s", got)
	}
	if got := receive(t, clientEvents); got != `"done"` {
		t.Fatalf("client event = %s", got)
	}
}

func TestPublicErrorsArePreservedAndInternalFailuresHidden(t *testing.T) {
	client, _ := newPair(t, serving{Handlers: map[string]handler{
		"public": func(context.Context, *engine.Peer, json.RawMessage) (any, error) {
			return nil, fmt.Errorf("wrapper: %w", &core.PublicError{Code: "conflict", Message: "Changed", Data: json.RawMessage(`{"revision":3}`)})
		},
		"private": func(context.Context, *engine.Peer, json.RawMessage) (any, error) {
			return nil, errors.New("private database password")
		},
		"panic": func(context.Context, *engine.Peer, json.RawMessage) (any, error) {
			panic("private panic details")
		},
		"unencodable": func(context.Context, *engine.Peer, json.RawMessage) (any, error) {
			return make(chan int), nil
		},
		"ok": func(context.Context, *engine.Peer, json.RawMessage) (any, error) { return true, nil },
	}}.with(engine.Options{}), engine.Options{})
	for _, test := range []struct{ method, code, message string }{
		{"public", "conflict", "Changed"},
		{"private", "internal", "Internal error"},
		{"panic", "internal", "Internal error"},
		{"unencodable", "internal", "Internal error"},
		{"missing", "method_not_found", "Unknown method"},
	} {
		t.Run(test.method, func(t *testing.T) {
			err := call(context.Background(), client, test.method, nil, nil)
			var public *core.PublicError
			if !errors.As(err, &public) || public.Code != test.code || public.Message != test.message {
				t.Fatalf("error = %v, want %s: %s", err, test.code, test.message)
			}
			if test.method == "public" && string(public.Data) != `{"revision":3}` {
				t.Fatalf("public error data = %s", public.Data)
			}
			if strings.Contains(err.Error(), "private") {
				t.Fatalf("internal error leaked: %v", err)
			}
		})
	}
	var result bool
	if err := call(context.Background(), client, "ok", nil, &result); err != nil || !result {
		t.Fatalf("connection did not survive handler failure: result=%v err=%v", result, err)
	}
}

func TestCallerCancellationReachesRemoteHandler(t *testing.T) {
	started, cancelled := make(chan struct{}), make(chan error, 1)
	client, server := newPair(t, serving{Handlers: map[string]handler{
		"wait": func(ctx context.Context, _ *engine.Peer, _ json.RawMessage) (any, error) {
			close(started)
			<-ctx.Done()
			cancelled <- ctx.Err()
			return nil, ctx.Err()
		},
	}}.with(engine.Options{}), engine.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	returned := make(chan error, 1)
	go func() { returned <- call(ctx, client, "wait", nil, nil) }()
	receive(t, started)
	cancel()
	if err := receive(t, returned); !errors.Is(err, context.Canceled) {
		t.Fatalf("caller result = %v", err)
	}
	if err := receive(t, cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("handler context = %v", err)
	}
	if client.Err() != nil || server.Err() != nil {
		t.Fatalf("request cancellation closed connection: client=%v server=%v", client.Err(), server.Err())
	}
}

func TestSaturationRejectsNewWorkButStillRoutesReverseResponses(t *testing.T) {
	reverseStarted, release := make(chan struct{}), make(chan struct{})
	client, _ := newPair(t, serving{Handlers: map[string]handler{
		"outer": func(ctx context.Context, peer *engine.Peer, _ json.RawMessage) (any, error) {
			var result string
			err := call(ctx, peer, "reverse", nil, &result)
			return result, err
		},
		"extra": func(context.Context, *engine.Peer, json.RawMessage) (any, error) { return "unexpected", nil },
	}}.with(engine.Options{MaxConcurrentHandlers: 1}), serving{Handlers: map[string]handler{
		"reverse": func(ctx context.Context, _ *engine.Peer, _ json.RawMessage) (any, error) {
			close(reverseStarted)
			select {
			case <-release:
				return "released", nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}}.with(engine.Options{}))
	type callResult struct {
		value string
		err   error
	}
	returned := make(chan callResult, 1)
	go func() {
		var value string
		err := call(context.Background(), client, "outer", nil, &value)
		returned <- callResult{value, err}
	}()
	receive(t, reverseStarted)
	err := call(context.Background(), client, "extra", nil, nil)
	var public *core.PublicError
	if !errors.As(err, &public) || public.Code != "busy" {
		t.Fatalf("saturated request returned %v, want busy", err)
	}
	close(release)
	if got := receive(t, returned); got.err != nil || got.value != "released" {
		t.Fatalf("pending reverse response was blocked by handler saturation: %+v", got)
	}
}

func TestStalledEventConsumerDisconnects(t *testing.T) {
	started := make(chan struct{})
	// A stalled consumer is paced for one write deadline before it is
	// disconnected; the deadline is short here so the pacing is not the wait.
	client, server := newPair(t, engine.Options{}, serving{Events: map[string]eventHandler{
		"progress": func(ctx context.Context, _ *engine.Peer, _ json.RawMessage) {
			close(started)
			<-ctx.Done()
		},
	}}.with(engine.Options{QueueCapacity: 1, WriteTimeout: 200 * time.Millisecond}))
	if err := emit(context.Background(), server, "progress", 1); err != nil {
		t.Fatal(err)
	}
	receive(t, started)
	for _, value := range []int{2, 3} {
		if err := emit(context.Background(), server, "progress", value); err != nil {
			t.Fatal(err)
		}
	}
	receive(t, client.Done())
	if !errors.Is(client.Err(), core.ErrBackpressure) {
		t.Fatalf("stalled event consumer error = %v", client.Err())
	}
	// R26: an ended carrier is a closed one, whatever its cause.
	if !errors.Is(client.Err(), transports.ErrClosed) {
		t.Fatalf("stalled event consumer error %v is not a closed carrier", client.Err())
	}
	receive(t, server.Done())
}

func TestDisconnectCancelsHandlersAndRejectsPendingCalls(t *testing.T) {
	started, stopped := make(chan struct{}), make(chan struct{})
	client, server := newPair(t, serving{Handlers: map[string]handler{
		"wait": func(ctx context.Context, _ *engine.Peer, _ json.RawMessage) (any, error) {
			close(started)
			<-ctx.Done()
			close(stopped)
			return nil, ctx.Err()
		},
	}}.with(engine.Options{}), engine.Options{})
	returned := make(chan error, 1)
	go func() { returned <- call(context.Background(), client, "wait", nil, nil) }()
	receive(t, started)
	_ = server.Close()
	receive(t, stopped)
	if err := receive(t, returned); err == nil {
		t.Fatal("pending call succeeded after disconnect")
	}
	receive(t, client.Done())
}

func TestMalformedWireFramesDisconnect(t *testing.T) {
	// Each row is a frame that would be served but for the one member named:
	// a traceparent of another form is refused as any other malformed frame is.
	for _, data := range []string{
		`{"version":2,"kind":"event","event":"progress","data":1}`,
		`{"version":1,"kind":"request","id":"s:1","method":"wait","params":null}`,
		`{"version":1,"kind":"response","id":"s:1","result":null,"error":{"code":"bad","message":"bad"}}`,
		`{"version":1,"kind":"event","event":"progress","data":1,"extra":true}`,
		`{"version":1,"kind":"event","event":"progress","data":1,"traceparent":"nonsense"}`,
		`{"version":1,"kind":"request","id":"c:1","method":"wait","params":{},"traceparent":"00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01"}`,
		`{"version":1,"kind":"request","id":"c:1","method":"wait","params":{},"traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-99"}`,
		`{"version":1,"kind":"cancel","id":"c:1","traceparent":""}`,
	} {
		t.Run(data, func(t *testing.T) {
			peer, raw := rawPeer(t, engine.ServerRole, engine.Options{})
			raw.write(data)
			receive(t, peer.Done())
			if peer.Err() == nil {
				t.Fatal("malformed frame closed without error")
			}
			// The refusal is the protocol's own close.
			if closed := raw.closed(); closed.Code != transports.CodeProtocol || closed.Reason == "" {
				t.Fatalf("the far side read %d %q, want 4011 with a reason", int(closed.Code), closed.Reason)
			}
		})
	}
}

// The members are optional on every kind and the peer emits none of its own:
// what a frame carries it carries past the decoder, and the frame is served.
// The hand-written frames name the root's paths in their canonical encoding,
// "5:outer" for [outer], since a raw method name is served by nothing.
func TestTraceContextTravelsOnEveryFrameKind(t *testing.T) {
	const trace = `"traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01","tracestate":"congo=t61rcWkgMzE"`
	events, started, cancelled := make(chan string, 2), make(chan struct{}), make(chan struct{})
	peer, raw := rawPeer(t, engine.ServerRole, serving{
		Events: map[string]eventHandler{
			"progress": func(_ context.Context, _ *engine.Peer, data json.RawMessage) { events <- string(data) },
		},
		Handlers: map[string]handler{
			"outer": func(ctx context.Context, peer *engine.Peer, _ json.RawMessage) (any, error) {
				var answer string
				if err := call(ctx, peer, "reverse", nil, &answer); err != nil {
					return nil, err
				}
				return answer, nil
			},
			"wait": func(ctx context.Context, _ *engine.Peer, _ json.RawMessage) (any, error) {
				close(started)
				<-ctx.Done()
				close(cancelled)
				return nil, ctx.Err()
			},
		},
	}.with(engine.Options{}))
	raw.write(`{"version":1,"kind":"event","event":"8:progress","data":1,` + trace + `}`)
	if got := receive(t, events); got != "1" {
		t.Fatalf("traced event = %s", got)
	}
	// A traced request is served, and the response to the reverse call it makes
	// is itself traced: both kinds cross the decoder in one exchange.
	raw.write(`{"version":1,"kind":"request","id":"c:1","method":"5:outer","params":{},` + trace + `}`)
	reverse := raw.read()
	if string(reverse["method"]) != `"7:reverse"` {
		t.Fatalf("reverse request = %v", reverse)
	}
	raw.write(`{"version":1,"kind":"response","id":` + string(reverse["id"]) + `,"result":"back",` + trace + `}`)
	if response := raw.read(); string(response["id"]) != `"c:1"` || string(response["result"]) != `"back"` {
		t.Fatalf("response to traced request = %v", response)
	}
	// An intermediary may strip one member and not the other.
	raw.write(`{"version":1,"kind":"event","event":"8:progress","data":2,"tracestate":"congo=t61rcWkgMzE"}`)
	if got := receive(t, events); got != "2" {
		t.Fatalf("event carrying tracestate alone = %s", got)
	}
	raw.write(`{"version":1,"kind":"request","id":"c:2","method":"4:wait","params":{},` + trace + `}`)
	raw.write(`{"version":1,"kind":"cancel","id":"c:2",` + trace + `}`)
	// The traced cancel is served: the request is answered cancelled. v0.6.0
	// held that its raw handler saw the cancellation; through the root a
	// cancel that arrives before the body starts settles the request without
	// running it, as v0.6.0's root did, so the answer is what is held here,
	// and a body that did start must have been cancelled.
	if response := raw.readUntil(member(`id="c:2"`)); string(response["error"]) != `{"code":"cancelled","message":"Request cancelled"}` {
		t.Fatalf("the cancelled request was answered %v", response)
	}
	select {
	case <-started:
		receive(t, cancelled)
	default:
	}
	if peer.Err() != nil {
		t.Fatalf("a traced frame closed the connection: %v", peer.Err())
	}
}

// A request whose method is no canonical path encoding is refused
// method_not_found and the connection goes on, as a v0.6.0 peer without that
// handler answered it: the raw method-name API is gone, and nothing serves a
// raw name.
func TestARawMethodNameIsAnsweredMethodNotFound(t *testing.T) {
	peer, raw := rawPeer(t, engine.ServerRole, serving{Handlers: map[string]handler{
		"echo": func(_ context.Context, _ *engine.Peer, params json.RawMessage) (any, error) { return params, nil },
	}}.with(engine.Options{}))
	raw.write(`{"version":1,"kind":"request","id":"c:1","method":"echo","params":1}`)
	if response := raw.read(); string(response["id"]) != `"c:1"` || string(response["error"]) != `{"code":"method_not_found","message":"Unknown method"}` {
		t.Fatalf("a raw method name was answered %v", response)
	}
	raw.write(`{"version":1,"kind":"request","id":"c:2","method":"4:echo","params":1}`)
	if response := raw.read(); string(response["id"]) != `"c:2"` || string(response["result"]) != `1` {
		t.Fatalf("the canonical path was answered %v", response)
	}
	if peer.Err() != nil {
		t.Fatalf("a raw method name ended the connection: %v", peer.Err())
	}
}

// TestSubprotocolIsNoneOverAnyOtherTransport: a peer that is not over a
// WebSocket negotiated nothing and says so.
func TestSubprotocolIsNoneOverAnyOtherTransport(t *testing.T) {
	client, server := newPair(t, engine.Options{}, engine.Options{})
	if client.Subprotocol() != "" || server.Subprotocol() != "" {
		t.Fatalf("subprotocols over a pipe = %q and %q", client.Subprotocol(), server.Subprotocol())
	}
}

// heldReturn is a return capability that records what it is answered and
// holds the one who answers it with the code named, until released.
type heldReturn struct {
	responses chan wire.ProfileFrame
	holdOn    string
	held      chan struct{}
	release   chan struct{}
	once      sync.Once
}

func newHeldReturn(holdOn string) *heldReturn {
	return &heldReturn{responses: make(chan wire.ProfileFrame, 8), holdOn: holdOn, held: make(chan struct{}), release: make(chan struct{})}
}

func (r *heldReturn) Send(_ []string, message wire.Message) error {
	if r.holdOn != "" && message.Frame.Error != nil && message.Frame.Error.Code == r.holdOn {
		r.once.Do(func() { close(r.held) })
		<-r.release
	}
	r.responses <- message.Frame
	return nil
}

func request(id string) wire.ProfileFrame {
	return wire.ProfileFrame{Version: 1, Kind: wire.ProfileRequest, ID: id, Params: json.RawMessage(`null`)}
}

// TestRequestsQueuedInTheRootAreAnsweredWhenThePeerEnds (R28): a request the
// root admitted and never handed to the peer, and a refusal it queued, are
// each answered when the peer ends — disconnected and the refusal — rather
// than left to their callers' deadlines. The root is held on purpose, inside
// the answer to a refusal, so that what follows it is still queued when the
// peer ends.
func TestRequestsQueuedInTheRootAreAnsweredWhenThePeerEnds(t *testing.T) {
	peer, raw := rawPeer(t, engine.ServerRole, engine.Options{MaxPendingRequests: 3, RequestTimeout: time.Minute})
	root := peer.Wire()
	first := newHeldReturn("invalid_message")
	firstAddress := &wire.ReturnAddress{Wire: first}
	// Admitted and handed to the peer: it reaches the wire.
	if err := root.Send([]string{"first"}, wire.Message{Frame: request("c:1"), Return: firstAddress}); err != nil {
		t.Fatal(err)
	}
	if sent := raw.read(); string(sent["method"]) != `"5:first"` {
		t.Fatalf("the first request reached the wire as %v", sent)
	}
	// The same return identity again is refused invalid_message; answering it
	// holds the root.
	if err := root.Send([]string{"first"}, wire.Message{Frame: request("c:1"), Return: firstAddress}); err != nil {
		t.Fatal(err)
	}
	receive(t, first.held)
	queued, refused := newHeldReturn(""), newHeldReturn("")
	if err := root.Send([]string{"queued"}, wire.Message{Frame: request("c:1"), Return: &wire.ReturnAddress{Wire: queued}}); err != nil {
		t.Fatal(err)
	}
	// And a call through the dispatch helper, queued behind it.
	sent := make(chan struct{})
	returned := make(chan error, 1)
	go func() {
		returned <- dispatch.Call(context.Background(), sendSignal{root, sent}, []string{"late"}, nil, nil, dispatch.CallOptions{Timeout: time.Minute})
	}()
	receive(t, sent)
	// The root holds three admitted, its bound: the next is a queued refusal.
	if err := root.Send([]string{"refused"}, wire.Message{Frame: request("c:1"), Return: &wire.ReturnAddress{Wire: refused}}); err != nil {
		t.Fatal(err)
	}

	_ = peer.Close()
	close(first.release)

	// The request already handed to the peer is answered by its own waiter,
	// disconnected: its context derives from the peer's, but the peer's end is
	// not a withdrawal. v0.6.0 answered cancelled or disconnected at random.
	answers := map[string]bool{}
	for range 2 {
		f := receive(t, first.responses)
		if f.Error == nil {
			t.Fatalf("the first request was answered %+v", f)
		}
		answers[f.Error.Code] = true
	}
	if !answers["invalid_message"] || !answers["disconnected"] || answers["cancelled"] {
		t.Fatalf("the first return capability was answered %v", answers)
	}
	if f := receive(t, queued.responses); f.Error == nil || f.Error.Code != "disconnected" || f.ID != "c:1" {
		t.Fatalf("the request queued in the root was answered %+v", f)
	}
	if f := receive(t, refused.responses); f.Error == nil || f.Error.Code != "busy" || f.Error.Message != "Outstanding call limit reached" {
		t.Fatalf("the refusal queued in the root was answered %+v", f)
	}
	var public *core.PublicError
	if err := receive(t, returned); !errors.As(err, &public) || public.Code != "disconnected" {
		t.Fatalf("a call queued in the root returned %v", err)
	}
	// Nothing queued in the root reached the wire.
	for {
		received, err := raw.conn.Receive(raw.ctx)
		if err != nil {
			break
		}
		if strings.Contains(string(received.Data), `"kind":"request"`) {
			t.Fatalf("a request queued in the root reached the wire: %s", received.Data)
		}
	}
}

// sendSignal is addressed access that says when a request it forwarded was
// taken.
type sendSignal struct {
	wire.AddressedWire
	sent chan struct{}
}

func (s sendSignal) Send(path []string, message wire.Message) error {
	err := s.AddressedWire.Send(path, message)
	if message.Frame.Kind == wire.ProfileRequest {
		close(s.sent)
	}
	return err
}
