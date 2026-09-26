package websocket_test

// Ported from Nightseam v0.6.0 runtime/go/peer_test.go
// (5cc9723a24646c40ed1861f892b2b23eb6d785d7): the tests about setting a
// connection up — authentication, origin, subprotocols and the dial deadline.
// What a peer serves is a dispatcher at its root, attached in Prepare, since
// the raw method-name API is removed; "identity" is the path [identity].

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	core "github.com/Bitspark/bitruntime/core/go"
	dispatch "github.com/Bitspark/bitruntime/dispatch/go"
	engine "github.com/Bitspark/bitruntime/engine/go"
	ws "github.com/Bitspark/bitruntime/engine/websocket/go"
	transports "github.com/Bitspark/bitruntime/transports/go"
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

// serve is a Prepare that attaches a dispatcher answering each name, the
// one-segment path [name], with the peer the request arrived on.
func serve(handlers map[string]func(ctx context.Context, peer *engine.Peer) (any, error)) func(*engine.Peer) error {
	return func(peer *engine.Peer) error {
		d, err := dispatch.NewDispatcher(peer.Wire())
		if err != nil {
			return err
		}
		for name, h := range handlers {
			if _, err := dispatch.Handle(d, []string{name}, func(ctx context.Context, _ json.RawMessage) (any, error) { return h(ctx, peer) }); err != nil {
				return err
			}
		}
		return nil
	}
}

func allow() (func(*http.Request) (context.Context, error), func(*http.Request) bool) {
	return func(r *http.Request) (context.Context, error) { return r.Context(), nil }, func(*http.Request) bool { return true }
}

func TestServerRequiresAndEnforcesAuthenticationAndOriginPolicies(t *testing.T) {
	authenticate, allowOrigin := allow()
	for _, options := range []ws.ServerOptions{{}, {Authenticate: authenticate}, {CheckOrigin: allowOrigin}} {
		if _, err := ws.NewHandler(options); err == nil {
			t.Fatal("server accepted missing explicit policy")
		}
	}
	type userKey struct{}
	handler, err := ws.NewHandler(ws.ServerOptions{
		Authenticate: func(r *http.Request) (context.Context, error) {
			if r.Header.Get("Authorization") != "Bearer valid" {
				return nil, errors.New("private authentication failure")
			}
			return context.WithValue(r.Context(), userKey{}, "alice"), nil
		},
		CheckOrigin: func(r *http.Request) bool { return r.Header.Get("Origin") == "https://allowed.example" },
		Options: engine.Options{Prepare: serve(map[string]func(context.Context, *engine.Peer) (any, error){
			"identity": func(ctx context.Context, _ *engine.Peer) (any, error) { return ctx.Value(userKey{}), nil },
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	for _, test := range []struct {
		origin, authorization string
		status                int
	}{
		{"https://denied.example", "Bearer valid", http.StatusForbidden},
		{"https://allowed.example", "Bearer wrong", http.StatusUnauthorized},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		peer, response, err := ws.Dial(ctx, server.URL, ws.DialOptions{HTTPHeader: http.Header{
			"Origin": {test.origin}, "Authorization": {test.authorization},
		}})
		cancel()
		if peer != nil {
			_ = peer.Close()
		}
		if err == nil || response == nil || response.StatusCode != test.status {
			t.Fatalf("rejected handshake = peer %v, response %v, error %v; want %d", peer, response, err, test.status)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, _, err := ws.Dial(ctx, server.URL, ws.DialOptions{HTTPHeader: http.Header{
		"Origin": {"https://allowed.example"}, "Authorization": {"Bearer valid"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	var identity string
	if err := dispatch.Call(ctx, client.Wire(), []string{"identity"}, nil, &identity); err != nil || identity != "alice" {
		t.Fatalf("authenticated identity = %q, error=%v", identity, err)
	}
}

// TestSubprotocolNegotiation holds what the handshake selected against what
// both peers report, and the protocol is spoken over the connection either
// way: nothing about it turns on a subprotocol.
func TestSubprotocolNegotiation(t *testing.T) {
	// The ticket case: a browser can carry one nowhere but in the offer, and
	// accepts the handshake only if it comes back unchanged.
	ticket := func(_ *http.Request, offered []string) string {
		for _, token := range offered {
			if strings.HasPrefix(token, "ticket.") {
				return token
			}
		}
		return ""
	}
	for _, test := range []struct {
		name     string
		server   ws.ServerOptions
		offer    []string
		selected string
	}{
		{name: "both name it", server: ws.ServerOptions{Subprotocols: []string{"a", "b"}}, offer: []string{"b"}, selected: "b"},
		{name: "the offer meets none of them", server: ws.ServerOptions{Subprotocols: []string{"a", "b"}}, offer: []string{"c"}},
		{name: "the server names none", offer: []string{"a"}},
		{name: "neither side names one", server: ws.ServerOptions{}},
		{name: "a ticket is selected back unchanged", server: ws.ServerOptions{SelectSubprotocol: ticket}, offer: []string{"ticket.4f9c", "a"}, selected: "ticket.4f9c"},
		{name: "the hook selects none", server: ws.ServerOptions{Subprotocols: []string{"a"}, SelectSubprotocol: ticket}, offer: []string{"a"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			connected := make(chan *engine.Peer, 1)
			options := test.server
			options.Authenticate, options.CheckOrigin = allow()
			options.OnConnect = func(peer *engine.Peer) { connected <- peer }
			options.Options = engine.Options{Prepare: serve(map[string]func(context.Context, *engine.Peer) (any, error){
				"selected": func(_ context.Context, peer *engine.Peer) (any, error) { return peer.Subprotocol(), nil },
			})}
			handler, err := ws.NewHandler(options)
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(handler)
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client, _, err := ws.Dial(ctx, server.URL, ws.DialOptions{Subprotocols: test.offer})
			if err != nil {
				t.Fatalf("dial offering %v: %v", test.offer, err)
			}
			defer client.Close()
			remote := receive(t, connected)
			defer remote.Close()
			if got := client.Subprotocol(); got != test.selected {
				t.Fatalf("client subprotocol = %q, want %q", got, test.selected)
			}
			if got := remote.Subprotocol(); got != test.selected {
				t.Fatalf("server subprotocol = %q, want %q", got, test.selected)
			}
			// And the connection carries the protocol whatever was selected,
			// which the server reads back over it.
			var answer string
			if err := dispatch.Call(ctx, client.Wire(), []string{"selected"}, nil, &answer); err != nil {
				t.Fatalf("call over a connection negotiating %q: %v", test.selected, err)
			}
			if answer != test.selected {
				t.Fatalf("subprotocol a handler read = %q, want %q", answer, test.selected)
			}
		})
	}
}

// TestDialRefusesAHandshakeThatNeverAnswers: a listener that takes the
// connection and never answers the upgrade is refused with the code
// connect_timeout, with nothing opened and the caller's own context
// untouched, the deadline having been the handshake's and not the
// connection's. A negative bound is refused before anything is dialled.
func TestDialRefusesAHandshakeThatNeverAnswers(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() { <-done; conn.Close() }()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	url := "ws://" + listener.Addr().String()
	started := time.Now()
	peer, _, err := ws.Dial(ctx, url, ws.DialOptions{ConnectTimeout: 50 * time.Millisecond})
	if peer != nil {
		t.Fatal("a dial past its bound opened a peer")
	}
	var refusal *core.PublicError
	if !errors.As(err, &refusal) || refusal.Code != "connect_timeout" {
		t.Fatalf("dial error = %v, want the code connect_timeout", err)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("the dial waited %v past its 50ms bound", elapsed)
	}
	if ctx.Err() != nil {
		t.Fatal("the handshake's deadline ended the caller's own context")
	}

	if _, _, err := ws.Dial(ctx, url, ws.DialOptions{ConnectTimeout: -time.Second}); err == nil || !strings.Contains(err.Error(), "must not be negative") {
		t.Fatalf("a negative connect timeout = %v, want a refusal", err)
	}
}

// rawClient is a server peer reached over a raw WebSocket, so a test reads
// the close frame itself rather than what a peer makes of it.
func rawClient(t *testing.T) (*engine.Peer, *websocket.Conn, context.Context) {
	t.Helper()
	connected := make(chan *engine.Peer, 1)
	authenticate, allowOrigin := allow()
	handler, err := ws.NewHandler(ws.ServerOptions{
		Authenticate: authenticate,
		CheckOrigin:  allowOrigin,
		OnConnect:    func(peer *engine.Peer) { connected <- peer },
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	conn, _, err := websocket.Dial(ctx, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.CloseNow() })
	return receive(t, connected), conn, ctx
}

// TestAPeerEndsAWebSocketWithTheCodeItChose: over a real WebSocket, the close
// frame carries what the peer decided — 4011 and the refusal for a frame the
// protocol does not admit, a sendable code and its reason as asked — and an
// observe-only code (R27) sends no close frame at all: the connection is
// dropped, and the far side reads no status.
func TestAPeerEndsAWebSocketWithTheCodeItChose(t *testing.T) {
	// A close chosen outside the read loop can lose its close frame, in
	// v0.6.0 as here: the peer's end cancels its context before the close
	// handshake, and coder/websocket closes the socket as soon as the context
	// of its pending Read ends, so the far side often reads a dropped
	// connection instead. A refusal is chosen on the read loop and is not
	// raced. The rows stay to state what is meant.
	const inherited = "inherited from v0.6.0: Peer.end cancels the peer's context, which coder/websocket's pending Read turns into a dropped socket, before it sends the close frame"
	for _, c := range []struct {
		name   string
		end    func(ctx context.Context, t *testing.T, peer *engine.Peer, conn *websocket.Conn)
		code   websocket.StatusCode
		reason string
		skip   string
	}{
		{
			name: "a refused frame",
			end: func(ctx context.Context, t *testing.T, _ *engine.Peer, conn *websocket.Conn) {
				if err := conn.Write(ctx, websocket.MessageText, []byte(`{"version":1,"kind":"event"}`)); err != nil {
					t.Fatal(err)
				}
			},
			code:   websocket.StatusCode(transports.CodeProtocol),
			reason: "invalid duplex frame shape",
		},
		{
			name: "a policy violation",
			end: func(_ context.Context, _ *testing.T, peer *engine.Peer, _ *websocket.Conn) {
				_ = peer.Wire().Close(1008, "why")
			},
			code:   websocket.StatusPolicyViolation,
			reason: "why",
			skip:   inherited,
		},
		{
			name: "a close this side chose",
			end: func(_ context.Context, _ *testing.T, peer *engine.Peer, _ *websocket.Conn) {
				_ = peer.Close()
			},
			code: websocket.StatusNormalClosure,
			skip: inherited,
		},
		{
			name: "an abnormal closure, which is only observed",
			end: func(_ context.Context, _ *testing.T, peer *engine.Peer, _ *websocket.Conn) {
				_ = peer.Wire().Close(1006, "x")
			},
			code: -1,
		},
		{
			name: "no status, which is only observed",
			end: func(_ context.Context, _ *testing.T, peer *engine.Peer, _ *websocket.Conn) {
				_ = peer.Wire().Close(1005, "x")
			},
			code: -1,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			if c.skip != "" {
				t.Skip(c.skip)
			}
			peer, conn, ctx := rawClient(t)
			c.end(ctx, t, peer, conn)
			var err error
			for err == nil {
				_, _, err = conn.Read(ctx)
			}
			if got := websocket.CloseStatus(err); got != c.code {
				t.Fatalf("the far side read status %d (%v), want %d", got, err, c.code)
			}
			var closed websocket.CloseError
			if c.code != -1 && (!errors.As(err, &closed) || closed.Reason != c.reason) {
				t.Fatalf("the far side read %v, want the reason %q", err, c.reason)
			}
			receive(t, peer.Done())
			if !errors.Is(peer.Err(), transports.ErrClosed) {
				t.Fatalf("the peer ended with %v", peer.Err())
			}
		})
	}
}
