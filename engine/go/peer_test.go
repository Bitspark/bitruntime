package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Bitspark/nightseam/duplex/go"
	ws "github.com/Bitspark/nightseam/runtime/go"
	"github.com/coder/websocket"
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

func newPair(t *testing.T, serverOptions, clientOptions ws.Options) (*ws.Peer, *ws.Peer) {
	t.Helper()
	connected := make(chan *ws.Peer, 1)
	handler, err := ws.NewHandler(ws.ServerOptions{
		Options:      serverOptions,
		Authenticate: func(r *http.Request) (context.Context, error) { return r.Context(), nil },
		CheckOrigin:  func(*http.Request) bool { return true },
		OnConnect:    func(peer *ws.Peer) { connected <- peer },
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	client, _, err := ws.Dial(ctx, server.URL, ws.DialOptions{Options: clientOptions})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	remote := receive(t, connected)
	t.Cleanup(func() { _ = remote.Close() })
	return client, remote
}

// rawPeer is one peer reached over a raw connection, so a test spells the
// frames it sends byte for byte rather than letting a peer encode them.
func rawPeer(t *testing.T, options ws.Options) (*ws.Peer, *websocket.Conn, context.Context) {
	t.Helper()
	connected := make(chan *ws.Peer, 1)
	handler, err := ws.NewHandler(ws.ServerOptions{
		Options:      options,
		Authenticate: func(r *http.Request) (context.Context, error) { return r.Context(), nil },
		CheckOrigin:  func(*http.Request) bool { return true },
		OnConnect:    func(peer *ws.Peer) { connected <- peer },
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

func readFrame(ctx context.Context, t *testing.T, conn *websocket.Conn) map[string]json.RawMessage {
	t.Helper()
	kind, data, err := conn.Read(ctx)
	if err != nil || kind != websocket.MessageText {
		t.Fatalf("read frame: kind=%v error=%v", kind, err)
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal(data, &members); err != nil {
		t.Fatalf("decode frame %s: %v", data, err)
	}
	return members
}

func TestReverseCallCompletesWhileOriginalRequestIsOutstanding(t *testing.T) {
	client, _ := newPair(t, ws.Options{Handlers: map[string]ws.Handler{
		"outer": func(ctx context.Context, peer *ws.Peer, _ json.RawMessage) (any, error) {
			var response int
			if err := peer.Call(ctx, "reverse", 6, &response); err != nil {
				return nil, err
			}
			return response + 1, nil
		},
		"inner": func(_ context.Context, _ *ws.Peer, data json.RawMessage) (any, error) {
			var value int
			if err := json.Unmarshal(data, &value); err != nil {
				return nil, err
			}
			return value * 7, nil
		},
	}}, ws.Options{Handlers: map[string]ws.Handler{
		"reverse": func(ctx context.Context, peer *ws.Peer, data json.RawMessage) (any, error) {
			var response int
			err := peer.Call(ctx, "inner", data, &response)
			return response, err
		},
	}})
	var result int
	if err := client.Call(context.Background(), "outer", nil, &result); err != nil {
		t.Fatal(err)
	}
	if result != 43 {
		t.Fatalf("nested duplex result = %d, want 43", result)
	}
}

func TestEventsTravelInBothDirections(t *testing.T) {
	clientEvents, serverEvents := make(chan string, 2), make(chan string, 2)
	eventHandler := func(output chan<- string) ws.EventHandler {
		return func(_ context.Context, _ *ws.Peer, data json.RawMessage) { output <- string(data) }
	}
	client, server := newPair(t,
		ws.Options{Events: map[string]ws.EventHandler{"progress": eventHandler(serverEvents)}},
		ws.Options{Events: map[string]ws.EventHandler{"progress": eventHandler(clientEvents)}})
	observed := make(chan ws.Event, 1)
	unsubscribe := client.OnEvent(func(_ context.Context, event ws.Event) { observed <- event })
	for _, value := range []int{1, 2} {
		if err := client.Emit(context.Background(), "progress", value); err != nil {
			t.Fatal(err)
		}
	}
	if err := server.Emit(context.Background(), "progress", "done"); err != nil {
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
	if got := receive(t, observed); got.Name != "progress" || string(got.Data) != `"done"` {
		t.Fatalf("observed event = %+v", got)
	}
	unsubscribe()
	unsubscribe()
}

func TestPublicErrorsArePreservedAndInternalFailuresHidden(t *testing.T) {
	client, _ := newPair(t, ws.Options{Handlers: map[string]ws.Handler{
		"public": func(context.Context, *ws.Peer, json.RawMessage) (any, error) {
			return nil, fmt.Errorf("wrapper: %w", &ws.PublicError{Code: "conflict", Message: "Changed", Data: json.RawMessage(`{"revision":3}`)})
		},
		"private": func(context.Context, *ws.Peer, json.RawMessage) (any, error) {
			return nil, errors.New("private database password")
		},
		"panic": func(context.Context, *ws.Peer, json.RawMessage) (any, error) {
			panic("private panic details")
		},
		"unencodable": func(context.Context, *ws.Peer, json.RawMessage) (any, error) {
			return make(chan int), nil
		},
		"ok": func(context.Context, *ws.Peer, json.RawMessage) (any, error) { return true, nil },
	}}, ws.Options{})
	for _, test := range []struct{ method, code, message string }{
		{"public", "conflict", "Changed"},
		{"private", "internal", "Internal error"},
		{"panic", "internal", "Internal error"},
		{"unencodable", "internal", "Internal error"},
		{"missing", "method_not_found", "Unknown method"},
	} {
		t.Run(test.method, func(t *testing.T) {
			err := client.Call(context.Background(), test.method, nil, nil)
			var public *ws.PublicError
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
	if err := client.Call(context.Background(), "ok", nil, &result); err != nil || !result {
		t.Fatalf("connection did not survive handler failure: result=%v err=%v", result, err)
	}
}

func TestCallerCancellationReachesRemoteHandler(t *testing.T) {
	started, cancelled := make(chan struct{}), make(chan error, 1)
	client, server := newPair(t, ws.Options{Handlers: map[string]ws.Handler{
		"wait": func(ctx context.Context, _ *ws.Peer, _ json.RawMessage) (any, error) {
			close(started)
			<-ctx.Done()
			cancelled <- ctx.Err()
			return nil, ctx.Err()
		},
	}}, ws.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	returned := make(chan error, 1)
	go func() { returned <- client.Call(ctx, "wait", nil, nil) }()
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
	client, _ := newPair(t, ws.Options{
		MaxConcurrentHandlers: 1,
		Handlers: map[string]ws.Handler{
			"outer": func(ctx context.Context, peer *ws.Peer, _ json.RawMessage) (any, error) {
				var result string
				err := peer.Call(ctx, "reverse", nil, &result)
				return result, err
			},
			"extra": func(context.Context, *ws.Peer, json.RawMessage) (any, error) { return "unexpected", nil },
		},
	}, ws.Options{Handlers: map[string]ws.Handler{
		"reverse": func(ctx context.Context, _ *ws.Peer, _ json.RawMessage) (any, error) {
			close(reverseStarted)
			select {
			case <-release:
				return "released", nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}})
	type callResult struct {
		value string
		err   error
	}
	returned := make(chan callResult, 1)
	go func() {
		var value string
		err := client.Call(context.Background(), "outer", nil, &value)
		returned <- callResult{value, err}
	}()
	receive(t, reverseStarted)
	err := client.Call(context.Background(), "extra", nil, nil)
	var public *ws.PublicError
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
	client, server := newPair(t, ws.Options{}, ws.Options{
		QueueCapacity: 1,
		WriteTimeout:  200 * time.Millisecond,
		Events: map[string]ws.EventHandler{
			"progress": func(ctx context.Context, _ *ws.Peer, _ json.RawMessage) {
				close(started)
				<-ctx.Done()
			},
		},
	})
	if err := server.Emit(context.Background(), "progress", 1); err != nil {
		t.Fatal(err)
	}
	receive(t, started)
	for _, value := range []int{2, 3} {
		if err := server.Emit(context.Background(), "progress", value); err != nil {
			t.Fatal(err)
		}
	}
	receive(t, client.Done())
	if !errors.Is(client.Err(), ws.ErrBackpressure) {
		t.Fatalf("stalled event consumer error = %v", client.Err())
	}
	receive(t, server.Done())
}

func TestDisconnectCancelsHandlersAndRejectsPendingCalls(t *testing.T) {
	started, stopped := make(chan struct{}), make(chan struct{})
	client, server := newPair(t, ws.Options{Handlers: map[string]ws.Handler{
		"wait": func(ctx context.Context, _ *ws.Peer, _ json.RawMessage) (any, error) {
			close(started)
			<-ctx.Done()
			close(stopped)
			return nil, ctx.Err()
		},
	}}, ws.Options{})
	returned := make(chan error, 1)
	go func() { returned <- client.Call(context.Background(), "wait", nil, nil) }()
	receive(t, started)
	_ = server.Close()
	receive(t, stopped)
	if err := receive(t, returned); err == nil {
		t.Fatal("pending call succeeded after disconnect")
	}
	receive(t, client.Done())
}

func TestServerRequiresAndEnforcesAuthenticationAndOriginPolicies(t *testing.T) {
	authenticate := func(r *http.Request) (context.Context, error) { return r.Context(), nil }
	allowOrigin := func(*http.Request) bool { return true }
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
		Options: ws.Options{Handlers: map[string]ws.Handler{
			"identity": func(ctx context.Context, _ *ws.Peer, _ json.RawMessage) (any, error) {
				return ctx.Value(userKey{}), nil
			},
		}},
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
	if err := client.Call(ctx, "identity", nil, &identity); err != nil || identity != "alice" {
		t.Fatalf("authenticated identity = %q, error=%v", identity, err)
	}
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
			peer, conn, ctx := rawPeer(t, ws.Options{})
			if err := conn.Write(ctx, websocket.MessageText, []byte(data)); err != nil {
				t.Fatal(err)
			}
			receive(t, peer.Done())
			if peer.Err() == nil {
				t.Fatal("malformed frame closed without error")
			}
		})
	}
}

// The members are optional on every kind and the peer emits none of its own:
// what a frame carries it carries past the decoder, and the frame is served.
func TestTraceContextTravelsOnEveryFrameKind(t *testing.T) {
	const trace = `"traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01","tracestate":"congo=t61rcWkgMzE"`
	events, cancelled := make(chan string, 2), make(chan struct{})
	peer, conn, ctx := rawPeer(t, ws.Options{
		Events: map[string]ws.EventHandler{
			"progress": func(_ context.Context, _ *ws.Peer, data json.RawMessage) { events <- string(data) },
		},
		Handlers: map[string]ws.Handler{
			"outer": func(ctx context.Context, peer *ws.Peer, _ json.RawMessage) (any, error) {
				var answer string
				if err := peer.Call(ctx, "reverse", nil, &answer); err != nil {
					return nil, err
				}
				return answer, nil
			},
			"wait": func(ctx context.Context, _ *ws.Peer, _ json.RawMessage) (any, error) {
				<-ctx.Done()
				close(cancelled)
				return nil, ctx.Err()
			},
		},
	})
	write := func(data string) {
		t.Helper()
		if err := conn.Write(ctx, websocket.MessageText, []byte(data)); err != nil {
			t.Fatal(err)
		}
	}
	write(`{"version":1,"kind":"event","event":"progress","data":1,` + trace + `}`)
	if got := receive(t, events); got != "1" {
		t.Fatalf("traced event = %s", got)
	}
	// A traced request is served, and the response to the reverse call it makes
	// is itself traced: both kinds cross the decoder in one exchange.
	write(`{"version":1,"kind":"request","id":"c:1","method":"outer","params":{},` + trace + `}`)
	reverse := readFrame(ctx, t, conn)
	if string(reverse["method"]) != `"reverse"` {
		t.Fatalf("reverse request = %v", reverse)
	}
	write(`{"version":1,"kind":"response","id":` + string(reverse["id"]) + `,"result":"back",` + trace + `}`)
	if response := readFrame(ctx, t, conn); string(response["id"]) != `"c:1"` || string(response["result"]) != `"back"` {
		t.Fatalf("response to traced request = %v", response)
	}
	// An intermediary may strip one member and not the other.
	write(`{"version":1,"kind":"event","event":"progress","data":2,"tracestate":"congo=t61rcWkgMzE"}`)
	if got := receive(t, events); got != "2" {
		t.Fatalf("event carrying tracestate alone = %s", got)
	}
	write(`{"version":1,"kind":"request","id":"c:2","method":"wait","params":{},` + trace + `}`)
	write(`{"version":1,"kind":"cancel","id":"c:2",` + trace + `}`)
	receive(t, cancelled)
	if peer.Err() != nil {
		t.Fatalf("a traced frame closed the connection: %v", peer.Err())
	}
}

// TestSubprotocolNegotiation holds what the handshake selected against what
// both peers report, and the profile is spoken over the connection either
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
			connected := make(chan *ws.Peer, 1)
			options := test.server
			options.Authenticate = func(r *http.Request) (context.Context, error) { return r.Context(), nil }
			options.CheckOrigin = func(*http.Request) bool { return true }
			options.OnConnect = func(peer *ws.Peer) { connected <- peer }
			options.Options = ws.Options{Handlers: map[string]ws.Handler{
				"selected": func(_ context.Context, peer *ws.Peer, _ json.RawMessage) (any, error) { return peer.Subprotocol(), nil },
			}}
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
			// And the connection carries the profile whatever was selected,
			// which the server reads back over it.
			var answer string
			if err := client.Call(ctx, "selected", nil, &answer); err != nil {
				t.Fatalf("call over a connection negotiating %q: %v", test.selected, err)
			}
			if answer != test.selected {
				t.Fatalf("subprotocol a handler read = %q, want %q", answer, test.selected)
			}
		})
	}
}

// TestSubprotocolIsNoneOverAnyOtherTransport: a peer that is not over a
// WebSocket negotiated nothing and says so.
func TestSubprotocolIsNoneOverAnyOtherTransport(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	clientConn, serverConn := duplex.Pipe(1 << 20)
	client, err := ws.NewPeer(ctx, clientConn, ws.ClientRole, ws.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	server, err := ws.NewPeer(ctx, serverConn, ws.ServerRole, ws.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	if client.Subprotocol() != "" || server.Subprotocol() != "" {
		t.Fatalf("subprotocols over a pipe = %q and %q", client.Subprotocol(), server.Subprotocol())
	}
}

// TestDialRefusesAHandshakeThatNeverAnswers: ConnectTimeout is the Go twin of
// TypeScript's connectTimeoutMs — a listener that takes the connection and
// never answers the upgrade is refused with the same code, connect_timeout,
// with nothing opened and the caller's own context untouched, the deadline
// having been the handshake's and not the connection's. A negative bound is
// refused before anything is dialled at all.
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
	var refusal *ws.PublicError
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
