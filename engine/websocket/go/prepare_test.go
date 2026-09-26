package websocket_test

// Ported from nightseam v0.6.0 runtime/go/prepare_test.go
// (5cc9723a24646c40ed1861f892b2b23eb6d785d7): Prepare on the WebSocket
// constructors.

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"

	core "github.com/Bitspark/bitruntime/core/go"
	dispatch "github.com/Bitspark/bitruntime/dispatch/go"
	engine "github.com/Bitspark/bitruntime/engine/go"
	ws "github.com/Bitspark/bitruntime/engine/websocket/go"
)

// A receiver attached in Prepare is there before the peer has read anything,
// so the client's first request — the natural first act of a consumer that
// came for something — meets it rather than method_not_found. The hook takes
// a millisecond here on purpose: with Prepare, how long the attachment takes
// cannot matter, because no frame is read while it runs. v0.6.0 held this
// with a tunnel whose channel.open was the first request, and measured the
// same install in OnConnect refusing 868 of 1000 opens; tunnels are not
// ported, and a dispatcher at the root is the attachment here.
func TestAReceiverAttachedInPrepareMeetsTheFirstRequest(t *testing.T) {
	authenticate, allowOrigin := allow()
	handler, err := ws.NewHandler(ws.ServerOptions{
		Options: engine.Options{Prepare: func(peer *engine.Peer) error {
			time.Sleep(time.Millisecond)
			return serve(map[string]func(context.Context, *engine.Peer) (any, error){
				"probe": func(context.Context, *engine.Peer) (any, error) { return true, nil },
			})(peer)
		}},
		Authenticate: authenticate,
		CheckOrigin:  allowOrigin,
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	iterations := 3000
	if testing.Short() {
		iterations = 250
	}
	for i := range iterations {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		client, _, err := ws.Dial(ctx, server.URL, ws.DialOptions{})
		if err != nil {
			cancel()
			t.Fatalf("iteration %d: dial: %v", i, err)
		}
		var ready bool
		if err := dispatch.Call(ctx, client.Wire(), []string{"probe"}, nil, &ready); err != nil || !ready {
			var public *core.PublicError
			if errors.As(err, &public) {
				t.Fatalf("iteration %d: the first request was refused %s", i, public.Code)
			}
			t.Fatalf("iteration %d: the first request was refused: %v", i, err)
		}
		_ = client.Close()
		cancel()
	}
}

// Prepare is where a peer's own receiver goes, and it holds the peer alone:
// what it attaches is attached to a peer nothing has reached yet. OnConnect
// runs after it, on a peer that is live.
func TestPrepareInstallsBeforeTheFirstFrameAndOnConnectSeesALivePeer(t *testing.T) {
	order := make(chan string, 2)
	authenticate, allowOrigin := allow()
	handler, err := ws.NewHandler(ws.ServerOptions{
		Options: engine.Options{Prepare: func(peer *engine.Peer) error {
			order <- "prepare"
			return serve(map[string]func(context.Context, *engine.Peer) (any, error){
				"probe": func(context.Context, *engine.Peer) (any, error) { return map[string]any{"ready": true}, nil },
			})(peer)
		}},
		Authenticate: authenticate,
		CheckOrigin:  allowOrigin,
		OnConnect:    func(*engine.Peer) { order <- "connect" },
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, _, err := ws.Dial(ctx, server.URL, ws.DialOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	var answer struct {
		Ready bool `json:"ready"`
	}
	if err := dispatch.Call(ctx, client.Wire(), []string{"probe"}, map[string]any{}, &answer); err != nil || !answer.Ready {
		t.Fatalf("the receiver Prepare attached did not answer: %v", err)
	}
	if first, second := receive(t, order), receive(t, order); first != "prepare" || second != "connect" {
		t.Fatalf("hooks ran %s then %s", first, second)
	}
}

func TestPrepareFailingFailsAcceptAndClosesTheSocket(t *testing.T) {
	refusal := errors.New("this server serves nothing")
	accepted := make(chan error, 1)
	authenticate, allowOrigin := allow()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := ws.Accept(w, r, ws.ServerOptions{
			Options:      engine.Options{Prepare: func(*engine.Peer) error { return refusal }},
			Authenticate: authenticate,
			CheckOrigin:  allowOrigin,
		})
		accepted <- err
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	// The upgrade was answered, so the client is told why the protocol will
	// not be spoken over it rather than left reading a socket in silence.
	_, _, err = conn.Read(ctx)
	var closed websocket.CloseError
	if !errors.As(err, &closed) || closed.Code != websocket.StatusPolicyViolation {
		t.Fatalf("the refused socket ended with %v", err)
	}
	if err := receive(t, accepted); !errors.Is(err, refusal) {
		t.Fatalf("Accept answered %v", err)
	}
}

func TestPrepareFailingFailsDial(t *testing.T) {
	refusal := errors.New("this client serves nothing")
	authenticate, allowOrigin := allow()
	handler, err := ws.NewHandler(ws.ServerOptions{Authenticate: authenticate, CheckOrigin: allowOrigin})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	peer, _, err := ws.Dial(ctx, server.URL, ws.DialOptions{Options: engine.Options{Prepare: func(*engine.Peer) error { return refusal }}})
	if !errors.Is(err, refusal) || peer != nil {
		t.Fatalf("Dial answered peer=%v error=%v", peer, err)
	}
}
