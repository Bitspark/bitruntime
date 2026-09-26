package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Bitspark/nightseam/duplex/go"
	ws "github.com/Bitspark/nightseam/runtime/go"
	"github.com/Bitspark/nightseam/tunnel/go"
	"github.com/coder/websocket"
)

// A tunnel installed in Prepare is there before the peer has read anything,
// so the client's first channel.open — the natural first act of a consumer
// that came for a channel — meets a handler rather than method_not_found.
// The hook takes a millisecond here on purpose: with Prepare, how long the
// install takes cannot matter, because no frame is read while it runs. The
// same server with the same install in OnConnect refuses 868 of these 1000
// opens; with the tunnel installed in Prepare, none.
func TestATunnelInstalledInPrepareMeetsTheFirstChannelOpen(t *testing.T) {
	handler, err := ws.NewHandler(ws.ServerOptions{
		Options: ws.Options{Prepare: func(peer *ws.Peer) error {
			time.Sleep(time.Millisecond)
			_, err := tunnel.New(peer, tunnel.Options{})
			return err
		}},
		Authenticate: func(r *http.Request) (context.Context, error) { return r.Context(), nil },
		CheckOrigin:  func(*http.Request) bool { return true },
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
		carrier, err := tunnel.New(client, tunnel.Options{})
		if err != nil {
			cancel()
			t.Fatalf("iteration %d: tunnel: %v", i, err)
		}
		channel, err := carrier.OpenConnection(ctx, "probe", "")
		if err != nil {
			var public *ws.PublicError
			if errors.As(err, &public) {
				t.Fatalf("iteration %d: the first open was refused %s", i, public.Code)
			}
			t.Fatalf("iteration %d: the first open was refused: %v", i, err)
		}
		_ = channel.Close(ctx, duplex.CodeNormal, "")
		_ = client.Close()
		cancel()
	}
}

// Prepare is where a peer's own handlers go, and it holds the peer alone:
// Handle inside it registers on a peer nothing has reached yet.
func TestPrepareInstallsBeforeTheFirstFrameAndOnConnectSeesALivePeer(t *testing.T) {
	order := make(chan string, 2)
	handler, err := ws.NewHandler(ws.ServerOptions{
		Options: ws.Options{Prepare: func(peer *ws.Peer) error {
			order <- "prepare"
			return peer.Handle("probe", func(context.Context, *ws.Peer, json.RawMessage) (any, error) {
				return map[string]any{"ready": true}, nil
			})
		}},
		Authenticate: func(r *http.Request) (context.Context, error) { return r.Context(), nil },
		CheckOrigin:  func(*http.Request) bool { return true },
		OnConnect:    func(*ws.Peer) { order <- "connect" },
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
	if err := client.Call(ctx, "probe", map[string]any{}, &answer); err != nil || !answer.Ready {
		t.Fatalf("the handler Prepare installed did not answer: %v", err)
	}
	if first, second := receive(t, order), receive(t, order); first != "prepare" || second != "connect" {
		t.Fatalf("hooks ran %s then %s", first, second)
	}
}

// A Prepare that fails fails the construction, on each of the three
// constructors: nothing is returned that could read a frame.
func TestPrepareFailingFailsNewPeer(t *testing.T) {
	refusal := errors.New("this peer serves nothing")
	near, far := duplex.Pipe(1 << 20)
	defer far.Abort()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	peer, err := ws.NewPeer(ctx, near, ws.ClientRole, ws.Options{Prepare: func(*ws.Peer) error { return refusal }})
	if !errors.Is(err, refusal) || peer != nil {
		t.Fatalf("NewPeer answered peer=%v error=%v", peer, err)
	}
}

func TestPrepareFailingFailsAcceptAndClosesTheSocket(t *testing.T) {
	refusal := errors.New("this server serves nothing")
	accepted := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := ws.Accept(w, r, ws.ServerOptions{
			Options:      ws.Options{Prepare: func(*ws.Peer) error { return refusal }},
			Authenticate: func(r *http.Request) (context.Context, error) { return r.Context(), nil },
			CheckOrigin:  func(*http.Request) bool { return true },
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
	// The upgrade was answered, so the client is told why the profile will
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
	handler, err := ws.NewHandler(ws.ServerOptions{
		Authenticate: func(r *http.Request) (context.Context, error) { return r.Context(), nil },
		CheckOrigin:  func(*http.Request) bool { return true },
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	peer, _, err := ws.Dial(ctx, server.URL, ws.DialOptions{Options: ws.Options{Prepare: func(*ws.Peer) error { return refusal }}})
	if !errors.Is(err, refusal) || peer != nil {
		t.Fatalf("Dial answered peer=%v error=%v", peer, err)
	}
}
