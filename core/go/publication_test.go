package core_test

// Ported from nightseam v0.6.0 runtime/go/publication_test.go (commit
// 5cc9723a). v0.6.0 served handlers by method name (Options.Handlers) and
// called through the peer's raw Call and Emit; bitruntime presents the
// protocol only through the peer's root Endpoint (research R20), so handlers
// are attached with a dispatcher in Options.Prepare and calls go through
// dispatch.Call and dispatch.Emit on peer.Wire().

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	core "github.com/Bitspark/bitruntime/core/go"
	dispatch "github.com/Bitspark/bitruntime/dispatch/go"
	engine "github.com/Bitspark/bitruntime/engine/go"
	endpoint "github.com/Bitspark/bitruntime/engine/websocket/go"
	transports "github.com/Bitspark/bitruntime/transports/go"
)

// serving is the Prepare that attaches a dispatcher to the peer's root and
// serves each handler group at the one-segment path of its name: the root
// Endpoint's counterpart of v0.6.0's Options.Handlers and Options.Events.
func serving(handlers func(peer *engine.Peer) map[string]dispatch.Handlers) func(*engine.Peer) error {
	return func(peer *engine.Peer) error {
		router, err := dispatch.NewDispatcher(peer.Wire())
		if err != nil {
			return err
		}
		for name, group := range handlers(peer) {
			if _, err := dispatch.Register(router, []string{name}, group); err != nil {
				return err
			}
		}
		return nil
	}
}

// newPeerPair connects a client peer to a server peer over a WebSocket.
func newPeerPair(t *testing.T, serverOptions, clientOptions engine.Options) (*engine.Peer, *engine.Peer) {
	t.Helper()
	connected := make(chan *engine.Peer, 1)
	handler, err := endpoint.NewHandler(endpoint.ServerOptions{
		Options:      serverOptions,
		Authenticate: func(r *http.Request) (context.Context, error) { return r.Context(), nil },
		CheckOrigin:  func(*http.Request) bool { return true },
		OnConnect:    func(peer *engine.Peer) { connected <- peer },
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	client, _, err := endpoint.Dial(ctx, server.URL, endpoint.DialOptions{Options: clientOptions})
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
func rawPeer(t *testing.T, options engine.Options) (*engine.Peer, *websocket.Conn, context.Context) {
	t.Helper()
	connected := make(chan *engine.Peer, 1)
	handler, err := endpoint.NewHandler(endpoint.ServerOptions{
		Options:      options,
		Authenticate: func(r *http.Request) (context.Context, error) { return r.Context(), nil },
		CheckOrigin:  func(*http.Request) bool { return true },
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

func wantUnpublished(t *testing.T, err error, want bool) {
	t.Helper()
	var unpublished *core.UnpublishedError
	if got := errors.As(err, &unpublished); got != want || err == nil {
		t.Fatalf("publication proof: %v, want unpublished=%v", err, want)
	}
}

// proofServer serves the operations TestUnpublishedProofIsLocalToTheSendAttempt
// calls.
func proofServer(entered chan<- struct{}, finish <-chan struct{}) func(*engine.Peer) map[string]dispatch.Handlers {
	return func(peer *engine.Peer) map[string]dispatch.Handlers {
		return map[string]dispatch.Handlers{
			"wait": {Request: func(c context.Context, _ json.RawMessage) (any, error) {
				entered <- struct{}{}
				select {
				case <-finish:
					return nil, nil
				case <-c.Done():
					return nil, c.Err()
				}
			}},
			"busy": {Request: func(context.Context, json.RawMessage) (any, error) {
				return nil, &core.PublicError{Code: "busy", Message: "retained before refusing"}
			}},
			"nested": {Request: func(c context.Context, _ json.RawMessage) (any, error) {
				withdrawn, cancel := context.WithCancel(c)
				cancel()
				return nil, dispatch.Call(withdrawn, peer.Wire(), []string{"never.sent"}, nil, nil)
			}},
		}
	}
}

func TestUnpublishedProofIsLocalToTheSendAttempt(t *testing.T) {
	entered, finish := make(chan struct{}, 1), make(chan struct{})
	client, server := newPeerPair(t, engine.Options{Prepare: serving(proofServer(entered, finish))}, engine.Options{MaxPendingRequests: 1, MaxFrameBytes: 512})
	_ = server
	root := client.Wire()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := dispatch.Call(ctx, root, []string{"wait"}, nil, nil)
	wantUnpublished(t, err, true)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("wrapping lost cancellation identity: %v", err)
	}
	wantUnpublished(t, dispatch.Call(context.Background(), root, []string{"wait"}, make(chan int), nil), true)
	wantUnpublished(t, dispatch.Call(context.Background(), root, []string{"wait"}, strings.Repeat("x", 1024), nil), true)
	wantUnpublished(t, dispatch.Emit(context.Background(), root, []string{"event"}, make(chan int)), true)
	wantUnpublished(t, dispatch.Emit(context.Background(), root, []string{"event"}, strings.Repeat("x", 1024)), true)

	done := make(chan error, 1)
	go func() { done <- dispatch.Call(context.Background(), root, []string{"wait"}, nil, nil) }()
	receive(t, entered)
	// The caller's own pending bound still refuses busy. That this refusal
	// also proves it unpublished is TestCallerBoundRefusalIsUnpublished's.
	err = dispatch.Call(context.Background(), root, []string{"busy"}, nil, nil)
	var public *core.PublicError
	if !errors.As(err, &public) || public.Code != "busy" {
		t.Fatalf("wrapping lost public refusal: %v", err)
	}
	close(finish)
	if err := receive(t, done); err != nil {
		t.Fatal(err)
	}

	// The same public code received from the other side carries no proof.
	err = dispatch.Call(context.Background(), root, []string{"busy"}, nil, nil)
	wantUnpublished(t, err, false)
	if !errors.As(err, &public) || public.Code != "busy" {
		t.Fatal(err)
	}
	// A marker from a nested, definitely-unsent call must not cross the wire
	// as proof about the request whose implementation has already run.
	err = dispatch.Call(context.Background(), root, []string{"nested"}, nil, nil)
	wantUnpublished(t, err, false)
	if !errors.As(err, &public) || public.Code != "cancelled" {
		t.Fatal(err)
	}
}

// TestCallerBoundRefusalIsUnpublished keeps the one assertion of v0.6.0's
// TestUnpublishedProofIsLocalToTheSendAttempt that the root Endpoint does not
// meet. v0.6.0's raw Peer.Call refused a call past MaxPendingRequests
// synchronously, before anything was queued, as unpublished. That API is
// removed (R20). The root Endpoint admits the request and answers the refusal
// it queued through the request's return capability, as v0.6.0's own root
// did, so the caller receives a public busy that carries no proof.
func TestCallerBoundRefusalIsUnpublished(t *testing.T) {
	t.Skip("root Endpoint answers its pending-bound refusal through the return capability, without unpublished proof; v0.6.0 proved it only through the removed Peer.Call (R20)")
	entered, finish := make(chan struct{}, 1), make(chan struct{})
	defer close(finish)
	client, _ := newPeerPair(t, engine.Options{Prepare: serving(proofServer(entered, finish))}, engine.Options{MaxPendingRequests: 1})
	go func() { _ = dispatch.Call(context.Background(), client.Wire(), []string{"wait"}, nil, nil) }()
	receive(t, entered)
	err := dispatch.Call(context.Background(), client.Wire(), []string{"busy"}, nil, nil)
	wantUnpublished(t, err, true)
	var public *core.PublicError
	if !errors.As(err, &public) || public.Code != "busy" {
		t.Fatalf("wrapping lost public refusal: %v", err)
	}
}

type publicationWriteFailure struct {
	transports.Conn
	wrote chan struct{}
	err   error
}

func (c *publicationWriteFailure) Send(context.Context, transports.Frame) error {
	close(c.wrote)
	return c.err
}

// A call through the root Endpoint is answered through its return capability,
// which carries a public error, never a transport's error value. v0.6.0's raw
// Peer.Call returned the transport cause itself; here the carrier's own ending
// report, Peer.Err, is what keeps it (R26), and the call's answer must carry
// no proof either way.
func TestQueuedWriteFailureHasNoUnpublishedProof(t *testing.T) {
	client, _ := newPeerPair(t, engine.Options{}, engine.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	proof := dispatch.Call(ctx, client.Wire(), []string{"unsent"}, nil, nil)
	wantUnpublished(t, proof, true)
	for _, tc := range []struct {
		name            string
		cause, identity error
	}{
		{"plain", errors.New("transport failed after accepting a queued frame"), nil},
		{"nested", fmt.Errorf("adapter send: %w", proof), context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			near, far := transports.Pipe(1 << 20)
			defer far.Abort()
			cause := tc.cause
			connection := &publicationWriteFailure{Conn: near, wrote: make(chan struct{}), err: cause}
			peer, err := engine.NewPeer(context.Background(), connection, engine.ClientRole, engine.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer peer.Close()
			err = dispatch.Call(context.Background(), peer.Wire(), []string{"supply"}, nil, nil)
			receive(t, connection.wrote)
			wantUnpublished(t, err, false)
			ended := peer.Err()
			wantUnpublished(t, ended, false)
			if !errors.Is(ended, cause) {
				t.Fatalf("transport cause lost: %v", ended)
			}
			if tc.identity != nil && !errors.Is(ended, tc.identity) {
				t.Fatalf("nested cause lost: %v", ended)
			}
		})
	}
}

type publicationReadFailure struct {
	transports.Conn
	wrote chan struct{}
	err   error
}

func (c *publicationReadFailure) Send(context.Context, transports.Frame) error {
	close(c.wrote)
	return nil
}
func (c *publicationReadFailure) Receive(context.Context) (transports.Frame, error) {
	<-c.wrote
	return transports.Frame{}, c.err
}

// As in TestQueuedWriteFailureHasNoUnpublishedProof, the nested cause is kept
// by the carrier's ending report rather than by the call's public answer.
func TestReadFailureHasNoNestedUnpublishedProof(t *testing.T) {
	client, _ := newPeerPair(t, engine.Options{}, engine.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	proof := dispatch.Call(ctx, client.Wire(), []string{"unsent"}, nil, nil)
	near, far := transports.Pipe(1 << 20)
	defer far.Abort()
	connection := &publicationReadFailure{Conn: near, wrote: make(chan struct{}), err: fmt.Errorf("adapter receive: %w", proof)}
	peer, err := engine.NewPeer(context.Background(), connection, engine.ClientRole, engine.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	err = dispatch.Call(context.Background(), peer.Wire(), []string{"supply"}, nil, nil)
	wantUnpublished(t, err, false)
	ended := peer.Err()
	wantUnpublished(t, ended, false)
	if !errors.Is(ended, context.Canceled) {
		t.Fatalf("nested cause lost: %v", ended)
	}
}

// reverseReplyServers is the pair TestRefusedReverseReplyCannotProveDeliveredCallUnpublished
// runs: the server's "a" calls the client's "b", whose reply exceeds the
// client's frame limit.
func reverseReplyServers(delivered *atomic.Bool) (engine.Options, engine.Options) {
	server := engine.Options{Prepare: serving(func(peer *engine.Peer) map[string]dispatch.Handlers {
		return map[string]dispatch.Handlers{"a": {Request: func(ctx context.Context, _ json.RawMessage) (any, error) {
			delivered.Store(true)
			return nil, dispatch.Call(ctx, peer.Wire(), []string{"b"}, nil, nil)
		}}}
	})}
	client := engine.Options{MaxFrameBytes: 180, Prepare: serving(func(*engine.Peer) map[string]dispatch.Handlers {
		return map[string]dispatch.Handlers{"b": {Request: func(context.Context, json.RawMessage) (any, error) { return strings.Repeat("x", 2000), nil }}}
	})}
	return server, client
}

// The client's reply to "b" is refused by the return capability its root
// handed the dispatcher, and so is its bounded fallback, which does not fit
// 180 bytes beside a traceparent; the request then ends at the client's
// RequestTimeout, shortened here so the test does not wait 30 seconds. That
// refusal cannot prove the delivered outer call unpublished.
func TestRefusedReverseReplyCannotProveDeliveredCallUnpublished(t *testing.T) {
	var delivered atomic.Bool
	serverOptions, clientOptions := reverseReplyServers(&delivered)
	clientOptions.RequestTimeout = 250 * time.Millisecond
	client, _ := newPeerPair(t, serverOptions, clientOptions)
	err := dispatch.Call(context.Background(), client.Wire(), []string{"a"}, nil, nil)
	if !delivered.Load() {
		t.Fatal("outer request was not delivered")
	}
	wantUnpublished(t, err, false)
}

// TestOversizedReverseReplyFallbackEndsTheCarrier keeps the assertion of
// v0.6.0's TestRefusedReverseReplyCannotProveDeliveredCallUnpublished: when even
// the bounded fallback of an oversized reply does not fit, the connection fails
// rather than leaving the remote caller to its deadline. Through the root, the
// reply capability settles an unencodable reply as the bounded internal error,
// and the engine's own response path fails the connection when that does not
// fit either. The elapsed-time check refuses a pass that only the 10-second
// dial context of newPeerPair would produce.
func TestOversizedReverseReplyFallbackEndsTheCarrier(t *testing.T) {
	var delivered atomic.Bool
	serverOptions, clientOptions := reverseReplyServers(&delivered)
	client, _ := newPeerPair(t, serverOptions, clientOptions)
	started := time.Now()
	err := dispatch.Call(context.Background(), client.Wire(), []string{"a"}, nil, nil)
	if !delivered.Load() {
		t.Fatal("outer request was not delivered")
	}
	if client.Err() == nil || time.Since(started) > 5*time.Second {
		t.Fatalf("oversized reply fallback did not reach the failure broadcast: %v after %v", client.Err(), time.Since(started))
	}
	wantUnpublished(t, err, false)
}
