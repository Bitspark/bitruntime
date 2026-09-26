package dispatch_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/Bitspark/bitruntime/core/go"
	dispatch "github.com/Bitspark/bitruntime/dispatch/go"
	engine "github.com/Bitspark/bitruntime/engine/go"
	enginews "github.com/Bitspark/bitruntime/engine/websocket/go"
)

// Delaying actual TCP writes makes producer/transport imbalance reproducible
// without depending on the OS socket-buffer size or a stopped remote reader.
type delayedWriteControl struct {
	enabled atomic.Bool
	delay   time.Duration
	started chan struct{}
	gate    <-chan struct{}
	once    sync.Once
}

type delayedWriteListener struct {
	net.Listener
	control *delayedWriteControl
}

func (l delayedWriteListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &delayedWriteConn{Conn: conn, control: l.control}, nil
}

type delayedWriteConn struct {
	net.Conn
	control *delayedWriteControl
}

func (c *delayedWriteConn) Write(data []byte) (int, error) {
	if c.control.enabled.Load() {
		c.control.once.Do(func() { close(c.control.started) })
		if c.control.gate != nil {
			<-c.control.gate
		}
		time.Sleep(c.control.delay)
	}
	return c.Conn.Write(data)
}

// delayedWritePair connects a client to a server whose socket writes the
// control holds, and returns them in that order.
func delayedWritePair(t *testing.T, control *delayedWriteControl, serverOptions, clientOptions engine.Options) (*engine.Peer, *engine.Peer) {
	t.Helper()
	connected := make(chan *engine.Peer, 1)
	handler, err := enginews.NewHandler(enginews.ServerOptions{
		Options:      serverOptions,
		Authenticate: func(r *http.Request) (context.Context, error) { return r.Context(), nil },
		CheckOrigin:  func(*http.Request) bool { return true },
		OnConnect: func(peer *engine.Peer) {
			// The HTTP upgrade has been flushed before introducing write delay.
			control.enabled.Store(true)
			connected <- peer
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(handler)
	server.Listener = delayedWriteListener{Listener: server.Listener, control: control}
	server.Start()
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	client, _, err := enginews.Dial(ctx, server.URL, enginews.DialOptions{Options: clientOptions})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	remote := receive(t, connected)
	t.Cleanup(func() { _ = remote.Close() })
	return client, remote
}

// rootTaken tells when the peer's root has taken a frame off its own queue:
// the root extracts each request's and event's trace before it hands the frame
// to the peer. Nightseam v0.6.0's raw Emit queued its frame synchronously;
// through the root, a test that needs the root's queue empty again waits here.
type rootTaken struct {
	core.Propagator
	taken chan struct{}
}

func newRootTaken() rootTaken {
	return rootTaken{Propagator: core.DefaultPropagator, taken: make(chan struct{}, 64)}
}

func (p rootTaken) Extract(ctx context.Context, trace core.Trace) context.Context {
	select {
	case p.taken <- struct{}{}:
	default:
	}
	return p.Propagator.Extract(ctx, trace)
}

// A full destination's consumer remains held while the caller finishes. The
// write deadline is deliberately much longer than the caller's bound: waiting
// for that deadline would make fan-out run at its slowest destination's pace.
//
// Nightseam v0.6.0 also counted the observer's terminal Backpressure events;
// bitruntime has no observer. Its raw Emit is the root's Emit here.
func TestWireSendEndsAFullCarrierWithoutWaitingForItsConsumer(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	control := &delayedWriteControl{started: make(chan struct{}), gate: release}
	_, destination := delayedWritePair(t, control, engine.Options{
		QueueCapacity: 1,
		WriteTimeout:  5 * time.Second,
	}, engine.Options{})
	if err := dispatch.Emit(context.Background(), destination.Wire(), []string{"first"}, 1); err != nil {
		t.Fatal(err)
	}
	receive(t, control.started)
	if err := dispatch.Emit(context.Background(), destination.Wire(), []string{"second"}, 2); err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() { finished <- dispatch.Emit(context.Background(), destination.Wire(), []string{"overflow"}, 3) }()
	select {
	case err := <-finished:
		if err != nil && !errors.Is(err, core.ErrBackpressure) {
			t.Fatalf("wire admission = %v, want admission or backpressure", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("send waited for a destination whose consumer is still held")
	}
	select {
	case <-destination.Done():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("wire dispatch waited for a destination whose consumer is still held")
	}
	if !errors.Is(destination.Err(), core.ErrBackpressure) {
		t.Fatalf("full carrier remained open: %v", destination.Err())
	}
}

func TestWireRequestEndsAFullCarrierWithoutWaitingForItsConsumer(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	control := &delayedWriteControl{started: make(chan struct{}), gate: release}
	root := newRootTaken()
	_, destination := delayedWritePair(t, control, engine.Options{QueueCapacity: 1, WriteTimeout: 5 * time.Second, Propagator: root}, engine.Options{})
	if err := dispatch.Emit(context.Background(), destination.Wire(), []string{"first"}, 1); err != nil {
		t.Fatal(err)
	}
	receive(t, root.taken)
	receive(t, control.started)
	if err := dispatch.Emit(context.Background(), destination.Wire(), []string{"second"}, 2); err != nil {
		t.Fatal(err)
	}
	receive(t, root.taken)
	finished := make(chan error, 1)
	go func() {
		finished <- dispatch.Call(context.Background(), destination.Wire(), []string{"overflow"}, nil, nil)
	}()
	select {
	case err := <-finished:
		if err == nil {
			t.Fatal("wire request into a full carrier succeeded")
		}
		// Root wire admission already succeeded. A downstream carrier refusal
		// is an ordinary result, not proof of pre-admission non-publication.
		wantUnpublished(t, err, false)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("wire request waited for a destination whose consumer is still held")
	}
	if !errors.Is(destination.Err(), core.ErrBackpressure) {
		t.Fatalf("full carrier remained open: %v", destination.Err())
	}
}

// Nightseam v0.6.0 also made a raw, paced call wait for the full queue here
// and withdrew it, proving that call unpublished; bitruntime removed the raw
// call, and a call through the root is never paced. What remains is the
// earlier admitted call: the immediate dispatch that ends the carrier cannot
// make it prove that it never left. Through the root it is answered
// disconnected, since an overflowed carrier is a closed one.
func TestOutputCancellationProofBelongsOnlyToTheUnadmittedCall(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	control := &delayedWriteControl{started: make(chan struct{}), gate: release}
	root := newRootTaken()
	_, destination := delayedWritePair(t, control, engine.Options{QueueCapacity: 1, WriteTimeout: 5 * time.Second, Propagator: root}, engine.Options{})
	accepted := make(chan error, 1)
	go func() {
		accepted <- dispatch.Call(context.Background(), destination.Wire(), []string{"accepted"}, nil, nil)
	}()
	// The earlier call is already in its carrier's write. Hold it there, then
	// occupy the only queue slot.
	receive(t, root.taken)
	receive(t, control.started)
	if err := dispatch.Emit(context.Background(), destination.Wire(), []string{"queued"}, nil); err != nil {
		t.Fatal(err)
	}
	receive(t, root.taken)
	// A subsequent immediate Wire dispatch ends this full carrier. Its failure
	// cannot make the earlier admitted call prove that it never left.
	if err := dispatch.Emit(context.Background(), destination.Wire(), []string{"overflow"}, nil); err != nil {
		t.Fatal(err)
	}
	earlier := receive(t, accepted)
	wantUnpublished(t, earlier, false)
	receive(t, destination.Done())
	if !errors.Is(destination.Err(), core.ErrBackpressure) {
		t.Fatalf("full carrier ended with %v", destination.Err())
	}
	// A call the peer's end cut off is disconnected, not withdrawn, although
	// its context derives from the peer's. Nightseam v0.6.0's root answered
	// cancelled here.
	var public *core.PublicError
	if !errors.As(earlier, &public) || public.Code != "disconnected" {
		t.Fatalf("earlier call = %v", earlier)
	}
}
