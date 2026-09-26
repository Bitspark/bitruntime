package engine_test

// Ported from Nightseam v0.6.0 runtime/go/backpressure_test.go
// (5cc9723a24646c40ed1861f892b2b23eb6d785d7). v0.6.0 held a peer's writes on a
// delayed TCP connection beneath a WebSocket; here a gate on the pipe's Send
// holds them at the seam, which is where the peer's writer meets its
// transport either way. Producers go through the root with the dispatch
// helpers instead of the removed raw Emit and Call.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/Bitspark/bitruntime/core/go"
	engine "github.com/Bitspark/bitruntime/engine/go"
	transports "github.com/Bitspark/bitruntime/transports/go"
)

// writeGate holds a connection's writes, once enabled, until its gate opens,
// so a producer/transport imbalance is reproducible without depending on how
// much a transport buffers.
type writeGate struct {
	enabled atomic.Bool
	started chan struct{}
	gate    <-chan struct{}
	once    sync.Once
}

func newWriteGate(gate <-chan struct{}) *writeGate {
	return &writeGate{started: make(chan struct{}), gate: gate}
}

func (g *writeGate) wrap(conn transports.Conn) transports.Conn {
	return &gatedConn{Conn: conn, control: g}
}

type gatedConn struct {
	transports.Conn
	control *writeGate
}

func (c *gatedConn) Send(ctx context.Context, frame transports.Frame) error {
	if c.control.enabled.Load() {
		c.control.once.Do(func() { close(c.control.started) })
		if c.control.gate != nil {
			select {
			case <-c.control.gate:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return c.Conn.Send(ctx, frame)
}

// gatedPair is newPair with the server's writes held by control from the
// start, as v0.6.0 enabled its delay once the upgrade was flushed, and the
// client's by clientControl once the test enables it.
func gatedPair(t *testing.T, control *writeGate, serverOptions, clientOptions engine.Options, clientControl ...*writeGate) (client, server *engine.Peer) {
	t.Helper()
	var wrapServer, wrapClient func(transports.Conn) transports.Conn
	if control != nil {
		control.enabled.Store(true)
		wrapServer = control.wrap
	}
	if len(clientControl) != 0 {
		wrapClient = clientControl[0].wrap
	}
	return connectPair(t, serverOptions, clientOptions, wrapServer, wrapClient)
}

func TestOutboundQueueDeliversAcceptedPrefixInOrder(t *testing.T) {
	for _, capacity := range []int{2, 8} {
		t.Run(fmt.Sprintf("capacity_%d", capacity), func(t *testing.T) {
			release := make(chan struct{})
			var releaseOnce sync.Once
			allowWrites := func() { releaseOnce.Do(func() { close(release) }) }
			defer allowWrites()
			received := make(chan int, capacity+1)
			control := newWriteGate(release)
			client, server := gatedPair(t, control, serving{Handlers: map[string]handler{
				"echo": func(_ context.Context, _ *engine.Peer, data json.RawMessage) (any, error) {
					return data, nil
				},
			}}.with(engine.Options{QueueCapacity: capacity, WriteTimeout: 5 * time.Second}), serving{Events: map[string]eventHandler{
				"replay.item": func(_ context.Context, _ *engine.Peer, data json.RawMessage) {
					var sequence int
					if err := json.Unmarshal(data, &sequence); err != nil {
						t.Error(err)
						sequence = -1
					}
					received <- sequence
				},
			}}.with(engine.Options{}))
			if err := emit(context.Background(), server, "replay.item", 0); err != nil {
				t.Fatal(err)
			}
			// Hold the transport's current write, then fill precisely the bounded
			// handoff. Each successful emit has been accepted without a reader.
			receive(t, control.started)
			for sequence := 1; sequence <= capacity; sequence++ {
				if err := emit(context.Background(), server, "replay.item", sequence); err != nil {
					t.Fatalf("accepted prefix item %d: %v", sequence, err)
				}
			}
			allowWrites()
			for want := range capacity + 1 {
				if got := receive(t, received); got != want {
					t.Fatalf("accepted sequence = %d, want %d", got, want)
				}
			}
			var echo string
			if err := call(context.Background(), client, "echo", "still connected", &echo); err != nil || echo != "still connected" {
				t.Fatalf("echo after accepted prefix = %q, error=%v", echo, err)
			}
			if client.Err() != nil || server.Err() != nil {
				t.Fatalf("accepted prefix disconnected peers: client=%v server=%v", client.Err(), server.Err())
			}
		})
	}
}

func TestOutboundQueuePreCancelledSendDoesNotDisconnect(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	allowWrites := func() { releaseOnce.Do(func() { close(release) }) }
	defer allowWrites()
	control := newWriteGate(release)
	received := make(chan int, 4)
	client, server := gatedPair(t, control, serving{Handlers: map[string]handler{
		"echo": func(_ context.Context, _ *engine.Peer, data json.RawMessage) (any, error) { return data, nil },
	}}.with(engine.Options{QueueCapacity: 2, WriteTimeout: 5 * time.Second}), serving{Events: map[string]eventHandler{
		"progress": func(_ context.Context, _ *engine.Peer, data json.RawMessage) {
			var value int
			if err := json.Unmarshal(data, &value); err != nil {
				received <- -1
				return
			}
			received <- value
		},
	}}.with(engine.Options{}))
	if err := emit(context.Background(), server, "progress", 0); err != nil {
		t.Fatal(err)
	}
	// Hold the first real write until the cancellation assertion is complete. This
	// guarantees the two queued frames cannot drain, even on a heavily loaded host.
	receive(t, control.started)
	for _, value := range []int{1, 2} {
		if err := emit(context.Background(), server, "progress", value); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := emit(ctx, server, "progress", 999)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-cancelled send = %v, want context canceled", err)
	}
	if client.Err() != nil || server.Err() != nil {
		t.Fatalf("unadmitted cancellation disconnected peers: client=%v server=%v", client.Err(), server.Err())
	}
	allowWrites()
	for want := range 3 {
		if got := receive(t, received); got != want {
			t.Fatalf("queued event = %d, want %d", got, want)
		}
	}
	if err := emit(context.Background(), server, "progress", 3); err != nil {
		t.Fatal(err)
	}
	if got := receive(t, received); got != 3 {
		t.Fatalf("cancelled event was published: received %d before marker 3", got)
	}
	var echo string
	if err := call(context.Background(), client, "echo", "alive", &echo); err != nil || echo != "alive" {
		t.Fatalf("echo after cancelled enqueue = %q, error=%v", echo, err)
	}
}

func TestOutboundQueueDoesNotDelayCancellationOfSentCall(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	allowWrites := func() { releaseOnce.Do(func() { close(release) }) }
	defer allowWrites()
	clientControl := newWriteGate(release)
	handlerStarted := make(chan struct{})
	client, server := gatedPair(t, nil, serving{Handlers: map[string]handler{
		"wait": func(ctx context.Context, _ *engine.Peer, _ json.RawMessage) (any, error) {
			close(handlerStarted)
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}}.with(engine.Options{}), engine.Options{QueueCapacity: 2, WriteTimeout: 5 * time.Second}, clientControl)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	returned := make(chan error, 1)
	go func() { returned <- call(ctx, client, "wait", nil, nil) }()
	// Only introduce congestion after the request has reached its handler.
	receive(t, handlerStarted)
	clientControl.enabled.Store(true)
	if err := emit(context.Background(), client, "progress", 0); err != nil {
		t.Fatal(err)
	}
	receive(t, clientControl.started)
	for _, value := range []int{1, 2} {
		if err := emit(context.Background(), client, "progress", value); err != nil {
			t.Fatal(err)
		}
	}
	cancel()
	select {
	case err := <-returned:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("sent call cancellation = %v, want context canceled", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("caller cancellation waited for space to enqueue the best-effort cancellation frame")
	}
	if client.Err() != nil || server.Err() != nil {
		t.Fatalf("call cancellation disconnected peers: client=%v server=%v", client.Err(), server.Err())
	}
	// Delivery of the remote cancellation is intentionally not required when the
	// output queue is full. Connection cleanup releases the waiting handler.
	allowWrites()
}

// TestInboundEventBurstIsPacedRatherThanDisconnected: a queue filled faster
// than its consumer drains it is a burst, not a stall, and the peer pacing it
// is what tells them apart — the events are delivered, in order, and the
// connection is whole.
func TestInboundEventBurstIsPacedRatherThanDisconnected(t *testing.T) {
	release := make(chan struct{})
	delivered := make(chan int, 8)
	client, server := newPair(t, engine.Options{}, serving{Events: map[string]eventHandler{
		"progress": func(_ context.Context, _ *engine.Peer, data json.RawMessage) {
			<-release
			var value int
			if err := json.Unmarshal(data, &value); err != nil {
				t.Error(err)
				return
			}
			delivered <- value
		},
	}}.with(engine.Options{QueueCapacity: 1, WriteTimeout: 5 * time.Second}))
	for _, value := range []int{1, 2, 3} {
		if err := emit(context.Background(), server, "progress", value); err != nil {
			t.Fatal(err)
		}
	}
	// The burst is held while the consumer is busy and drains when it is not.
	close(release)
	for want := 1; want <= 3; want++ {
		if got := receive(t, delivered); got != want {
			t.Fatalf("event %d arrived where %d was due", got, want)
		}
	}
	select {
	case <-client.Done():
		t.Fatalf("a burst that drained ended the connection: %v", client.Err())
	default:
	}
}

// TestOutstandingCallLimitRefusesWithoutEndingTheConnection: the caller's own
// bound. The call past it is refused busy where it stands — no frame — and the
// connection serves the next call, which is what makes it a refusal and not a
// failure.
func TestOutstandingCallLimitRefusesWithoutEndingTheConnection(t *testing.T) {
	started := make(chan struct{}, 4)
	client, _ := newPair(t, serving{Handlers: map[string]handler{
		"wait": func(ctx context.Context, _ *engine.Peer, _ json.RawMessage) (any, error) {
			started <- struct{}{}
			<-ctx.Done()
			return nil, ctx.Err()
		},
		"echo": func(_ context.Context, _ *engine.Peer, params json.RawMessage) (any, error) { return params, nil },
	}}.with(engine.Options{}), engine.Options{MaxPendingRequests: 2})

	ctx, cancel := context.WithCancel(context.Background())
	var waiting sync.WaitGroup
	for range 2 {
		waiting.Add(1)
		go func() { defer waiting.Done(); _ = call(ctx, client, "wait", nil, nil) }()
	}
	receive(t, started)
	receive(t, started)

	err := call(context.Background(), client, "wait", nil, nil)
	var public *core.PublicError
	if !errors.As(err, &public) || public.Code != "busy" {
		t.Fatalf("the call past the limit ended with %v, not busy", err)
	}

	cancel()
	waiting.Wait()
	// A withdrawn call returns once its cancellation is queued in the root, and
	// keeps its place under the bound until that cancellation drains; v0.6.0's
	// raw Call freed it before returning, its root did not. The next call is
	// therefore allowed to meet busy while the two drain, and nothing else.
	deadline := time.Now().Add(5 * time.Second)
	for {
		var echoed int
		err := call(context.Background(), client, "echo", 7, &echoed)
		if errors.As(err, &public) && public.Code == "busy" && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		if err != nil || echoed != 7 {
			t.Fatalf("the connection did not serve on: %v, %d", err, echoed)
		}
		break
	}
	if client.Err() != nil {
		t.Fatalf("the refusal ended the connection: %v", client.Err())
	}
}
