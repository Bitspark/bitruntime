package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ws "github.com/Bitspark/nightseam/runtime/go"
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

func delayedWritePair(t *testing.T, control *delayedWriteControl, serverOptions, clientOptions ws.Options, clientWriteControl ...*delayedWriteControl) (*ws.Peer, *ws.Peer) {
	t.Helper()
	connected := make(chan *ws.Peer, 1)
	handler, err := ws.NewHandler(ws.ServerOptions{
		Options:      serverOptions,
		Authenticate: func(r *http.Request) (context.Context, error) { return r.Context(), nil },
		CheckOrigin:  func(*http.Request) bool { return true },
		OnConnect: func(peer *ws.Peer) {
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
	dialOptions := ws.DialOptions{Options: clientOptions}
	if len(clientWriteControl) != 0 {
		transport := &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			return &delayedWriteConn{Conn: conn, control: clientWriteControl[0]}, nil
		}}
		t.Cleanup(transport.CloseIdleConnections)
		dialOptions.HTTPClient = &http.Client{Transport: transport}
	}
	client, _, err := ws.Dial(ctx, server.URL, dialOptions)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	remote := receive(t, connected)
	t.Cleanup(func() { _ = remote.Close() })
	return client, remote
}

func TestOutboundQueueDeliversAcceptedPrefixInOrder(t *testing.T) {
	for _, capacity := range []int{2, 8} {
		t.Run(fmt.Sprintf("capacity_%d", capacity), func(t *testing.T) {
			release := make(chan struct{})
			var releaseOnce sync.Once
			allowWrites := func() { releaseOnce.Do(func() { close(release) }) }
			defer allowWrites()
			received := make(chan int, capacity+1)
			control := &delayedWriteControl{started: make(chan struct{}), gate: release}
			client, server := delayedWritePair(t, control, ws.Options{
				QueueCapacity: capacity,
				WriteTimeout:  5 * time.Second,
				Handlers: map[string]ws.Handler{
					"echo": func(_ context.Context, _ *ws.Peer, data json.RawMessage) (any, error) {
						return data, nil
					},
				},
			}, ws.Options{Events: map[string]ws.EventHandler{
				"replay.item": func(_ context.Context, _ *ws.Peer, data json.RawMessage) {
					var sequence int
					if err := json.Unmarshal(data, &sequence); err != nil {
						t.Error(err)
						sequence = -1
					}
					received <- sequence
				},
			}})
			if err := server.Emit(context.Background(), "replay.item", 0); err != nil {
				t.Fatal(err)
			}
			// Hold the transport's current write, then fill precisely the bounded
			// handoff. Each successful emit has been accepted without a reader.
			receive(t, control.started)
			for sequence := 1; sequence <= capacity; sequence++ {
				if err := server.Emit(context.Background(), "replay.item", sequence); err != nil {
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
			if err := client.Call(context.Background(), "echo", "still connected", &echo); err != nil || echo != "still connected" {
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
	control := &delayedWriteControl{started: make(chan struct{}), gate: release}
	received := make(chan int, 4)
	client, server := delayedWritePair(t, control, ws.Options{
		QueueCapacity: 2,
		WriteTimeout:  5 * time.Second,
		Handlers: map[string]ws.Handler{
			"echo": func(_ context.Context, _ *ws.Peer, data json.RawMessage) (any, error) { return data, nil },
		},
	}, ws.Options{Events: map[string]ws.EventHandler{
		"progress": func(_ context.Context, _ *ws.Peer, data json.RawMessage) {
			var value int
			if err := json.Unmarshal(data, &value); err != nil {
				received <- -1
				return
			}
			received <- value
		},
	}})
	if err := server.Emit(context.Background(), "progress", 0); err != nil {
		t.Fatal(err)
	}
	// Hold the first real write until the cancellation assertion is complete. This
	// guarantees the two queued frames cannot drain, even on a heavily loaded host.
	receive(t, control.started)
	for _, value := range []int{1, 2} {
		if err := server.Emit(context.Background(), "progress", value); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := server.Emit(ctx, "progress", 999)
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
	if err := server.Emit(context.Background(), "progress", 3); err != nil {
		t.Fatal(err)
	}
	if got := receive(t, received); got != 3 {
		t.Fatalf("cancelled event was published: received %d before marker 3", got)
	}
	var echo string
	if err := client.Call(context.Background(), "echo", "alive", &echo); err != nil || echo != "alive" {
		t.Fatalf("echo after cancelled enqueue = %q, error=%v", echo, err)
	}
}

func TestOutboundQueueDoesNotDelayCancellationOfSentCall(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	allowWrites := func() { releaseOnce.Do(func() { close(release) }) }
	defer allowWrites()
	serverControl := &delayedWriteControl{started: make(chan struct{})}
	clientControl := &delayedWriteControl{started: make(chan struct{}), gate: release}
	handlerStarted := make(chan struct{})
	client, server := delayedWritePair(t, serverControl, ws.Options{Handlers: map[string]ws.Handler{
		"wait": func(ctx context.Context, _ *ws.Peer, _ json.RawMessage) (any, error) {
			close(handlerStarted)
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}}, ws.Options{QueueCapacity: 2, WriteTimeout: 5 * time.Second}, clientControl)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	returned := make(chan error, 1)
	go func() { returned <- client.Call(ctx, "wait", nil, nil) }()
	// Only introduce congestion after the request has reached its handler.
	receive(t, handlerStarted)
	clientControl.enabled.Store(true)
	if err := client.Emit(context.Background(), "progress", 0); err != nil {
		t.Fatal(err)
	}
	receive(t, clientControl.started)
	for _, value := range []int{1, 2} {
		if err := client.Emit(context.Background(), "progress", value); err != nil {
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
// connection is whole. Before the queue was paced this ended the connection
// on the third event.
func TestInboundEventBurstIsPacedRatherThanDisconnected(t *testing.T) {
	release := make(chan struct{})
	delivered := make(chan int, 8)
	client, server := newPair(t, ws.Options{}, ws.Options{
		QueueCapacity: 1,
		WriteTimeout:  5 * time.Second,
		Events: map[string]ws.EventHandler{
			"progress": func(_ context.Context, _ *ws.Peer, data json.RawMessage) {
				<-release
				var value int
				if err := json.Unmarshal(data, &value); err != nil {
					t.Error(err)
					return
				}
				delivered <- value
			},
		},
	})
	for _, value := range []int{1, 2, 3} {
		if err := server.Emit(context.Background(), "progress", value); err != nil {
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
// bound. The call past it is refused busy where it stands — no frame, no
// request an observer is told of — and the connection serves the next call,
// which is what makes it a refusal and not a failure.
func TestOutstandingCallLimitRefusesWithoutEndingTheConnection(t *testing.T) {
	started := make(chan struct{}, 4)
	client, _ := newPair(t, ws.Options{Handlers: map[string]ws.Handler{
		"wait": func(ctx context.Context, _ *ws.Peer, _ json.RawMessage) (any, error) {
			started <- struct{}{}
			<-ctx.Done()
			return nil, ctx.Err()
		},
		"echo": func(_ context.Context, _ *ws.Peer, params json.RawMessage) (any, error) { return params, nil },
	}}, ws.Options{MaxPendingRequests: 2})

	ctx, cancel := context.WithCancel(context.Background())
	var waiting sync.WaitGroup
	for range 2 {
		waiting.Add(1)
		go func() { defer waiting.Done(); _ = client.Call(ctx, "wait", nil, nil) }()
	}
	receive(t, started)
	receive(t, started)

	err := client.Call(context.Background(), "wait", nil, nil)
	var public *ws.PublicError
	if !errors.As(err, &public) || public.Code != "busy" {
		t.Fatalf("the call past the limit ended with %v, not busy", err)
	}

	cancel()
	waiting.Wait()
	var echoed int
	if err := client.Call(context.Background(), "echo", 7, &echoed); err != nil || echoed != 7 {
		t.Fatalf("the connection did not serve on: %v, %d", err, echoed)
	}
}
