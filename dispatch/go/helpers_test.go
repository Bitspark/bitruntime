package dispatch_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	core "github.com/Bitspark/bitruntime/core/go"
	dispatch "github.com/Bitspark/bitruntime/dispatch/go"
	engine "github.com/Bitspark/bitruntime/engine/go"
	enginews "github.com/Bitspark/bitruntime/engine/websocket/go"
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

// newPair connects a client peer to a server peer over a WebSocket and returns
// them in that order. The server takes the first options.
func newPair(t *testing.T, serverOptions, clientOptions engine.Options) (*engine.Peer, *engine.Peer) {
	t.Helper()
	connected := make(chan *engine.Peer, 1)
	handler, err := enginews.NewHandler(enginews.ServerOptions{
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
	client, _, err := enginews.Dial(ctx, server.URL, enginews.DialOptions{Options: clientOptions})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	remote := receive(t, connected)
	t.Cleanup(func() { _ = remote.Close() })
	return client, remote
}

// sentFrames records what a connection's writer hands its transport, in that
// order, as "kind name". It stands where nightseam v0.6.0's tests read the
// observer's FrameSent events, which bitruntime does not have. A frame is
// recorded before it is sent, so whatever the far side has received is
// already recorded.
type sentFrames struct {
	transports.Conn
	mu   sync.Mutex
	sent []string
}

func (c *sentFrames) Send(ctx context.Context, frame transports.Frame) error {
	var f struct {
		Kind   string `json:"kind"`
		Method string `json:"method"`
		Event  string `json:"event"`
	}
	_ = json.Unmarshal(frame.Data, &f)
	c.record(f.Kind + " " + f.Method + f.Event)
	return c.Conn.Send(ctx, frame)
}

// Close records the close this side decided on as "close code reason".
func (c *sentFrames) Close(ctx context.Context, code transports.Code, reason string) error {
	c.record(fmt.Sprintf("close %d %s", code, reason))
	return c.Conn.Close(ctx, code, reason)
}

func (c *sentFrames) record(line string) {
	c.mu.Lock()
	c.sent = append(c.sent, line)
	c.mu.Unlock()
}

// await returns the recorded lines of the given kinds once there are count of
// them, or those there are after five seconds.
func (c *sentFrames) await(t *testing.T, count int, kinds ...string) []string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var got []string
		c.mu.Lock()
		for _, line := range c.sent {
			for _, kind := range kinds {
				if strings.HasPrefix(line, kind+" ") {
					got = append(got, line)
				}
			}
		}
		c.mu.Unlock()
		if len(got) >= count || time.Now().After(deadline) {
			return got
		}
		time.Sleep(time.Millisecond)
	}
}

// recordedPair connects a client peer to a server peer over a pipe, recording
// what each sends, and returns the client, the server and their records.
func recordedPair(t *testing.T, serverOptions, clientOptions engine.Options) (*engine.Peer, *engine.Peer, *sentFrames, *sentFrames) {
	t.Helper()
	near, far := transports.Pipe(1 << 20)
	sent, served := &sentFrames{Conn: near}, &sentFrames{Conn: far}
	server, err := engine.NewPeer(context.Background(), served, engine.ServerRole, serverOptions)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	client, err := engine.NewPeer(context.Background(), sent, engine.ClientRole, clientOptions)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client, server, sent, served
}

// rawClient is a server peer reached over a connection the test speaks
// bitwire/1 on byte for byte, as the removed raw method-name API once let a
// test choose a frame's method name.
type rawClient struct {
	conn transports.Conn
	next int
}

func newRawClient(t *testing.T, serverOptions engine.Options) (*engine.Peer, *rawClient) {
	t.Helper()
	near, far := transports.Pipe(1 << 20)
	server, err := engine.NewPeer(context.Background(), far, engine.ServerRole, serverOptions)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close(); _ = near.Abort() })
	return server, &rawClient{conn: near}
}

// call sends one request under the given method name and returns its response.
func (c *rawClient) call(t *testing.T, method string) wire.ProfileFrame {
	t.Helper()
	c.next++
	id := fmt.Sprintf("c:%d", c.next)
	request, err := json.Marshal(map[string]any{"version": 1, "kind": "request", "id": id, "method": method, "params": map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.conn.Send(ctx, transports.Frame{Kind: transports.Text, Data: request}); err != nil {
		t.Fatal(err)
	}
	received, err := c.conn.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var response wire.ProfileFrame
	if err := json.Unmarshal(received.Data, &response); err != nil {
		t.Fatal(err)
	}
	if response.Kind != wire.ProfileResponse || response.ID != id {
		t.Fatalf("raw %q answered %+v", method, response)
	}
	return response
}

func testBinding(t *testing.T, endpoint wire.Endpoint) *dispatch.Dispatcher {
	t.Helper()
	binding, err := dispatch.NewDispatcher(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = binding.Close(transports.CodeNormal, "done") })
	return binding
}

type wireReplySink struct{ replies chan wire.ProfileFrame }

// A return capability refuses what it does not implement, as every addressed
// receiver in this profile does; this one carries outcomes and nothing else.
func (s *wireReplySink) Send(path []string, message wire.Message) error {
	if len(path) != 0 {
		return errors.New("this return capability carries outcomes only")
	}
	s.replies <- message.Frame
	return nil
}

func wantUnpublished(t *testing.T, err error, want bool) {
	t.Helper()
	var unpublished *core.UnpublishedError
	if got := errors.As(err, &unpublished); got != want || err == nil {
		t.Fatalf("publication proof: %v, want unpublished=%v", err, want)
	}
}

// invocationEndpoint is an endpoint written against the public contract alone:
// Receive holds one attachment and deliver hands it a message exactly as it
// arrived. It is the part of nightseam v0.6.0's invocation fixture the
// dispatcher tests use.
type invocationEndpoint struct {
	mu       sync.Mutex
	receiver *wire.Receiver
	closed   bool
}

func newInvocationEndpoint() *invocationEndpoint { return &invocationEndpoint{} }

// Send loops back into this endpoint's own attachment, asynchronously.
func (e *invocationEndpoint) Send(path []string, message wire.Message) error {
	go e.deliver(path, message)
	return nil
}

func (e *invocationEndpoint) Receive(receiver wire.Receiver) (func(), error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, transports.ErrClosed
	}
	if e.receiver != nil {
		return nil, core.ErrReceiverExists
	}
	held := receiver
	e.receiver = &held
	return func() {
		e.mu.Lock()
		if e.receiver == &held {
			e.receiver = nil
		}
		e.mu.Unlock()
	}, nil
}

func (e *invocationEndpoint) Close(code wire.Code, reason string) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	receiver := e.receiver
	e.receiver = nil
	e.mu.Unlock()
	if receiver != nil && receiver.Closed != nil {
		receiver.Closed(code, reason)
	}
	return nil
}

func (e *invocationEndpoint) deliver(path []string, message wire.Message) {
	e.mu.Lock()
	receiver := e.receiver
	e.mu.Unlock()
	if receiver != nil && receiver.Message != nil {
		receiver.Message(path, message)
	}
}
