package dispatch_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	core "github.com/Bitspark/bitruntime/core/go"
	dispatch "github.com/Bitspark/bitruntime/dispatch/go"
	engine "github.com/Bitspark/bitruntime/engine/go"
)

// waiting serves ["wait"] with a handler that answers only when cancelled and
// reports that cancellation.
func waiting(t *testing.T, peer *engine.Peer) <-chan struct{} {
	t.Helper()
	cancelled := make(chan struct{}, 1)
	if _, err := dispatch.Handle(testBinding(t, peer.Wire()), []string{"wait"}, func(ctx context.Context, _ json.RawMessage) (any, error) {
		<-ctx.Done()
		cancelled <- struct{}{}
		return nil, ctx.Err()
	}); err != nil {
		t.Fatal(err)
	}
	return cancelled
}

// A peer's deadline is its caller's (bitwire/1): an application's own call
// fails locally with the deadline that passed, never with a public error, and
// the remote handler is cancelled (bitruntime#23).
func TestAPeersDeadlineFailsItsOwnCallerLocally(t *testing.T) {
	client, server := newPair(t, engine.Options{}, engine.Options{RequestTimeout: 200 * time.Millisecond})
	cancelled := waiting(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := dispatch.Call(ctx, client.Wire(), []string{"wait"}, nil, nil)
	var public *core.PublicError
	if !errors.Is(err, context.DeadlineExceeded) || errors.As(err, &public) {
		t.Fatalf("the call past its peer's deadline returned %v", err)
	}
	if ctx.Err() != nil {
		t.Fatal("the caller's own deadline passed instead of the peer's")
	}
	receive(t, cancelled)
}

// A forwarded request whose next peer's deadline passes is answered cancelled
// across the wire; request_timeout is a caller's own error and never a frame.
func TestAForwardedCallsDeadlineIsCancelledOnTheWire(t *testing.T) {
	caller, forwarder := newPair(t, engine.Options{}, engine.Options{})
	onward, handler := newPair(t, engine.Options{}, engine.Options{RequestTimeout: 200 * time.Millisecond})
	cancelled := waiting(t, handler)
	stop, err := core.Forward(forwarder.Wire(), onward.Wire())
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = dispatch.Call(ctx, caller.Wire(), []string{"wait"}, nil, nil)
	var public *core.PublicError
	if !errors.As(err, &public) || public.Code != "cancelled" {
		t.Fatalf("the forwarded call past the onward peer's deadline returned %v", err)
	}
	receive(t, cancelled)
}
