package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	core "github.com/Bitspark/bitruntime/core/go"
	dispatch "github.com/Bitspark/bitruntime/dispatch/go"
	engine "github.com/Bitspark/bitruntime/engine/go"
	transports "github.com/Bitspark/bitruntime/transports/go"
	wire "github.com/Bitspark/bitwire/wire/go"
)

// TestAFailedPrepareReleasesWhatItAttached: a receiver Prepare attached to the
// root is told the peer ended even when Prepare fails, as in v0.6.0, where
// selecting the root inside Prepare started it.
func TestAFailedPrepareReleasesWhatItAttached(t *testing.T) {
	a, b := transports.Pipe(1 << 20)
	defer a.Abort()
	closed := make(chan struct{})
	refusal := errors.New("prepare failed")
	_, err := engine.NewPeer(context.Background(), b, engine.ServerRole, engine.Options{Prepare: func(p *engine.Peer) error {
		if _, err := p.Wire().Receive(wire.Receiver{Closed: func(wire.Code, string) { close(closed) }}); err != nil {
			return err
		}
		return refusal
	}})
	if !errors.Is(err, refusal) {
		t.Fatalf("NewPeer returned %v", err)
	}
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("the receiver Prepare attached was never told the peer ended")
	}
}

// TestACallCutOffByThePeersContextIsDisconnected: a peer ends when the context
// it was made with ends. A call its root had handed to the peer is then
// disconnected, not withdrawn.
func TestACallCutOffByThePeersContextIsDisconnected(t *testing.T) {
	for range 20 {
		a, b := transports.Pipe(1 << 20)
		started := make(chan struct{})
		server, err := engine.NewPeer(context.Background(), b, engine.ServerRole, engine.Options{Prepare: func(p *engine.Peer) error {
			d, err := dispatch.NewDispatcher(p.Wire())
			if err != nil {
				return err
			}
			_, err = dispatch.Handle(d, []string{"wait"}, func(ctx context.Context, _ json.RawMessage) (any, error) {
				close(started)
				<-ctx.Done()
				return nil, ctx.Err()
			})
			return err
		}})
		if err != nil {
			t.Fatal(err)
		}
		made, end := context.WithCancel(context.Background())
		client, err := engine.NewPeer(made, a, engine.ClientRole, engine.Options{})
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- dispatch.Call(context.Background(), client.Wire(), []string{"wait"}, nil, nil) }()
		<-started
		end()
		var public *core.PublicError
		if err := <-done; !errors.As(err, &public) || public.Code != "disconnected" {
			t.Fatalf("the call cut off by the peer's context returned %v", err)
		}
		_ = server.Close()
	}
}

// TestAnInvalidPathIsErrInvalidPath: every addressed carrier classifies a path
// segment outside Unicode-scalar text the same way.
func TestAnInvalidPathIsErrInvalidPath(t *testing.T) {
	left, right, err := core.NewPair(core.PairOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer left.Close(transports.CodeNormal, "")
	a, b := transports.Pipe(1 << 20)
	peer, err := engine.NewPeer(context.Background(), a, engine.ClientRole, engine.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	defer b.Abort()
	event := wire.Message{Frame: wire.ProfileFrame{Version: 1, Kind: wire.ProfileEvent, Data: json.RawMessage("1")}}
	for _, access := range []wire.AddressedWire{left, right, peer.Wire()} {
		if err := access.Send([]string{"\xff"}, event); !errors.Is(err, core.ErrInvalidPath) {
			t.Fatalf("%T: %v", access, err)
		}
	}
}
