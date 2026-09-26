package engine_test

// Ported from Nightseam v0.6.0 runtime/go/prepare_test.go
// (5cc9723a24646c40ed1861f892b2b23eb6d785d7). The tests of Accept and Dial
// live beside them in engine/websocket/go.

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	dispatch "github.com/Bitspark/bitruntime/dispatch/go"
	engine "github.com/Bitspark/bitruntime/engine/go"
	transports "github.com/Bitspark/bitruntime/transports/go"
)

// A Prepare that fails fails the construction: nothing is returned that could
// read a frame.
func TestPrepareFailingFailsNewPeer(t *testing.T) {
	refusal := errors.New("this peer serves nothing")
	near, far := transports.Pipe(1 << 20)
	defer far.Abort()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	peer, err := engine.NewPeer(ctx, near, engine.ClientRole, engine.Options{Prepare: func(*engine.Peer) error { return refusal }})
	if !errors.Is(err, refusal) || peer != nil {
		t.Fatalf("NewPeer answered peer=%v error=%v", peer, err)
	}
}

// A receiver attached in Prepare is there before the peer has read anything:
// a request already waiting on the connection when the peer is made meets it
// rather than method_not_found, however long the attachment takes. v0.6.0
// held this with a tunnel installed in Prepare; tunnels are not ported, and a
// dispatcher at the root is what a peer serves through here. The WebSocket
// form, many dials over, is in engine/websocket/go.
func TestAReceiverAttachedInPrepareMeetsTheFirstRequest(t *testing.T) {
	near, far := transports.Pipe(1 << 20)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	raw := &rawSide{t: t, ctx: ctx, conn: far}
	defer far.Abort()
	raw.write(`{"version":1,"kind":"request","id":"c:1","method":"5:probe","params":{}}`)
	peer, err := engine.NewPeer(ctx, near, engine.ServerRole, engine.Options{Prepare: func(peer *engine.Peer) error {
		time.Sleep(10 * time.Millisecond)
		d, err := dispatch.NewDispatcher(peer.Wire())
		if err != nil {
			return err
		}
		_, err = dispatch.Handle(d, []string{"probe"}, func(context.Context, json.RawMessage) (any, error) {
			return map[string]any{"ready": true}, nil
		})
		return err
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	if response := raw.read(); string(response["id"]) != `"c:1"` || string(response["result"]) != `{"ready":true}` {
		t.Fatalf("the first request was answered %v", response)
	}
}
