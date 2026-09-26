package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Bitspark/nightseam/duplex/go"
	ws "github.com/Bitspark/nightseam/runtime/go"
)

func wantUnpublished(t *testing.T, err error, want bool) {
	t.Helper()
	var unpublished *ws.UnpublishedError
	if got := errors.As(err, &unpublished); got != want || err == nil {
		t.Fatalf("publication proof: %v, want unpublished=%v", err, want)
	}
}

func TestUnpublishedProofIsLocalToTheSendAttempt(t *testing.T) {
	entered, finish := make(chan struct{}, 1), make(chan struct{})
	client, server := newPair(t, ws.Options{Handlers: map[string]ws.Handler{
		"wait": func(c context.Context, _ *ws.Peer, _ json.RawMessage) (any, error) {
			entered <- struct{}{}
			select {
			case <-finish:
				return nil, nil
			case <-c.Done():
				return nil, c.Err()
			}
		},
		"busy": func(context.Context, *ws.Peer, json.RawMessage) (any, error) {
			return nil, &ws.PublicError{Code: "busy", Message: "retained before refusing"}
		},
		"nested": func(c context.Context, peer *ws.Peer, _ json.RawMessage) (any, error) {
			withdrawn, cancel := context.WithCancel(c)
			cancel()
			return nil, peer.Call(withdrawn, "never.sent", nil, nil)
		},
	}}, ws.Options{MaxPendingRequests: 1, MaxFrameBytes: 512})
	_ = server
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := client.Call(ctx, "wait", nil, nil)
	wantUnpublished(t, err, true)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("wrapping lost cancellation identity: %v", err)
	}
	wantUnpublished(t, client.Call(context.Background(), "wait", make(chan int), nil), true)
	wantUnpublished(t, client.Call(context.Background(), "wait", strings.Repeat("x", 1024), nil), true)
	wantUnpublished(t, client.Emit(context.Background(), "event", make(chan int)), true)
	wantUnpublished(t, client.Emit(context.Background(), "event", strings.Repeat("x", 1024)), true)

	done := make(chan error, 1)
	go func() { done <- client.Call(context.Background(), "wait", nil, nil) }()
	receive(t, entered)
	err = client.Call(context.Background(), "busy", nil, nil)
	wantUnpublished(t, err, true)
	var public *ws.PublicError
	if !errors.As(err, &public) || public.Code != "busy" {
		t.Fatalf("wrapping lost public refusal: %v", err)
	}
	close(finish)
	if err := receive(t, done); err != nil {
		t.Fatal(err)
	}

	// The same public code received from the other side carries no proof.
	err = client.Call(context.Background(), "busy", nil, nil)
	wantUnpublished(t, err, false)
	if !errors.As(err, &public) || public.Code != "busy" {
		t.Fatal(err)
	}
	// A marker from a nested, definitely-unsent call must not cross the wire
	// as proof about the request whose implementation has already run.
	err = client.Call(context.Background(), "nested", nil, nil)
	wantUnpublished(t, err, false)
	if !errors.As(err, &public) || public.Code != "cancelled" {
		t.Fatal(err)
	}
}

type publicationWriteFailure struct {
	duplex.Conn
	wrote chan struct{}
	err   error
}

func (c *publicationWriteFailure) Send(context.Context, duplex.Frame) error {
	close(c.wrote)
	return c.err
}

func TestQueuedWriteFailureHasNoUnpublishedProof(t *testing.T) {
	client, _ := newPair(t, ws.Options{}, ws.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	proof := client.Call(ctx, "unsent", nil, nil)
	wantUnpublished(t, proof, true)
	for _, tc := range []struct {
		name            string
		cause, identity error
	}{
		{"plain", errors.New("transport failed after accepting a queued frame"), nil},
		{"nested", fmt.Errorf("adapter send: %w", proof), context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			near, far := duplex.Pipe(1 << 20)
			defer far.Abort()
			cause := tc.cause
			connection := &publicationWriteFailure{Conn: near, wrote: make(chan struct{}), err: cause}
			peer, err := ws.NewPeer(context.Background(), connection, ws.ClientRole, ws.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer peer.Close()
			err = peer.Call(context.Background(), "supply", nil, nil)
			receive(t, connection.wrote)
			wantUnpublished(t, err, false)
			if !errors.Is(err, cause) {
				t.Fatalf("transport cause lost: %v", err)
			}
			if tc.identity != nil && !errors.Is(err, tc.identity) {
				t.Fatalf("nested cause lost: %v", err)
			}
		})
	}
}

type publicationReadFailure struct {
	duplex.Conn
	wrote chan struct{}
	err   error
}

func (c *publicationReadFailure) Send(context.Context, duplex.Frame) error {
	close(c.wrote)
	return nil
}
func (c *publicationReadFailure) Receive(context.Context) (duplex.Frame, error) {
	<-c.wrote
	return duplex.Frame{}, c.err
}

func TestReadFailureHasNoNestedUnpublishedProof(t *testing.T) {
	client, _ := newPair(t, ws.Options{}, ws.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	proof := client.Call(ctx, "unsent", nil, nil)
	near, far := duplex.Pipe(1 << 20)
	defer far.Abort()
	connection := &publicationReadFailure{Conn: near, wrote: make(chan struct{}), err: fmt.Errorf("adapter receive: %w", proof)}
	peer, err := ws.NewPeer(context.Background(), connection, ws.ClientRole, ws.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	err = peer.Call(context.Background(), "supply", nil, nil)
	wantUnpublished(t, err, false)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("nested cause lost: %v", err)
	}
}

func TestRefusedReverseReplyCannotProveDeliveredCallUnpublished(t *testing.T) {
	var delivered atomic.Bool
	client, _ := newPair(t, ws.Options{Handlers: map[string]ws.Handler{
		"a": func(ctx context.Context, peer *ws.Peer, _ json.RawMessage) (any, error) {
			delivered.Store(true)
			return nil, peer.Call(ctx, "b", nil, nil)
		},
	}}, ws.Options{MaxFrameBytes: 180, Handlers: map[string]ws.Handler{
		"b": func(context.Context, *ws.Peer, json.RawMessage) (any, error) { return strings.Repeat("x", 2000), nil },
	}})
	err := client.Call(context.Background(), "a", nil, nil)
	if !delivered.Load() {
		t.Fatal("outer request was not delivered")
	}
	if client.Err() == nil {
		t.Fatal("oversized reply fallback did not reach the failure broadcast")
	}
	wantUnpublished(t, err, false)
}
