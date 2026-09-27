package engine_test

// Ported from nightseam v0.6.0 runtime/go/seam_test.go
// (5cc9723a24646c40ed1861f892b2b23eb6d785d7), with the close-code tests for
// research R27 beside the ones it held.

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	engine "github.com/Bitspark/bitruntime/engine/go"
	transports "github.com/Bitspark/bitruntime/transports/go"
)

// TestPeerSpeaksTheProfileOverAnyConnection: two peers over an in-memory
// pipe, no socket anywhere, complete a call, a reverse call, an event and a
// cancellation, and a closed pipe ends both. The protocol is written to the
// seam, not to a WebSocket.
func TestPeerSpeaksTheProfileOverAnyConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clientConn, serverConn := transports.Pipe(1 << 20)
	blocked := make(chan struct{})
	server, err := engine.NewPeer(ctx, serverConn, engine.ServerRole, serving{Handlers: map[string]handler{
		"echo": func(ctx context.Context, p *engine.Peer, raw json.RawMessage) (any, error) {
			var s string
			if err := json.Unmarshal(raw, &s); err != nil {
				return nil, err
			}
			var back string
			if err := call(ctx, p, "reverse", s, &back); err != nil {
				return nil, err
			}
			return back, nil
		},
		"block": func(ctx context.Context, p *engine.Peer, raw json.RawMessage) (any, error) {
			<-ctx.Done()
			close(blocked)
			return nil, ctx.Err()
		},
	}}.with(engine.Options{}))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	observed := make(chan json.RawMessage, 1)
	client, err := engine.NewPeer(ctx, clientConn, engine.ClientRole, serving{
		Handlers: map[string]handler{"reverse": func(ctx context.Context, p *engine.Peer, raw json.RawMessage) (any, error) {
			var s string
			if err := json.Unmarshal(raw, &s); err != nil {
				return nil, err
			}
			runes := []rune(s)
			for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
				runes[i], runes[j] = runes[j], runes[i]
			}
			return string(runes), nil
		}},
		Events: map[string]eventHandler{"changed": func(ctx context.Context, p *engine.Peer, raw json.RawMessage) {
			observed <- raw
		}},
	}.with(engine.Options{}))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	var result string
	if err := call(ctx, client, "echo", "seam", &result); err != nil {
		t.Fatal(err)
	}
	if result != "maes" {
		t.Fatalf("a call and its reverse call over the pipe returned %q", result)
	}
	if err := emit(ctx, server, "changed", map[string]int{"count": 7}); err != nil {
		t.Fatal(err)
	}
	select {
	case data := <-observed:
		if string(data) != `{"count":7}` {
			t.Fatalf("the event arrived as %s", data)
		}
	case <-ctx.Done():
		t.Fatal("no event arrived over the pipe")
	}
	short, cancelShort := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancelShort()
	if err := call(short, client, "block", nil, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a cancelled call returned %v", err)
	}
	select {
	case <-blocked:
	case <-ctx.Done():
		t.Fatal("the cancellation never reached the handler over the pipe")
	}
	if err := clientConn.Close(ctx, transports.CodeNormal, "done"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-server.Done():
	case <-ctx.Done():
		t.Fatal("closing the connection did not end the server peer")
	}
	var closed *transports.CloseError
	if err := server.Err(); !errors.As(err, &closed) || closed.Code != transports.CodeNormal || closed.Reason != "done" {
		t.Fatalf("the server peer ended with %v, not the close it was sent", err)
	}
}

// TestPeerRefusesAFrameOverItsLimit: a connection whose maker set a laxer
// limit than the peer's still cannot hand the peer an oversized frame.
func TestPeerRefusesAFrameOverItsLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	near, far := transports.Pipe(1 << 20)
	peer, err := engine.NewPeer(ctx, near, engine.ClientRole, engine.Options{MaxFrameBytes: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	big := make([]byte, 600)
	for i := range big {
		big[i] = ' '
	}
	copy(big, `{"version":1,"kind":"event","event":"e","data":1`)
	big[len(big)-1] = '}'
	if err := far.Send(ctx, transports.Frame{Kind: transports.Text, Data: big}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-peer.Done():
	case <-ctx.Done():
		t.Fatal("the peer accepted a frame over its limit")
	}
	// v0.6.0's error was the refusal itself. R26: an ended peer's error is a
	// closed carrier that keeps its cause, so the refusal is what it wraps.
	if err := peer.Err(); !errors.Is(err, transports.ErrClosed) || !strings.HasSuffix(err.Error(), "duplex frame exceeds size limit") {
		t.Fatalf("the peer ended with %v", err)
	}
	// And the far side is told with 1009, which bitwire/1 binds for a frame
	// over the receiver's limit (SCOPE, Limits). v0.6.0's peer sent 4011.
	var closed *transports.CloseError
	if _, err := far.Receive(ctx); !errors.As(err, &closed) || closed.Code != transports.CodeTooLarge || closed.Reason != "frame exceeds the receive limit" {
		t.Fatalf("the far side read %v", err)
	}
}

// TestHowAPeerEndsAConnectionIsWhatTheFarSideReads: the protocol closes with
// 4011 and a reason when the other side broke it, so that an intermediary
// between the two has a code to act on; a close this side chose carries 1000;
// and a transport there is nothing to say over is aborted, which the far side
// reads as 1006. v0.6.0 also held what its observer was told; observers are
// removed, so the far side's reading is what is held.
func TestHowAPeerEndsAConnectionIsWhatTheFarSideReads(t *testing.T) {
	for _, c := range []struct {
		name   string
		end    func(ctx context.Context, cancel context.CancelFunc, peer *engine.Peer, far transports.Conn)
		code   transports.Code
		reason string
		local  bool
	}{
		{
			name: "a malformed frame is refused",
			end: func(ctx context.Context, _ context.CancelFunc, _ *engine.Peer, far transports.Conn) {
				_ = far.Send(ctx, transports.Frame{Kind: transports.Text, Data: []byte(`{"version":1,"kind":"event"}`)})
			},
			code:   transports.CodeProtocol,
			reason: "invalid duplex frame shape",
			local:  true,
		},
		{
			name: "a frame of the wrong kind is refused",
			end: func(ctx context.Context, _ context.CancelFunc, _ *engine.Peer, far transports.Conn) {
				_ = far.Send(ctx, transports.Frame{Kind: transports.Binary, Data: []byte{0}})
			},
			code:   transports.CodeProtocol,
			reason: "duplex requires JSON text frames",
			local:  true,
		},
		{
			name:  "a close this side chose",
			end:   func(_ context.Context, _ context.CancelFunc, peer *engine.Peer, _ transports.Conn) { _ = peer.Close() },
			code:  transports.CodeNormal,
			local: true,
		},
		{
			name: "a context that ended",
			end: func(_ context.Context, cancel context.CancelFunc, _ *engine.Peer, _ transports.Conn) {
				cancel()
			},
			code:  transports.CodeAbnormalClosure,
			local: true,
		},
		{
			name: "a far side that aborted",
			end: func(_ context.Context, _ context.CancelFunc, _ *engine.Peer, far transports.Conn) {
				_ = far.Abort()
			},
			code:  transports.CodeAbnormalClosure,
			local: false,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			near, far := transports.Pipe(1 << 20)
			peer, err := engine.NewPeer(ctx, near, engine.ServerRole, engine.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer peer.Close()
			c.end(ctx, cancel, peer, far)
			receive(t, peer.Done())
			if err := peer.Err(); !errors.Is(err, transports.ErrClosed) {
				t.Fatalf("the peer ended with %v, not a closed carrier", err)
			}
			if !c.local {
				// What ended it is the far side's abort, and the error keeps it.
				var remote *transports.CloseError
				if err := peer.Err(); !errors.As(err, &remote) || remote.Code != c.code {
					t.Fatalf("the peer ended with %v, want the far side's %d", err, int(c.code))
				}
				return
			}
			// The far side reads what this side sent, which is the whole reason
			// the code is decided here rather than reported here.
			read, cancelRead := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelRead()
			var wire *transports.CloseError
			if _, err := far.Receive(read); !errors.As(err, &wire) {
				t.Fatalf("the far side read %v, not a close", err)
			}
			if wire.Code != c.code || wire.Reason != c.reason {
				t.Fatalf("the far side read %d %q, want %d %q", int(wire.Code), wire.Reason, int(c.code), c.reason)
			}
		})
	}
}

// TestAnObserveOnlyCloseCodeAbortsTheConnection (R27): a code that may only
// be observed — no status, an abnormal closure, a failed TLS handshake — is
// never transmitted. The root asked to close with one aborts, and the far
// side observes the abort itself: 1006 with no reason, not the code or the
// reason it was asked to send.
func TestAnObserveOnlyCloseCodeAbortsTheConnection(t *testing.T) {
	for _, code := range []transports.Code{transports.CodeNoStatus, transports.CodeAbnormalClosure, transports.CodeTLSHandshake} {
		t.Run(strconv.Itoa(int(code)), func(t *testing.T) {
			peer, raw := rawPeer(t, engine.ServerRole, engine.Options{})
			if err := peer.Wire().Close(code, "x"); err != nil {
				t.Fatal(err)
			}
			if closed := raw.closed(); closed.Code != transports.CodeAbnormalClosure || closed.Reason != "" {
				t.Fatalf("the far side read %d %q, want an abort", int(closed.Code), closed.Reason)
			}
			receive(t, peer.Done())
			if err := peer.Err(); !errors.Is(err, transports.ErrClosed) {
				t.Fatalf("the peer ended with %v", err)
			}
		})
	}
}

// TestASendableCloseCodeTravelsWithItsReason (R27): a code that may be sent is
// the close the far side reads, with the reason given.
func TestASendableCloseCodeTravelsWithItsReason(t *testing.T) {
	for _, c := range []struct {
		code   transports.Code
		reason string
	}{
		{transports.CodePolicyViolation, "why"},
		{transports.CodeNormal, ""},
		{transports.CodeApplicationFirst, "application"},
	} {
		t.Run(strconv.Itoa(int(c.code)), func(t *testing.T) {
			peer, raw := rawPeer(t, engine.ServerRole, engine.Options{})
			if err := peer.Wire().Close(c.code, c.reason); err != nil {
				t.Fatal(err)
			}
			if closed := raw.closed(); closed.Code != c.code || closed.Reason != c.reason {
				t.Fatalf("the far side read %d %q, want %d %q", int(closed.Code), closed.Reason, int(c.code), c.reason)
			}
			receive(t, peer.Done())
			if err := peer.Err(); !errors.Is(err, transports.ErrClosed) {
				t.Fatalf("the peer ended with %v", err)
			}
		})
	}
}
