package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Bitspark/nightseam/duplex/go"
)

// TestPeerSpeaksTheProfileOverAnyConnection: two peers over an in-memory
// pipe, no socket anywhere, complete a call, a reverse call, an event and a
// cancellation, and a closed pipe ends both. The profile is written to the
// seam, not to a WebSocket.
func TestPeerSpeaksTheProfileOverAnyConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clientConn, serverConn := duplex.Pipe(1 << 20)
	blocked := make(chan struct{})
	server, err := NewPeer(ctx, serverConn, ServerRole, Options{Handlers: map[string]Handler{
		"echo": func(ctx context.Context, p *Peer, raw json.RawMessage) (any, error) {
			var s string
			if err := json.Unmarshal(raw, &s); err != nil {
				return nil, err
			}
			var back string
			if err := p.Call(ctx, "reverse", s, &back); err != nil {
				return nil, err
			}
			return back, nil
		},
		"block": func(ctx context.Context, p *Peer, raw json.RawMessage) (any, error) {
			<-ctx.Done()
			close(blocked)
			return nil, ctx.Err()
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	observed := make(chan Event, 1)
	client, err := NewPeer(ctx, clientConn, ClientRole, Options{
		Handlers: map[string]Handler{"reverse": func(ctx context.Context, p *Peer, raw json.RawMessage) (any, error) {
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
		Events: map[string]EventHandler{"changed": func(ctx context.Context, p *Peer, raw json.RawMessage) {
			observed <- Event{Name: "changed", Data: raw}
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	var result string
	if err := client.Call(ctx, "echo", "seam", &result); err != nil {
		t.Fatal(err)
	}
	if result != "maes" {
		t.Fatalf("a call and its reverse call over the pipe returned %q", result)
	}
	if err := server.Emit(ctx, "changed", map[string]int{"count": 7}); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-observed:
		if string(event.Data) != `{"count":7}` {
			t.Fatalf("the event arrived as %s", event.Data)
		}
	case <-ctx.Done():
		t.Fatal("no event arrived over the pipe")
	}
	short, cancelShort := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancelShort()
	if err := client.Call(short, "block", nil, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a cancelled call returned %v", err)
	}
	select {
	case <-blocked:
	case <-ctx.Done():
		t.Fatal("the cancellation never reached the handler over the pipe")
	}
	if err := clientConn.Close(ctx, duplex.CodeNormal, "done"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-server.Done():
	case <-ctx.Done():
		t.Fatal("closing the connection did not end the server peer")
	}
	var closed *duplex.CloseError
	if err := server.Err(); !errors.As(err, &closed) || closed.Code != duplex.CodeNormal || closed.Reason != "done" {
		t.Fatalf("the server peer ended with %v, not the close it was sent", err)
	}
}

// TestPeerRefusesAFrameOverItsLimit: a connection whose maker set a laxer
// limit than the peer's still cannot hand the peer an oversized frame.
func TestPeerRefusesAFrameOverItsLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	near, far := duplex.Pipe(1 << 20)
	peer, err := NewPeer(ctx, near, ClientRole, Options{MaxFrameBytes: 512})
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
	if err := far.Send(ctx, duplex.Frame{Kind: duplex.Text, Data: big}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-peer.Done():
	case <-ctx.Done():
		t.Fatal("the peer accepted a frame over its limit")
	}
	if err := peer.Err(); err == nil || err.Error() != "duplex frame exceeds size limit" {
		t.Fatalf("the peer ended with %v", err)
	}
}

// TestHowAPeerEndsAConnectionIsWhatItsObserverIsTold: the profile closes with
// 4011 and a reason when the other side broke it, so that a gateway or a
// proxy between the two has a code to act on; a close this side chose carries
// 1000; and a transport there is nothing to say over is aborted, which the
// far side reads as 1006. The observer is told the code the wire carried in
// every one of them and never one it did not.
func TestHowAPeerEndsAConnectionIsWhatItsObserverIsTold(t *testing.T) {
	for _, c := range []struct {
		name   string
		end    func(ctx context.Context, cancel context.CancelFunc, peer *Peer, far duplex.Conn)
		code   duplex.Code
		reason string
		local  bool
	}{
		{
			name: "a malformed frame is refused",
			end: func(ctx context.Context, _ context.CancelFunc, _ *Peer, far duplex.Conn) {
				_ = far.Send(ctx, duplex.Frame{Kind: duplex.Text, Data: []byte(`{"version":1,"kind":"event"}`)})
			},
			code:   duplex.CodeDuplex,
			reason: "invalid duplex frame shape",
			local:  true,
		},
		{
			name: "a frame of the wrong kind is refused",
			end: func(ctx context.Context, _ context.CancelFunc, _ *Peer, far duplex.Conn) {
				_ = far.Send(ctx, duplex.Frame{Kind: duplex.Binary, Data: []byte{0}})
			},
			code:   duplex.CodeDuplex,
			reason: "duplex requires JSON text frames",
			local:  true,
		},
		{
			name:  "a close this side chose",
			end:   func(context.Context, context.CancelFunc, *Peer, duplex.Conn) {},
			code:  duplex.CodeNormal,
			local: true,
		},
		{
			name: "a context that ended",
			end: func(_ context.Context, cancel context.CancelFunc, _ *Peer, _ duplex.Conn) {
				cancel()
			},
			code:  duplex.CodeAbnormalClosure,
			local: true,
		},
		{
			name: "a far side that aborted",
			end: func(_ context.Context, _ context.CancelFunc, _ *Peer, far duplex.Conn) {
				_ = far.Abort()
			},
			code:  duplex.CodeAbnormalClosure,
			local: false,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			near, far := duplex.Pipe(1 << 20)
			closes := make(chan ConnectionClosed, 1)
			peer, err := NewPeer(ctx, near, ServerRole, Options{Observer: observerFunc(func(event ObserverEvent) {
				if closed, ok := event.(ConnectionClosed); ok {
					closes <- closed
				}
			})})
			if err != nil {
				t.Fatal(err)
			}
			c.end(ctx, cancel, peer, far)
			if c.code == duplex.CodeNormal {
				_ = peer.Close()
			}
			var closed ConnectionClosed
			select {
			case closed = <-closes:
			case <-time.After(5 * time.Second):
				t.Fatal("the connection did not end")
			}
			if closed.Code != int(c.code) || closed.Reason != c.reason || closed.Local != c.local {
				t.Fatalf("the observer was told %d %q local=%v, want %d %q local=%v",
					closed.Code, closed.Reason, closed.Local, int(c.code), c.reason, c.local)
			}
			// And the far side reads what this side sent, which is the whole
			// reason the code is decided here rather than reported here.
			if !c.local {
				return
			}
			read, cancelRead := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelRead()
			var wire *duplex.CloseError
			if _, err := far.Receive(read); !errors.As(err, &wire) {
				t.Fatalf("the far side read %v, not a close", err)
			}
			if wire.Code != c.code || wire.Reason != c.reason {
				t.Fatalf("the far side read %d %q, want %d %q", int(wire.Code), wire.Reason, int(c.code), c.reason)
			}
		})
	}
}
