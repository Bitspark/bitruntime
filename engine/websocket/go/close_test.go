package websocket_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	engine "github.com/Bitspark/bitruntime/engine/go"
	websocket "github.com/Bitspark/bitruntime/engine/websocket/go"
	transports "github.com/Bitspark/bitruntime/transports/go"
)

// TestAClosingPeerDeliversItsCodeOverAWebSocket: a peer closed from another
// goroutine while its reader waits must transmit the code it chose. Nightseam
// v0.6.0 cancelled the reader's context first, and a WebSocket whose pending
// read is cancelled drops the socket, so the far side often observed 1006.
func TestAClosingPeerDeliversItsCodeOverAWebSocket(t *testing.T) {
	for i := range 50 {
		peers := make(chan *engine.Peer, 1)
		handler, err := websocket.NewHandler(websocket.ServerOptions{
			Authenticate: func(*http.Request) (context.Context, error) { return context.Background(), nil },
			CheckOrigin:  func(*http.Request) bool { return true },
			OnConnect:    func(p *engine.Peer) { peers <- p },
		})
		if err != nil {
			t.Fatal(err)
		}
		server := httptest.NewServer(handler)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		client, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), websocket.DialOptions{})
		if err != nil {
			t.Fatal(err)
		}
		accepted := <-peers
		// The server's reader is waiting for a frame when another goroutine
		// closes the peer.
		time.Sleep(5 * time.Millisecond)
		_ = accepted.Wire().Close(4001, "chosen")
		select {
		case <-client.Done():
		case <-ctx.Done():
			t.Fatal("the client never saw the close")
		}
		var closed *transports.CloseError
		if !errors.As(client.Err(), &closed) || closed.Code != 4001 || closed.Reason != "chosen" {
			t.Fatalf("run %d: the client observed %v, not 4001 chosen", i, client.Err())
		}
		cancel()
		server.Close()
	}
}
