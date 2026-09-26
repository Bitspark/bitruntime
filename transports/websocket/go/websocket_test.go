package ws_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/Bitspark/nightseam/duplex/go"
	"github.com/Bitspark/nightseam/duplex/go/duplextest"
	"github.com/Bitspark/nightseam/duplex/go/ws"
)

// connect opens a WebSocket pair over a test server and wraps both ends.
func connect(t *testing.T, limit int64) (duplex.Conn, duplex.Conn) {
	t.Helper()
	accepted := make(chan *websocket.Conn, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		accepted <- conn
		<-r.Context().Done()
	}))
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	serverSide := <-accepted
	return ws.New(client, limit), ws.New(serverSide, limit)
}

// TestWebSocketIsAConformingTransport: a WebSocket keeps every promise of
// the seam, so the profile and the relays above it may forget it is one.
func TestWebSocketIsAConformingTransport(t *testing.T) {
	duplextest.Run(t, connect)
}
