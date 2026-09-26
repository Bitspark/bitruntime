package websocket_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	transports "github.com/Bitspark/bitruntime/transports/go"
	"github.com/Bitspark/bitruntime/transports/go/transporttest"
	ws "github.com/Bitspark/bitruntime/transports/websocket/go"
)

// connect opens a WebSocket pair over a test server and wraps both ends.
func connect(t *testing.T, limit int64) (transports.Conn, transports.Conn) {
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
	transporttest.Run(t, connect)
}
