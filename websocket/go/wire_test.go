package websocket_test

import (
	"context"
	"encoding/hex"
	core "github.com/Bitspark/bitruntime/core/go"
	ws "github.com/Bitspark/bitruntime/websocket/go"
	ontos "github.com/Bitspark/bitwire/ontos/go/core"
	wire "github.com/Bitspark/bitwire/wire/go"
	carrier "github.com/coder/websocket"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func take[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("observation timed out")
		var v T
		return v
	}
}
func start(t *testing.T, options ws.Options, connected func(*ws.Endpoint, *http.Request)) (*ws.Server, *httptest.Server, string) {
	t.Helper()
	s, err := ws.NewServer(options, connected)
	if err != nil {
		t.Fatal(err)
	}
	h := httptest.NewServer(s)
	t.Cleanup(func() { s.Close(); h.Close() })
	return s, h, "ws" + strings.TrimPrefix(h.URL, "http") + "/wire"
}
func TestDuplexAndReleasedOwnership(t *testing.T) {
	peers := make(chan *ws.Endpoint, 1)
	s, h, url := start(t, ws.Options{}, func(e *ws.Endpoint, _ *http.Request) { peers <- e })
	a, err := ws.Dial(context.Background(), url, ws.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b := take(t, peers)
	got := make(chan ontos.Value, 4)
	b.Receive(func(e ontos.Value) { got <- e })
	a.Receive(func(e ontos.Value) { got <- e })
	e := ontos.NewTuple(ontos.NewAtom([]byte{1, 2}))
	for range 2 {
		if err = a.Send(e); err != nil {
			t.Fatal(err)
		}
	}
	first := take(t, got)
	second := take(t, got)
	if !first.Equal(second) || !first.Equal(e) {
		t.Fatal("opaque bytes or repeated admissions changed")
	}
	if err = b.Send(e); err != nil {
		t.Fatal(err)
	}
	take(t, got)
	s.Close()
	take(t, a.Closed())
	take(t, b.Closed())
	response, err := http.Get(h.URL)
	if err != nil {
		t.Fatal("host HTTP server was closed", err)
	}
	response.Body.Close()
	if response.StatusCode != 503 {
		t.Fatal(response.Status)
	}
}
func TestIndependentBytesAndFailures(t *testing.T) {
	for _, sample := range []struct {
		name  string
		kind  carrier.MessageType
		hex   string
		valid bool
	}{
		{"independent empty vector", carrier.MessageBinary, "0000", true},
		{"truncated", carrier.MessageBinary, "0106", false}, {"text", carrier.MessageText, "6869", false},
	} {
		t.Run(sample.name, func(t *testing.T) {
			peers := make(chan *ws.Endpoint, 1)
			_, _, url := start(t, ws.Options{}, func(e *ws.Endpoint, _ *http.Request) { peers <- e })
			raw, _, err := carrier.Dial(context.Background(), url, &carrier.DialOptions{Subprotocols: []string{wire.WebSocketProtocol}})
			if err != nil {
				t.Fatal(err)
			}
			defer raw.CloseNow()
			peer := take(t, peers)
			got := make(chan ontos.Value, 1)
			peer.Receive(func(e ontos.Value) { got <- e })
			bytes, _ := hex.DecodeString(sample.hex)
			if err = raw.Write(context.Background(), sample.kind, bytes); err != nil {
				t.Fatal(err)
			}
			if sample.valid {
				e := take(t, got)
				if !e.Equal(ontos.NewAtom(nil)) {
					t.Fatal(e)
				}
			} else {
				take(t, peer.Closed())
				if peer.Termination().Kind != "failed" {
					t.Fatal(peer.Termination())
				}
			}
		})
	}
}
func TestNegotiationOriginAuthorizationAndTLS(t *testing.T) {
	_, _, url := start(t, ws.Options{}, func(*ws.Endpoint, *http.Request) {})
	if c, _, err := carrier.Dial(context.Background(), url, nil); err == nil {
		c.CloseNow()
		t.Fatal("missing protocol admitted")
	}
	if c, err := ws.Dial(context.Background(), url, ws.Options{Header: http.Header{"Origin": []string{"https://elsewhere.invalid"}}}); err == nil {
		c.Close()
		t.Fatal("foreign origin admitted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if c, err := ws.Dial(ctx, url, ws.Options{}); err == nil {
		c.Close()
		t.Fatal("canceled establishment admitted")
	}
	_, _, denied := start(t, ws.Options{Authorize: func(*http.Request) bool { panic("secret") }}, func(*ws.Endpoint, *http.Request) { t.Error("unauthorized callback") })
	if c, err := ws.Dial(context.Background(), denied, ws.Options{}); err == nil {
		c.Close()
		t.Fatal("panicking authorization admitted")
	}
	s, _ := ws.NewServer(ws.Options{}, func(*ws.Endpoint, *http.Request) {})
	tls := httptest.NewTLSServer(s)
	defer tls.Close()
	defer s.Close()
	address := "wss" + strings.TrimPrefix(tls.URL, "https") + "/wire"
	if c, err := ws.Dial(context.Background(), address, ws.Options{}); err == nil {
		c.Close()
		t.Fatal("untrusted certificate accepted")
	}
	c, err := ws.Dial(context.Background(), address, ws.Options{HTTPClient: tls.Client()})
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
}
func TestDetachedOverflowAndDispatchedWork(t *testing.T) {
	peers := make(chan *ws.Endpoint, 1)
	_, _, url := start(t, ws.Options{PairOptions: core.PairOptions{MaxQueuedMessages: 1}}, func(e *ws.Endpoint, _ *http.Request) { peers <- e })
	a, err := ws.Dial(context.Background(), url, ws.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b := take(t, peers)
	e := ontos.NewAtom(nil)
	a.Send(e)
	a.Send(e)
	take(t, b.Closed())
	if b.Termination().Kind != "failed" {
		t.Fatal(b.Termination())
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	_, _, url = start(t, ws.Options{}, func(e *ws.Endpoint, _ *http.Request) {
		e.Receive(func(ontos.Value) { close(entered); <-release; close(done) })
	})
	c, err := ws.Dial(context.Background(), url, ws.Options{})
	if err != nil {
		t.Fatal(err)
	}
	c.Send(e)
	take(t, entered)
	c.Close()
	close(release)
	take(t, done)
}

func TestRefusedSendAndForcedClose(t *testing.T) {
	peers := make(chan *ws.Endpoint, 1)
	_, _, address := start(t, ws.Options{CloseTimeout: 20 * time.Millisecond}, func(e *ws.Endpoint, _ *http.Request) { peers <- e })
	raw, _, err := carrier.Dial(context.Background(), address, &carrier.DialOptions{Subprotocols: []string{wire.WebSocketProtocol}})
	if err != nil {
		t.Fatal(err)
	}
	defer raw.CloseNow()
	server := take(t, peers)
	finished := make(chan struct{})
	go func() { server.Close(); close(finished) }()
	// The raw peer never reads, so it cannot complete a close handshake.
	take(t, finished)
	a, err := ws.Dial(context.Background(), address, ws.Options{PairOptions: core.PairOptions{MaxQueuedBytes: 1}})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if err = a.Send(ontos.NewAtom(nil)); err == nil {
		t.Fatal("output budget was ignored")
	}
	select {
	case <-a.Closed():
		t.Fatal("outgoing refusal closed the wire")
	default:
	}
}
