package websocket

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/Bitspark/bitruntime/core/go"
	wire "github.com/Bitspark/bitwire/wire/go"
	carrier "github.com/coder/websocket"
)

// Options configure host effects and finite resource limits, never service semantics.
type Options struct {
	core.PairOptions
	CloseTimeout     time.Duration
	HandshakeTimeout time.Duration
	HTTPClient       *http.Client
	Header           http.Header
	OriginPatterns   []string
	Authorize        func(*http.Request) bool
	Path             string
}
type normalizedOptions struct{ Options }

func normalize(options Options) (normalizedOptions, error) {
	limits, err := core.NormalizeOptions(options.PairOptions)
	if err != nil {
		return normalizedOptions{}, err
	}
	options.PairOptions = limits
	if options.CloseTimeout < 0 || options.HandshakeTimeout < 0 {
		return normalizedOptions{}, errors.New("wire timeouts must be positive")
	}
	if options.CloseTimeout == 0 {
		options.CloseTimeout = 5 * time.Second
	}
	if options.HandshakeTimeout == 0 {
		options.HandshakeTimeout = 10 * time.Second
	}
	if options.Path == "" {
		options.Path = "/wire"
	}
	if !strings.HasPrefix(options.Path, "/") || strings.ContainsAny(options.Path, "?# \t\r\n") {
		return normalizedOptions{}, errors.New("wire path must be an absolute URL pathname")
	}
	options.Header = options.Header.Clone()
	options.OriginPatterns = slices.Clone(options.OriginPatterns)
	return normalizedOptions{options}, nil
}

// Dial uses normal HTTP/TLS verification. Its context governs establishment only.
func Dial(ctx context.Context, address string, options Options) (*Endpoint, error) {
	o, err := normalize(options)
	if err != nil {
		return nil, err
	}
	u, err := url.Parse(address)
	if err != nil || (u.Scheme != "ws" && u.Scheme != "wss") || u.User != nil || u.Fragment != "" || u.Host == "" {
		return nil, errors.New("wire URL must be ws: or wss: without credentials or fragment")
	}
	ctx, cancel := context.WithTimeout(ctx, o.HandshakeTimeout)
	defer cancel()
	client := o.HTTPClient
	if client == nil {
		client = &http.Client{}
	}
	copyClient := *client
	transport := &captureTransport{base: copyClient.Transport}
	if transport.base == nil {
		transport.base = http.DefaultTransport
	}
	copyClient.Transport = transport
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	conn, _, err := carrier.Dial(ctx, address, &carrier.DialOptions{HTTPClient: &copyClient,
		HTTPHeader: o.Header, Subprotocols: []string{wire.WebSocketProtocol}, CompressionMode: carrier.CompressionDisabled})
	if err != nil {
		return nil, err
	}
	if conn.Subprotocol() != wire.WebSocketProtocol {
		_ = conn.CloseNow()
		return nil, errors.New("wire subprotocol negotiation failed")
	}
	return newEndpoint(conn, transport.stream, o), nil
}

// Accept establishes one endpoint; the caller remains responsible for its HTTP server.
func Accept(w http.ResponseWriter, r *http.Request, options Options) (*Endpoint, error) {
	o, err := normalize(options)
	if err != nil {
		http.Error(w, "invalid wire configuration", http.StatusInternalServerError)
		return nil, err
	}
	if r.URL.Path != o.Path {
		http.NotFound(w, r)
		return nil, errors.New("wire path not found")
	}
	offered := false
	for _, header := range r.Header.Values("Sec-WebSocket-Protocol") {
		for _, protocol := range strings.Split(header, ",") {
			offered = offered || strings.TrimSpace(protocol) == wire.WebSocketProtocol
		}
	}
	if !offered {
		http.Error(w, "wire subprotocol required", http.StatusBadRequest)
		return nil, errors.New("wire subprotocol required")
	}
	if o.Authorize != nil && !authorized(o.Authorize, r) {
		http.Error(w, "wire connection refused", http.StatusForbidden)
		return nil, errors.New("wire connection refused")
	}
	writer := &captureHijacker{ResponseWriter: w}
	conn, err := carrier.Accept(writer, r, &carrier.AcceptOptions{Subprotocols: []string{wire.WebSocketProtocol},
		OriginPatterns: o.OriginPatterns, CompressionMode: carrier.CompressionDisabled})
	if err != nil {
		return nil, err
	}
	return newEndpoint(conn, writer.stream, o), nil
}

// Retain the stream acquired by this endpoint, so its force-close deadline can
// interrupt a handshake even after the library has taken over the read context.
// The caller's HTTP client, transport and server remain caller-owned.
type captureTransport struct {
	base   http.RoundTripper
	stream io.Closer
}

func (t *captureTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	response, err := t.base.RoundTrip(r)
	if err == nil && response.StatusCode == http.StatusSwitchingProtocols {
		t.stream = response.Body
	}
	return response, err
}

type captureHijacker struct {
	http.ResponseWriter
	stream net.Conn
}

func (w *captureHijacker) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, rw, err := http.NewResponseController(w.ResponseWriter).Hijack()
	if err == nil {
		w.stream = conn
	}
	return conn, rw, err
}

func authorized(check func(*http.Request) bool, r *http.Request) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	return check(r)
}
