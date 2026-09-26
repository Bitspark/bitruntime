package runtime

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"

	"github.com/Bitspark/nightseam/duplex/go"
	"github.com/Bitspark/nightseam/duplex/go/ws"
)

// ServerOptions requires an explicit authentication and origin policy. The
// authenticated context is the base context for every incoming invocation.
type ServerOptions struct {
	Options      Options
	Authenticate func(*http.Request) (context.Context, error)
	CheckOrigin  func(*http.Request) bool
	// OnConnect is called with a peer that is already live: it has read
	// frames and may have answered them. It is where a server uses the peer
	// — calls it, keeps it, waits on it. What a peer must serve is installed
	// in Options.Prepare, which runs before it reads anything; a handler
	// installed here can be too late for the other side's first request.
	OnConnect func(*Peer)
	// Subprotocols are what the server will select, in its own order of
	// preference, from what a client offers; empty selects none, which is
	// the default and what every consumer that sets nothing keeps. The
	// profile names itself here nowhere and refuses nothing on this ground
	// (docs/wire/profile.md).
	Subprotocols []string
	// SelectSubprotocol answers with the one subprotocol to select out of
	// what this request offered, "" for none. It is the selection, not a
	// filter over Subprotocols: a browser's ticket travels in the offer and
	// is accepted only if it is selected back unchanged, which no fixed
	// list can do. Nil selects the first offered that Subprotocols names.
	SelectSubprotocol func(r *http.Request, offered []string) string
}

func (o ServerOptions) validate() error {
	if o.Authenticate == nil || o.CheckOrigin == nil {
		return errors.New("duplex server requires explicit authentication and origin policies")
	}
	_, err := o.Options.normalized()
	return err
}

// Accept upgrades an authenticated request to a WebSocket and speaks the
// profile over it. The caller must keep its HTTP handler alive until
// Peer.Done; NewHandler implements that lifetime contract.
func Accept(w http.ResponseWriter, r *http.Request, options ServerOptions) (*Peer, error) {
	if err := options.validate(); err != nil {
		http.Error(w, "Invalid server configuration", http.StatusInternalServerError)
		return nil, err
	}
	if !options.CheckOrigin(r) {
		http.Error(w, "Origin denied", http.StatusForbidden)
		return nil, errors.New("duplex origin denied")
	}
	ctx, err := options.Authenticate(r)
	if err != nil || ctx == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return nil, errors.New("duplex authentication failed")
	}
	o, _ := options.Options.normalized()
	accept := &websocket.AcceptOptions{InsecureSkipVerify: true, Subprotocols: options.Subprotocols}
	if options.SelectSubprotocol != nil {
		// The library selects the first offered that it is given; given the
		// hook's one answer, the hook is the selection. An answer nobody
		// offered selects none, as offering none does.
		accept.Subprotocols = nil
		if selected := options.SelectSubprotocol(r, offeredSubprotocols(r)); selected != "" {
			accept.Subprotocols = []string{selected}
		}
	}
	socket, err := websocket.Accept(w, r, accept)
	if err != nil {
		return nil, err
	}
	conn := ws.New(socket, o.MaxFrameBytes)
	peer, err := newPeer(ctx, conn, ServerRole, options.Options, socket.Subprotocol())
	if err != nil {
		// Everything else newPeer refuses was refused by validate above, so
		// this is Prepare's own error: the upgrade is answered and the
		// profile will not be spoken over it, and a socket left open in
		// silence behind a 101 is the one thing the client cannot read.
		refuseConnection(conn, o.WriteTimeout)
		return nil, err
	}
	return peer, nil
}

// refuseConnection ends a socket the handshake opened and the peer above it
// never took, with the code a policy refusal carries everywhere (1008) rather
// than the abort a live peer's failure would leave.
func refuseConnection(conn duplex.Conn, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_ = conn.Close(ctx, duplex.CodePolicyViolation, "the connection was refused before the profile began")
}

// NewHandler serves the profile at an HTTP endpoint: each request that
// passes CheckOrigin and Authenticate is upgraded to a WebSocket and becomes
// a server-role peer with the options given. The generated binding's
// NewHandler wraps this with the family's handlers installed.
func NewHandler(options ServerOptions) (http.Handler, error) {
	if err := options.validate(); err != nil {
		return nil, err
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		peer, err := Accept(w, r, options)
		if err != nil {
			return
		}
		defer peer.Close()
		if options.OnConnect != nil {
			options.OnConnect(peer)
		}
		select {
		case <-peer.Done():
		case <-r.Context().Done():
		}
	}), nil
}

// DialOptions is what Dial opens a WebSocket with: the peer's Options, the
// headers and client of the HTTP upgrade, the bound on the handshake, and
// the subprotocols to offer.
type DialOptions struct {
	Options    Options
	HTTPHeader http.Header
	HTTPClient *http.Client
	// ConnectTimeout bounds the handshake alone, as TypeScript's
	// connectTimeoutMs does, and takes the same default of 30 seconds where
	// it is zero. A dial that has not become a connection by then is refused
	// with the code connect_timeout and nothing is opened; the connection's
	// own lifetime is the context's, as it is without one.
	ConnectTimeout time.Duration
	// Subprotocols are offered to the server in order of preference; the
	// default offers none. A server that selects none leaves the connection
	// with none and the profile is spoken over it either way — but a browser
	// refuses a handshake whose offer went unselected, so a client that
	// offers must be met by a server that selects (docs/wire/profile.md).
	Subprotocols []string
}

// Dial opens a WebSocket and speaks the profile over it. It uses ctx for the
// handshake and the connection lifetime. Use a separate context for each
// Call; cancelling the dialing context disconnects the peer.
func Dial(ctx context.Context, url string, options DialOptions) (*Peer, *http.Response, error) {
	if ctx == nil {
		return nil, nil, errors.New("duplex dial requires a context")
	}
	if options.ConnectTimeout < 0 {
		return nil, nil, errors.New("duplex connect timeout must not be negative")
	}
	o, err := options.Options.normalized()
	if err != nil {
		return nil, nil, err
	}
	timeout := options.ConnectTimeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	// The deadline is the handshake's and not the connection's: once the
	// upgrade is answered, net/http stops cancelling the body it handed over,
	// so the peer below outlives this context and ends with ctx instead.
	dialing, settled := context.WithTimeout(ctx, timeout)
	defer settled()
	socket, response, err := websocket.Dial(dialing, url, &websocket.DialOptions{HTTPHeader: options.HTTPHeader, HTTPClient: options.HTTPClient, Subprotocols: options.Subprotocols})
	if err != nil {
		if ctx.Err() == nil && errors.Is(dialing.Err(), context.DeadlineExceeded) {
			return nil, response, &PublicError{Code: "connect_timeout", Message: "Connection timed out."}
		}
		return nil, response, err
	}
	conn := ws.New(socket, o.MaxFrameBytes)
	peer, err := newPeer(ctx, conn, ClientRole, options.Options, socket.Subprotocol())
	if err != nil {
		// As in Accept: the only error left here is Prepare's, and the
		// server is told the connection was refused rather than left with a
		// socket this side will never speak over.
		refuseConnection(conn, o.WriteTimeout)
		return nil, response, err
	}
	return peer, response, nil
}

// offeredSubprotocols is what the request offers as Sec-WebSocket-Protocol,
// in the client's order of preference, across however many header lines it
// spelled them over.
func offeredSubprotocols(r *http.Request) []string {
	var offered []string
	for _, line := range r.Header.Values("Sec-WebSocket-Protocol") {
		for _, token := range strings.Split(line, ",") {
			if token = strings.TrimSpace(token); token != "" {
				offered = append(offered, token)
			}
		}
	}
	return offered
}
