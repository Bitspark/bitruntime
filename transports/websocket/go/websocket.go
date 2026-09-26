// Package websocket carries a frames duplex connection over a WebSocket, and
// is the only package that knows the seam is one: a message is a frame, a
// close frame is a close, and the read limit is the receive limit.
package websocket

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/coder/websocket"

	transports "github.com/Bitspark/bitruntime/transports/go"
)

// New wraps an open WebSocket as a frames duplex connection with the given
// receive limit: a message larger than it fails the read and closes the
// connection with 1009, as the seam requires. Never read or write the
// WebSocket after handing it over.
func New(conn *websocket.Conn, limit int64) transports.Conn {
	conn.SetReadLimit(limit)
	return &connection{conn: conn}
}

type connection struct {
	conn *websocket.Conn
	mu   sync.Mutex
	done bool
}

// Subprotocol is what the WebSocket handshake selected, "" for none.
func (c *connection) Subprotocol() string { return c.conn.Subprotocol() }

func (c *connection) closed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.done
}

func (c *connection) end() {
	c.mu.Lock()
	c.done = true
	c.mu.Unlock()
}

func kindOf(t websocket.MessageType) (transports.Kind, error) {
	switch t {
	case websocket.MessageText:
		return transports.Text, nil
	case websocket.MessageBinary:
		return transports.Binary, nil
	}
	return 0, fmt.Errorf("websocket message of unknown type %d", int(t))
}

func typeOf(k transports.Kind) (websocket.MessageType, error) {
	switch k {
	case transports.Text:
		return websocket.MessageText, nil
	case transports.Binary:
		return websocket.MessageBinary, nil
	}
	return 0, transports.ErrNoKind
}

func (c *connection) Send(ctx context.Context, frame transports.Frame) error {
	if c.closed() {
		return transports.ErrClosed
	}
	t, err := typeOf(frame.Kind)
	if err != nil {
		return err
	}
	if err := c.conn.Write(ctx, t, frame.Data); err != nil {
		return c.translate(ctx, err)
	}
	return nil
}

func (c *connection) Receive(ctx context.Context) (transports.Frame, error) {
	if c.closed() {
		return transports.Frame{}, transports.ErrClosed
	}
	t, data, err := c.conn.Read(ctx)
	if err != nil {
		if errors.Is(err, websocket.ErrMessageTooBig) {
			// The library has sent its 1009 and would now wait for a close
			// handshake the sender has no reason to start. The connection is
			// dead by the seam's contract, so end it: the sender observes the
			// 1009 if it read it first, or the dropped transport otherwise.
			c.end()
			_ = c.conn.CloseNow()
			return transports.Frame{}, err
		}
		return transports.Frame{}, c.translate(ctx, err)
	}
	kind, err := kindOf(t)
	if err != nil {
		_ = c.Abort()
		return transports.Frame{}, err
	}
	return transports.Frame{Kind: kind, Data: data}, nil
}

// translate says what an error of the WebSocket means at the seam: the
// context's own end, ErrClosed once this side ended the connection, the
// remote side's close with its code and reason, or — for any other failure of
// a connection that is now unusable — an abnormal closure, which is what a side
// observes when the transport dropped. Every outcome but the context's is a
// closed carrier.
func (c *connection) translate(ctx context.Context, err error) error {
	if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
		return ctx.Err()
	}
	if c.closed() {
		return transports.ErrClosed
	}
	var closeErr websocket.CloseError
	if errors.As(err, &closeErr) {
		return &transports.CloseError{Code: transports.Code(closeErr.Code), Reason: closeErr.Reason}
	}
	return &transports.CloseError{Code: transports.CodeAbnormalClosure, Reason: err.Error()}
}

// Close sends a close frame with the code and reason and waits for the
// remote side's acknowledgement; the WebSocket library bounds that wait
// itself, so ctx is not consulted. An observe-only code is refused before the
// library sees it: the library would refuse to send 1006 and then drop the
// socket anyway, leaving the remote side to observe the very code refused.
func (c *connection) Close(ctx context.Context, code transports.Code, reason string) error {
	if !transports.Sendable(code) {
		return transports.ErrUnsendableCode
	}
	if c.closed() {
		return transports.ErrClosed
	}
	c.end()
	return c.conn.Close(websocket.StatusCode(code), reason)
}

// Abort closes the socket at once with no close frame.
func (c *connection) Abort() error {
	c.end()
	return c.conn.CloseNow()
}
