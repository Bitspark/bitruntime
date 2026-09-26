// Package ws carries a frames duplex connection over a WebSocket. It is the
// one transport there is today, and the only package that knows the
// seam is a WebSocket: a message is a frame, a close frame is a close, and
// the read limit is the receive limit.
package ws

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/coder/websocket"

	"github.com/Bitspark/nightseam/duplex/go"
)

// New wraps an open WebSocket as a frames duplex connection with the given
// receive limit: a message larger than it fails the read and the
// connection, as the seam requires. Never read or write the WebSocket after
// handing it over.
func New(conn *websocket.Conn, limit int64) duplex.Conn {
	conn.SetReadLimit(limit)
	return &connection{conn: conn}
}

type connection struct {
	conn *websocket.Conn
	mu   sync.Mutex
	done bool
}

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

func kindOf(t websocket.MessageType) (duplex.Kind, error) {
	switch t {
	case websocket.MessageText:
		return duplex.Text, nil
	case websocket.MessageBinary:
		return duplex.Binary, nil
	}
	return 0, fmt.Errorf("websocket message of unknown type %d", int(t))
}

func typeOf(k duplex.Kind) (websocket.MessageType, error) {
	switch k {
	case duplex.Text:
		return websocket.MessageText, nil
	case duplex.Binary:
		return websocket.MessageBinary, nil
	}
	return 0, duplex.ErrNoKind
}

func (c *connection) Send(ctx context.Context, frame duplex.Frame) error {
	if c.closed() {
		return duplex.ErrClosed
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

func (c *connection) Receive(ctx context.Context) (duplex.Frame, error) {
	if c.closed() {
		return duplex.Frame{}, duplex.ErrClosed
	}
	t, data, err := c.conn.Read(ctx)
	if err != nil {
		return duplex.Frame{}, c.translate(ctx, err)
	}
	kind, err := kindOf(t)
	if err != nil {
		_ = c.Abort()
		return duplex.Frame{}, err
	}
	return duplex.Frame{Kind: kind, Data: data}, nil
}

// translate says what an error of the WebSocket means at the seam: the
// context's own end, ErrClosed once this side ended the connection, the
// remote side's close with its code and reason, or the failure itself.
func (c *connection) translate(ctx context.Context, err error) error {
	if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
		return ctx.Err()
	}
	if c.closed() {
		return duplex.ErrClosed
	}
	var closeErr websocket.CloseError
	if errors.As(err, &closeErr) {
		return &duplex.CloseError{Code: duplex.Code(closeErr.Code), Reason: closeErr.Reason}
	}
	return err
}

// Close sends a close frame with the code and reason and waits for the
// remote side's acknowledgement; the WebSocket library bounds that wait
// itself, so ctx is not consulted.
func (c *connection) Close(ctx context.Context, code duplex.Code, reason string) error {
	if c.closed() {
		return duplex.ErrClosed
	}
	c.end()
	return c.conn.Close(websocket.StatusCode(code), reason)
}

// Abort closes the socket at once with no close frame.
func (c *connection) Abort() error {
	c.end()
	return c.conn.CloseNow()
}
