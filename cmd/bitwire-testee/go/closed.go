// Replaces the close latch of nightseam v0.6.0 conformance/go/testee/observe.go (5cc9723).

package main

import (
	"context"
	"errors"
	"sync"
	"time"

	transports "github.com/Bitspark/bitruntime/transports/go"
)

// watched is a peer's connection, watched for how it ended: the close or the
// abort its peer chose, or the close the remote sent first. That is the code
// peer.await_close answers with. v0.6.0's testee read it off the close its
// runtime's observer was told of; bitruntime has no observer, and a peer's
// Err says why it ended but not the code: a frame it refused (4011) and a
// transport it gave up on (1006) end it alike.
type watched struct {
	transports.Conn
	once  sync.Once
	ended chan struct{}
	code  transports.Code
}

func watch(c transports.Conn) *watched {
	return &watched{Conn: c, ended: make(chan struct{})}
}

// end latches the first way the connection ended.
func (w *watched) end(code transports.Code) {
	w.once.Do(func() {
		w.code = code
		close(w.ended)
	})
}

// endedBy is the code the connection ended under, once it has, waiting until
// deadline for it.
func (w *watched) endedBy(deadline <-chan time.Time) (transports.Code, bool) {
	select {
	case <-w.ended:
		return w.code, true
	case <-deadline:
		return 0, false
	}
}

func (w *watched) Receive(ctx context.Context) (transports.Frame, error) {
	frame, err := w.Conn.Receive(ctx)
	if err != nil {
		w.saw(err)
	}
	return frame, err
}

func (w *watched) Send(ctx context.Context, frame transports.Frame) error {
	err := w.Conn.Send(ctx, frame)
	if err != nil {
		w.saw(err)
	}
	return err
}

// saw notes an error the transport reported: the remote's close, with its
// code, or this side's refusal of a frame over its receive limit, which every
// transport reports as a local close with 1009. Any other error is this
// side's to act on, and its close or abort says how the connection ended.
func (w *watched) saw(err error) {
	var closeErr *transports.CloseError
	if errors.As(err, &closeErr) {
		w.end(closeErr.Code)
	}
}

func (w *watched) Close(ctx context.Context, code transports.Code, reason string) error {
	if transports.Sendable(code) {
		w.end(code)
	}
	return w.Conn.Close(ctx, code, reason)
}

func (w *watched) Abort() error {
	w.end(transports.CodeAbnormalClosure)
	return w.Conn.Abort()
}

// Subprotocol is what the handshake beneath selected, which the engine
// reports as its peer's.
func (w *watched) Subprotocol() string {
	if negotiated, ok := w.Conn.(interface{ Subprotocol() string }); ok {
		return negotiated.Subprotocol()
	}
	return ""
}
