package duplex

import (
	"context"
	"fmt"
	"sync"
)

// Pipe returns two connected ends in memory: what one sends, the other
// receives, in order. Each direction holds at most a few frames in flight,
// as a socket holds some bytes; past that a Send waits for a Receive, so
// the pipe carries backpressure as a transport does. A Close on one end is
// a CloseError on the other; an Abort is CodeAbnormalClosure there, as a
// dropped socket would be. It carries the duplex profile in tests without
// a socket, and it is the transport a protocol is held to before a real one
// is.
func Pipe(limit int64) (Conn, Conn) {
	a := &pipeEnd{limit: limit, frames: make(chan Frame, inFlight), done: make(chan struct{})}
	b := &pipeEnd{limit: limit, frames: make(chan Frame, inFlight), done: make(chan struct{})}
	a.remote, b.remote = b, a
	return a, b
}

// inFlight is how many frames a pipe holds per direction before a Send
// waits.
const inFlight = 8

type pipeEnd struct {
	limit  int64
	frames chan Frame // frames this end sends; the remote end receives from it
	remote *pipeEnd
	done   chan struct{}
	once   sync.Once
	mu     sync.Mutex
	closed *CloseError // what the remote will be told; nil after Abort
}

func (e *pipeEnd) end(closed *CloseError) {
	e.once.Do(func() {
		e.mu.Lock()
		e.closed = closed
		e.mu.Unlock()
		close(e.done)
	})
}

// remoteError is what this end returns once the remote end ended: its
// close, or an abnormal closure if it aborted.
func (e *pipeEnd) remoteError() error {
	e.remote.mu.Lock()
	defer e.remote.mu.Unlock()
	if e.remote.closed != nil {
		return &CloseError{Code: e.remote.closed.Code, Reason: e.remote.closed.Reason}
	}
	return &CloseError{Code: CodeAbnormalClosure}
}

func (e *pipeEnd) Send(ctx context.Context, frame Frame) error {
	if frame.Kind != Text && frame.Kind != Binary {
		return ErrNoKind
	}
	select {
	case <-e.done:
		return ErrClosed
	case <-e.remote.done:
		return e.remoteError()
	default:
	}
	data := make([]byte, len(frame.Data))
	copy(data, frame.Data)
	select {
	case e.frames <- Frame{Kind: frame.Kind, Data: data}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-e.done:
		return ErrClosed
	case <-e.remote.done:
		return e.remoteError()
	}
}

func (e *pipeEnd) Receive(ctx context.Context) (Frame, error) {
	select {
	case <-e.done:
		return Frame{}, ErrClosed
	default:
	}
	deliver := func(frame Frame) (Frame, error) {
		if e.limit > 0 && int64(len(frame.Data)) > e.limit {
			_ = e.Abort()
			return Frame{}, fmt.Errorf("duplex frame of %d bytes exceeds the receive limit of %d", len(frame.Data), e.limit)
		}
		return frame, nil
	}
	select {
	case frame := <-e.remote.frames:
		return deliver(frame)
	case <-ctx.Done():
		return Frame{}, ctx.Err()
	case <-e.done:
		return Frame{}, ErrClosed
	case <-e.remote.done:
		// What the remote sent before it closed is still delivered, in order,
		// before its close is.
		select {
		case frame := <-e.remote.frames:
			return deliver(frame)
		default:
			return Frame{}, e.remoteError()
		}
	}
}

func (e *pipeEnd) Close(ctx context.Context, code Code, reason string) error {
	e.end(&CloseError{Code: code, Reason: reason})
	return nil
}

func (e *pipeEnd) Abort() error {
	e.end(nil)
	return nil
}
