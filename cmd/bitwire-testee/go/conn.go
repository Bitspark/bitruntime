// Ported from nightseam v0.6.0 conformance/go/testee/conn.go (5cc9723).

package main

import (
	"context"
	"encoding/base64"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"

	transports "github.com/Bitspark/bitruntime/transports/go"
	ws "github.com/Bitspark/bitruntime/transports/websocket/go"
)

// A connection under control: the seam's Conn, and what it received
// while the runner was not asking. Eager consumption reads as frames
// arrive; lazy reads only when conn.receive asks, which is the one way a
// sender is made to wait.
type conn struct {
	transports.Conn
	lazy   bool
	frames *inbox[received]
	ended  chan struct{}
	end    error
	once   sync.Once
	cancel context.CancelFunc
}

// received is one frame, or the error that ended receiving.
type received struct {
	frame transports.Frame
	err   error
}

func wrap(c transports.Conn, lazy bool) *conn {
	ctx, cancel := context.WithCancel(context.Background())
	w := &conn{Conn: c, lazy: lazy, frames: newInbox[received](), ended: make(chan struct{}), cancel: cancel}
	if !lazy {
		go func() {
			for {
				frame, err := c.Receive(ctx)
				if err != nil {
					w.finish(err)
					return
				}
				w.frames.put(received{frame: frame})
			}
		}()
	}
	return w
}

func (c *conn) finish(err error) {
	c.once.Do(func() {
		c.end = err
		c.frames.put(received{err: err})
		c.frames.close()
		close(c.ended)
	})
}

func (c *conn) shutdown() {
	c.cancel()
	_ = c.Abort()
	c.finish(transports.ErrClosed)
}

// receive is the next frame: what the reader held, or, lazily, what the
// connection gives now.
func (c *conn) receive(within time.Duration) (transports.Frame, error) {
	if c.lazy {
		ctx, cancel := context.WithTimeout(context.Background(), within)
		defer cancel()
		frame, err := c.Receive(ctx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return transports.Frame{}, fail("timeout", "nothing received within %s", within)
			}
			c.finish(err)
		}
		return frame, err
	}
	item, ok, _ := c.frames.await(within, func(received) bool { return true })
	if !ok {
		return transports.Frame{}, fail("timeout", "nothing received within %s", within)
	}
	if item.err != nil {
		// A close stays the first thing every later receive sees.
		c.frames.put(item)
		return transports.Frame{}, item.err
	}
	return item.frame, nil
}

// closeError renders how a connection ended as the driver's error: closed
// with the remote's code and reason, or failed.
func closeError(err error) *failure {
	var closeErr *transports.CloseError
	if errors.As(err, &closeErr) {
		f := fail("closed", "%v", err)
		f.Members = map[string]any{"close_code": int(closeErr.Code), "reason": closeErr.Reason}
		return f
	}
	if errors.Is(err, transports.ErrClosed) {
		return fail("closed", "%v", err)
	}
	return fail("failed", "%v", err)
}

// listener accepts one WebSocket and hands it over as a connection.
type listener struct {
	server   *http.Server
	url      string
	accepted chan transports.Conn
}

func listen(limit int64) (*listener, error) {
	socket, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	l := &listener{accepted: make(chan transports.Conn, 1), url: "ws://" + socket.Addr().String()}
	l.server = &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		select {
		case l.accepted <- ws.New(s, limit):
		default:
			_ = s.Close(websocket.StatusPolicyViolation, "one connection is accepted")
		}
	})}
	go func() { _ = l.server.Serve(socket) }()
	return l, nil
}

// shutdown stops listening, and aborts a connection nobody accepted.
func (l *listener) shutdown() {
	_ = l.server.Close()
	select {
	case c := <-l.accepted:
		_ = c.Abort()
	default:
	}
}

func dial(url string, limit int64) (transports.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	socket, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		return nil, err
	}
	return ws.New(socket, limit), nil
}

func (r request) lazy() (bool, error) {
	mode, err := r.string("consume")
	if err != nil {
		return false, err
	}
	switch mode {
	case "", "eager":
		return false, nil
	case "lazy":
		return true, nil
	}
	return false, invalid("consume is eager or lazy")
}

func (t *testee) seamOps() map[string]func(request) (any, error) {
	return map[string]func(request) (any, error){
		"conn.listen": func(r request) (any, error) {
			limit, err := r.int("limit", 1<<20)
			if err != nil {
				return nil, err
			}
			l, err := listen(limit)
			if err != nil {
				return nil, err
			}
			return map[string]any{"handle": t.mint("l", l), "url": l.url}, nil
		},
		"conn.accept": func(r request) (any, error) {
			l, err := object[*listener](t, r, "listener")
			if err != nil {
				return nil, err
			}
			within, err := r.within()
			if err != nil {
				return nil, err
			}
			lazy, err := r.lazy()
			if err != nil {
				return nil, err
			}
			select {
			case c := <-l.accepted:
				return map[string]any{"handle": t.mint("c", wrap(c, lazy))}, nil
			case <-time.After(within):
				return nil, fail("timeout", "nobody connected within %s", within)
			}
		},
		"conn.dial": func(r request) (any, error) {
			url, err := r.mustString("url")
			if err != nil {
				return nil, err
			}
			limit, err := r.int("limit", 1<<20)
			if err != nil {
				return nil, err
			}
			lazy, err := r.lazy()
			if err != nil {
				return nil, err
			}
			c, err := dial(url, limit)
			if err != nil {
				return nil, fail("failed", "%v", err)
			}
			return map[string]any{"handle": t.mint("c", wrap(c, lazy))}, nil
		},
		"conn.pipe": func(r request) (any, error) {
			limit, err := r.int("limit", 1<<20)
			if err != nil {
				return nil, err
			}
			lazy, err := r.lazy()
			if err != nil {
				return nil, err
			}
			a, b := transports.Pipe(limit)
			return map[string]any{"a": t.mint("c", wrap(a, lazy)), "b": t.mint("c", wrap(b, lazy))}, nil
		},
		"conn.send": func(r request) (any, error) {
			c, err := object[*conn](t, r, "connection")
			if err != nil {
				return nil, err
			}
			kind, err := r.mustString("kind")
			if err != nil {
				return nil, err
			}
			within, err := r.within()
			if err != nil {
				return nil, err
			}
			var frame transports.Frame
			switch kind {
			case "text":
				text, err := r.string("text")
				if err != nil {
					return nil, err
				}
				frame = transports.Frame{Kind: transports.Text, Data: []byte(text)}
			case "binary":
				encoded, err := r.string("base64")
				if err != nil {
					return nil, err
				}
				data, err := base64.StdEncoding.DecodeString(encoded)
				if err != nil {
					return nil, invalid("base64: %v", err)
				}
				frame = transports.Frame{Kind: transports.Binary, Data: data}
			default:
				return nil, invalid("kind is text or binary")
			}
			ctx, cancel := context.WithTimeout(context.Background(), within)
			defer cancel()
			if err := c.Send(ctx, frame); err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					return nil, fail("timeout", "the send did not complete within %s", within)
				}
				return nil, closeError(err)
			}
			return nil, nil
		},
		"conn.receive": func(r request) (any, error) {
			c, err := object[*conn](t, r, "connection")
			if err != nil {
				return nil, err
			}
			within, err := r.within()
			if err != nil {
				return nil, err
			}
			frame, err := c.receive(within)
			if err != nil {
				if f, is := err.(*failure); is {
					return nil, f
				}
				return nil, closeError(err)
			}
			if frame.Kind == transports.Binary {
				return map[string]any{"kind": "binary", "base64": base64.StdEncoding.EncodeToString(frame.Data)}, nil
			}
			return map[string]any{"kind": "text", "text": string(frame.Data)}, nil
		},
		"conn.close": func(r request) (any, error) {
			c, err := object[*conn](t, r, "connection")
			if err != nil {
				return nil, err
			}
			code, err := r.int("code", int64(transports.CodeNormal))
			if err != nil {
				return nil, err
			}
			reason, err := r.string("reason")
			if err != nil {
				return nil, err
			}
			within, err := r.within()
			if err != nil {
				return nil, err
			}
			ctx, cancel := context.WithTimeout(context.Background(), within)
			defer cancel()
			err = c.Close(ctx, transports.Code(code), reason)
			if errors.Is(err, transports.ErrUnsendableCode) {
				// Nothing was sent and the connection stays as it was.
				return nil, invalid("close code %d may only be observed", code)
			}
			c.finish(transports.ErrClosed)
			if err != nil && !errors.Is(err, transports.ErrClosed) {
				if errors.Is(err, context.DeadlineExceeded) {
					return nil, fail("timeout", "the close did not complete within %s", within)
				}
				return nil, closeError(err)
			}
			return nil, nil
		},
		"conn.abort": func(r request) (any, error) {
			c, err := object[*conn](t, r, "connection")
			if err != nil {
				return nil, err
			}
			_ = c.Abort()
			c.finish(transports.ErrClosed)
			return nil, nil
		},
		"conn.await_close": func(r request) (any, error) {
			c, err := object[*conn](t, r, "connection")
			if err != nil {
				return nil, err
			}
			within, err := r.within()
			if err != nil {
				return nil, err
			}
			if c.lazy {
				// Nothing reads a lazy connection unasked; read until it ends.
				ctx, cancel := context.WithTimeout(context.Background(), within)
				defer cancel()
				for {
					if _, err := c.Receive(ctx); err != nil {
						if errors.Is(err, context.DeadlineExceeded) {
							return nil, fail("timeout", "the connection did not end within %s", within)
						}
						c.finish(err)
						break
					}
				}
			}
			select {
			case <-c.ended:
			case <-time.After(within):
				return nil, fail("timeout", "the connection did not end within %s", within)
			}
			var closeErr *transports.CloseError
			if errors.As(c.end, &closeErr) {
				return map[string]any{"code": int(closeErr.Code), "reason": closeErr.Reason}, nil
			}
			return map[string]any{"code": int(transports.CodeAbnormalClosure), "reason": ""}, nil
		},
	}
}
