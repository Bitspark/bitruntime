// Package websocket implements bitwire's binary message carrier.
package websocket

import (
	"context"
	"errors"
	ontos "github.com/Bitspark/bitwire/ontos/go/core"
	"io"
	"sync"
	"time"

	"github.com/Bitspark/bitruntime/core/go"
	wire "github.com/Bitspark/bitwire/wire/go"
	carrier "github.com/coder/websocket"
)

type queuedMessage struct {
	message ontos.Value
	size    int
}

// Endpoint owns one WebSocket and implements the shared interface directly.
type Endpoint struct {
	mu         sync.Mutex
	conn       *carrier.Conn
	stream     io.Closer
	limits     core.PairOptions
	closeAfter time.Duration
	ctx        context.Context
	cancel     context.CancelFunc
	workers    sync.WaitGroup
	stop       chan struct{}
	closed     chan struct{}
	readWake   chan struct{}
	writeWake  chan struct{}
	ended      bool
	result     wire.Termination
	handler    func(ontos.Value)
	attachment uint64
	in         []queuedMessage
	inBytes    int
	out        [][]byte
	outBytes   int
	outCount   int
}

func newEndpoint(conn *carrier.Conn, stream io.Closer, options normalizedOptions) *Endpoint {
	ctx, cancel := context.WithCancel(context.Background())
	e := &Endpoint{conn: conn, stream: stream, limits: options.PairOptions, closeAfter: options.CloseTimeout,
		ctx: ctx, cancel: cancel, stop: make(chan struct{}), closed: make(chan struct{}),
		readWake: make(chan struct{}, 1), writeWake: make(chan struct{}, 1)}
	conn.SetReadLimit(int64(e.limits.MaxMessageBytes))
	e.workers.Add(2)
	go e.read()
	go e.write()
	go e.dispatch()
	return e
}

func (e *Endpoint) Send(message ontos.Value) error {
	bytes, err := wire.EncodeMessage(message, e.limits.MaxMessageBytes)
	if err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.ended {
		return errors.New("wire is closed")
	}
	if e.outCount >= e.limits.MaxQueuedMessages || len(bytes) > e.limits.MaxQueuedBytes-e.outBytes {
		return errors.New("wire output queue is full")
	}
	e.out = append(e.out, bytes)
	e.outBytes += len(bytes)
	e.outCount++
	wake(e.writeWake)
	return nil // Local admission, before carrier delivery or application execution.
}

func (e *Endpoint) Receive(handler func(ontos.Value)) (func(), error) {
	if handler == nil {
		return nil, errors.New("wire handler must be nonnil")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.ended {
		return nil, errors.New("wire is closed")
	}
	if e.handler != nil {
		return nil, errors.New("wire already has a receive handler")
	}
	e.attachment++
	token := e.attachment
	e.handler = handler
	wake(e.readWake)
	var once sync.Once
	return func() {
		once.Do(func() {
			e.mu.Lock()
			defer e.mu.Unlock()
			if token == e.attachment {
				e.handler = nil
			}
		})
	}, nil
}

func (e *Endpoint) Closed() <-chan struct{} { return e.closed }
func (e *Endpoint) Termination() wire.Termination {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.result
}
func (e *Endpoint) Close() error {
	e.end(wire.Termination{Kind: "closed"})
	<-e.closed
	return nil
}

func (e *Endpoint) end(result wire.Termination) {
	e.mu.Lock()
	if e.ended {
		e.mu.Unlock()
		return
	}
	e.ended, e.result, e.handler = true, result, nil
	e.in, e.out = nil, nil
	e.inBytes, e.outBytes, e.outCount = 0, 0, 0
	close(e.stop)
	e.mu.Unlock()
	go func() {
		if result.Kind == "closed" {
			forced := make(chan struct{})
			timer := time.AfterFunc(e.closeAfter, func() {
				// CloseNow waits for an already-started handshake. That handshake
				// can own its own read context, so cancelling e.ctx is insufficient.
				_ = e.stream.Close()
				e.cancel()
				close(forced)
			})
			_ = e.conn.Close(carrier.StatusNormalClosure, "")
			if !timer.Stop() {
				<-forced
			}
		} else {
			_ = e.conn.CloseNow()
		}
		e.cancel()
		e.workers.Wait()
		close(e.closed) // The socket and carrier workers have actually been released.
	}()
}

func (e *Endpoint) read() {
	defer e.workers.Done()
	for {
		kind, bytes, err := e.conn.Read(e.ctx)
		if err != nil {
			code := carrier.CloseStatus(err)
			if code == carrier.StatusNormalClosure || code == carrier.StatusGoingAway {
				e.end(wire.Termination{Kind: "closed"})
			} else {
				e.end(wire.Termination{Kind: "failed", Message: "wire socket read failed"})
			}
			return
		}
		if kind != carrier.MessageBinary {
			e.end(wire.Termination{Kind: "failed", Message: "binary wire message required"})
			return
		}
		message, err := wire.DecodeMessage(bytes, e.limits.MaxMessageBytes)
		if err != nil {
			e.end(wire.Termination{Kind: "failed", Message: "invalid wire message"})
			return
		}
		e.mu.Lock()
		if e.ended {
			e.mu.Unlock()
			return
		}
		if len(e.in) >= e.limits.MaxQueuedMessages || len(bytes) > e.limits.MaxQueuedBytes-e.inBytes {
			e.mu.Unlock()
			e.end(wire.Termination{Kind: "failed", Message: "wire input queue is full"})
			return
		}
		e.in = append(e.in, queuedMessage{message, len(bytes)})
		e.inBytes += len(bytes)
		wake(e.readWake)
		e.mu.Unlock()
	}
}

func (e *Endpoint) write() {
	defer e.workers.Done()
	for {
		select {
		case <-e.stop:
			return
		case <-e.writeWake:
		}
		for {
			e.mu.Lock()
			if e.ended || len(e.out) == 0 {
				e.mu.Unlock()
				break
			}
			bytes := e.out[0]
			e.out[0], e.out = nil, e.out[1:]
			e.mu.Unlock()
			if err := e.conn.Write(e.ctx, carrier.MessageBinary, bytes); err != nil {
				e.end(wire.Termination{Kind: "failed", Message: "wire socket write failed"})
				return
			}
			e.mu.Lock()
			if !e.ended {
				e.outBytes -= len(bytes)
				e.outCount--
			}
			e.mu.Unlock()
		}
	}
}

func (e *Endpoint) dispatch() {
	for {
		select {
		case <-e.stop:
			return
		case <-e.readWake:
		}
		for {
			e.mu.Lock()
			if e.ended || e.handler == nil || len(e.in) == 0 {
				e.mu.Unlock()
				break
			}
			item, handler := e.in[0], e.handler
			e.in[0], e.in = queuedMessage{}, e.in[1:]
			e.inBytes -= item.size
			e.mu.Unlock()
			// Dequeued work is dispatched. Closure does not cancel that application work.
			func() {
				defer func() {
					if recover() != nil {
						e.end(wire.Termination{Kind: "failed", Message: "wire receive handler failed"})
					}
				}()
				handler(item.message)
			}()
		}
	}
}

func wake(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

var _ wire.Endpoint = (*Endpoint)(nil)
