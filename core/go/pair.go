// Package core implements generic local message delivery and byte-keyed trees.
package core

import (
	"errors"
	ontos "github.com/Bitspark/bitwire/ontos/go/core"
	wire "github.com/Bitspark/bitwire/wire/go"
	"sync"
)

type PairOptions struct{ MaxMessageBytes, MaxQueuedBytes, MaxQueuedMessages int }

func NormalizeOptions(o PairOptions) (PairOptions, error) {
	if o.MaxMessageBytes < 0 || o.MaxQueuedBytes < 0 || o.MaxQueuedMessages < 0 {
		return o, errors.New("wire limits must be positive")
	}
	if o.MaxMessageBytes == 0 {
		o.MaxMessageBytes = wire.DefaultMaxMessageBytes
	}
	if o.MaxQueuedBytes == 0 {
		o.MaxQueuedBytes = 64 * 1024 * 1024
	}
	if o.MaxQueuedMessages == 0 {
		o.MaxQueuedMessages = 1024
	}
	return o, nil
}

type queued struct {
	e    ontos.Value
	size int
}
type pairState struct {
	mu   sync.Mutex
	ends [2]*PairEndpoint
}
type PairEndpoint struct {
	state      *pairState
	index      int
	options    PairOptions
	queue      []queued
	bytes      int
	handler    func(ontos.Value)
	attachment uint64
	ended      bool
	result     wire.Termination
	closed     chan struct{}
	wake       chan struct{}
}

func NewPair(options PairOptions) (*PairEndpoint, *PairEndpoint, error) {
	o, err := NormalizeOptions(options)
	if err != nil {
		return nil, nil, err
	}
	s := &pairState{}
	for i := range s.ends {
		s.ends[i] = &PairEndpoint{state: s, index: i, options: o, closed: make(chan struct{}), wake: make(chan struct{}, 1)}
	}
	for _, e := range s.ends {
		go e.dispatch()
	}
	return s.ends[0], s.ends[1], nil
}
func (e *PairEndpoint) Send(message ontos.Value) error {
	bytes, err := wire.EncodeMessage(message, e.options.MaxMessageBytes)
	if err != nil {
		return err
	}
	s := e.state
	s.mu.Lock()
	defer s.mu.Unlock()
	peer := s.ends[1-e.index]
	if e.ended || peer.ended {
		return errors.New("wire is closed")
	}
	if len(bytes) > peer.options.MaxMessageBytes || len(peer.queue) >= peer.options.MaxQueuedMessages || len(bytes) > peer.options.MaxQueuedBytes-peer.bytes {
		s.finish(wire.Termination{Kind: "failed", Message: "wire input limit exceeded"})
		return errors.New("wire input limit exceeded")
	}
	peer.queue = append(peer.queue, queued{message, len(bytes)})
	peer.bytes += len(bytes)
	peer.signal()
	return nil
}
func (e *PairEndpoint) Receive(handler func(ontos.Value)) (func(), error) {
	if handler == nil {
		return nil, errors.New("wire handler must be nonnil")
	}
	s := e.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if e.ended {
		return nil, errors.New("wire is closed")
	}
	if e.handler != nil {
		return nil, errors.New("wire already has a receive handler")
	}
	e.attachment++
	token := e.attachment
	e.handler = handler
	e.signal()
	var once sync.Once
	return func() {
		once.Do(func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			if token == e.attachment {
				e.handler = nil
			}
		})
	}, nil
}
func (e *PairEndpoint) Closed() <-chan struct{} { return e.closed }
func (e *PairEndpoint) Termination() wire.Termination {
	e.state.mu.Lock()
	defer e.state.mu.Unlock()
	return e.result
}
func (e *PairEndpoint) Close() error {
	e.state.mu.Lock()
	e.state.finish(wire.Termination{Kind: "closed"})
	e.state.mu.Unlock()
	return nil
}
func (s *pairState) finish(result wire.Termination) {
	for _, e := range s.ends {
		if !e.ended {
			e.ended = true
			e.result = result
			e.handler = nil
			e.queue = nil
			e.bytes = 0
			close(e.closed)
		}
	}
}
func (e *PairEndpoint) signal() {
	select {
	case e.wake <- struct{}{}:
	default:
	}
}
func (e *PairEndpoint) dispatch() {
	for {
		select {
		case <-e.closed:
			return
		case <-e.wake:
		}
		for {
			e.state.mu.Lock()
			if e.ended || e.handler == nil || len(e.queue) == 0 {
				e.state.mu.Unlock()
				break
			}
			item := e.queue[0]
			e.queue[0] = queued{}
			e.queue = e.queue[1:]
			e.bytes -= item.size
			handler := e.handler
			e.state.mu.Unlock()
			// Dequeued work is already dispatched. Closure does not cancel that work.
			func() {
				defer func() {
					if recover() != nil {
						e.state.mu.Lock()
						e.state.finish(wire.Termination{Kind: "failed", Message: "wire receive handler failed"})
						e.state.mu.Unlock()
					}
				}()
				handler(item.e)
			}()
		}
	}
}

var _ wire.Endpoint = (*PairEndpoint)(nil)
