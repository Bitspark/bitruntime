package websocket

import (
	"errors"
	"net"
	"net/http"
	"sync"

	wire "github.com/Bitspark/bitwire/wire/go"
)

// Server is an HTTP handler owning its accepted endpoints. Only Listen owns a listener.
type Server struct {
	mu        sync.Mutex
	options   Options
	connected func(*Endpoint, *http.Request)
	wires     map[*Endpoint]struct{}
	stopping  bool
	upgrading sync.WaitGroup
	once      sync.Once
	closed    chan struct{}
	http      *http.Server
	listener  net.Listener
}

func NewServer(options Options, connected func(*Endpoint, *http.Request)) (*Server, error) {
	o, err := normalize(options)
	if err != nil {
		return nil, err
	}
	if connected == nil {
		return nil, errors.New("wire connection handler must be nonnil")
	}
	return &Server{options: o.Options, connected: connected, wires: make(map[*Endpoint]struct{}), closed: make(chan struct{})}, nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	if s.stopping {
		s.mu.Unlock()
		http.Error(w, "wire server is closed", http.StatusServiceUnavailable)
		return
	}
	s.upgrading.Add(1)
	s.mu.Unlock()
	var e *Endpoint
	var err error
	var stopping bool
	func() {
		defer s.upgrading.Done()
		e, err = Accept(w, r, s.options)
		s.mu.Lock()
		stopping = s.stopping
		if err == nil && !stopping {
			s.wires[e] = struct{}{}
		}
		s.mu.Unlock()
		if e != nil && stopping {
			_ = e.Close()
		}
	}()
	if err != nil || stopping {
		return
	}
	go func() {
		<-e.Closed()
		s.mu.Lock()
		delete(s.wires, e)
		s.mu.Unlock()
	}()
	defer func() {
		if recover() != nil {
			e.end(wire.Termination{Kind: "failed", Message: "wire connection setup failed"})
		}
	}()
	s.connected(e, r)
}

func Listen(address string, options Options, connected func(*Endpoint, *http.Request)) (*Server, error) {
	s, err := NewServer(options, connected)
	if err != nil {
		return nil, err
	}
	if address == "" {
		address = "127.0.0.1:0"
	}
	s.listener, err = net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	s.http = &http.Server{Handler: s, ReadHeaderTimeout: s.options.HandshakeTimeout}
	go func() {
		_ = s.http.Serve(s.listener)
		_ = s.Close()
	}()
	return s, nil
}

// URL is nonempty only for the convenience listener created by Listen.
func (s *Server) URL() string {
	if s.listener == nil {
		return ""
	}
	return "ws://" + s.listener.Addr().String() + s.options.Path
}
func (s *Server) Closed() <-chan struct{} { return s.closed }
func (s *Server) Close() error {
	s.once.Do(func() {
		s.mu.Lock()
		s.stopping = true
		s.mu.Unlock()
		if s.http != nil {
			_ = s.http.Close()
		}
		s.upgrading.Wait()
		s.mu.Lock()
		all := make([]*Endpoint, 0, len(s.wires))
		for e := range s.wires {
			all = append(all, e)
		}
		s.mu.Unlock()
		var closing sync.WaitGroup
		for _, e := range all {
			closing.Add(1)
			go func() { defer closing.Done(); _ = e.Close() }()
		}
		closing.Wait()
		close(s.closed)
	})
	<-s.closed
	return nil
}
