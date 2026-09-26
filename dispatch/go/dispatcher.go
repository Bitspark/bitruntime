package runtime

import (
	"errors"
	"slices"
	"sync"

	"github.com/Bitspark/nightseam/duplex/go"
)

// HandlerRegistry is the explicit registration capability used by generated
// bindings. Closing it releases its registrations, not its borrowed carrier.
type HandlerRegistry interface {
	duplex.Wire
	Register([]string, duplex.Receiver) (func(), error)
	Close(duplex.Code, string) error
}

type dispatchRegistration struct {
	path     []string
	receiver duplex.Receiver
}
type dispatchRoute struct {
	name   string
	prefix bool
}

// Dispatcher owns one endpoint attachment and an explicit exact/longest-prefix
// routing policy. It captures each request's traversal on the invocation its
// return capability carries, through the public vocabulary alone, and refuses a
// request whose return capability carries none rather than routing it with
// weaker detach and cancellation guarantees. An opaque wrapper is therefore as
// good as a native endpoint: the lifecycle travels with the unchanged return
// capability, and nothing here recognizes a concrete type.
type Dispatcher struct {
	root        duplex.Endpoint
	ownEndpoint bool
	mu          sync.Mutex
	closed      bool
	detach      func()
	routes      map[dispatchRoute]*dispatchRegistration
}

// DispatcherOptions explicitly transfers closure authority for an endpoint the
// caller owns. Borrowed endpoints remain the default.
type DispatcherOptions struct{ OwnEndpoint bool }

func NewDispatcher(root duplex.Endpoint, options ...DispatcherOptions) (*Dispatcher, error) {
	if root == nil {
		return nil, errors.New("dispatcher requires an endpoint")
	}
	d := &Dispatcher{root: root, routes: map[dispatchRoute]*dispatchRegistration{}}
	if len(options) > 0 {
		d.ownEndpoint = options[0].OwnEndpoint
	}
	detach, err := root.Receive(duplex.Receiver{Message: d.deliver, Closed: func(code duplex.Code, reason string) { _ = d.Close(code, reason) }})
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	closed := d.closed
	if !closed {
		d.detach = detach
	}
	d.mu.Unlock()
	if closed {
		detach()
		return nil, ErrClosed
	}
	return d, nil
}

func (d *Dispatcher) Send(path []string, message duplex.Message) error {
	d.mu.Lock()
	closed := d.closed
	d.mu.Unlock()
	if closed {
		return ErrClosed
	}
	return d.root.Send(path, message)
}
func (d *Dispatcher) Register(path []string, receiver duplex.Receiver) (func(), error) {
	return d.register(path, receiver, false)
}
func (d *Dispatcher) RegisterPrefix(path []string, receiver duplex.Receiver) (func(), error) {
	return d.register(path, receiver, true)
}
func (d *Dispatcher) register(path []string, receiver duplex.Receiver, prefix bool) (func(), error) {
	name, err := duplex.EncodePath(path)
	if err != nil {
		return nil, err
	}
	key := dispatchRoute{name, prefix}
	registration := &dispatchRegistration{path: slices.Clone(path), receiver: receiver}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil, ErrClosed
	}
	if d.routes[key] != nil {
		return nil, duplex.ErrReceiverExists
	}
	d.routes[key] = registration
	return func() {
		d.mu.Lock()
		if d.routes[key] == registration {
			delete(d.routes, key)
		}
		d.mu.Unlock()
	}, nil
}
func (d *Dispatcher) match(path []string, name string) *dispatchRegistration {
	if exact := d.routes[dispatchRoute{name, false}]; exact != nil {
		return exact
	}
	var selected *dispatchRegistration
	for key, candidate := range d.routes {
		if key.prefix && len(candidate.path) <= len(path) && (selected == nil || len(candidate.path) > len(selected.path)) && slices.Equal(candidate.path, path[:len(candidate.path)]) {
			selected = candidate
		}
	}
	return selected
}
func (d *Dispatcher) deliver(path []string, message duplex.Message) {
	name, err := duplex.EncodePath(path)
	if err != nil {
		return
	}
	// A control belongs to the traversal that captured it, never to the
	// registration in force now. Handing it to the invocation is what keeps a
	// detach or a rebind from retargeting an admitted request.
	if message.Frame.Kind == duplex.ProfileCancel {
		_ = RelayInvocationControl(message)
		return
	}
	d.mu.Lock()
	var registration *dispatchRegistration
	if !d.closed {
		registration = d.match(path, name)
	}
	d.mu.Unlock()
	if registration == nil || registration.receiver.Message == nil {
		if message.Frame.Kind == duplex.ProfileRequest {
			_ = sendWireResponse(message, nil, &PublicError{Code: "method_not_found", Message: "Unknown method"})
		}
		return
	}
	delivered := slices.Clone(path)
	if message.Frame.Kind != duplex.ProfileRequest {
		registration.receiver.Message(delivered, message)
		return
	}
	capture, err := CaptureInvocation(message, func(control duplex.Message) {
		registration.receiver.Message(slices.Clone(delivered), control)
	})
	if err != nil {
		// A bound reached is a refusal to try again at; a capability that
		// carries no lifecycle is a request this dispatcher cannot route with
		// the guarantees it advertises.
		refusal := &PublicError{Code: "invalid_message", Message: "Invocation requires the lifecycle its return capability carries"}
		if errors.Is(err, ErrInvocationLimit) {
			refusal = &PublicError{Code: "busy", Message: "Invocation participation limit reached"}
		}
		_ = sendWireResponse(message, nil, refusal)
		return
	}
	defer capture.Ready()
	registration.receiver.Message(delivered, message)
}

func (d *Dispatcher) Close(code duplex.Code, reason string) error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	detach, routes := d.detach, d.routes
	d.detach, d.routes = nil, nil
	d.mu.Unlock()
	if detach != nil {
		detach()
	}
	for _, registration := range routes {
		if registration.receiver.Closed != nil {
			func() { defer func() { _ = recover() }(); registration.receiver.Closed(code, reason) }()
		}
	}
	if d.ownEndpoint {
		return d.root.Close(code, reason)
	}
	return nil
}

// Select returns a receiving view of this shared dispatcher. The view owns its
// prefix route and never acquires closure authority over the root endpoint.
func (d *Dispatcher) Select(path []string) *SelectedEndpoint {
	return &SelectedEndpoint{owner: d, prefix: slices.Clone(path)}
}

type SelectedEndpoint struct {
	owner      *Dispatcher
	prefix     []string
	mu         sync.Mutex
	closed     bool
	attachment *selectedAttachment
}
type selectedAttachment struct {
	receiver duplex.Receiver
	detach   func()
}

func (s *SelectedEndpoint) Select(path []string) *SelectedEndpoint {
	return s.owner.Select(append(slices.Clone(s.prefix), path...))
}
func (s *SelectedEndpoint) Send(path []string, message duplex.Message) error {
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return ErrClosed
	}
	return s.owner.Send(append(slices.Clone(s.prefix), path...), message)
}
func (s *SelectedEndpoint) Receive(receiver duplex.Receiver) (func(), error) {
	attachment := &selectedAttachment{receiver: receiver}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrClosed
	}
	if s.attachment != nil {
		s.mu.Unlock()
		return nil, duplex.ErrReceiverExists
	}
	s.attachment = attachment
	s.mu.Unlock()
	detach, err := s.owner.RegisterPrefix(s.prefix, duplex.Receiver{
		Message: func(path []string, message duplex.Message) {
			if receiver.Message != nil {
				receiver.Message(slices.Clone(path[len(s.prefix):]), message)
			} else if message.Frame.Kind == duplex.ProfileRequest {
				_ = sendWireResponse(message, nil, &PublicError{Code: "method_not_found", Message: "Unknown method"})
			}
		},
		Closed: func(code duplex.Code, reason string) { s.remove(attachment, true, code, reason) },
	})
	s.mu.Lock()
	active := s.attachment == attachment
	if err != nil && active {
		s.attachment = nil
	}
	if err == nil && active {
		attachment.detach = detach
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if !active {
		detach()
		return nil, ErrClosed
	}
	return func() { s.remove(attachment, false, 0, "") }, nil
}
func (s *SelectedEndpoint) remove(attachment *selectedAttachment, tell bool, code duplex.Code, reason string) {
	s.mu.Lock()
	if s.attachment != attachment {
		s.mu.Unlock()
		return
	}
	s.attachment = nil
	detach := attachment.detach
	s.mu.Unlock()
	if detach != nil {
		detach()
	}
	if tell && attachment.receiver.Closed != nil {
		attachment.receiver.Closed(code, reason)
	}
}
func (s *SelectedEndpoint) Close(code duplex.Code, reason string) error {
	s.mu.Lock()
	s.closed = true
	attachment := s.attachment
	s.mu.Unlock()
	if attachment != nil {
		s.remove(attachment, true, code, reason)
	}
	return nil
}
