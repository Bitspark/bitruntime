package dispatch

import (
	"errors"
	"slices"
	"sync"

	core "github.com/Bitspark/bitruntime/core/go"
	"github.com/Bitspark/bitruntime/internal/profile/go"
	transports "github.com/Bitspark/bitruntime/transports/go"
	wire "github.com/Bitspark/bitwire/wire/go"
)

type dispatchRegistration struct {
	path     []string
	receiver wire.Receiver
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
// capability, and nothing here recognizes a concrete type. A cancellation goes
// to the traversal that captured its request, never to the route now in force.
type Dispatcher struct {
	root        wire.Endpoint
	ownEndpoint bool
	mu          sync.Mutex
	closed      bool
	detach      func()
	routes      map[dispatchRoute]*dispatchRegistration
}

// DispatcherOptions explicitly transfers closure authority for an endpoint the
// caller owns. Borrowed endpoints remain the default.
type DispatcherOptions struct{ OwnEndpoint bool }

// NewDispatcher attaches to root and routes what it delivers. The endpoint is
// borrowed unless options transfer its closure.
func NewDispatcher(root wire.Endpoint, options ...DispatcherOptions) (*Dispatcher, error) {
	if root == nil {
		return nil, errors.New("bitruntime: a dispatcher requires an endpoint")
	}
	d := &Dispatcher{root: root, routes: map[dispatchRoute]*dispatchRegistration{}}
	if len(options) > 0 {
		d.ownEndpoint = options[0].OwnEndpoint
	}
	detach, err := root.Receive(wire.Receiver{Message: d.deliver, Closed: func(code wire.Code, reason string) { _ = d.Close(code, reason) }})
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
		return nil, transports.ErrClosed
	}
	return d, nil
}

// Send sends through the borrowed root.
func (d *Dispatcher) Send(path []string, message wire.Message) error {
	d.mu.Lock()
	closed := d.closed
	d.mu.Unlock()
	if closed {
		return transports.ErrClosed
	}
	return d.root.Send(path, message)
}

// Register routes exactly path to receiver. A path has one registration.
func (d *Dispatcher) Register(path []string, receiver wire.Receiver) (func(), error) {
	return d.register(path, receiver, false)
}

// RegisterPrefix routes path and every path beneath it, the longest registered
// prefix winning, unless an exact registration matches.
func (d *Dispatcher) RegisterPrefix(path []string, receiver wire.Receiver) (func(), error) {
	return d.register(path, receiver, true)
}

func (d *Dispatcher) register(path []string, receiver wire.Receiver, prefix bool) (func(), error) {
	name, err := profile.EncodePath(path)
	if err != nil {
		return nil, core.ErrInvalidPath
	}
	key := dispatchRoute{name, prefix}
	registration := &dispatchRegistration{path: slices.Clone(path), receiver: receiver}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil, transports.ErrClosed
	}
	if d.routes[key] != nil {
		return nil, core.ErrReceiverExists
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

func (d *Dispatcher) deliver(path []string, message wire.Message) {
	name, err := profile.EncodePath(path)
	if err != nil {
		return
	}
	// A control belongs to the traversal that captured it, never to the
	// registration in force now. Handing it to the invocation is what keeps a
	// detach or a rebind from retargeting an admitted request.
	if message.Frame.Kind == wire.ProfileCancel {
		_ = core.RelayInvocationControl(message)
		return
	}
	d.mu.Lock()
	var registration *dispatchRegistration
	if !d.closed {
		registration = d.match(path, name)
	}
	d.mu.Unlock()
	if registration == nil || registration.receiver.Message == nil {
		if message.Frame.Kind == wire.ProfileRequest {
			_ = core.Respond(message, nil, &core.PublicError{Code: "method_not_found", Message: "Unknown method"})
		}
		return
	}
	delivered := slices.Clone(path)
	if message.Frame.Kind != wire.ProfileRequest {
		registration.receiver.Message(delivered, message)
		return
	}
	capture, err := core.CaptureInvocation(message, func(control wire.Message) {
		registration.receiver.Message(slices.Clone(delivered), control)
	})
	if err != nil {
		// A bound reached is a refusal to try again at; a capability that
		// carries no lifecycle is a request this dispatcher cannot route with
		// the guarantees it advertises.
		refusal := &core.PublicError{Code: "invalid_message", Message: "Invocation requires the lifecycle its return capability carries"}
		if errors.Is(err, core.ErrInvocationLimit) {
			refusal = &core.PublicError{Code: "busy", Message: "Invocation participation limit reached"}
		}
		_ = core.Respond(message, nil, refusal)
		return
	}
	defer capture.Ready()
	registration.receiver.Message(delivered, message)
}

// Close releases the routes and the root attachment, and closes the root only
// when this dispatcher owns it.
func (d *Dispatcher) Close(code wire.Code, reason string) error {
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

// SelectedEndpoint is a receiving view of a shared dispatcher at a prefix.
type SelectedEndpoint struct {
	owner      *Dispatcher
	prefix     []string
	mu         sync.Mutex
	closed     bool
	attachment *selectedAttachment
}
type selectedAttachment struct {
	receiver wire.Receiver
	detach   func()
}

// Select narrows the view by a further prefix.
func (s *SelectedEndpoint) Select(path []string) *SelectedEndpoint {
	return s.owner.Select(append(slices.Clone(s.prefix), path...))
}

// Send sends beneath the view's prefix through the shared root.
func (s *SelectedEndpoint) Send(path []string, message wire.Message) error {
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return transports.ErrClosed
	}
	return s.owner.Send(append(slices.Clone(s.prefix), path...), message)
}

// Receive attaches the view's one receiver, which sees paths relative to the
// view's prefix.
func (s *SelectedEndpoint) Receive(receiver wire.Receiver) (func(), error) {
	attachment := &selectedAttachment{receiver: receiver}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, transports.ErrClosed
	}
	if s.attachment != nil {
		s.mu.Unlock()
		return nil, core.ErrReceiverExists
	}
	s.attachment = attachment
	s.mu.Unlock()
	detach, err := s.owner.RegisterPrefix(s.prefix, wire.Receiver{
		Message: func(path []string, message wire.Message) {
			if receiver.Message != nil {
				receiver.Message(slices.Clone(path[len(s.prefix):]), message)
			} else if message.Frame.Kind == wire.ProfileRequest {
				_ = core.Respond(message, nil, &core.PublicError{Code: "method_not_found", Message: "Unknown method"})
			}
		},
		Closed: func(code wire.Code, reason string) { s.remove(attachment, true, code, reason) },
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
		return nil, transports.ErrClosed
	}
	return func() { s.remove(attachment, false, 0, "") }, nil
}

func (s *SelectedEndpoint) remove(attachment *selectedAttachment, tell bool, code wire.Code, reason string) {
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

// Close ends the view's route; it never closes the shared root.
func (s *SelectedEndpoint) Close(code wire.Code, reason string) error {
	s.mu.Lock()
	s.closed = true
	attachment := s.attachment
	s.mu.Unlock()
	if attachment != nil {
		s.remove(attachment, true, code, reason)
	}
	return nil
}
