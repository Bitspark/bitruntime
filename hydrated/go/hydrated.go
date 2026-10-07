// Package hydrated is evidence for bitwire decision 0019 (proposed): the first
// edition of the hydrated wire protocol, bitwire/hydrated/1, realized in Go. It
// is not a released API; the record decides, this package demonstrates.
//
// A Scope is one participant's hydration state for one incarnation. Its
// dispatcher owns the participant's addressed receive slot and hands the Scope
// every value addressed to the participant's own path, with the received context
// the composition establishes. Domain code sees only Values, Wires and Endpoints.
package hydrated

import (
	"crypto/rand"
	"errors"
	"fmt"
	"strconv"
	"sync"

	ontos "github.com/Bitspark/bitwire/ontos/go/core"
	wire "github.com/Bitspark/bitwire/wire/go"
)

// Value is a hydrated value: a ground ontos value, a Tuple or a Wire.
type Value any

// Wire sends one hydrated value. Only a local Endpoint's sending face or a proxy
// imported in the same namespace can travel inside a hydrated value (D7).
type Wire interface{ Send(Value) error }

// Context is what the composition establishes about an arrival, for example a
// checked origin. It is never part of a value (D4).
type Context any

// Local is the context of a send that never left the process.
type Local struct{}

// Refusal names a refusal. At the owner it is a host diagnostic; at the sender
// it refuses the send before admission. It is never an outcome (D8).
type Refusal string

func (r Refusal) Error() string { return string(r) }

const (
	MalformedFrame   Refusal = "malformed-frame"
	StaleScope       Refusal = "stale-scope"
	UnknownExport    Refusal = "unknown-export"
	Limit            Refusal = "limit"
	TargetRefused    Refusal = "target-refused"
	Unexportable     Refusal = "unexportable"
	ForeignNamespace Refusal = "foreign-namespace"
	ScopeEnded       Refusal = "scope-ended"
)

var (
	header   = ontos.NewAtom([]byte("bitwire/hydrated/1"))
	tupleTag = ontos.NewAtom([]byte{1})
	wireTag  = ontos.NewAtom([]byte{2})
)

// Tuple is a hydrated tuple with at least one Wire beneath it. NewTuple returns
// the ground tuple when there is none (D1).
type Tuple struct{ items []Value }

// NewTuple captures items. Each must be an ontos value, a Tuple or a Wire. An
// Endpoint is captured as its sending face: a value never carries receive or
// close authority, on any path (D7).
func NewTuple(items ...Value) (Value, error) {
	captured := make([]Value, len(items))
	for i, item := range items {
		captured[i] = sendOnly(item)
	}
	ground := make([]ontos.Value, 0, len(captured))
	live := false
	for _, item := range captured {
		switch x := item.(type) {
		case ontos.Atom:
			ground = append(ground, x)
		case ontos.Tuple:
			ground = append(ground, x)
		case Tuple, Wire:
			live = true
		default:
			return nil, errors.New("hydrated: not a value")
		}
	}
	if live {
		return Tuple{captured}, nil
	}
	return ontos.NewTuple(ground...), nil
}

// sendOnly projects an Endpoint to its sending face.
func sendOnly(v Value) Value {
	if e, ok := v.(*Endpoint); ok {
		return e.face
	}
	return v
}

// Items returns the tuple's items.
func (t Tuple) Items() []Value { return append([]Value(nil), t.items...) }

// Items returns any tuple's items, ground or hydrated.
func Items(v Value) ([]Value, bool) {
	switch x := v.(type) {
	case Tuple:
		return x.Items(), true
	case ontos.Tuple:
		out := make([]Value, x.Len())
		for i, item := range x.Items() {
			out[i] = item
		}
		return out, true
	}
	return nil, false
}

// Namespace identifies one set of participants addressed by absolute paths.
// Proxies remember theirs, so recognition is runtime-wide within it (D6).
type Namespace struct{ name string }

// NewNamespace creates a namespace.
func NewNamespace(name string) *Namespace { return &Namespace{name} }

// Reference is the ground form of a live Wire (D2, D3).
type Reference struct {
	Path      wire.Path
	Scope, ID ontos.Atom
}

func (r Reference) value() ontos.Value {
	segments := make([]ontos.Value, len(r.Path))
	for i, a := range r.Path {
		segments[i] = a
	}
	return ontos.NewTuple(wireTag, ontos.NewTuple(ontos.NewTuple(segments...), r.Scope, r.ID))
}

// Payload is the reference's ground payload ((path...), scope, id), for
// composition bootstrap over ground data.
func (r Reference) Payload() ontos.Value {
	return r.value().(ontos.Tuple).At(1)
}

// ReadReference reads a reference payload.
func ReadReference(v ontos.Value) (Reference, error) { return readReference(v, DefaultLimits.Depth) }

func (r Reference) key() string {
	k := strconv.Itoa(len(r.Path))
	for _, a := range r.Path {
		k += "/" + strconv.Itoa(a.Len()) + ":" + string(a.Bytes())
	}
	return k + "|" + string(r.Scope.Bytes()) + "|" + string(r.ID.Bytes())
}

// Endpoint is a local HydratedEndpoint: one receiver, ordered non-inline
// dispatch, a bounded queue and an owner lifetime. Its sending face is what a
// value carries; closing it ends its export in every scope (D7).
type Endpoint struct {
	mu      sync.Mutex
	queue   []delivery
	limit   int
	handler func(Value, Context)
	gen     uint64 // receiver generation: a detach removes only its own receiver
	failure error  // set when a receiver failed and so terminated the endpoint
	closed  bool
	done    chan struct{}
	wake    chan struct{}
	face    *face
	exports map[*Scope]ontos.Atom
}

type delivery struct {
	value   Value
	context Context
}

// face is an Endpoint's sending face. It grants sending only.
type face struct{ e *Endpoint }

func (f *face) Send(v Value) error { return f.e.admit(sendOnly(v), Local{}) }

// NewEndpoint creates an endpoint whose queue admits at most limit values.
func NewEndpoint(limit int) *Endpoint {
	if limit <= 0 {
		limit = 1
	}
	e := &Endpoint{limit: limit, done: make(chan struct{}), wake: make(chan struct{}, 1), exports: map[*Scope]ontos.Atom{}}
	e.face = &face{e}
	go e.dispatch()
	return e
}

// Wire is the endpoint's sending face.
func (e *Endpoint) Wire() Wire { return e.face }

// Send admits a value locally, as its sending face does.
func (e *Endpoint) Send(v Value) error { return e.face.Send(v) }

// Receive attaches the one receiver. Values are dispatched in admission order,
// never inline with the admitting send.
func (e *Endpoint) Receive(handler func(Value, Context)) (detach func(), err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.handler != nil {
		return nil, errors.New("hydrated: receiver already attached")
	}
	e.gen++
	gen := e.gen
	e.handler = handler
	e.signal()
	return func() {
		e.mu.Lock()
		if e.gen == gen {
			e.handler = nil
		}
		e.mu.Unlock()
	}, nil
}

// Failure reports the receiver failure that terminated the endpoint, if any.
func (e *Endpoint) Failure() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.failure
}

// Closed is closed when the endpoint closes.
func (e *Endpoint) Closed() <-chan struct{} { return e.done }

// Close ends the endpoint and withdraws its export from every scope.
func (e *Endpoint) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	exports := e.exports
	e.exports = map[*Scope]ontos.Atom{}
	e.queue = nil
	close(e.done)
	e.mu.Unlock()
	for s, id := range exports {
		s.withdraw(e, id)
	}
	return nil
}

func (e *Endpoint) admit(v Value, ctx Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed || len(e.queue) >= e.limit {
		return TargetRefused
	}
	e.queue = append(e.queue, delivery{v, ctx})
	e.signal()
	return nil
}

func (e *Endpoint) signal() {
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

func (e *Endpoint) dispatch() {
	for {
		select {
		case <-e.done:
			return
		case <-e.wake:
		}
		for {
			e.mu.Lock()
			if e.closed || e.handler == nil || len(e.queue) == 0 {
				e.mu.Unlock()
				break
			}
			d, h := e.queue[0], e.handler
			e.queue = e.queue[1:]
			e.mu.Unlock()
			if err := deliverTo(h, d); err != nil {
				// A receiver failure terminates the endpoint, which ends its export.
				e.mu.Lock()
				e.failure = err
				e.mu.Unlock()
				_ = e.Close()
				return
			}
		}
	}
}

func deliverTo(h func(Value, Context), d delivery) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("hydrated: receiver failed: %v", r)
		}
	}()
	h(d.value, d.context)
	return nil
}

// register records an export; it refuses a closed endpoint.
func (e *Endpoint) register(s *Scope, id ontos.Atom) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return false
	}
	e.exports[s] = id
	return true
}

func (e *Endpoint) unregister(s *Scope) {
	e.mu.Lock()
	delete(e.exports, s)
	e.mu.Unlock()
}

func (e *Endpoint) isClosed() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.closed
}

// proxy is an imported Wire. It carries its reference and namespace, so any
// participant in the namespace forwards it unchanged (D6). A proxy with a dead
// reason is this participant's own stale or withdrawn reference: its sends are
// refused locally (D7, returning home).
type proxy struct {
	ref  Reference
	ns   *Namespace
	via  *Scope
	dead Refusal
}

func (p *proxy) Send(v Value) error {
	if p.dead != "" {
		return p.dead
	}
	return p.via.send(p.ref, v)
}

// Limits bounds a scope (D8).
type Limits struct{ Exports, Nodes, Depth, Bytes int }

// DefaultLimits are finite defaults for evidence; production configures its own.
var DefaultLimits = Limits{Exports: 64, Nodes: 4096, Depth: 64, Bytes: 1 << 20}

// Scope is one participant's hydration state for one incarnation (Terms).
type Scope struct {
	encodeMu   sync.Mutex // encoding and registration are one step per scope (D5)
	mu         sync.Mutex
	path       wire.Path
	token      ontos.Atom
	ns         *Namespace
	sender     wire.AddressedWire
	limits     Limits
	byID       map[string]*Endpoint
	byEndpoint map[*Endpoint]ontos.Atom
	closed     bool
}

// NewScope creates a scope at path in ns with a fresh random token. sender
// carries frames to owner paths; the composition supplies its routing.
func NewScope(ns *Namespace, path wire.Path, sender wire.AddressedWire, limits Limits) (*Scope, error) {
	if limits.Exports <= 0 || limits.Nodes <= 0 || limits.Depth <= 0 || limits.Bytes <= 0 {
		return nil, errors.New("hydrated: limits must be positive")
	}
	token := make([]byte, 16)
	if _, err := rand.Read(token); err != nil {
		return nil, err
	}
	return &Scope{
		path: append(wire.Path(nil), path...), token: ontos.NewAtom(token), ns: ns, sender: sender,
		limits: limits, byID: map[string]*Endpoint{}, byEndpoint: map[*Endpoint]ontos.Atom{},
	}, nil
}

// Token is the scope token.
func (s *Scope) Token() ontos.Atom { return s.token }

// Live reports the number of live exports.
func (s *Scope) Live() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.byEndpoint)
}

// Expose exports an endpoint's face for composition bootstrap, such as a
// service's well-known reference. Domain adapters never call it.
func (s *Scope) Expose(e *Endpoint) (Reference, error) {
	s.encodeMu.Lock()
	defer s.encodeMu.Unlock()
	staged := map[*Endpoint]ontos.Atom{}
	id, err := s.exportID(e, staged)
	if err != nil {
		return Reference{}, err
	}
	if err := s.commit(staged); err != nil {
		return Reference{}, err
	}
	return Reference{s.path, s.token, id}, nil
}

// Connect returns a proxy for a reference, for composition bootstrap.
func (s *Scope) Connect(r Reference) Wire { return s.importRef(r, map[string]Wire{}) }

// Close ends the scope: its exports end and its references go stale. It closes
// no domain endpoint.
func (s *Scope) Close() {
	s.mu.Lock()
	s.closed = true
	endpoints := s.byEndpoint
	s.byEndpoint, s.byID = map[*Endpoint]ontos.Atom{}, map[string]*Endpoint{}
	s.mu.Unlock()
	for e := range endpoints {
		e.unregister(s)
	}
}

func (s *Scope) withdraw(e *Endpoint, id ontos.Atom) {
	s.mu.Lock()
	if s.byID[string(id.Bytes())] == e {
		delete(s.byID, string(id.Bytes()))
		delete(s.byEndpoint, e)
	}
	s.mu.Unlock()
}

// exportID returns e's export id in this scope, staging a new one if needed. A
// new id is 16 octets from a cryptographically secure source, redrawn if it
// equals a live or staged id, so no reference reveals another's (D3).
func (s *Scope) exportID(e *Endpoint, staged map[*Endpoint]ontos.Atom) (ontos.Atom, error) {
	if e.isClosed() {
		return ontos.Atom{}, Unexportable
	}
	if id, ok := staged[e]; ok {
		return id, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ontos.Atom{}, ScopeEnded
	}
	if id, ok := s.byEndpoint[e]; ok {
		return id, nil
	}
	if len(s.byEndpoint)+len(staged) >= s.limits.Exports {
		return ontos.Atom{}, Limit
	}
	for {
		b := make([]byte, 16)
		if _, err := rand.Read(b); err != nil {
			return ontos.Atom{}, err
		}
		id := ontos.NewAtom(b)
		if s.byID[string(b)] == nil && !stagedID(staged, id) {
			staged[e] = id
			return id, nil
		}
	}
}

func stagedID(staged map[*Endpoint]ontos.Atom, id ontos.Atom) bool {
	for _, other := range staged {
		if other.Equal(id) {
			return true
		}
	}
	return false
}

// commit registers staged exports before the frame is admitted (D5). An
// endpoint closed meanwhile is not registered; its reference is simply dead.
func (s *Scope) commit(staged map[*Endpoint]ontos.Atom) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ScopeEnded
	}
	for e, id := range staged {
		if _, ok := s.byEndpoint[e]; ok {
			continue
		}
		if e.register(s, id) {
			s.byEndpoint[e] = id
			s.byID[string(id.Bytes())] = e
		}
	}
	return nil
}

type budget struct{ nodes, bytes int }

// The node, depth and byte bounds count the hydrated value, identically when
// sending and receiving (D8): each atom, tuple and Wire leaf is one node at its
// tuple depth; bytes are atom bytes plus each Wire leaf's reference material.
func refBytes(path wire.Path) int {
	n := 32 // scope token and export id
	for _, a := range path {
		n += a.Len()
	}
	return n
}

func (s *Scope) spend(b *budget, depth, n int) error {
	b.nodes++
	b.bytes += n
	if b.nodes > s.limits.Nodes || depth > s.limits.Depth || b.bytes > s.limits.Bytes {
		return Limit
	}
	return nil
}

// send encodes v atomically and registers its new exports in one step per
// scope, then admits one frame to the owner path (D5).
func (s *Scope) send(dest Reference, v Value) error {
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return ScopeEnded
	}
	frame, err := s.stage(dest, v)
	if err != nil {
		return err
	}
	return s.sender.Send(dest.Path, frame)
}

func (s *Scope) stage(dest Reference, v Value) (ontos.Value, error) {
	s.encodeMu.Lock()
	defer s.encodeMu.Unlock()
	staged := map[*Endpoint]ontos.Atom{}
	body, err := s.encode(v, 0, staged, &budget{})
	if err != nil {
		return nil, err
	}
	frame := ontos.NewTuple(header, dest.Scope, dest.ID, body)
	if _, err := wire.EncodeMessage(frame, s.limits.Bytes); err != nil {
		return nil, Limit
	}
	return frame, s.commit(staged)
}

func (s *Scope) encode(v Value, depth int, staged map[*Endpoint]ontos.Atom, b *budget) (ontos.Value, error) {
	switch x := v.(type) {
	case ontos.Atom:
		return x, s.spend(b, depth, x.Len())
	case ontos.Tuple, Tuple:
		if err := s.spend(b, depth, 0); err != nil {
			return nil, err
		}
		items, _ := Items(x)
		out := make([]ontos.Value, len(items))
		for i, item := range items {
			var err error
			if out[i], err = s.encode(item, depth+1, staged, b); err != nil {
				return nil, err
			}
		}
		return ontos.NewTuple(tupleTag, ontos.NewTuple(out...)), nil
	case *Endpoint:
		return s.encode(x.face, depth, staged, b)
	case *face:
		if err := s.spend(b, depth, refBytes(s.path)); err != nil {
			return nil, err
		}
		id, err := s.exportID(x.e, staged)
		if err != nil {
			return nil, err
		}
		return Reference{s.path, s.token, id}.value(), nil
	case *proxy:
		if x.ns != s.ns {
			return nil, ForeignNamespace
		}
		return x.ref.value(), s.spend(b, depth, refBytes(x.ref.Path))
	case Wire:
		return nil, Unexportable
	}
	return nil, errors.New("hydrated: not a value")
}

// Deliver decides one value addressed to this participant's own path (D4). The
// returned refusal is a host diagnostic; nothing is sent back.
func (s *Scope) Deliver(path wire.Path, message ontos.Value, ctx Context) error {
	if !wire.PathEqual(path, s.path) {
		return MalformedFrame
	}
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return ScopeEnded
	}
	if _, err := wire.EncodeMessage(message, s.limits.Bytes); err != nil {
		return Limit // the whole frame counts, reference material included (D8)
	}
	f, ok := message.(ontos.Tuple)
	if !ok || f.Len() != 4 || !header.Equal(f.At(0)) {
		return MalformedFrame
	}
	token, ok1 := f.At(1).(ontos.Atom)
	id, ok2 := f.At(2).(ontos.Atom)
	if !ok1 || !ok2 || token.Len() != 16 || id.Len() != 16 {
		return MalformedFrame
	}
	if !token.Equal(s.token) {
		return StaleScope
	}
	s.mu.Lock()
	target := s.byID[string(id.Bytes())]
	s.mu.Unlock()
	if target == nil {
		return UnknownExport
	}
	value, err := s.decode(f.At(3), 0, map[string]Wire{}, &budget{})
	if err != nil {
		return err
	}
	return target.admit(value, ctx)
}

// decode reads a body. Occurrences of one reference within the value share one
// proxy; nothing is retained across messages (D7).
func (s *Scope) decode(v ontos.Value, depth int, interned map[string]Wire, b *budget) (Value, error) {
	if a, ok := v.(ontos.Atom); ok {
		return a, s.spend(b, depth, a.Len())
	}
	t, ok := v.(ontos.Tuple)
	if !ok || t.Len() != 2 {
		return nil, MalformedFrame
	}
	tag, ok := t.At(0).(ontos.Atom)
	if !ok || tag.Len() != 1 {
		return nil, MalformedFrame
	}
	switch {
	case tag.Equal(tupleTag):
		children, ok := t.At(1).(ontos.Tuple)
		if !ok {
			return nil, MalformedFrame
		}
		if err := s.spend(b, depth, 0); err != nil {
			return nil, err
		}
		items := make([]Value, children.Len())
		for i, c := range children.Items() {
			var err error
			if items[i], err = s.decode(c, depth+1, interned, b); err != nil {
				return nil, err
			}
		}
		return NewTuple(items...)
	case tag.Equal(wireTag):
		r, err := readReference(t.At(1), s.limits.Depth)
		if err != nil {
			return nil, err
		}
		if err := s.spend(b, depth, refBytes(r.Path)); err != nil {
			return nil, err
		}
		return s.importRef(r, interned), nil
	}
	return nil, MalformedFrame
}

func readReference(v ontos.Value, maxDepth int) (Reference, error) {
	t, ok := v.(ontos.Tuple)
	if !ok || t.Len() != 3 {
		return Reference{}, MalformedFrame
	}
	p, ok := t.At(0).(ontos.Tuple)
	if !ok || p.Len() > maxDepth {
		return Reference{}, MalformedFrame
	}
	path := make(wire.Path, p.Len())
	for i, segment := range p.Items() {
		a, ok := segment.(ontos.Atom)
		if !ok {
			return Reference{}, MalformedFrame
		}
		path[i] = a
	}
	scope, ok1 := t.At(1).(ontos.Atom)
	id, ok2 := t.At(2).(ontos.Atom)
	if !ok1 || !ok2 || scope.Len() != 16 || id.Len() != 16 {
		return Reference{}, MalformedFrame
	}
	return Reference{path, scope, id}, nil
}

// importRef turns a reference into a Wire. A reference to this participant in
// its current scope returns home to the original face; a stale or withdrawn one
// becomes a Wire whose sends are refused. Liveness is judged on send (D7).
func (s *Scope) importRef(r Reference, interned map[string]Wire) Wire {
	if wire.PathEqual(r.Path, s.path) {
		if !r.Scope.Equal(s.token) {
			return &proxy{ref: r, ns: s.ns, via: s, dead: StaleScope}
		}
		s.mu.Lock()
		e := s.byID[string(r.ID.Bytes())]
		s.mu.Unlock()
		if e == nil {
			return &proxy{ref: r, ns: s.ns, via: s, dead: UnknownExport}
		}
		return e.face
	}
	k := r.key()
	if w, ok := interned[k]; ok {
		return w
	}
	p := &proxy{ref: r, ns: s.ns, via: s}
	interned[k] = p
	return p
}
