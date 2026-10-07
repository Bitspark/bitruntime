// Package hydrated realizes bitwire decision 0019, the hydrated wire protocol's
// first edition, over bitwire's public hydrated declarations and its shared pure
// codec. The codec owns the grammar, validation and bounds; this package owns
// export ids and staging, reference recognition, liveness and endpoints.
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

// The runtime realizes bitwire's public hydrated declarations.
var (
	_ wire.HydratedEndpoint = (*Endpoint)(nil)
	_ wire.HydratedWire     = (*face)(nil)
	_ wire.HydratedWire     = (*proxy)(nil)
	_ wire.HydratedTuple    = Tuple{}
)

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

// Tuple is a hydrated tuple with at least one wire.HydratedWire beneath it. NewTuple returns
// the ground tuple when there is none (D1).
type Tuple struct{ items []wire.HydratedValue }

// NewTuple captures items. Each must be an ontos value, a Tuple or a wire.HydratedWire. An
// Endpoint is captured as its sending face: a value never carries receive or
// close authority, on any path (D7).
func NewTuple(items ...wire.HydratedValue) (wire.HydratedValue, error) {
	captured := make([]wire.HydratedValue, len(items))
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
		case Tuple, *face, *proxy:
			live = true
		case wire.HydratedWire:
			return nil, Unexportable // only the runtime's own Wires are leaves (D1)
		default:
			return nil, errors.New("hydrated: not a value")
		}
	}
	if live {
		return Tuple{captured}, nil
	}
	return ontos.NewTuple(ground...), nil
}

// sendOnly projects an Endpoint to its sending face, and the zero Tuple, the
// only one NewTuple does not make, to the ground empty tuple (D1, D7).
func sendOnly(v wire.HydratedValue) wire.HydratedValue {
	switch x := v.(type) {
	case *Endpoint:
		return x.face
	case Tuple:
		if x.items == nil {
			return ontos.NewTuple()
		}
	}
	return v
}

// Items returns the tuple's items.
func (t Tuple) Items() []wire.HydratedValue { return append([]wire.HydratedValue(nil), t.items...) }

// Items returns any tuple's items, ground or hydrated.
func Items(v wire.HydratedValue) ([]wire.HydratedValue, bool) {
	switch x := v.(type) {
	case Tuple:
		return x.Items(), true
	case ontos.Tuple:
		out := make([]wire.HydratedValue, x.Len())
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

// Reference is the ground form of a live wire.HydratedWire (D2, D3).
type Reference struct {
	Path      wire.Path
	Scope, ID ontos.Atom
}

func (r Reference) data() (wire.HydratedReference, error) {
	return wire.NewHydratedReference(r.Path, r.Scope, r.ID)
}

// Payload is the reference's ground payload ((path...), scope, id), for
// composition bootstrap over ground data.
func (r Reference) Payload() (ontos.Value, error) {
	d, err := r.data()
	if err != nil {
		return nil, refusal(err)
	}
	v, err := wire.PackHydratedReference(d, DefaultLimits.codec())
	return v, refusal(err)
}

// ReadReference reads a reference payload.
func ReadReference(v ontos.Value) (Reference, error) {
	d, err := wire.UnpackHydratedReference(v, DefaultLimits.codec())
	if err != nil {
		return Reference{}, refusal(err)
	}
	return Reference{d.Path(), d.Scope(), d.ID()}, nil
}

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
	handler func(wire.HydratedValue, wire.ReceivedContext)
	gen     uint64           // receiver generation: a detach removes only its own receiver
	end     wire.Termination // the first termination, final once closed is set
	closed  bool
	done    chan struct{}
	wake    chan struct{}
	face    *face
	exports map[*Scope]ontos.Atom
}

type delivery struct {
	value   wire.HydratedValue
	context wire.ReceivedContext
}

// face is an Endpoint's sending face. It grants sending only.
type face struct{ e *Endpoint }

func (f *face) Send(v wire.HydratedValue) error {
	projected := sendOnly(v)
	if err := checkLocal(projected); err != nil {
		return err
	}
	return f.e.admit(projected, Local{})
}

// checkLocal validates a value for local delivery as dehydration would for a
// remote one (D1, D7). A Tuple recognized its leaves when NewTuple built it and
// is immutable, so only the top level needs checking; nothing is traversed.
func checkLocal(v wire.HydratedValue) error {
	switch v.(type) {
	case ontos.Atom, ontos.Tuple, Tuple, *face, *proxy:
		return nil
	case wire.HydratedWire:
		return Unexportable
	}
	return errors.New("hydrated: not a value")
}

// NewEndpoint creates an endpoint whose queue admits at most limit values. A
// bound below one is a programming error.
func NewEndpoint(limit int) *Endpoint {
	if limit < 1 {
		panic("hydrated: an endpoint bound must be at least one")
	}
	e := &Endpoint{limit: limit, done: make(chan struct{}), wake: make(chan struct{}, 1), exports: map[*Scope]ontos.Atom{}}
	e.face = &face{e}
	go e.dispatch()
	return e
}

// wire.HydratedWire is the endpoint's sending face.
func (e *Endpoint) Wire() wire.HydratedWire { return e.face }

// Send admits a value locally, as its sending face does.
func (e *Endpoint) Send(v wire.HydratedValue) error { return e.face.Send(v) }

// Receive attaches the one receiver. Values are dispatched in admission order,
// never inline with the admitting send.
func (e *Endpoint) Receive(handler func(wire.HydratedValue, wire.ReceivedContext)) (detach func(), err error) {
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

// Termination reports how the endpoint ended, as bitwire's Endpoint contract
// does: "failed" with the receiver's failure, or "closed". It is the zero value
// while the endpoint is open. The first termination is final: it is set before
// Closed is closed, and a receiver failing after Close leaves it "closed".
func (e *Endpoint) Termination() wire.Termination {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.end
}

// Closed is closed when the endpoint closes.
func (e *Endpoint) Closed() <-chan struct{} { return e.done }

// Close ends the endpoint and withdraws its export from every scope.
func (e *Endpoint) Close() error {
	e.terminate(wire.Termination{Kind: "closed"})
	return nil
}

// terminate ends the endpoint once: the first termination, by Close or by a
// receiver failure, is the one reported, and later ones change nothing.
func (e *Endpoint) terminate(end wire.Termination) {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return
	}
	e.closed = true
	e.end = end
	exports := e.exports
	e.exports = map[*Scope]ontos.Atom{}
	e.queue = nil
	close(e.done)
	e.mu.Unlock()
	for s, id := range exports {
		s.withdraw(e, id)
	}
}

func (e *Endpoint) admit(v wire.HydratedValue, ctx wire.ReceivedContext) error {
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
				// A receiver failure terminates the endpoint, which ends its
				// export, unless the endpoint has already ended.
				e.terminate(wire.Termination{Kind: "failed", Message: err.Error()})
				return
			}
		}
	}
}

func deliverTo(h func(wire.HydratedValue, wire.ReceivedContext), d delivery) (err error) {
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

// proxy is an imported wire.HydratedWire. It carries its reference and namespace, so any
// participant in the namespace forwards it unchanged (D6). A proxy with a dead
// reason is this participant's own stale or withdrawn reference: its sends are
// refused locally (D7, returning home).
type proxy struct {
	ref  Reference
	ns   *Namespace
	via  *Scope
	dead Refusal
}

func (p *proxy) Send(v wire.HydratedValue) error {
	if p.dead != "" {
		return p.dead
	}
	return p.via.send(p.ref, v)
}

// Limits bounds a scope (D8).
type Limits struct{ Exports, Nodes, Depth, Bytes int }

// DefaultLimits are finite defaults; a composition configures its own.
var DefaultLimits = Limits{Exports: 64, Nodes: 4096, Depth: 64, Bytes: 1 << 20}

// codec is the shared codec's share of the bounds; Exports stays the runtime's.
func (l Limits) codec() wire.HydratedCodecLimits {
	return wire.HydratedCodecLimits{Nodes: l.Nodes, Depth: l.Depth, Bytes: l.Bytes}
}

// refusal names a codec failure as the protocol's refusal (D8).
func refusal(err error) error {
	switch {
	case errors.Is(err, wire.ErrHydratedLimit):
		return Limit
	case errors.Is(err, wire.ErrHydratedMalformed):
		return MalformedFrame
	}
	return err
}

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
func (s *Scope) Connect(r Reference) wire.HydratedWire {
	return s.importRef(r, map[string]wire.HydratedWire{})
}

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

// send stages v and registers its new exports in one step per scope, then
// admits one frame to the owner path (D5).
func (s *Scope) send(dest Reference, v wire.HydratedValue) error {
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

func (s *Scope) stage(dest Reference, v wire.HydratedValue) (ontos.Value, error) {
	s.encodeMu.Lock()
	defer s.encodeMu.Unlock()
	staged := map[*Endpoint]ontos.Atom{}
	body, err := s.toData(v, 0, staged, &budget{})
	if err != nil {
		return nil, err
	}
	frame, err := wire.PackHydratedFrame(wire.HydratedFrame{Scope: dest.Scope, ID: dest.ID, Body: body}, s.limits.codec())
	if err != nil {
		return nil, refusal(err)
	}
	return frame, s.commit(staged)
}

// budget counts a value as D8 does, while converting it. refs holds one
// descriptor per distinct face or proxy, shared by all its occurrences, and
// paths the least encoded size of the paths those descriptors copied.
type budget struct {
	nodes, bytes, paths int
	refs                map[wire.HydratedWire]wire.HydratedReference
}

// leastPathBytes is a lower bound on a path's encoded size: a tag and a length
// octet per segment, plus its bytes. A frame holding the path is at least this
// large, so refusing beyond the byte bound never refuses a valid value.
func leastPathBytes(path wire.Path) int {
	n := 0
	for _, a := range path {
		n += 2 + a.Len()
	}
	return n
}

// reference returns w's descriptor, copying its path once per conversion.
func (s *Scope) reference(b *budget, w wire.HydratedWire, path wire.Path, scope ontos.Atom, id func() (ontos.Atom, error)) (wire.HydratedData, error) {
	if r, ok := b.refs[w]; ok {
		return r, nil
	}
	if b.paths += leastPathBytes(path); b.paths > s.limits.Bytes {
		return nil, Limit
	}
	i, err := id()
	if err != nil {
		return nil, err
	}
	r, err := wire.NewHydratedReference(path, scope, i)
	if err != nil {
		return nil, refusal(err)
	}
	if b.refs == nil {
		b.refs = map[wire.HydratedWire]wire.HydratedReference{}
	}
	b.refs[w] = r
	return r, nil
}

// refBytes is a Wire leaf's value bytes under D8: its path bytes and 32.
func refBytes(path wire.Path) int {
	n := 32
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

// toData maps a live value onto the codec's structural data, staging an export
// for each new face. It counts each occurrence as D8 does and refuses before
// allocating, so a compact value with shared subtrees cannot expand past the
// bounds. The codec stays authoritative for every bound.
func (s *Scope) toData(v wire.HydratedValue, depth int, staged map[*Endpoint]ontos.Atom, b *budget) (wire.HydratedData, error) {
	switch x := v.(type) {
	case ontos.Atom:
		if err := s.spend(b, depth, x.Len()); err != nil {
			return nil, err
		}
		return wire.NewHydratedDataAtom(x), nil
	case ontos.Tuple, Tuple:
		if err := s.spend(b, depth, 0); err != nil {
			return nil, err
		}
		// Index the tuple and grow the output as children are counted: nothing
		// is copied or reserved at the tuple's size before the budget allows it.
		n, at := tupleIndex(x)
		var out []wire.HydratedData
		for i := 0; i < n; i++ {
			d, err := s.toData(at(i), depth+1, staged, b)
			if err != nil {
				return nil, err
			}
			out = append(out, d)
		}
		t, err := wire.NewHydratedDataTuple(out...)
		return t, refusal(err)
	case *Endpoint:
		return s.toData(x.face, depth, staged, b)
	case *face:
		if err := s.spend(b, depth, refBytes(s.path)); err != nil {
			return nil, err
		}
		return s.reference(b, x, s.path, s.token, func() (ontos.Atom, error) { return s.exportID(x.e, staged) })
	case *proxy:
		if x.ns != s.ns {
			return nil, ForeignNamespace
		}
		if err := s.spend(b, depth, refBytes(x.ref.Path)); err != nil {
			return nil, err
		}
		return s.reference(b, x, x.ref.Path, x.ref.Scope, func() (ontos.Atom, error) { return x.ref.ID, nil })
	case wire.HydratedWire:
		return nil, Unexportable
	}
	return nil, errors.New("hydrated: not a value")
}

// tupleIndex gives a tuple's length and indexed access without copying it.
func tupleIndex(v wire.HydratedValue) (int, func(int) wire.HydratedValue) {
	switch x := v.(type) {
	case ontos.Tuple:
		return x.Len(), func(i int) wire.HydratedValue { return x.At(i) }
	case Tuple:
		return len(x.items), func(i int) wire.HydratedValue { return x.items[i] }
	}
	return 0, nil
}

// fromData maps decoded structural data onto live values. Occurrences of one
// reference within the value share one proxy; nothing is retained across
// messages (D7).
func (s *Scope) fromData(d wire.HydratedData, interned map[string]wire.HydratedWire) (wire.HydratedValue, error) {
	switch x := d.(type) {
	case wire.HydratedDataAtom:
		return x.Value(), nil
	case wire.HydratedDataTuple:
		items := make([]wire.HydratedValue, x.Len())
		for i, c := range x.Items() {
			var err error
			if items[i], err = s.fromData(c, interned); err != nil {
				return nil, err
			}
		}
		return NewTuple(items...)
	case wire.HydratedReference:
		return s.importRef(Reference{x.Path(), x.Scope(), x.ID()}, interned), nil
	}
	return nil, MalformedFrame
}

// encodeBody and decodeBody are the body halves of a frame.
func (s *Scope) encodeBody(v wire.HydratedValue, staged map[*Endpoint]ontos.Atom) (ontos.Value, error) {
	d, err := s.toData(v, 0, staged, &budget{})
	if err != nil {
		return nil, err
	}
	body, err := wire.PackHydratedBody(d, s.limits.codec())
	return body, refusal(err)
}

func (s *Scope) decodeBody(body ontos.Value, interned map[string]wire.HydratedWire) (wire.HydratedValue, error) {
	d, err := wire.UnpackHydratedBody(body, s.limits.codec())
	if err != nil {
		return nil, refusal(err)
	}
	return s.fromData(d, interned)
}

// Deliver decides one value addressed to this participant's own path (D4). The
// returned refusal is a host diagnostic; nothing is sent back.
func (s *Scope) Deliver(path wire.Path, message ontos.Value, ctx wire.ReceivedContext) error {
	if !wire.PathEqual(path, s.path) {
		return MalformedFrame
	}
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return ScopeEnded
	}
	frame, err := wire.UnpackHydratedFrame(message, s.limits.codec())
	if err != nil {
		return refusal(err)
	}
	if !frame.Scope.Equal(s.token) {
		return StaleScope
	}
	s.mu.Lock()
	target := s.byID[string(frame.ID.Bytes())]
	s.mu.Unlock()
	if target == nil {
		return UnknownExport
	}
	value, err := s.fromData(frame.Body, map[string]wire.HydratedWire{})
	if err != nil {
		return err
	}
	return target.admit(value, ctx)
}

// importRef turns a reference into a wire.HydratedWire. A reference to this participant in
// its current scope returns home to the original face; a stale or withdrawn one
// becomes a wire.HydratedWire whose sends are refused. Liveness is judged on send (D7).
func (s *Scope) importRef(r Reference, interned map[string]wire.HydratedWire) wire.HydratedWire {
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
