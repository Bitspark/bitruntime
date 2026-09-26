package core

import (
	"errors"
	"strconv"
	"sync"
	"sync/atomic"

	wire "github.com/Bitspark/bitwire/wire/go"
)

// An admitted request's return capability is the invocation, presented as
// addressed access. The empty path carries its outcome, as it always has; these operations
// carry its lifecycle. They are ordinary events of the profile — a layer's own
// vocabulary, as `channel.` is the tunnel's — and a participant needs nothing
// of bitruntime's to speak them but the return capability it was handed.
//
// The capture or body a verb is about is one opaque segment after the
// operation, because a path is what addresses a thing. The verbs never reach a
// peer root and never cross a physical hop, so they take no built-in family and
// reserve no namespace there; a return capability's path space is the
// invocation's alone.
const (
	// InvocationCapture claims one immutable routing decision for this
	// traversal. Its message carries the capture's control sink as the return
	// address. Admission is the capture; a refusal is an error from Send.
	InvocationCapture = "invocation.capture"
	// InvocationReady says the captured request has been delivered. A control
	// latched before this arrives is pushed to the sink now.
	InvocationReady = "invocation.ready"
	// InvocationRelease drops a capture whose traversal wants no more controls.
	InvocationRelease = "invocation.release"
	// InvocationBegin takes one execution lease. Admission is the lease.
	InvocationBegin = "invocation.begin"
	// InvocationDone reports that an executing body actually finished.
	InvocationDone = "invocation.done"
	// InvocationControl relays a cancellation into the invocation, which
	// latches it and pushes it to every ready capture exactly once.
	InvocationControl = "invocation.control"
)

// DefaultInvocationCaptures bounds the captures one admitted invocation may
// take. It bounds traversal depth and shallow fan-out together, since a
// capture is taken once per routing boundary crossed.
const DefaultInvocationCaptures = 64

// DefaultInvocationBodies bounds the execution leases one admitted invocation
// may take.
const DefaultInvocationBodies = 64

var (
	// ErrInvocationUnsupported is the refusal a participant receives from a
	// return capability that does not speak this vocabulary. A dispatcher
	// answers it explicitly rather than routing with weaker guarantees.
	ErrInvocationUnsupported = errors.New("return capability does not carry an invocation lifecycle")
	// ErrInvocationEnded refuses participation once the invocation retired.
	ErrInvocationEnded = errors.New("invocation no longer admits participation")
	// ErrInvocationLimit refuses participation beyond a bound.
	ErrInvocationLimit = errors.New("invocation participation limit reached")
	// ErrInvocationDuplicate refuses a participant identifier already in use.
	ErrInvocationDuplicate = errors.New("invocation participant already exists")
)

// InvocationLimits bounds the total captures and execution leases of one
// admitted invocation. Both are totals, so neither depth nor fan-out can grow
// the state an invocation retains.
type InvocationLimits struct{ Captures, Bodies int }

// DefaultInvocationLimits are the bounds an admitting runtime uses when it
// states none of its own.
func DefaultInvocationLimits() InvocationLimits {
	return InvocationLimits{Captures: DefaultInvocationCaptures, Bodies: DefaultInvocationBodies}
}

var invocationIdentifier atomic.Uint64

func nextInvocationIdentifier() string {
	return strconv.FormatUint(invocationIdentifier.Add(1), 10)
}

func invocationEvent() wire.ProfileFrame {
	return wire.ProfileFrame{Version: 1, Kind: wire.ProfileEvent, Data: []byte("null")}
}

// Invocation is the lifecycle an admitting runtime keeps for one admitted
// request, and the answer its return capability gives to the vocabulary above.
// A runtime that is not bitruntime composes it — or answers the same paths
// itself — and the same participants work against either.
type Invocation struct {
	mu           sync.Mutex
	limits       InvocationLimits
	captures     map[string]*invocationCapture
	bodies       map[string]struct{}
	taken        int
	begun        int
	unready      int
	running      int
	controls     int
	control      *wire.Message
	settled      bool
	dispatchDone bool
	retired      bool
	onRetired    func()
}

type invocationCapture struct {
	sink     wire.AddressedWire
	ready    bool
	notified bool
}

// NewInvocation creates the lifecycle of one admitted request. onRetired runs
// once, outside every lifecycle lock, when no permitted future participation
// and no already admitted control can still need the captures.
func NewInvocation(limits InvocationLimits, onRetired func()) *Invocation {
	if limits.Captures < 1 {
		limits.Captures = DefaultInvocationCaptures
	}
	if limits.Bodies < 1 {
		limits.Bodies = DefaultInvocationBodies
	}
	return &Invocation{
		limits:    limits,
		captures:  map[string]*invocationCapture{},
		bodies:    map[string]struct{}{},
		unready:   1,
		onRetired: onRetired,
	}
}

// Deliver answers one operation of the invocation vocabulary. A return
// capability routes every nonempty path here; the empty path stays its own.
func (v *Invocation) Deliver(path []string, message wire.Message) error {
	if v == nil {
		return ErrInvocationUnsupported
	}
	if len(path) == 0 {
		return ErrInvocationUnsupported
	}
	switch path[0] {
	case InvocationControl:
		if len(path) != 1 || message.Frame.Kind != wire.ProfileCancel {
			return ErrInvocationUnsupported
		}
		v.latch(message)
		return nil
	case InvocationCapture, InvocationReady, InvocationRelease, InvocationBegin, InvocationDone:
	default:
		return ErrInvocationUnsupported
	}
	if len(path) != 2 || message.Frame.Kind != wire.ProfileEvent {
		return ErrInvocationUnsupported
	}
	identifier := path[1]
	switch path[0] {
	case InvocationCapture:
		if message.Return == nil || message.Return.Wire == nil {
			return ErrInvocationUnsupported
		}
		return v.capture(identifier, message.Return.Wire)
	case InvocationReady:
		v.ready(identifier)
	case InvocationRelease:
		v.release(identifier)
	case InvocationBegin:
		return v.begin(identifier)
	case InvocationDone:
		v.done(identifier)
	}
	return nil
}

// Settle fixes the outcome. It neither finishes a body nor drains a control.
func (v *Invocation) Settle() {
	if v == nil {
		return
	}
	v.mu.Lock()
	v.settled = true
	retire := v.retireLocked()
	v.mu.Unlock()
	invocationNotify(retire)
}

// DispatchDone reports that the admitted request's own delivery has returned.
func (v *Invocation) DispatchDone() {
	if v == nil {
		return
	}
	v.mu.Lock()
	if !v.dispatchDone {
		v.dispatchDone = true
		v.unready--
	}
	retire := v.retireLocked()
	v.mu.Unlock()
	invocationNotify(retire)
}

// Retired reports whether the invocation has released its captures.
func (v *Invocation) Retired() bool {
	if v == nil {
		return true
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.retired
}

func (v *Invocation) capture(identifier string, sink wire.AddressedWire) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.retired {
		return ErrInvocationEnded
	}
	if _, exists := v.captures[identifier]; exists {
		return ErrInvocationDuplicate
	}
	if v.taken >= v.limits.Captures {
		return ErrInvocationLimit
	}
	v.taken++
	v.unready++
	v.captures[identifier] = &invocationCapture{sink: sink}
	return nil
}

func (v *Invocation) ready(identifier string) {
	v.mu.Lock()
	capture := v.captures[identifier]
	if capture == nil || capture.ready {
		v.mu.Unlock()
		return
	}
	capture.ready = true
	v.unready--
	var sinks []wire.AddressedWire
	var control wire.Message
	if v.control != nil && !capture.notified {
		capture.notified = true
		sinks, control = []wire.AddressedWire{capture.sink}, *v.control
		v.controls++
	}
	retire := v.retireLocked()
	v.mu.Unlock()
	invocationNotify(retire)
	if sinks != nil {
		v.push(sinks, control)
	}
}

func (v *Invocation) release(identifier string) {
	v.mu.Lock()
	capture := v.captures[identifier]
	if capture == nil {
		v.mu.Unlock()
		return
	}
	if !capture.ready {
		capture.ready = true
		v.unready--
	}
	delete(v.captures, identifier)
	retire := v.retireLocked()
	v.mu.Unlock()
	invocationNotify(retire)
}

func (v *Invocation) begin(identifier string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.retired {
		return ErrInvocationEnded
	}
	if _, exists := v.bodies[identifier]; exists {
		return ErrInvocationDuplicate
	}
	if v.begun >= v.limits.Bodies {
		return ErrInvocationLimit
	}
	v.begun++
	v.running++
	v.bodies[identifier] = struct{}{}
	return nil
}

func (v *Invocation) done(identifier string) {
	v.mu.Lock()
	if _, exists := v.bodies[identifier]; !exists {
		v.mu.Unlock()
		return
	}
	delete(v.bodies, identifier)
	v.running--
	retire := v.retireLocked()
	v.mu.Unlock()
	invocationNotify(retire)
}

// latch records the first cancellation and pushes it to every capture that
// is already ready. A capture installed while it is latched receives it when
// its own request delivery becomes ready. Further controls coalesce.
func (v *Invocation) latch(message wire.Message) {
	v.mu.Lock()
	if v.retired || v.control != nil {
		v.mu.Unlock()
		return
	}
	v.control = &message
	var sinks []wire.AddressedWire
	for _, capture := range v.captures {
		if capture.ready && !capture.notified {
			capture.notified = true
			sinks = append(sinks, capture.sink)
		}
	}
	v.controls++
	v.mu.Unlock()
	v.push(sinks, message)
}

// push runs participant code outside the lock and retains one control
// reservation until every selected continuation has returned, so that
// retirement cannot reclaim a capture a control is still reaching.
func (v *Invocation) push(sinks []wire.AddressedWire, message wire.Message) {
	defer func() {
		v.mu.Lock()
		v.controls--
		retire := v.retireLocked()
		v.mu.Unlock()
		invocationNotify(retire)
	}()
	for _, sink := range sinks {
		func() {
			defer func() { _ = recover() }()
			_ = sink.Send(nil, message)
		}()
	}
}

func (v *Invocation) retireLocked() func() {
	if v.retired || !v.settled || v.unready != 0 || v.running != 0 || v.controls != 0 {
		return nil
	}
	v.retired = true
	v.captures, v.bodies, v.control = nil, nil, nil
	callback := v.onRetired
	v.onRetired = nil
	return callback
}

func invocationNotify(callback func()) {
	if callback != nil {
		callback()
	}
}

// invocationSink is the Wire a capture is pushed its control through. It
// accepts the invocation's cancellation at its own origin and nothing else.
type invocationSink struct{ control func(wire.Message) }

func (s *invocationSink) Send(path []string, message wire.Message) error {
	if len(path) != 0 || message.Frame.Kind != wire.ProfileCancel {
		return errors.New("an invocation control sink carries cancellation only")
	}
	s.control(message)
	return nil
}

func invocationWire(message wire.Message) (wire.AddressedWire, error) {
	if message.Return == nil || message.Return.Wire == nil {
		return nil, ErrInvocationUnsupported
	}
	return message.Return.Wire, nil
}

// InvocationCaptureHandle is one immutable routing decision a participant took
// for one traversal of one admitted invocation. Repeated traversal of the same
// dispatcher takes a fresh handle, so no two traversals share a slot.
type InvocationCaptureHandle struct {
	wire       wire.AddressedWire
	identifier string
	once       sync.Once
	released   sync.Once
}

// CaptureInvocation claims a routing decision for this traversal and supplies
// the sink the invocation pushes its cancellation to. The sink receives the
// control at most once, after Ready and never before.
//
// A return capability that does not speak the vocabulary refuses, and the
// refusal is the caller's to answer: routing an invocation-aware request with
// weaker cancellation guarantees is exactly what this reports instead.
func CaptureInvocation(message wire.Message, control func(wire.Message)) (*InvocationCaptureHandle, error) {
	access, err := invocationWire(message)
	if err != nil {
		return nil, err
	}
	if control == nil {
		return nil, errors.New("an invocation capture requires a control sink")
	}
	handle := &InvocationCaptureHandle{wire: access, identifier: nextInvocationIdentifier()}
	sent := wire.Message{Frame: invocationEvent(), Return: &wire.ReturnAddress{Wire: &invocationSink{control: control}}}
	if err := access.Send([]string{InvocationCapture, handle.identifier}, sent); err != nil {
		return nil, err
	}
	return handle, nil
}

// Ready says the captured request has been delivered. A cancellation latched
// while the capture was being installed reaches the sink now.
func (c *InvocationCaptureHandle) Ready() {
	if c == nil {
		return
	}
	c.once.Do(func() {
		_ = c.wire.Send([]string{InvocationReady, c.identifier}, wire.Message{Frame: invocationEvent()})
	})
}

// Release drops the capture. Ready and Release are each idempotent, and a
// release after Ready gives up only this traversal's remaining controls.
func (c *InvocationCaptureHandle) Release() {
	if c == nil {
		return
	}
	c.released.Do(func() {
		_ = c.wire.Send([]string{InvocationRelease, c.identifier}, wire.Message{Frame: invocationEvent()})
	})
}

// InvocationBodyHandle is one execution lease of one admitted invocation.
type InvocationBodyHandle struct {
	wire       wire.AddressedWire
	identifier string
	once       sync.Once
}

// BeginInvocationBody takes an execution lease for work this participant owns.
// The invocation does not retire while the lease is held, so an early answer
// to the caller — a deadline, a withdrawal — never retires an invocation whose
// body is still running.
func BeginInvocationBody(message wire.Message) (*InvocationBodyHandle, error) {
	access, err := invocationWire(message)
	if err != nil {
		return nil, err
	}
	handle := &InvocationBodyHandle{wire: access, identifier: nextInvocationIdentifier()}
	if err := access.Send([]string{InvocationBegin, handle.identifier}, wire.Message{Frame: invocationEvent()}); err != nil {
		return nil, err
	}
	return handle, nil
}

// Done reports that the body actually finished. It is idempotent and releases
// only the lease it took: a participant cannot finish another owner's work.
func (b *InvocationBodyHandle) Done() {
	if b == nil {
		return
	}
	b.once.Do(func() {
		_ = b.wire.Send([]string{InvocationDone, b.identifier}, wire.Message{Frame: invocationEvent()})
	})
}

// RelayInvocationControl hands a cancellation to the invocation it names, which
// latches it and pushes it to the traversals that captured it. A router that
// receives a control frame relays it here rather than resolving a route of its
// own: the capture, not the current registration, decides where it goes.
func RelayInvocationControl(message wire.Message) error {
	access, err := invocationWire(message)
	if err != nil {
		return err
	}
	return access.Send([]string{InvocationControl}, message)
}
