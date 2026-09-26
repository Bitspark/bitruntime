package core_test

// Ported from nightseam v0.6.0 runtime/go/invocation_experiment_test.go
// (commit 5cc9723a): the second independent lifecycle participant, a pure
// forwarding route and an opaque wrapper, using public facilities only.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/Bitspark/bitruntime/core/go"
	dispatch "github.com/Bitspark/bitruntime/dispatch/go"
	transports "github.com/Bitspark/bitruntime/transports/go"
	wire "github.com/Bitspark/bitwire/wire/go"
)

// ledgerEndpoint is the second independent integration. It shares no ledger
// with the first and composes none of bitruntime's lifecycle: it answers the
// invocation vocabulary itself, out of its own state, using the operation
// paths and nothing else. If the dispatcher works against this, the boundary
// is public in fact and not only in name.
type ledgerEndpoint struct {
	mu          sync.Mutex
	receiver    *wire.Receiver
	captureCap  int
	bodyCap     int
	calls       map[string]*ledgerCall
	next        atomic.Uint64
	retirements atomic.Int64
	refusals    atomic.Int64
}

type ledgerCall struct {
	mu        sync.Mutex
	owner     *ledgerEndpoint
	address   *wire.ReturnAddress
	outcome   chan wire.Message
	sinks     map[string]wire.AddressedWire
	delivered map[string]bool
	told      map[string]bool
	bodies    map[string]bool
	takenCaps int
	takenBody int
	pending   int
	control   *wire.Message
	settled   bool
	retired   bool
}

func newLedgerEndpoint(captures, bodies int) *ledgerEndpoint {
	return &ledgerEndpoint{captureCap: captures, bodyCap: bodies, calls: map[string]*ledgerCall{}}
}

func (e *ledgerEndpoint) Send([]string, wire.Message) error { return nil }
func (e *ledgerEndpoint) Close(code wire.Code, reason string) error {
	e.mu.Lock()
	receiver := e.receiver
	e.receiver = nil
	e.mu.Unlock()
	if receiver != nil && receiver.Closed != nil {
		receiver.Closed(code, reason)
	}
	return nil
}
func (e *ledgerEndpoint) Receive(receiver wire.Receiver) (func(), error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.receiver != nil {
		return nil, core.ErrReceiverExists
	}
	held := receiver
	e.receiver = &held
	return func() {
		e.mu.Lock()
		if e.receiver == &held {
			e.receiver = nil
		}
		e.mu.Unlock()
	}, nil
}

// ledgerReturn answers the outcome at its own origin and the invocation
// vocabulary everywhere else, refusing any path it does not implement.
type ledgerReturn struct{ call *ledgerCall }

func (r *ledgerReturn) Send(path []string, message wire.Message) error {
	call := r.call
	if len(path) == 0 {
		if message.Frame.Kind != wire.ProfileResponse {
			return errors.New("invalid outcome")
		}
		select {
		case call.outcome <- message:
		default:
		}
		call.settle()
		return nil
	}
	switch path[0] {
	case core.InvocationControl:
		if len(path) != 1 || message.Frame.Kind != wire.ProfileCancel {
			return errors.New("unknown invocation operation")
		}
		call.latch(message)
		return nil
	case core.InvocationCapture:
		if len(path) != 2 || message.Return == nil || message.Return.Wire == nil {
			return errors.New("unknown invocation operation")
		}
		return call.capture(path[1], message.Return.Wire)
	case core.InvocationReady:
		if len(path) != 2 {
			return errors.New("unknown invocation operation")
		}
		call.markReady(path[1])
		return nil
	case core.InvocationRelease:
		if len(path) != 2 {
			return errors.New("unknown invocation operation")
		}
		call.release(path[1])
		return nil
	case core.InvocationBegin:
		if len(path) != 2 {
			return errors.New("unknown invocation operation")
		}
		return call.begin(path[1])
	case core.InvocationDone:
		if len(path) != 2 {
			return errors.New("unknown invocation operation")
		}
		call.done(path[1])
		return nil
	}
	return errors.New("unknown invocation operation")
}

func (c *ledgerCall) capture(identifier string, sink wire.AddressedWire) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.retired {
		return errors.New("retired")
	}
	if c.takenCaps >= c.owner.captureCap {
		c.owner.refusals.Add(1)
		return errors.New("capture bound reached")
	}
	c.takenCaps++
	c.pending++
	c.sinks[identifier] = sink
	return nil
}

func (c *ledgerCall) markReady(identifier string) {
	c.mu.Lock()
	sink := c.sinks[identifier]
	if sink == nil || c.delivered[identifier] {
		c.mu.Unlock()
		return
	}
	c.delivered[identifier] = true
	c.pending--
	var deliver wire.AddressedWire
	var control wire.Message
	if c.control != nil && !c.told[identifier] {
		c.told[identifier] = true
		deliver, control = sink, *c.control
	}
	c.mu.Unlock()
	if deliver != nil {
		_ = deliver.Send(nil, control)
	}
	c.retire()
}

func (c *ledgerCall) release(identifier string) {
	c.mu.Lock()
	if _, exists := c.sinks[identifier]; !exists {
		c.mu.Unlock()
		return
	}
	if !c.delivered[identifier] {
		c.pending--
	}
	delete(c.sinks, identifier)
	delete(c.delivered, identifier)
	c.mu.Unlock()
	c.retire()
}

func (c *ledgerCall) begin(identifier string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.retired {
		return errors.New("retired")
	}
	if c.takenBody >= c.owner.bodyCap {
		c.owner.refusals.Add(1)
		return errors.New("body bound reached")
	}
	c.takenBody++
	c.bodies[identifier] = true
	return nil
}

func (c *ledgerCall) done(identifier string) {
	c.mu.Lock()
	if !c.bodies[identifier] {
		c.mu.Unlock()
		return
	}
	delete(c.bodies, identifier)
	c.mu.Unlock()
	c.retire()
}

func (c *ledgerCall) latch(message wire.Message) {
	c.mu.Lock()
	if c.retired || c.control != nil {
		c.mu.Unlock()
		return
	}
	c.control = &message
	var sinks []wire.AddressedWire
	for identifier, sink := range c.sinks {
		if c.delivered[identifier] && !c.told[identifier] {
			c.told[identifier] = true
			sinks = append(sinks, sink)
		}
	}
	c.mu.Unlock()
	for _, sink := range sinks {
		_ = sink.Send(nil, message)
	}
}

func (c *ledgerCall) settle() {
	c.mu.Lock()
	c.settled = true
	c.mu.Unlock()
	c.retire()
}

func (c *ledgerCall) retire() {
	c.mu.Lock()
	if c.retired || !c.settled || c.pending != 0 || len(c.bodies) != 0 {
		c.mu.Unlock()
		return
	}
	c.retired = true
	c.sinks, c.delivered, c.told, c.control = nil, nil, nil, nil
	c.mu.Unlock()
	c.owner.retirements.Add(1)
}

func (e *ledgerEndpoint) admit(path []string, params json.RawMessage) (*ledgerCall, chan wire.Message) {
	identifier := fmt.Sprintf("l:%d", e.next.Add(1))
	call := &ledgerCall{owner: e, outcome: make(chan wire.Message, 1), sinks: map[string]wire.AddressedWire{},
		delivered: map[string]bool{}, told: map[string]bool{}, bodies: map[string]bool{}, pending: 1}
	call.address = &wire.ReturnAddress{Wire: &ledgerReturn{call: call}}
	e.mu.Lock()
	e.calls[identifier] = call
	receiver := e.receiver
	e.mu.Unlock()
	if receiver != nil && receiver.Message != nil {
		receiver.Message(path, wire.Message{
			Frame:  wire.ProfileFrame{Version: 1, Kind: wire.ProfileRequest, ID: identifier, Params: params},
			Return: call.address,
		})
	}
	// The request's own delivery has returned.
	call.mu.Lock()
	call.pending--
	call.mu.Unlock()
	call.retire()
	return call, call.outcome
}

func (c *ledgerCall) cancel(identifier string) {
	_ = c.address.Wire.Send([]string{core.InvocationControl}, wire.Message{
		Frame:  wire.ProfileFrame{Version: 1, Kind: wire.ProfileCancel, ID: identifier},
		Return: c.address,
	})
}

func (c *ledgerCall) isRetired() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.retired
}

// TestASecondIntegrationParticipatesWithNoSharedLedger runs bitruntime's
// dispatcher and its handler registration over an endpoint that implements
// the lifecycle itself, through an opaque wrapper, with no shared state and
// no concrete type recognized on either side.
func TestASecondIntegrationParticipatesWithNoSharedLedger(t *testing.T) {
	endpoint := newLedgerEndpoint(8, 8)
	router, err := dispatch.NewDispatcher(opaqueEndpoint{endpoint})
	if err != nil {
		t.Fatal(err)
	}
	view, err := dispatch.NewDispatcher(router.Select([]string{"space"}))
	if err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}, 1), make(chan struct{})
	observed := make(chan error, 1)
	if _, err := dispatch.Handle(view, []string{"read"}, func(ctx context.Context, _ json.RawMessage) (any, error) {
		started <- struct{}{}
		<-release
		observed <- ctx.Err()
		return "answer", nil
	}); err != nil {
		t.Fatal(err)
	}
	call, outcome := endpoint.admit([]string{"space", "read"}, nil)
	<-started
	if call.isRetired() {
		t.Fatal("retired while the body was running")
	}
	call.cancel("l:1")
	close(release)
	if err := <-observed; err == nil {
		t.Fatal("cancellation did not reach the captured traversal of the second integration")
	}
	select {
	case <-outcome:
	case <-time.After(5 * time.Second):
		t.Fatal("no outcome")
	}
	for range 500 {
		if call.isRetired() {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !call.isRetired() {
		t.Fatal("the second integration never retired its invocation")
	}
	if endpoint.retirements.Load() != 1 {
		t.Fatalf("retirements: %d", endpoint.retirements.Load())
	}
}

// conduitEndpoint is half of a pure route: what is sent on one half is
// delivered to the other half's attachment, verbatim, with the message's
// return capability untouched. It correlates nothing and admits nothing, so a
// composition over it is pure forwarding rather than a carrier hop.
type conduitEndpoint struct {
	mu       sync.Mutex
	receiver *wire.Receiver
	other    *conduitEndpoint
}

func newConduit() (*conduitEndpoint, *conduitEndpoint) {
	near, far := &conduitEndpoint{}, &conduitEndpoint{}
	near.other, far.other = far, near
	return near, far
}

func (c *conduitEndpoint) Send(path []string, message wire.Message) error {
	c.other.mu.Lock()
	receiver := c.other.receiver
	c.other.mu.Unlock()
	if receiver == nil || receiver.Message == nil {
		return transports.ErrClosed
	}
	receiver.Message(path, message)
	return nil
}
func (c *conduitEndpoint) Receive(receiver wire.Receiver) (func(), error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.receiver != nil {
		return nil, core.ErrReceiverExists
	}
	held := receiver
	c.receiver = &held
	return func() {
		c.mu.Lock()
		if c.receiver == &held {
			c.receiver = nil
		}
		c.mu.Unlock()
	}, nil
}
func (c *conduitEndpoint) Close(wire.Code, string) error { return nil }

// TestForwardingPreservesLifecycleParticipation admits on one integration and
// forwards through an opaque wrapper and a pure route into a dispatcher on the
// far side. The lifecycle travels with the preserved return capability rather
// than being reconstructed at the boundary, and detach and rebind on the far
// side leave the captured traversal owning the control.
func TestForwardingPreservesLifecycleParticipation(t *testing.T) {
	origin := newLedgerEndpoint(8, 8)
	near, far := newConduit()
	stop, err := core.Forward(opaqueEndpoint{origin}, opaqueEndpoint{near})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stop)
	router, err := dispatch.NewDispatcher(far)
	if err != nil {
		t.Fatal(err)
	}
	controls, requests := make(chan wire.Message, 4), make(chan wire.Message, 4)
	detach, err := router.Register([]string{"read"}, wire.Receiver{Message: func(_ []string, m wire.Message) {
		if m.Frame.Kind == wire.ProfileCancel {
			controls <- m
			return
		}
		requests <- m
	}})
	if err != nil {
		t.Fatal(err)
	}
	call, _ := origin.admit([]string{"read"}, nil)
	var admitted wire.Message
	select {
	case admitted = <-requests:
	case <-time.After(5 * time.Second):
		t.Fatal("the forwarded request never arrived")
	}
	if admitted.Return != call.address {
		t.Fatal("forwarding did not preserve the original return capability")
	}
	detach()
	rebound := make(chan wire.Message, 4)
	if _, err := router.Register([]string{"read"}, wire.Receiver{Message: func(_ []string, m wire.Message) { rebound <- m }}); err != nil {
		t.Fatal(err)
	}
	call.cancel("l:1")
	select {
	case m := <-controls:
		if m.Frame.Kind != wire.ProfileCancel {
			t.Fatalf("control was %q", m.Frame.Kind)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the control did not follow the captured traversal across the forwarder")
	}
	select {
	case <-rebound:
		t.Fatal("the control reached the rebound registration")
	default:
	}
}

// TestAQueuedControlCannotReachAReusedIdentity is the first identity-reuse
// race: a control already queued against one invocation keeps that invocation,
// so a later invocation reusing the same textual identifier is untouched.
func TestAQueuedControlCannotReachAReusedIdentity(t *testing.T) {
	endpoint := newLedgerEndpoint(8, 8)
	router, err := dispatch.NewDispatcher(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(chan string, 8)
	if _, err := router.Register([]string{"read"}, wire.Receiver{Message: func(_ []string, m wire.Message) {
		seen <- string(m.Frame.Kind) + ":" + m.Frame.ID
	}}); err != nil {
		t.Fatal(err)
	}
	first, _ := endpoint.admit([]string{"read"}, nil)
	if got := <-seen; got != "request:l:1" {
		t.Fatalf("first request: %s", got)
	}
	// The first invocation settles and retires before its old control is
	// released. Its control ticket is the invocation itself, not a key.
	first.settle()
	stale := first.address
	second, _ := endpoint.admit([]string{"read"}, nil)
	if got := <-seen; got != "request:l:2" {
		t.Fatalf("second request: %s", got)
	}
	// The stale control names the first invocation's identifier and travels on
	// the first invocation's own return capability. It reaches nothing.
	_ = stale.Wire.Send([]string{core.InvocationControl}, wire.Message{
		Frame:  wire.ProfileFrame{Version: 1, Kind: wire.ProfileCancel, ID: "l:1"},
		Return: stale,
	})
	select {
	case got := <-seen:
		t.Fatalf("a stale control was delivered: %s", got)
	case <-time.After(200 * time.Millisecond):
	}
	second.cancel("l:2")
	select {
	case got := <-seen:
		if got != "cancel:l:2" {
			t.Fatalf("live control: %s", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the live invocation's control never arrived")
	}
}

// TestAResponseRacingAQueuedControlRetiresOnce drives a response and a control
// at one invocation concurrently, many times, under the race detector.
func TestAResponseRacingAQueuedControlRetiresOnce(t *testing.T) {
	endpoint := newLedgerEndpoint(8, 8)
	router, err := dispatch.NewDispatcher(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dispatch.Handle(router, []string{"read"}, func(ctx context.Context, _ json.RawMessage) (any, error) {
		return "answer", nil
	}); err != nil {
		t.Fatal(err)
	}
	for i := range 64 {
		call, outcome := endpoint.admit([]string{"read"}, nil)
		var wait sync.WaitGroup
		wait.Add(1)
		go func() { defer wait.Done(); call.cancel(fmt.Sprintf("l:%d", i+1)) }()
		select {
		case <-outcome:
		case <-time.After(5 * time.Second):
			t.Fatal("no outcome")
		}
		wait.Wait()
	}
	for range 500 {
		if endpoint.retirements.Load() == 64 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("retirements after 64 raced completions: %d", endpoint.retirements.Load())
}

// TestCaptureBoundRefusesRatherThanGrowing checks the refusal a bounded
// integration gives, and that the dispatcher answers it instead of routing.
func TestCaptureBoundRefusesRatherThanGrowing(t *testing.T) {
	endpoint := newLedgerEndpoint(1, 8)
	outer, err := dispatch.NewDispatcher(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	inner, err := dispatch.NewDispatcher(outer.Select([]string{"a"}))
	if err != nil {
		t.Fatal(err)
	}
	delivered := make(chan struct{}, 1)
	if _, err := inner.Register([]string{"read"}, wire.Receiver{Message: func([]string, wire.Message) { delivered <- struct{}{} }}); err != nil {
		t.Fatal(err)
	}
	_, outcome := endpoint.admit([]string{"a", "read"}, nil)
	select {
	case answer := <-outcome:
		// The refusal is this integration's own: a dispatcher reports busy for
		// a bound it can recognize as one, and invalid_message for a refusal
		// whose reason a facility did not spell in the agreed vocabulary.
		if answer.Frame.Error == nil || answer.Frame.Error.Code != "invalid_message" {
			t.Fatalf("a traversal past the capture bound answered %+v", answer.Frame.Error)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no refusal")
	}
	select {
	case <-delivered:
		t.Fatal("the inner receiver was reached past the bound")
	default:
	}
	if endpoint.refusals.Load() == 0 {
		t.Fatal("the bound was never exercised")
	}
}
