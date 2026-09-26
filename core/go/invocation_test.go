package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Bitspark/nightseam/duplex/go"
	ws "github.com/Bitspark/nightseam/runtime/go"
)

// invocationEndpoint is an endpoint written against the public contract alone.
// It admits requests, answers the invocation vocabulary out of its own ledger,
// and imports nothing of Nightseam's but the vocabulary's paths. It is one of
// the two independent integrations #439 requires.
type invocationEndpoint struct {
	mu          sync.Mutex
	receiver    *duplex.Receiver
	closed      bool
	limits      ws.InvocationLimits
	admitted    map[string]*ws.Invocation
	returns     map[string]*duplex.ReturnAddress
	outcomes    map[string]chan duplex.Message
	retirements atomic.Int64
	next        atomic.Uint64
}

func newInvocationEndpoint(limits ws.InvocationLimits) *invocationEndpoint {
	return &invocationEndpoint{limits: limits, admitted: map[string]*ws.Invocation{},
		returns: map[string]*duplex.ReturnAddress{}, outcomes: map[string]chan duplex.Message{}}
}

// invocationReturn is this endpoint's return capability. The empty path is the
// outcome; every other path is the invocation's own vocabulary.
type invocationReturn struct {
	owner      *invocationEndpoint
	identifier string
	invocation *ws.Invocation
}

func (r *invocationReturn) Send(path []string, message duplex.Message) error {
	if len(path) != 0 {
		return r.invocation.Deliver(path, message)
	}
	if message.Frame.Kind != duplex.ProfileResponse {
		return errors.New("invalid outcome")
	}
	r.owner.mu.Lock()
	outcome := r.owner.outcomes[r.identifier]
	r.owner.mu.Unlock()
	if outcome != nil {
		select {
		case outcome <- message:
		default:
		}
	}
	r.invocation.Settle()
	return nil
}

// Send loops back into this endpoint's own attachment, asynchronously, so a
// composition above it can be traversed more than once in one invocation
// without running destination code on the sender's stack.
func (e *invocationEndpoint) Send(path []string, message duplex.Message) error {
	go e.deliver(path, message)
	return nil
}

func (e *invocationEndpoint) Receive(receiver duplex.Receiver) (func(), error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, ws.ErrClosed
	}
	if e.receiver != nil {
		return nil, duplex.ErrReceiverExists
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

func (e *invocationEndpoint) Close(code duplex.Code, reason string) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	receiver := e.receiver
	e.receiver = nil
	e.mu.Unlock()
	if receiver != nil && receiver.Closed != nil {
		receiver.Closed(code, reason)
	}
	return nil
}

// admit delivers one request through the attached receiver with a fresh
// invocation, and returns the channel its outcome arrives on.
func (e *invocationEndpoint) admit(path []string, params json.RawMessage) (string, chan duplex.Message) {
	identifier := fmt.Sprintf("x:%d", e.next.Add(1))
	outcome := make(chan duplex.Message, 1)
	invocation := ws.NewInvocation(e.limits, func() { e.retirements.Add(1) })
	address := &duplex.ReturnAddress{Wire: &invocationReturn{owner: e, identifier: identifier, invocation: invocation}}
	e.mu.Lock()
	e.admitted[identifier] = invocation
	e.returns[identifier] = address
	e.outcomes[identifier] = outcome
	receiver := e.receiver
	e.mu.Unlock()
	message := duplex.Message{
		Frame:  duplex.ProfileFrame{Version: 1, Kind: duplex.ProfileRequest, ID: identifier, Params: params},
		Return: address,
	}
	if receiver != nil && receiver.Message != nil {
		receiver.Message(path, message)
	}
	invocation.DispatchDone()
	return identifier, outcome
}

// deliver hands a message to the attachment exactly as it arrived, without
// admitting an invocation of this endpoint's own.
func (e *invocationEndpoint) deliver(path []string, message duplex.Message) {
	e.mu.Lock()
	receiver := e.receiver
	e.mu.Unlock()
	if receiver != nil && receiver.Message != nil {
		receiver.Message(path, message)
	}
}

func (e *invocationEndpoint) cancel(identifier string) {
	e.mu.Lock()
	invocation, address := e.admitted[identifier], e.returns[identifier]
	e.mu.Unlock()
	if invocation == nil {
		return
	}
	_ = invocation.Deliver([]string{ws.InvocationControl}, duplex.Message{
		Frame:  duplex.ProfileFrame{Version: 1, Kind: duplex.ProfileCancel, ID: identifier},
		Return: address,
	})
}

func (e *invocationEndpoint) invocation(identifier string) *ws.Invocation {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.admitted[identifier]
}

// opaqueEndpoint wraps another endpoint with nothing but the contract. It
// passes the complete message, its return capability and its relative path
// through, and recognizes no concrete type on either side.
type opaqueEndpoint struct{ inner duplex.Endpoint }

func (o opaqueEndpoint) Send(path []string, message duplex.Message) error {
	return o.inner.Send(path, message)
}
func (o opaqueEndpoint) Receive(receiver duplex.Receiver) (func(), error) {
	return o.inner.Receive(receiver)
}
func (o opaqueEndpoint) Close(code duplex.Code, reason string) error {
	return o.inner.Close(code, reason)
}

func TestIndependentEndpointParticipatesThroughThePublicVocabulary(t *testing.T) {
	endpoint := newInvocationEndpoint(ws.DefaultInvocationLimits())
	dispatch, err := ws.NewDispatcher(opaqueEndpoint{endpoint})
	if err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}, 1), make(chan struct{})
	cancelled := make(chan error, 1)
	if _, err := ws.HandleWire(dispatch, []string{"read"}, func(ctx context.Context, _ json.RawMessage) (any, error) {
		started <- struct{}{}
		<-release
		cancelled <- ctx.Err()
		return "answer", nil
	}); err != nil {
		t.Fatal(err)
	}
	identifier, outcome := endpoint.admit([]string{"read"}, nil)
	<-started
	invocation := endpoint.invocation(identifier)
	if invocation.Retired() {
		t.Fatal("an invocation retired while its body was still running")
	}
	endpoint.cancel(identifier)
	close(release)
	if err := <-cancelled; err == nil {
		t.Fatal("cancellation did not reach the captured traversal")
	}
	select {
	case <-outcome:
	case <-time.After(5 * time.Second):
		t.Fatal("no outcome")
	}
	waitRetired(t, invocation)
	if endpoint.retirements.Load() != 1 {
		t.Fatalf("retirements: %d", endpoint.retirements.Load())
	}
}

func TestCapturedTraversalSurvivesDetachAndRebind(t *testing.T) {
	endpoint := newInvocationEndpoint(ws.DefaultInvocationLimits())
	dispatch, err := ws.NewDispatcher(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	first, second := make(chan duplex.Message, 4), make(chan duplex.Message, 4)
	detach, err := dispatch.Register([]string{"read"}, duplex.Receiver{Message: func(_ []string, m duplex.Message) { first <- m }})
	if err != nil {
		t.Fatal(err)
	}
	identifier, _ := endpoint.admit([]string{"read"}, nil)
	if got := (<-first).Frame.Kind; got != duplex.ProfileRequest {
		t.Fatalf("first receiver saw %q", got)
	}
	detach()
	if _, err := dispatch.Register([]string{"read"}, duplex.Receiver{Message: func(_ []string, m duplex.Message) { second <- m }}); err != nil {
		t.Fatal(err)
	}
	endpoint.cancel(identifier)
	select {
	case m := <-first:
		if m.Frame.Kind != duplex.ProfileCancel || m.Frame.ID != identifier {
			t.Fatalf("captured receiver got %q %q", m.Frame.Kind, m.Frame.ID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation did not reach the receiver that was captured")
	}
	select {
	case <-second:
		t.Fatal("cancellation reached the rebound receiver")
	default:
	}
}

func TestEachTraversalOfOneDispatcherCapturesSeparately(t *testing.T) {
	endpoint := newInvocationEndpoint(ws.DefaultInvocationLimits())
	dispatch, err := ws.NewDispatcher(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	// One dispatcher visited twice in one traversal: its outer route forwards
	// back into its own inner route. Each visit is a capture of its own.
	seen := make(chan string, 8)
	if _, err := dispatch.Register([]string{"outer"}, duplex.Receiver{Message: func(_ []string, m duplex.Message) {
		if m.Frame.Kind == duplex.ProfileRequest {
			go func() { _ = dispatch.Send([]string{"inner"}, m) }()
		}
		seen <- "outer:" + string(m.Frame.Kind)
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := dispatch.Register([]string{"inner"}, duplex.Receiver{Message: func(_ []string, m duplex.Message) {
		seen <- "inner:" + string(m.Frame.Kind)
	}}); err != nil {
		t.Fatal(err)
	}
	identifier, _ := endpoint.admit([]string{"outer"}, nil)
	collect(t, seen, 2, map[string]bool{"outer:request": true, "inner:request": true})
	endpoint.cancel(identifier)
	collect(t, seen, 2, map[string]bool{"outer:cancel": true, "inner:cancel": true})
}

func TestLatchedCancellationReachesACaptureInstalledAfterIt(t *testing.T) {
	endpoint := newInvocationEndpoint(ws.DefaultInvocationLimits())
	dispatch, err := ws.NewDispatcher(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	inner, err := ws.NewDispatcher(dispatch.Select([]string{"a"}))
	if err != nil {
		t.Fatal(err)
	}
	controls := make(chan duplex.Message, 4)
	var identifier atomic.Value
	identifier.Store("")
	// The outer receiver cancels the invocation before the inner capture is
	// installed. The latch is what carries the control to the later capture.
	if _, err := inner.Register([]string{"read"}, duplex.Receiver{Message: func(_ []string, m duplex.Message) {
		if m.Frame.Kind == duplex.ProfileRequest {
			endpoint.cancel(m.Frame.ID)
			return
		}
		controls <- m
	}}); err != nil {
		t.Fatal(err)
	}
	endpoint.admit([]string{"a", "read"}, nil)
	_ = identifier
	select {
	case m := <-controls:
		if m.Frame.Kind != duplex.ProfileCancel {
			t.Fatalf("latched control was %q", m.Frame.Kind)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a capture installed while cancellation was latched never received it")
	}
}

func TestInvocationBoundsCapturesAndBodies(t *testing.T) {
	endpoint := newInvocationEndpoint(ws.InvocationLimits{Captures: 2, Bodies: 1})
	identifier, _ := endpoint.admit([]string{"read"}, nil)
	invocation := endpoint.invocation(identifier)
	message := duplex.Message{Frame: duplex.ProfileFrame{Version: 1, Kind: duplex.ProfileRequest, ID: identifier},
		Return: &duplex.ReturnAddress{Wire: &invocationReturn{owner: endpoint, identifier: identifier, invocation: invocation}}}
	for i := range 2 {
		if _, err := ws.CaptureInvocation(message, func(duplex.Message) {}); err != nil {
			t.Fatalf("capture %d refused: %v", i, err)
		}
	}
	if _, err := ws.CaptureInvocation(message, func(duplex.Message) {}); !errors.Is(err, ws.ErrInvocationLimit) {
		t.Fatalf("capture beyond the bound: %v", err)
	}
	body, err := ws.BeginInvocationBody(message)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ws.BeginInvocationBody(message); !errors.Is(err, ws.ErrInvocationLimit) {
		t.Fatalf("body beyond the bound: %v", err)
	}
	// A released capture gives back no slot: the bound is a total, so neither
	// depth nor shallow fan-out can grow what one invocation retains.
	body.Done()
	if _, err := ws.BeginInvocationBody(message); !errors.Is(err, ws.ErrInvocationLimit) {
		t.Fatalf("a finished body returned its slot: %v", err)
	}
}

func TestRetirementWaitsForTheBodyAndTheControlDrain(t *testing.T) {
	endpoint := newInvocationEndpoint(ws.DefaultInvocationLimits())
	identifier, _ := endpoint.admit([]string{"read"}, nil)
	invocation := endpoint.invocation(identifier)
	message := duplex.Message{Frame: duplex.ProfileFrame{Version: 1, Kind: duplex.ProfileRequest, ID: identifier},
		Return: &duplex.ReturnAddress{Wire: &invocationReturn{owner: endpoint, identifier: identifier, invocation: invocation}}}
	capture, err := ws.CaptureInvocation(message, func(duplex.Message) {})
	if err != nil {
		t.Fatal(err)
	}
	body, err := ws.BeginInvocationBody(message)
	if err != nil {
		t.Fatal(err)
	}
	invocation.Settle()
	if invocation.Retired() {
		t.Fatal("retired with an undelivered capture and a running body")
	}
	capture.Ready()
	if invocation.Retired() {
		t.Fatal("retired while the body was still running")
	}
	body.Done()
	if !invocation.Retired() {
		t.Fatal("did not retire once settled with nothing outstanding")
	}
	if _, err := ws.CaptureInvocation(message, func(duplex.Message) {}); !errors.Is(err, ws.ErrInvocationEnded) {
		t.Fatalf("a retired invocation admitted a capture: %v", err)
	}
	if _, err := ws.BeginInvocationBody(message); !errors.Is(err, ws.ErrInvocationEnded) {
		t.Fatalf("a retired invocation admitted a body: %v", err)
	}
}

func TestSequentialCompletionsBeyondCapacityRetainNothing(t *testing.T) {
	endpoint := newInvocationEndpoint(ws.InvocationLimits{Captures: 2, Bodies: 2})
	dispatch, err := ws.NewDispatcher(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ws.HandleWire(dispatch, []string{"read"}, func(context.Context, json.RawMessage) (any, error) { return "answer", nil }); err != nil {
		t.Fatal(err)
	}
	var invocations []*ws.Invocation
	for range 32 {
		identifier, outcome := endpoint.admit([]string{"read"}, nil)
		select {
		case <-outcome:
		case <-time.After(5 * time.Second):
			t.Fatal("no outcome")
		}
		invocations = append(invocations, endpoint.invocation(identifier))
	}
	for i, invocation := range invocations {
		waitRetired(t, invocation)
		if i == 0 {
			continue
		}
	}
	if got := endpoint.retirements.Load(); got != 32 {
		t.Fatalf("retirements after 32 sequential completions: %d", got)
	}
}

// A bound the runtime's own facility keeps is a busy refusal: something to
// try again at, not a request that was malformed.
func TestATraversalPastTheCaptureBoundIsRefusedAsBusy(t *testing.T) {
	endpoint := newInvocationEndpoint(ws.InvocationLimits{Captures: 1, Bodies: 8})
	outer, err := ws.NewDispatcher(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	inner, err := ws.NewDispatcher(outer.Select([]string{"a"}))
	if err != nil {
		t.Fatal(err)
	}
	delivered := make(chan struct{}, 1)
	if _, err := inner.Register([]string{"read"}, duplex.Receiver{Message: func([]string, duplex.Message) { delivered <- struct{}{} }}); err != nil {
		t.Fatal(err)
	}
	_, outcome := endpoint.admit([]string{"a", "read"}, nil)
	select {
	case answer := <-outcome:
		if answer.Frame.Error == nil || answer.Frame.Error.Code != "busy" {
			t.Fatalf("a traversal past the bound answered %+v", answer.Frame.Error)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no refusal")
	}
	select {
	case <-delivered:
		t.Fatal("the inner receiver was reached past the bound")
	default:
	}
}

func TestADispatcherRefusesAnInvocationWithoutALifecycle(t *testing.T) {
	endpoint := newInvocationEndpoint(ws.DefaultInvocationLimits())
	dispatch, err := ws.NewDispatcher(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	delivered := make(chan struct{}, 1)
	if _, err := dispatch.Register([]string{"read"}, duplex.Receiver{Message: func([]string, duplex.Message) { delivered <- struct{}{} }}); err != nil {
		t.Fatal(err)
	}
	answered := make(chan duplex.Message, 1)
	bare := &bareReturn{answer: func(m duplex.Message) { answered <- m }}
	// A request arrives through the contract alone, with a return capability
	// that carries no lifecycle. It is refused explicitly, on its own original
	// return capability, rather than routed with weaker guarantees.
	endpoint.deliver([]string{"read"}, duplex.Message{
		Frame:  duplex.ProfileFrame{Version: 1, Kind: duplex.ProfileRequest, ID: "b:1", Params: []byte("null")},
		Return: &duplex.ReturnAddress{Wire: bare},
	})
	select {
	case refusal := <-answered:
		if refusal.Frame.Error == nil || refusal.Frame.Error.Code != "invalid_message" {
			t.Fatalf("refusal was %+v", refusal.Frame.Error)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("an unmanaged invocation was neither routed nor refused")
	}
	select {
	case <-delivered:
		t.Fatal("an unmanaged invocation was routed with weaker guarantees")
	default:
	}
	if bare.uses.Load() != 1 {
		t.Fatalf("the refusal did not use the original return capability once: %d", bare.uses.Load())
	}
}

type bareReturn struct {
	answer func(duplex.Message)
	uses   atomic.Int64
}

func (b *bareReturn) Send(path []string, message duplex.Message) error {
	if len(path) != 0 {
		return errors.New("this return capability carries no lifecycle")
	}
	b.uses.Add(1)
	b.answer(message)
	return nil
}

func waitRetired(t *testing.T, invocation *ws.Invocation) {
	t.Helper()
	for range 500 {
		if invocation.Retired() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("invocation never retired")
}

func collect(t *testing.T, seen chan string, count int, want map[string]bool) {
	t.Helper()
	got := map[string]bool{}
	for range count {
		select {
		case value := <-seen:
			got[value] = true
		case <-time.After(5 * time.Second):
			t.Fatalf("saw %v, wanted %v", got, want)
		}
	}
	for value := range want {
		if !got[value] {
			t.Fatalf("saw %v, wanted %v", got, want)
		}
	}
}
