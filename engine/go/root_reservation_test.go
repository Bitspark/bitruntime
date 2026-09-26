package engine

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	core "github.com/Bitspark/bitruntime/core/go"
	"github.com/Bitspark/bitruntime/internal/request/go"
	transports "github.com/Bitspark/bitruntime/transports/go"
	wire "github.com/Bitspark/bitwire/wire/go"
)

// Ported from Nightseam v0.6.0 runtime/go/wire_cancel_reservation_test.go.
// These tests read the peer's outgoing queue and complete its pending calls
// directly, so they live beside the root they exercise.

type wireDrainGate struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *wireDrainGate) open() { g.once.Do(func() { close(g.release) }) }

type wireReservationPropagator struct {
	core.Propagator
	gates chan *wireDrainGate
}

func (p *wireReservationPropagator) Extract(ctx context.Context, trace core.Trace) context.Context {
	select {
	case gate := <-p.gates:
		close(gate.started)
		select {
		case <-gate.release:
		case <-ctx.Done():
		}
	default:
	}
	return p.Propagator.Extract(ctx, trace)
}
func (p *wireReservationPropagator) pause(t *testing.T) *wireDrainGate {
	gate := &wireDrainGate{started: make(chan struct{}), release: make(chan struct{})}
	p.gates <- gate
	t.Cleanup(gate.open)
	return gate
}

type wireReservationSink struct {
	replies chan wire.ProfileFrame
	onReply func(wire.ProfileFrame)
}

func (s *wireReservationSink) Send(_ []string, message wire.Message) error {
	if s.onReply != nil {
		s.onReply(message.Frame)
	}
	s.replies <- message.Frame
	return nil
}

func wireReservationAwait[T any](t *testing.T, values <-chan T) T {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("wire reservation barrier did not arrive")
		var zero T
		return zero
	}
}

// Only the root dispatcher runs. Its actual peer admission writes into an
// independently drained carrier queue, so a full root queue is tested without
// a second, unrelated transport saturation masking the root's result.
func wireReservationPeer(t *testing.T) (*Peer, wire.AddressedWire, *wireReservationPropagator) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	near, far := transports.Pipe(1 << 20)
	propagator := &wireReservationPropagator{Propagator: core.DefaultPropagator, gates: make(chan *wireDrainGate, 1)}
	options, err := (Options{QueueCapacity: 1, MaxPendingRequests: 1, Propagator: propagator}).Normalized()
	if err != nil {
		t.Fatal(err)
	}
	peer := &Peer{ctx: ctx, cancel: cancel, conn: near, options: options, prefix: "c:", remotePrefix: "s:", done: make(chan struct{}), pending: map[string]chan request.Result{}, incoming: map[string]context.CancelFunc{}, outputs: make(chan queuedFrame, 16)}
	peer.root = &rootWire{peer: peer, wake: make(chan struct{}, 1), incoming: map[returnKey]*routedCall{}}
	go peer.root.run()
	t.Cleanup(func() { _ = peer.Close(); _ = far.Abort() })
	return peer, peer.Wire(), propagator
}

func wireReservationMessage(kind wire.ProfileKind, id string, address *wire.ReturnAddress) wire.Message {
	frame := wire.ProfileFrame{Version: 1, Kind: kind, ID: id}
	if kind == wire.ProfileRequest {
		frame.Params = json.RawMessage(`{}`)
	} else if kind == wire.ProfileEvent {
		frame.Data = json.RawMessage(`null`)
	}
	return wire.Message{Frame: frame, Return: address}
}
func wireReservationSend(t *testing.T, root wire.AddressedWire, kind wire.ProfileKind, id string, address *wire.ReturnAddress) {
	t.Helper()
	if err := root.Send([]string{"operation"}, wireReservationMessage(kind, id, address)); err != nil {
		t.Fatal(err)
	}
}

func TestRootWireCancellationHasReservedAdmissionAndKeepsFIFO(t *testing.T) {
	peer, root, propagator := wireReservationPeer(t)
	sink := &wireReservationSink{replies: make(chan wire.ProfileFrame, 8)}
	address := &wire.ReturnAddress{Wire: sink}
	gate := propagator.pause(t)
	wireReservationSend(t, root, wire.ProfileRequest, "c:1", address)
	wireReservationAwait(t, gate.started)
	wireReservationSend(t, root, wire.ProfileEvent, "", nil) // The sole data slot is occupied.
	wireReservationSend(t, root, wire.ProfileCancel, "c:1", address)
	for range 4 {
		wireReservationSend(t, root, wire.ProfileCancel, "c:1", address)
		wireReservationSend(t, root, wire.ProfileCancel, "c:999", address)
	}
	if err := peer.Err(); err != nil {
		t.Fatalf("cancellation ended its carrier: %v", err)
	}
	gate.open()
	var kinds []string
	for range 3 {
		kinds = append(kinds, wireReservationAwait(t, peer.outputs).frame.Kind)
	}
	if !reflect.DeepEqual(kinds, []string{"request", "event", "cancel"}) {
		t.Fatalf("physical admission order = %v", kinds)
	}
	if reply := wireReservationAwait(t, sink.replies); reply.Error == nil || reply.Error.Code != "cancelled" {
		t.Fatalf("cancel reply = %+v", reply)
	}
	// A fence after duplicate, unknown and settled controls proves none escaped.
	wireReservationSend(t, root, wire.ProfileCancel, "c:1", address)
	wireReservationSend(t, root, wire.ProfileEvent, "", nil)
	if got := wireReservationAwait(t, peer.outputs).frame.Kind; got != "event" {
		t.Fatalf("stale control reached the carrier: %s", got)
	}
	if err := peer.Err(); err != nil {
		t.Fatalf("carrier ended: %v", err)
	}
}

func TestRootWireCompletedCallRetainsItsQueuedCancellationBudget(t *testing.T) {
	peer, root, propagator := wireReservationPeer(t)
	first := &wireReservationSink{replies: make(chan wire.ProfileFrame, 4)}
	address := &wire.ReturnAddress{Wire: first}
	second := &wireReservationSink{replies: make(chan wire.ProfileFrame, 4)}
	secondAddress := &wire.ReturnAddress{Wire: second}
	reentrant := make(chan error, 1)
	first.onReply = func(wire.ProfileFrame) {
		reentrant <- root.Send([]string{"operation"}, wireReservationMessage(wire.ProfileRequest, "c:2", secondAddress))
	}
	wireReservationSend(t, root, wire.ProfileRequest, "c:1", address)
	requested := wireReservationAwait(t, peer.outputs).frame
	gate := propagator.pause(t)
	wireReservationSend(t, root, wire.ProfileEvent, "", nil)
	wireReservationAwait(t, gate.started)
	wireReservationSend(t, root, wire.ProfileCancel, "c:1", address)
	// Complete before the root can drain the control. The response callback
	// attempts to spend the same one-request budget while its stale control is
	// still queued; it must receive busy, not replenish control capacity.
	peer.mu.Lock()
	reply := peer.pending[requested.ID]
	peer.mu.Unlock()
	reply <- request.Result{Value: json.RawMessage(`7`)}
	if err := wireReservationAwait(t, reentrant); err != nil {
		t.Fatalf("refusal admission closed carrier: %v", err)
	}
	if response := wireReservationAwait(t, first.replies); string(response.Result) != "7" {
		t.Fatalf("first response = %+v", response)
	}
	gate.open()
	if got := wireReservationAwait(t, peer.outputs).frame.Kind; got != "event" {
		t.Fatalf("queued event = %s", got)
	}
	if response := wireReservationAwait(t, second.replies); response.Error == nil || response.Error.Code != "busy" {
		t.Fatalf("retained reservation allowed another request: %+v", response)
	}
	// Reuse the original local identity after the control drains. The stale
	// cancellation must neither cancel it nor delete its new state.
	first.onReply = nil
	wireReservationSend(t, root, wire.ProfileRequest, "c:1", address)
	third := wireReservationAwait(t, peer.outputs).frame
	if third.Kind != "request" {
		t.Fatalf("stale control was emitted: %+v", third)
	}
	peer.mu.Lock()
	reply = peer.pending[third.ID]
	peer.mu.Unlock()
	reply <- request.Result{Value: json.RawMessage(`9`)}
	if response := wireReservationAwait(t, first.replies); string(response.Result) != "9" {
		t.Fatalf("reused identity response = %+v", response)
	}
	if err := peer.Err(); err != nil {
		t.Fatalf("carrier ended: %v", err)
	}
}

func TestRootWireCancellationReservationDoesNotIncreaseDataCapacity(t *testing.T) {
	peer, root, propagator := wireReservationPeer(t)
	gate := propagator.pause(t)
	wireReservationSend(t, root, wire.ProfileEvent, "", nil)
	wireReservationAwait(t, gate.started)
	wireReservationSend(t, root, wire.ProfileEvent, "", nil)
	if err := root.Send([]string{"operation"}, wireReservationMessage(wire.ProfileEvent, "", nil)); !errors.Is(err, core.ErrBackpressure) {
		t.Fatalf("extra data admission = %v", err)
	}
	if !errors.Is(peer.Err(), core.ErrBackpressure) {
		t.Fatalf("full data carrier remained open: %v", peer.Err())
	}
}
