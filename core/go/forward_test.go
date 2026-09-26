package core_test

// Ported from Nightseam v0.6.0 runtime/go/wire_namespace_test.go (commit
// 5cc9723a): the ForwardWire tests, which concern core.Forward alone.

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"

	core "github.com/Bitspark/bitruntime/core/go"
	dispatch "github.com/Bitspark/bitruntime/dispatch/go"
	engine "github.com/Bitspark/bitruntime/engine/go"
	transports "github.com/Bitspark/bitruntime/transports/go"
	wire "github.com/Bitspark/bitwire/wire/go"
)

type wireReplySink struct{ replies chan wire.ProfileFrame }

// A return capability refuses what it does not implement, as every addressed
// receiver in this profile does; this one carries outcomes and nothing else.
func (s *wireReplySink) Send(path []string, message wire.Message) error {
	if len(path) != 0 {
		return errors.New("this return capability carries outcomes only")
	}
	s.replies <- message.Frame
	return nil
}

type forwardRegistrationWire struct {
	receiver   wire.Receiver
	sent       []wire.Message
	paths      [][]string
	detached   int
	closed     int
	receiveErr error
	sendErr    error
}

func (w *forwardRegistrationWire) Send(path []string, message wire.Message) error {
	w.paths = append(w.paths, path)
	w.sent = append(w.sent, message)
	return w.sendErr
}
func (w *forwardRegistrationWire) Receive(receiver wire.Receiver) (func(), error) {
	if w.receiveErr != nil {
		return nil, w.receiveErr
	}
	w.receiver = receiver
	var once sync.Once
	return func() { once.Do(func() { w.detached++ }) }, nil
}
func (w *forwardRegistrationWire) Close(wire.Code, string) error { w.closed++; return nil }

// The last section asserts research 0001 row 14 rather than v0.6.0: a message
// the destination refuses fails only that message. v0.6.0 answered the refused
// request and then detached both directions (left.detached == 1 and
// right.detached == 1); forwarding now goes on.
func TestForwardWirePreservesMessagesAndOwnsOnlyRegistrations(t *testing.T) {
	left, right := &forwardRegistrationWire{}, &forwardRegistrationWire{}
	detach, err := core.Forward(left, right)
	if err != nil {
		t.Fatal(err)
	}
	returning := &wire.ReturnAddress{Wire: left}
	path := []string{"unknown", "a.b", "", "😀"}
	for _, kind := range []wire.ProfileKind{wire.ProfileRequest, wire.ProfileResponse, wire.ProfileEvent, wire.ProfileCancel} {
		message := wire.Message{Frame: wire.ProfileFrame{Version: 1, Kind: kind, ID: "c:1", Params: json.RawMessage(`{"n":1e3}`)}, Return: returning}
		left.receiver.Message(path, message)
		right.receiver.Message(path, message)
		if !reflect.DeepEqual(left.sent[len(left.sent)-1], message) || !reflect.DeepEqual(right.sent[len(right.sent)-1], message) {
			t.Fatalf("%s changed", kind)
		}
		if left.sent[len(left.sent)-1].Return != returning || right.sent[len(right.sent)-1].Return != returning {
			t.Fatal("return capability changed")
		}
	}
	if !reflect.DeepEqual(left.paths[0], path) || !reflect.DeepEqual(right.paths[0], path) {
		t.Fatal("forward path changed")
	}
	left.receiver.Closed(1000, "ended")
	detach()
	detach()
	if left.detached != 1 || right.detached != 1 || left.closed != 0 || right.closed != 0 {
		t.Fatalf("lifecycle: left=%+v right=%+v", left, right)
	}
	left, right = &forwardRegistrationWire{}, &forwardRegistrationWire{receiveErr: errors.New("installation refused")}
	if _, err := core.Forward(left, right); err == nil || left.detached != 1 || left.closed != 0 {
		t.Fatalf("partial install leaked: %+v %v", left, err)
	}
	left, right = &forwardRegistrationWire{}, &forwardRegistrationWire{sendErr: core.Unpublished(&core.PublicError{Code: "busy", Message: "Busy"})}
	if _, err := core.Forward(left, right); err != nil {
		t.Fatal(err)
	}
	sink := &wireReplySink{replies: make(chan wire.ProfileFrame, 1)}
	left.receiver.Message(path, wire.Message{Frame: wire.ProfileFrame{Version: 1, Kind: wire.ProfileRequest, ID: "c:1"}, Return: &wire.ReturnAddress{Wire: sink}})
	response := receive(t, sink.replies)
	if response.Error == nil || response.Error.Code != "busy" || left.detached != 0 || right.detached != 0 || right.closed != 0 {
		t.Fatalf("a refused message was not refused alone: %+v %+v %+v", response, left, right)
	}
	right.sendErr = nil
	event := wire.Message{Frame: wire.ProfileFrame{Version: 1, Kind: wire.ProfileEvent, Data: json.RawMessage("null")}}
	left.receiver.Message(path, event)
	if !reflect.DeepEqual(right.sent[len(right.sent)-1], event) {
		t.Fatal("forwarding did not go on after a refused message")
	}
}

// R26: a request whose destination refuses it as closed — ended, or ended by
// backpressure — is answered disconnected, not internal.
func TestForwardAnswersARequestRefusedByAnEndedDestinationDisconnected(t *testing.T) {
	for name, refusal := range map[string]error{
		"closed":       core.Unpublished(transports.ErrClosed),
		"backpressure": core.Unpublished(core.Ended(core.ErrBackpressure)),
	} {
		t.Run(name, func(t *testing.T) {
			left, right := &forwardRegistrationWire{}, &forwardRegistrationWire{sendErr: refusal}
			if _, err := core.Forward(left, right); err != nil {
				t.Fatal(err)
			}
			sink := &wireReplySink{replies: make(chan wire.ProfileFrame, 1)}
			left.receiver.Message([]string{"call"}, wire.Message{Frame: wire.ProfileFrame{Version: 1, Kind: wire.ProfileRequest, ID: "c:1"}, Return: &wire.ReturnAddress{Wire: sink}})
			if response := receive(t, sink.replies); response.Error == nil || response.Error.Code != "disconnected" || response.ID != "c:1" {
				t.Fatalf("request refused by an ended destination: %+v", response)
			}
		})
	}
}

func TestForwardWireCarriesUnknownPathsAndReverseCallsAcrossPeers(t *testing.T) {
	client, middleIn := newPeerPair(t, engine.Options{}, engine.Options{})
	middleOut, server := newPeerPair(t, engine.Options{}, engine.Options{})
	inbound := testBinding(t, core.Mount(map[string]wire.Endpoint{"in": testBinding(t, middleIn.Wire()).Select([]string{"gateway"})})).Select([]string{"in"})
	outbound := testBinding(t, core.Mount(map[string]wire.Endpoint{"out": testBinding(t, middleOut.Wire()).Select([]string{"service"})})).Select([]string{"out"})
	caller := testBinding(t, testBinding(t, client.Wire()).Select([]string{"gateway"}))
	implementation := testBinding(t, testBinding(t, server.Wire()).Select([]string{"service"}))
	detach, err := core.Forward(inbound, outbound)
	if err != nil {
		t.Fatal(err)
	}
	defer detach()
	_, err = dispatch.Handle(caller, []string{"reverse", "dynamic"}, func(_ context.Context, value json.RawMessage) (any, error) { return value, nil })
	if err != nil {
		t.Fatal(err)
	}
	_, err = dispatch.Handle(implementation, []string{"arbitrary", "nested", "call"}, func(ctx context.Context, value json.RawMessage) (any, error) {
		var result string
		if err := dispatch.Call(ctx, implementation, []string{"reverse", "dynamic"}, value, &result); err != nil {
			return nil, err
		}
		return result + " returned", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var result string
	if err := dispatch.Call(context.Background(), caller, []string{"arbitrary", "nested", "call"}, "callback", &result); err != nil || result != "callback returned" {
		t.Fatalf("forwarded reverse call = %q %v", result, err)
	}
	events := make(chan string, 2)
	_, err = dispatch.Register(implementation, []string{"arbitrary", "nested", "event"}, dispatch.Handlers{Event: func(_ context.Context, value json.RawMessage) error {
		var text string
		_ = json.Unmarshal(value, &text)
		events <- text
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := dispatch.Emit(context.Background(), caller, []string{"arbitrary", "nested", "event"}, "observed"); err != nil {
		t.Fatal(err)
	}
	if got := receive(t, events); got != "observed" {
		t.Fatal(got)
	}
	started, ended := make(chan struct{}), make(chan struct{})
	_, err = dispatch.Handle(implementation, []string{"arbitrary", "cancel"}, func(ctx context.Context, _ json.RawMessage) (any, error) {
		close(started)
		<-ctx.Done()
		close(ended)
		return nil, ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() { finished <- dispatch.Call(ctx, caller, []string{"arbitrary", "cancel"}, nil, nil) }()
	receive(t, started)
	detach()
	cancel()
	if err := receive(t, finished); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	receive(t, ended)
	for _, peer := range []*engine.Peer{client, middleIn, middleOut, server} {
		if err := peer.Err(); err != nil {
			t.Fatalf("forward detach closed peer: %v", err)
		}
	}
}
