package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/Bitspark/nightseam/duplex/go"
	ws "github.com/Bitspark/nightseam/runtime/go"
)

type namespaceObservation struct {
	Picked string
	Path   []string
}

func namespaceReceiver(picked string, events chan namespaceObservation) duplex.Receiver {
	return duplex.Receiver{Message: func(path []string, message duplex.Message) {
		observation := namespaceObservation{picked, append([]string{}, path...)}
		if message.Frame.Kind == duplex.ProfileEvent {
			events <- observation
			return
		}
		if message.Frame.Kind != duplex.ProfileRequest {
			return
		}
		data, _ := json.Marshal(observation)
		_ = message.Return.Wire.Send(nil, duplex.Message{Frame: duplex.ProfileFrame{Version: 1, Kind: duplex.ProfileResponse, ID: message.Frame.ID, Result: data}})
	}}
}

func TestDispatcherUsesExactThenLongestSegmentPrefix(t *testing.T) {
	client, server := newPair(t, ws.Options{}, ws.Options{})
	events := make(chan namespaceObservation, 20)
	wire := testBinding(t, server.Wire())
	for _, route := range []struct {
		path []string
		name string
	}{{nil, "root"}, {[]string{"a"}, "a"}, {[]string{"a", "b"}, "ab"}} {
		if _, err := wire.RegisterPrefix(route.path, namespaceReceiver(route.name, events)); err != nil {
			t.Fatal(err)
		}
		if _, err := wire.RegisterPrefix(route.path, namespaceReceiver("duplicate", events)); !errors.Is(err, duplex.ErrReceiverExists) {
			t.Fatalf("duplicate namespace = %v", err)
		}
	}
	detach, err := wire.Register([]string{"a"}, namespaceReceiver("exact", events))
	if err != nil {
		t.Fatal(err)
	}
	for _, route := range []struct {
		path   []string
		picked string
	}{
		{[]string{"a"}, "exact"}, {[]string{"a", "b", "leaf"}, "ab"}, {[]string{"a", "bc"}, "a"}, {[]string{"a.b", "leaf"}, "root"}, {[]string{"", "😀"}, "root"},
	} {
		var got namespaceObservation
		if err := ws.CallWire(context.Background(), client.Wire(), route.path, nil, &got); err != nil {
			t.Fatal(err)
		}
		want := namespaceObservation{route.picked, route.path}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("request = %+v; want %+v", got, want)
		}
		if err := ws.EmitWire(context.Background(), client.Wire(), route.path, nil); err != nil {
			t.Fatal(err)
		}
		if got := receive(t, events); !reflect.DeepEqual(got, want) {
			t.Fatalf("event = %+v; want %+v", got, want)
		}
	}
	detach()
	detach()
	var got namespaceObservation
	if err := ws.CallWire(context.Background(), client.Wire(), []string{"a"}, nil, &got); err != nil || got.Picked != "a" {
		t.Fatalf("exact detach did not expose namespace: %+v %v", got, err)
	}
	if err := server.Handle("ordinary", func(context.Context, *ws.Peer, json.RawMessage) (any, error) { return "raw", nil }); err != nil {
		t.Fatal(err)
	}
	var raw string
	if err := client.Call(context.Background(), "ordinary", nil, &raw); err != nil || raw != "raw" {
		t.Fatalf("raw handler = %q %v", raw, err)
	}
	for _, name := range []string{"unknown.raw", "01:a", "1:a.invalid"} {
		err := client.Call(context.Background(), name, nil, nil)
		var public *ws.PublicError
		if !errors.As(err, &public) || public.Code != "method_not_found" {
			t.Fatalf("namespace captured noncanonical name %q: %v", name, err)
		}
	}
}

func TestDispatcherCancellationKeepsOriginalRegistration(t *testing.T) {
	client, server := newPair(t, ws.Options{}, ws.Options{})
	serverBinding := testBinding(t, server.Wire())
	started := make(chan *duplex.ReturnAddress, 1)
	cancelled := make(chan *duplex.ReturnAddress, 1)
	detach, err := serverBinding.RegisterPrefix([]string{"worker"}, duplex.Receiver{Message: func(_ []string, m duplex.Message) {
		if m.Frame.Kind == duplex.ProfileRequest {
			started <- m.Return
		}
		if m.Frame.Kind == duplex.ProfileCancel {
			cancelled <- m.Return
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- ws.CallWire(ctx, client.Wire(), []string{"worker", "dynamic"}, nil, nil) }()
	original := receive(t, started)
	detach()
	replacement := make(chan duplex.ProfileKind, 4)
	if _, err := serverBinding.RegisterPrefix([]string{"worker"}, duplex.Receiver{Message: func(_ []string, m duplex.Message) { replacement <- m.Frame.Kind }}); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := receive(t, result); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if got := receive(t, cancelled); got != original {
		t.Fatal("cancellation changed the original return capability")
	}
	select {
	case got := <-replacement:
		t.Fatalf("replacement received old request's %s", got)
	default:
	}
}

func TestStructuredWireBridgePreservesTraceVerbatim(t *testing.T) {
	client, server := newPair(t, ws.Options{}, ws.Options{})
	serverBinding := testBinding(t, server.Wire())
	frames := make(chan duplex.ProfileFrame, 8)
	_, err := serverBinding.Register([]string{"trace"}, duplex.Receiver{Message: func(_ []string, m duplex.Message) {
		frames <- m.Frame
		if m.Frame.Kind == duplex.ProfileRequest {
			_ = m.Return.Wire.Send(nil, duplex.Message{Frame: duplex.ProfileFrame{Version: 1, Kind: duplex.ProfileResponse, ID: m.Frame.ID, Result: json.RawMessage(`null`)}})
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	sink := &wireReplySink{replies: make(chan duplex.ProfileFrame, 2)}
	address := &duplex.ReturnAddress{Wire: sink}
	for _, trace := range []ws.Trace{{Parent: "00-11111111111111111111111111111111-2222222222222222-01", State: "vendor=value"}, {}} {
		for _, kind := range []duplex.ProfileKind{duplex.ProfileRequest, duplex.ProfileEvent} {
			frame := duplex.ProfileFrame{Version: 1, Kind: kind, Traceparent: trace.Parent, Tracestate: trace.State}
			if kind == duplex.ProfileRequest {
				frame.ID = "c:1"
				frame.Params = json.RawMessage(`null`)
			} else {
				frame.Data = json.RawMessage(`null`)
			}
			if err := client.Wire().Send([]string{"trace"}, duplex.Message{Frame: frame, Return: address}); err != nil {
				t.Fatal(err)
			}
			got := receive(t, frames)
			if got.Traceparent != trace.Parent || got.Tracestate != trace.State {
				t.Fatalf("structured %s trace changed: %+v; want %+v", kind, got, trace)
			}
			if kind == duplex.ProfileRequest {
				reply := receive(t, sink.replies)
				if reply.Traceparent != trace.Parent || reply.Tracestate != trace.State {
					t.Fatalf("response trace changed: %+v", reply)
				}
			}
		}
	}
}

func TestRegisterWireGroupsRequestAndEventAtOnePath(t *testing.T) {
	client, server := newPair(t, ws.Options{}, ws.Options{})
	serverBinding := testBinding(t, server.Wire())
	events := make(chan string, 2)
	detach, err := ws.RegisterWire(serverBinding, []string{"shared"}, ws.WireHandlers{
		Request: func(_ context.Context, value json.RawMessage) (any, error) { return value, nil },
		Event: func(_ context.Context, value json.RawMessage) error {
			var text string
			_ = json.Unmarshal(value, &text)
			events <- text
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var got string
	if err := ws.CallWire(context.Background(), client.Wire(), []string{"shared"}, "response", &got); err != nil || got != "response" {
		t.Fatalf("grouped request = %q %v", got, err)
	}
	if err := ws.EmitWire(context.Background(), client.Wire(), []string{"shared"}, "event"); err != nil {
		t.Fatal(err)
	}
	if got := receive(t, events); got != "event" {
		t.Fatal(got)
	}
	detach()
	detach()
	_, err = ws.RegisterWire(serverBinding, []string{"shared"}, ws.WireHandlers{Event: func(context.Context, json.RawMessage) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	err = ws.CallWire(context.Background(), client.Wire(), []string{"shared"}, nil, nil)
	var public *ws.PublicError
	if !errors.As(err, &public) || public.Code != "method_not_found" {
		t.Fatalf("event-only request did not refuse: %v", err)
	}
	if _, err := ws.RegisterWire(serverBinding, []string{"empty"}, ws.WireHandlers{}); err == nil {
		t.Fatal("registered empty handlers")
	}
}

// This fixture created and owns the carrier, so its registry explicitly owns
// fatal-handler closure. Ordinary borrowed dispatchers only detach themselves.
type ownedEventRegistry struct {
	*ws.Dispatcher
	endpoint duplex.Endpoint
}

func (r ownedEventRegistry) Close(code duplex.Code, reason string) error {
	_ = r.Dispatcher.Close(code, reason)
	return r.endpoint.Close(code, reason)
}

func TestRegisterWireEventFailuresEndOnlyTheirCarrierWithSanitizedReason(t *testing.T) {
	for _, panics := range []bool{false, true} {
		t.Run(map[bool]string{false: "error", true: "panic"}[panics], func(t *testing.T) {
			log := &recorder{}
			client, server := newPair(t, ws.Options{Observer: log}, ws.Options{})
			serverBinding := ownedEventRegistry{testBinding(t, server.Wire()), server.Wire()}
			_, err := ws.RegisterWire(serverBinding, []string{"rejected"}, ws.WireHandlers{Event: func(context.Context, json.RawMessage) error {
				if panics {
					panic("private failure")
				}
				return errors.New("private failure")
			}})
			if err != nil {
				t.Fatal(err)
			}
			if err := ws.EmitWire(context.Background(), client.Wire(), []string{"rejected"}, nil); err != nil {
				t.Fatal(err)
			}
			receive(t, server.Done())
			wireChannelAwait(t, log, 1, func(event ws.ObserverEvent) bool { _, ok := event.(ws.ConnectionClosed); return ok })
			for _, event := range log.all() {
				if closed, ok := event.(ws.ConnectionClosed); ok {
					if closed.Code != 1002 || closed.Reason != "wire event rejected" {
						t.Fatalf("event ending = %+v", closed)
					}
					return
				}
			}
			t.Fatal("no carrier ending was observed")
		})
	}
}

type forwardRegistrationWire struct {
	receiver   duplex.Receiver
	sent       []duplex.Message
	paths      [][]string
	detached   int
	closed     int
	receiveErr error
	sendErr    error
}

func (w *forwardRegistrationWire) Send(path []string, message duplex.Message) error {
	w.paths = append(w.paths, path)
	w.sent = append(w.sent, message)
	return w.sendErr
}
func (w *forwardRegistrationWire) Receive(receiver duplex.Receiver) (func(), error) {
	if w.receiveErr != nil {
		return nil, w.receiveErr
	}
	w.receiver = receiver
	var once sync.Once
	return func() { once.Do(func() { w.detached++ }) }, nil
}
func (w *forwardRegistrationWire) Close(duplex.Code, string) error { w.closed++; return nil }

func TestForwardWirePreservesMessagesAndOwnsOnlyRegistrations(t *testing.T) {
	left, right := &forwardRegistrationWire{}, &forwardRegistrationWire{}
	detach, err := ws.ForwardWire(left, right)
	if err != nil {
		t.Fatal(err)
	}
	returning := &duplex.ReturnAddress{Wire: left}
	path := []string{"unknown", "a.b", "", "😀"}
	for _, kind := range []duplex.ProfileKind{duplex.ProfileRequest, duplex.ProfileResponse, duplex.ProfileEvent, duplex.ProfileCancel} {
		message := duplex.Message{Frame: duplex.ProfileFrame{Version: 1, Kind: kind, ID: "c:1", Params: json.RawMessage(`{"n":1e3}`)}, Return: returning}
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
	if _, err := ws.ForwardWire(left, right); err == nil || left.detached != 1 || left.closed != 0 {
		t.Fatalf("partial install leaked: %+v %v", left, err)
	}
	left, right = &forwardRegistrationWire{}, &forwardRegistrationWire{sendErr: ws.Unpublished(&ws.PublicError{Code: "busy", Message: "Busy"})}
	if _, err := ws.ForwardWire(left, right); err != nil {
		t.Fatal(err)
	}
	sink := &wireReplySink{replies: make(chan duplex.ProfileFrame, 1)}
	left.receiver.Message(path, duplex.Message{Frame: duplex.ProfileFrame{Version: 1, Kind: duplex.ProfileRequest, ID: "c:1"}, Return: &duplex.ReturnAddress{Wire: sink}})
	response := receive(t, sink.replies)
	if response.Error == nil || response.Error.Code != "busy" || left.detached != 1 || right.detached != 1 || right.closed != 0 {
		t.Fatalf("failed forwarding did not refuse and detach: %+v %+v %+v", response, left, right)
	}
}

func TestForwardWireCarriesUnknownPathsAndReverseCallsAcrossPeers(t *testing.T) {
	client, middleIn := newPair(t, ws.Options{}, ws.Options{})
	middleOut, server := newPair(t, ws.Options{}, ws.Options{})
	inbound := testBinding(t, duplex.Mount(map[string]duplex.Endpoint{"in": testBinding(t, middleIn.Wire()).Select([]string{"gateway"})})).Select([]string{"in"})
	outbound := testBinding(t, duplex.Mount(map[string]duplex.Endpoint{"out": testBinding(t, middleOut.Wire()).Select([]string{"service"})})).Select([]string{"out"})
	caller := testBinding(t, testBinding(t, client.Wire()).Select([]string{"gateway"}))
	implementation := testBinding(t, testBinding(t, server.Wire()).Select([]string{"service"}))
	detach, err := ws.ForwardWire(inbound, outbound)
	if err != nil {
		t.Fatal(err)
	}
	defer detach()
	_, err = ws.HandleWire(caller, []string{"reverse", "dynamic"}, func(_ context.Context, value json.RawMessage) (any, error) { return value, nil })
	if err != nil {
		t.Fatal(err)
	}
	_, err = ws.HandleWire(implementation, []string{"arbitrary", "nested", "call"}, func(ctx context.Context, value json.RawMessage) (any, error) {
		var result string
		if err := ws.CallWire(ctx, implementation, []string{"reverse", "dynamic"}, value, &result); err != nil {
			return nil, err
		}
		return result + " returned", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var result string
	if err := ws.CallWire(context.Background(), caller, []string{"arbitrary", "nested", "call"}, "callback", &result); err != nil || result != "callback returned" {
		t.Fatalf("forwarded reverse call = %q %v", result, err)
	}
	events := make(chan string, 2)
	_, err = ws.RegisterWire(implementation, []string{"arbitrary", "nested", "event"}, ws.WireHandlers{Event: func(_ context.Context, value json.RawMessage) error {
		var text string
		_ = json.Unmarshal(value, &text)
		events <- text
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := ws.EmitWire(context.Background(), caller, []string{"arbitrary", "nested", "event"}, "observed"); err != nil {
		t.Fatal(err)
	}
	if got := receive(t, events); got != "observed" {
		t.Fatal(got)
	}
	started, ended := make(chan struct{}), make(chan struct{})
	_, err = ws.HandleWire(implementation, []string{"arbitrary", "cancel"}, func(ctx context.Context, _ json.RawMessage) (any, error) {
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
	go func() { finished <- ws.CallWire(ctx, caller, []string{"arbitrary", "cancel"}, nil, nil) }()
	receive(t, started)
	detach()
	cancel()
	if err := receive(t, finished); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	receive(t, ended)
	for _, peer := range []*ws.Peer{client, middleIn, middleOut, server} {
		if err := peer.Err(); err != nil {
			t.Fatalf("forward detach closed peer: %v", err)
		}
	}
}
