package dispatch_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	core "github.com/Bitspark/bitruntime/core/go"
	dispatch "github.com/Bitspark/bitruntime/dispatch/go"
	engine "github.com/Bitspark/bitruntime/engine/go"
	transports "github.com/Bitspark/bitruntime/transports/go"
	wire "github.com/Bitspark/bitwire/wire/go"
)

type namespaceObservation struct {
	Picked string
	Path   []string
}

func namespaceReceiver(picked string, events chan namespaceObservation) wire.Receiver {
	return wire.Receiver{Message: func(path []string, message wire.Message) {
		observation := namespaceObservation{picked, append([]string{}, path...)}
		if message.Frame.Kind == wire.ProfileEvent {
			events <- observation
			return
		}
		if message.Frame.Kind != wire.ProfileRequest {
			return
		}
		data, _ := json.Marshal(observation)
		_ = message.Return.Wire.Send(nil, wire.Message{Frame: wire.ProfileFrame{Version: 1, Kind: wire.ProfileResponse, ID: message.Frame.ID, Result: data}})
	}}
}

func TestDispatcherUsesExactThenLongestSegmentPrefix(t *testing.T) {
	client, server := newPair(t, engine.Options{}, engine.Options{})
	events := make(chan namespaceObservation, 20)
	root := testBinding(t, server.Wire())
	for _, route := range []struct {
		path []string
		name string
	}{{nil, "root"}, {[]string{"a"}, "a"}, {[]string{"a", "b"}, "ab"}} {
		if _, err := root.RegisterPrefix(route.path, namespaceReceiver(route.name, events)); err != nil {
			t.Fatal(err)
		}
		if _, err := root.RegisterPrefix(route.path, namespaceReceiver("duplicate", events)); !errors.Is(err, core.ErrReceiverExists) {
			t.Fatalf("duplicate namespace = %v", err)
		}
	}
	detach, err := root.Register([]string{"a"}, namespaceReceiver("exact", events))
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
		if err := dispatch.Call(context.Background(), client.Wire(), route.path, nil, &got); err != nil {
			t.Fatal(err)
		}
		want := namespaceObservation{route.picked, route.path}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("request = %+v; want %+v", got, want)
		}
		if err := dispatch.Emit(context.Background(), client.Wire(), route.path, nil); err != nil {
			t.Fatal(err)
		}
		if got := receive(t, events); !reflect.DeepEqual(got, want) {
			t.Fatalf("event = %+v; want %+v", got, want)
		}
	}
	detach()
	detach()
	var got namespaceObservation
	if err := dispatch.Call(context.Background(), client.Wire(), []string{"a"}, nil, &got); err != nil || got.Picked != "a" {
		t.Fatalf("exact detach did not expose namespace: %+v %v", got, err)
	}
}

// A frame's method name reaches the root's receiver only as the canonical
// encoding of a path. Nightseam v0.6.0 also registered a raw handler here and
// answered its non-path name; bitruntime removed the raw method-name API, so a
// name that encodes no path is answered method_not_found even beside a root
// namespace that would take every path.
func TestRootNamespaceTakesOnlyCanonicalPathEncodings(t *testing.T) {
	events := make(chan namespaceObservation, 1)
	_, raw := newRawClient(t, engine.Options{Prepare: func(p *engine.Peer) error {
		root, err := dispatch.NewDispatcher(p.Wire())
		if err != nil {
			return err
		}
		_, err = root.RegisterPrefix(nil, namespaceReceiver("root", events))
		return err
	}})
	response := raw.call(t, "1:a3:b.c")
	var got namespaceObservation
	if response.Error != nil || json.Unmarshal(response.Result, &got) != nil || !reflect.DeepEqual(got, namespaceObservation{"root", []string{"a", "b.c"}}) {
		t.Fatalf("canonical name reached %+v: %+v", got, response)
	}
	for _, name := range []string{"ordinary", "unknown.raw", "01:a", "1:a.invalid"} {
		if response := raw.call(t, name); response.Error == nil || response.Error.Code != "method_not_found" {
			t.Fatalf("namespace captured noncanonical name %q: %+v", name, response)
		}
	}
}

func TestDispatcherCancellationKeepsOriginalRegistration(t *testing.T) {
	client, server := newPair(t, engine.Options{}, engine.Options{})
	serverBinding := testBinding(t, server.Wire())
	started := make(chan *wire.ReturnAddress, 1)
	cancelled := make(chan *wire.ReturnAddress, 1)
	detach, err := serverBinding.RegisterPrefix([]string{"worker"}, wire.Receiver{Message: func(_ []string, m wire.Message) {
		if m.Frame.Kind == wire.ProfileRequest {
			started <- m.Return
		}
		if m.Frame.Kind == wire.ProfileCancel {
			cancelled <- m.Return
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- dispatch.Call(ctx, client.Wire(), []string{"worker", "dynamic"}, nil, nil) }()
	original := receive(t, started)
	detach()
	replacement := make(chan wire.ProfileKind, 4)
	if _, err := serverBinding.RegisterPrefix([]string{"worker"}, wire.Receiver{Message: func(_ []string, m wire.Message) { replacement <- m.Frame.Kind }}); err != nil {
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
	client, server := newPair(t, engine.Options{}, engine.Options{})
	serverBinding := testBinding(t, server.Wire())
	frames := make(chan wire.ProfileFrame, 8)
	_, err := serverBinding.Register([]string{"trace"}, wire.Receiver{Message: func(_ []string, m wire.Message) {
		frames <- m.Frame
		if m.Frame.Kind == wire.ProfileRequest {
			_ = m.Return.Wire.Send(nil, wire.Message{Frame: wire.ProfileFrame{Version: 1, Kind: wire.ProfileResponse, ID: m.Frame.ID, Result: json.RawMessage(`null`)}})
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	sink := &wireReplySink{replies: make(chan wire.ProfileFrame, 2)}
	address := &wire.ReturnAddress{Wire: sink}
	for _, trace := range []core.Trace{{Parent: "00-11111111111111111111111111111111-2222222222222222-01", State: "vendor=value"}, {}} {
		for _, kind := range []wire.ProfileKind{wire.ProfileRequest, wire.ProfileEvent} {
			frame := wire.ProfileFrame{Version: 1, Kind: kind, Traceparent: trace.Parent, Tracestate: trace.State}
			if kind == wire.ProfileRequest {
				frame.ID = "c:1"
				frame.Params = json.RawMessage(`null`)
			} else {
				frame.Data = json.RawMessage(`null`)
			}
			if err := client.Wire().Send([]string{"trace"}, wire.Message{Frame: frame, Return: address}); err != nil {
				t.Fatal(err)
			}
			got := receive(t, frames)
			if got.Traceparent != trace.Parent || got.Tracestate != trace.State {
				t.Fatalf("structured %s trace changed: %+v; want %+v", kind, got, trace)
			}
			if kind == wire.ProfileRequest {
				reply := receive(t, sink.replies)
				if reply.Traceparent != trace.Parent || reply.Tracestate != trace.State {
					t.Fatalf("response trace changed: %+v", reply)
				}
			}
		}
	}
}

func TestRegisterGroupsRequestAndEventAtOnePath(t *testing.T) {
	client, server := newPair(t, engine.Options{}, engine.Options{})
	serverBinding := testBinding(t, server.Wire())
	events := make(chan string, 2)
	detach, err := dispatch.Register(serverBinding, []string{"shared"}, dispatch.Handlers{
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
	if err := dispatch.Call(context.Background(), client.Wire(), []string{"shared"}, "response", &got); err != nil || got != "response" {
		t.Fatalf("grouped request = %q %v", got, err)
	}
	if err := dispatch.Emit(context.Background(), client.Wire(), []string{"shared"}, "event"); err != nil {
		t.Fatal(err)
	}
	if got := receive(t, events); got != "event" {
		t.Fatal(got)
	}
	detach()
	detach()
	_, err = dispatch.Register(serverBinding, []string{"shared"}, dispatch.Handlers{Event: func(context.Context, json.RawMessage) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	err = dispatch.Call(context.Background(), client.Wire(), []string{"shared"}, nil, nil)
	var public *core.PublicError
	if !errors.As(err, &public) || public.Code != "method_not_found" {
		t.Fatalf("event-only request did not refuse: %v", err)
	}
	if _, err := dispatch.Register(serverBinding, []string{"empty"}, dispatch.Handlers{}); err == nil {
		t.Fatal("registered empty handlers")
	}
}

// This fixture created and owns the carrier, so its registry explicitly owns
// fatal-handler closure. Ordinary borrowed dispatchers only detach themselves.
type ownedEventRegistry struct {
	*dispatch.Dispatcher
	endpoint wire.Endpoint
}

func (r ownedEventRegistry) Close(code wire.Code, reason string) error {
	_ = r.Dispatcher.Close(code, reason)
	return r.endpoint.Close(code, reason)
}

// Nightseam v0.6.0 read the carrier's ending from the server's observer; here
// the server's connection records the close it was asked for, and the client
// reads that close on its side of the pipe.
func TestRegisterEventFailuresEndOnlyTheirCarrierWithSanitizedReason(t *testing.T) {
	for _, panics := range []bool{false, true} {
		t.Run(map[bool]string{false: "error", true: "panic"}[panics], func(t *testing.T) {
			client, server, _, served := recordedPair(t, engine.Options{}, engine.Options{})
			serverBinding := ownedEventRegistry{testBinding(t, server.Wire()), server.Wire()}
			_, err := dispatch.Register(serverBinding, []string{"rejected"}, dispatch.Handlers{Event: func(context.Context, json.RawMessage) error {
				if panics {
					panic("private failure")
				}
				return errors.New("private failure")
			}})
			if err != nil {
				t.Fatal(err)
			}
			if err := dispatch.Emit(context.Background(), client.Wire(), []string{"rejected"}, nil); err != nil {
				t.Fatal(err)
			}
			receive(t, server.Done())
			if got := served.await(t, 1, "close"); !reflect.DeepEqual(got, []string{"close 1002 wire event rejected"}) {
				t.Fatalf("event ending = %v", got)
			}
			receive(t, client.Done())
			var closed *transports.CloseError
			if !errors.As(client.Err(), &closed) || closed.Code != 1002 || closed.Reason != "wire event rejected" {
				t.Fatalf("event ending = %v", client.Err())
			}
		})
	}
}

func TestForwardCarriesUnknownPathsAndReverseCallsAcrossPeers(t *testing.T) {
	client, middleIn := newPair(t, engine.Options{}, engine.Options{})
	middleOut, server := newPair(t, engine.Options{}, engine.Options{})
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
