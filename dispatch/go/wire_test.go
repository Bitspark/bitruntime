package dispatch_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/Bitspark/bitruntime/core/go"
	dispatch "github.com/Bitspark/bitruntime/dispatch/go"
	engine "github.com/Bitspark/bitruntime/engine/go"
	"github.com/Bitspark/bitruntime/internal/profile/go"
	transports "github.com/Bitspark/bitruntime/transports/go"
	wire "github.com/Bitspark/bitwire/wire/go"
)

func TestReceiverDeadlineWinsImmediateHandlerRefusal(t *testing.T) {
	previous := runtime.GOMAXPROCS(4)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	client, server := newPair(t, engine.Options{RequestTimeout: 100 * time.Microsecond}, engine.Options{})
	serverBinding := testBinding(t, server.Wire())
	if _, err := dispatch.Handle(serverBinding, []string{"deadline"}, func(ctx context.Context, _ json.RawMessage) (any, error) {
		<-ctx.Done()
		return nil, &core.PublicError{Code: "declined", Message: "Body completed at deadline"}
	}); err != nil {
		t.Fatal(err)
	}
	// Exercise the real timer/body race: completion can run before the
	// asynchronous deadline callback, but the deadline is already selected.
	for i := range 1024 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := dispatch.Call(ctx, client.Wire(), []string{"deadline"}, nil, nil)
		cancel()
		var public *core.PublicError
		if !errors.As(err, &public) || public.Code != "cancelled" {
			t.Fatalf("call %d deadline response = %v", i, err)
		}
	}
}

func TestWireCancellationRetainsExecutingHandlerBudget(t *testing.T) {
	for _, mode := range []string{"cancel", "caller-deadline", "receiver-deadline", "public-refusal"} {
		for _, route := range []string{"wire", "forwarded", "peer"} {
			t.Run(mode+"/"+route, func(t *testing.T) {
				options := engine.Options{MaxConcurrentHandlers: 1}
				if mode == "receiver-deadline" {
					options.RequestTimeout = 100 * time.Millisecond
				}
				entered, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
				var released sync.Once
				t.Cleanup(func() { released.Do(func() { close(release) }) })
				var calls atomic.Int32
				handler := func(ctx context.Context, _ json.RawMessage) (any, error) {
					if calls.Add(1) == 1 {
						close(entered)
						<-ctx.Done()
						close(cancelled)
						<-release
						if mode == "public-refusal" {
							return nil, &core.PublicError{Code: "cancelled", Message: "Application refusal"}
						}
					}
					return "finished", nil
				}
				if route == "peer" {
					// The peer's raw Handle is removed; its root carries the
					// request, with the handler installed before the peer reads.
					options.Prepare = func(p *engine.Peer) error {
						binding, err := dispatch.NewDispatcher(p.Wire())
						if err != nil {
							return err
						}
						_, err = dispatch.Handle(binding, []string{"hold"}, handler)
						return err
					}
				}
				client, server := newPair(t, options, engine.Options{})
				call := func(ctx context.Context) error { return dispatch.Call(ctx, client.Wire(), []string{"hold"}, nil, nil) }
				if route != "peer" {
					model := server.Wire()
					if route == "forwarded" {
						left, right, pairErr := core.NewPair(core.PairOptions{})
						if pairErr != nil {
							t.Fatal(pairErr)
						}
						t.Cleanup(func() { _ = left.Close(transports.CodeNormal, "") })
						stop, forwardErr := core.Forward(server.Wire(), left)
						if forwardErr != nil {
							t.Fatal(forwardErr)
						}
						t.Cleanup(stop)
						model = right
					}
					modelBinding := testBinding(t, model)
					if _, err := dispatch.Handle(modelBinding, []string{"hold"}, handler); err != nil {
						t.Fatal(err)
					}
				}
				ctx, cancel := context.WithCancel(context.Background())
				if mode == "caller-deadline" {
					cancel()
					ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
				}
				defer cancel()
				result := make(chan error, 1)
				go func() { result <- call(ctx) }()
				receive(t, entered)
				if mode == "cancel" || mode == "public-refusal" {
					cancel()
				}
				if mode == "receiver-deadline" {
					var public *core.PublicError
					if err := receive(t, result); !errors.As(err, &public) || public.Code != "cancelled" {
						t.Fatalf("receiver deadline = %v", err)
					}
				} else {
					if err := receive(t, result); !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("caller cancellation = %v", err)
					}
				}
				receive(t, cancelled)
				// Repeated round trips keep exercising admission while the first body
				// is explicitly held, independent of when its wrapper is scheduled.
				for range 10 {
					err := call(context.Background())
					var public *core.PublicError
					if !errors.As(err, &public) || public.Code != "busy" {
						t.Fatalf("request admitted while cancelled body still runs: calls=%d, err=%v", calls.Load(), err)
					}
				}
				if calls.Load() != 1 {
					t.Fatalf("executed %d bodies at a limit of one", calls.Load())
				}
				released.Do(func() { close(release) })
				ready, stop := context.WithTimeout(context.Background(), 5*time.Second)
				defer stop()
				for {
					err := call(ready)
					if err == nil {
						break
					}
					var public *core.PublicError
					if !errors.As(err, &public) || public.Code != "busy" {
						t.Fatal(err)
					}
				}
			})
		}
	}
}

type wireVerifiedKey struct{}
type wireContextPropagator struct{ core.Propagator }

func (p wireContextPropagator) Extract(ctx context.Context, trace core.Trace) context.Context {
	return context.WithValue(p.Propagator.Extract(ctx, trace), wireVerifiedKey{}, true)
}

func TestWireKeepsReceivedContextWithoutForwardingApplicationMetadata(t *testing.T) {
	client, server := newPair(t, engine.Options{Propagator: wireContextPropagator{core.DefaultPropagator}}, engine.Options{})
	clientBinding := testBinding(t, client.Wire())
	_, err := dispatch.Handle(clientBinding, []string{"reverse"}, func(ctx context.Context, _ json.RawMessage) (any, error) { return core.MetaFrom(ctx), nil })
	if err != nil {
		t.Fatal(err)
	}
	serverBinding := testBinding(t, server.Wire())
	_, err = dispatch.Handle(serverBinding, []string{"check"}, func(ctx context.Context, _ json.RawMessage) (any, error) {
		var reverse core.Meta
		if err := dispatch.Call(ctx, server.Wire(), []string{"reverse"}, nil, &reverse); err != nil {
			return nil, err
		}
		return map[string]any{"verified": ctx.Value(wireVerifiedKey{}) == true, "received": core.MetaFrom(ctx), "reverse": reverse}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Verified bool
		Received core.Meta
		Reverse  core.Meta
	}
	if err := dispatch.Call(core.WithMeta(context.Background(), core.Meta{"credential": "one-call"}), client.Wire(), []string{"check"}, nil, &got); err != nil {
		t.Fatal(err)
	}
	if !got.Verified || got.Received["credential"] != "one-call" || len(got.Reverse) != 0 {
		t.Fatalf("wire request context = %+v", got)
	}
}

// nightseam v0.6.0 also counted the peer's HandlerPanic observations here;
// bitruntime has no observer.
func TestWireHandlerPanicStaysPrivate(t *testing.T) {
	client, server := newPair(t, engine.Options{}, engine.Options{})
	serverBinding := testBinding(t, server.Wire())
	_, err := dispatch.Handle(serverBinding, []string{"panic"}, func(context.Context, json.RawMessage) (any, error) { panic("private failure") })
	if err != nil {
		t.Fatal(err)
	}
	err = dispatch.Call(context.Background(), client.Wire(), []string{"panic"}, nil, nil)
	var public *core.PublicError
	if !errors.As(err, &public) || public.Code != "internal" || public.Message != "Internal error" {
		t.Fatalf("panic response = %v", err)
	}
	if server.Err() != nil {
		t.Fatalf("panic ended carrier: %v", server.Err())
	}
}

func TestWirePreservesRequestAndEventAdmissionOrder(t *testing.T) {
	client, server, sent, _ := recordedPair(t, engine.Options{}, engine.Options{})
	sink := &wireReplySink{replies: make(chan wire.ProfileFrame, 40)}
	address := &wire.ReturnAddress{Wire: sink}
	root := client.Wire()
	var want []string
	serverBinding := testBinding(t, server.Wire())
	for i := range 40 {
		path := []string{"ordered", fmt.Sprint(i)}
		_, err := dispatch.Handle(serverBinding, path, func(context.Context, json.RawMessage) (any, error) { return nil, nil })
		if err != nil {
			t.Fatal(err)
		}
		name, _ := profile.EncodePath(path)
		for _, kind := range []wire.ProfileKind{wire.ProfileRequest, wire.ProfileEvent} {
			frame := wire.ProfileFrame{Version: 1, Kind: kind}
			if kind == wire.ProfileRequest {
				frame.ID = fmt.Sprintf("c:%d", i+1)
				frame.Params = json.RawMessage("{}")
			} else {
				frame.Data = json.RawMessage("null")
			}
			if err := root.Send(path, wire.Message{Frame: frame, Return: address}); err != nil {
				t.Fatal(err)
			}
			want = append(want, string(kind)+" "+name)
		}
	}
	for range 40 {
		receive(t, sink.replies)
	}
	if got := sent.await(t, len(want), "request", "event"); !reflect.DeepEqual(got, want) {
		t.Fatalf("per-wire send order changed:\n got %v\nwant %v", got, want)
	}
}

func TestWirePreservesCancellationBeforeTheFollowingEvent(t *testing.T) {
	client, server, sent, _ := recordedPair(t, engine.Options{}, engine.Options{})
	started := make(chan struct{})
	eventReceived := make(chan struct{})
	serverBinding := testBinding(t, server.Wire())
	if _, err := dispatch.Register(serverBinding, []string{"after"}, dispatch.Handlers{Event: func(context.Context, json.RawMessage) error { close(eventReceived); return nil }}); err != nil {
		t.Fatal(err)
	}
	_, err := dispatch.Handle(serverBinding, []string{"wait"}, func(ctx context.Context, _ json.RawMessage) (any, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	sink := &wireReplySink{replies: make(chan wire.ProfileFrame, 1)}
	address := &wire.ReturnAddress{Wire: sink}
	root := client.Wire()
	if err := root.Send([]string{"wait"}, wire.Message{Frame: wire.ProfileFrame{Version: 1, Kind: wire.ProfileRequest, ID: "c:1", Params: json.RawMessage("{}")}, Return: address}); err != nil {
		t.Fatal(err)
	}
	receive(t, started)
	if err := root.Send([]string{"wait"}, wire.Message{Frame: wire.ProfileFrame{Version: 1, Kind: wire.ProfileCancel, ID: "c:1"}, Return: address}); err != nil {
		t.Fatal(err)
	}
	if err := dispatch.Emit(context.Background(), root, []string{"after"}, nil); err != nil {
		t.Fatal(err)
	}
	receive(t, sink.replies)
	receive(t, eventReceived)
	var kinds []string
	for _, line := range sent.await(t, 3, "request", "cancel", "event", "response") {
		kind, _, _ := strings.Cut(line, " ")
		kinds = append(kinds, kind)
	}
	if !reflect.DeepEqual(kinds, []string{"request", "cancel", "event"}) {
		t.Fatalf("send order = %v", kinds)
	}
}

func TestMountedWireKeepsIndependentOriginsAndCancellation(t *testing.T) {
	client, server := newPair(t, engine.Options{}, engine.Options{})
	started := make(chan string, 2)
	finished := make(chan string, 2)
	allow := make(chan struct{})
	defer close(allow)
	serverBinding := testBinding(t, server.Wire())
	_, err := dispatch.Handle(serverBinding, []string{"worker", "run"}, func(ctx context.Context, raw json.RawMessage) (any, error) {
		var name string
		if err := json.Unmarshal(raw, &name); err != nil {
			return nil, err
		}
		started <- name
		select {
		case <-ctx.Done():
			finished <- name
			return nil, ctx.Err()
		case <-allow:
			return name, nil
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	// Both views select the same existing carrier. Each Call has its own
	// local return address, so cancelling one cannot cancel the other's id.
	access := core.At(core.Mount(map[string]wire.Endpoint{"service": client.Wire()}), []string{"service", "worker"})
	first, cancelFirst := context.WithCancel(context.Background())
	second, cancelSecond := context.WithCancel(context.Background())
	defer cancelFirst()
	defer cancelSecond()
	var calls sync.WaitGroup
	results := make(chan error, 2)
	for _, call := range []struct {
		ctx  context.Context
		name string
	}{{first, "first"}, {second, "second"}} {
		calls.Add(1)
		go func() {
			defer calls.Done()
			results <- dispatch.Call(call.ctx, access, []string{"run"}, call.name, nil)
		}()
	}
	receive(t, started)
	receive(t, started)
	cancelFirst()
	if err := receive(t, results); !errors.Is(err, context.Canceled) {
		t.Fatalf("first cancellation = %v", err)
	}
	if name := receive(t, finished); name != "first" {
		t.Fatalf("cancelled %q, want first", name)
	}
	var echoed string
	_, err = dispatch.Handle(serverBinding, []string{"worker", "echo"}, func(_ context.Context, value json.RawMessage) (any, error) { return value, nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := dispatch.Call(context.Background(), access, []string{"echo"}, "still open", &echoed); err != nil || echoed != "still open" {
		t.Fatalf("sibling call after cancellation = %q, %v", echoed, err)
	}
	cancelSecond()
	if err := receive(t, results); !errors.Is(err, context.Canceled) {
		t.Fatalf("second cancellation = %v", err)
	}
	if name := receive(t, finished); name != "second" {
		t.Fatalf("second cancellation reached %q", name)
	}
	calls.Wait()
}

func TestWirePathPreservesOpaqueSegmentsOverTheExistingEnvelope(t *testing.T) {
	client, server := newPair(t, engine.Options{}, engine.Options{})
	serverBinding := testBinding(t, server.Wire())
	for _, route := range []struct {
		path []string
		want string
	}{{[]string{"a.b"}, "one segment"}, {[]string{"a", "b"}, "two segments"}, {[]string{""}, "empty segment"}} {
		_, err := dispatch.Handle(serverBinding, route.path, func(context.Context, json.RawMessage) (any, error) { return route.want, nil })
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, route := range []struct {
		path []string
		want string
	}{{[]string{"a.b"}, "one segment"}, {[]string{"a", "b"}, "two segments"}, {[]string{""}, "empty segment"}} {
		var got string
		if err := dispatch.Call(context.Background(), client.Wire(), route.path, nil, &got); err != nil || got != route.want {
			t.Fatalf("path %q = %q, %v; want %q", route.path, got, err, route.want)
		}
		if err := dispatch.Call(context.Background(), core.At(client.Wire(), route.path), nil, nil, &got); err != nil || got != route.want {
			t.Fatalf("selected leaf %q = %q, %v; want %q", route.path, got, err, route.want)
		}
	}
}
