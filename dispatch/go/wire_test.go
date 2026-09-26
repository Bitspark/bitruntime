package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Bitspark/nightseam/duplex/go"
	ws "github.com/Bitspark/nightseam/runtime/go"
)

func testBinding(t *testing.T, endpoint duplex.Endpoint) *ws.Dispatcher {
	t.Helper()
	binding, err := ws.NewDispatcher(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = binding.Close(duplex.CodeNormal, "done") })
	return binding
}

type wireReplySink struct{ replies chan duplex.ProfileFrame }

func TestReceiverDeadlineWinsImmediateHandlerRefusal(t *testing.T) {
	previous := runtime.GOMAXPROCS(4)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	ended := make(chan ws.RequestEnded, 1)
	client, server := newPair(t, ws.Options{RequestTimeout: 100 * time.Microsecond, Observer: observerFunc(func(event ws.ObserverEvent) {
		if e, ok := event.(ws.RequestEnded); ok && e.Incoming {
			ended <- e
		}
	})}, ws.Options{})
	if err := server.Handle("deadline", func(ctx context.Context, _ *ws.Peer, _ json.RawMessage) (any, error) {
		<-ctx.Done()
		return nil, &ws.PublicError{Code: "declined", Message: "Body completed at deadline"}
	}); err != nil {
		t.Fatal(err)
	}
	// Exercise the real timer/body race: completion can run before the
	// asynchronous deadline callback, but the deadline is already selected.
	for i := range 1024 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := client.Call(ctx, "deadline", nil, nil)
		cancel()
		var public *ws.PublicError
		if !errors.As(err, &public) || public.Code != "cancelled" {
			t.Fatalf("call %d deadline response = %v", i, err)
		}
		if e := receive(t, ended); e.Outcome != ws.OutcomeTimedOut || e.ErrorCode != "request_timeout" {
			t.Fatalf("call %d deadline outcome = %+v", i, e)
		}
	}
}

func TestWireCancellationRetainsExecutingHandlerBudget(t *testing.T) {
	for _, mode := range []string{"cancel", "caller-deadline", "receiver-deadline", "public-refusal"} {
		for _, route := range []string{"wire", "forwarded", "peer"} {
			t.Run(mode+"/"+route, func(t *testing.T) {
				ended := make(chan ws.RequestEnded, 16)
				options := ws.Options{MaxConcurrentHandlers: 1, Observer: observerFunc(func(event ws.ObserverEvent) {
					if e, ok := event.(ws.RequestEnded); ok && e.Incoming {
						ended <- e
					}
				})}
				if mode == "receiver-deadline" {
					options.RequestTimeout = 100 * time.Millisecond
				}
				client, server := newPair(t, options, ws.Options{})
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
							return nil, &ws.PublicError{Code: "cancelled", Message: "Application refusal"}
						}
					}
					return "finished", nil
				}
				var err error
				call := func(ctx context.Context) error { return ws.CallWire(ctx, client.Wire(), []string{"hold"}, nil, nil) }
				if route == "peer" {
					err = server.Handle("hold", func(ctx context.Context, _ *ws.Peer, params json.RawMessage) (any, error) {
						return handler(ctx, params)
					})
					call = func(ctx context.Context) error { return client.Call(ctx, "hold", nil, nil) }
				} else {
					model := server.Wire()
					if route == "forwarded" {
						left, right, pairErr := ws.NewWirePair(ws.Options{})
						if pairErr != nil {
							t.Fatal(pairErr)
						}
						t.Cleanup(func() { _ = left.Close(duplex.CodeNormal, "") })
						stop, forwardErr := ws.ForwardWire(server.Wire(), left)
						if forwardErr != nil {
							t.Fatal(forwardErr)
						}
						t.Cleanup(stop)
						model = right
					}
					modelBinding := testBinding(t, model)
					_, err = ws.HandleWire(modelBinding, []string{"hold"}, handler)
				}
				if err != nil {
					t.Fatal(err)
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
					var public *ws.PublicError
					if err := receive(t, result); !errors.As(err, &public) || public.Code != "cancelled" {
						t.Fatalf("receiver deadline = %v", err)
					}
				} else {
					if err := receive(t, result); !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("caller cancellation = %v", err)
					}
				}
				receive(t, cancelled)
				if mode == "receiver-deadline" {
					if e := receive(t, ended); e.Outcome != ws.OutcomeTimedOut || e.ErrorCode != "request_timeout" {
						t.Fatalf("receiver deadline outcome = %+v", e)
					}
				}
				// Repeated round trips keep exercising admission while the first body
				// is explicitly held, independent of when its wrapper is scheduled.
				for range 10 {
					err := call(context.Background())
					var public *ws.PublicError
					if !errors.As(err, &public) || public.Code != "busy" {
						t.Fatalf("request admitted while cancelled body still runs: calls=%d, err=%v", calls.Load(), err)
					}
					if e := receive(t, ended); e.ErrorCode != "busy" {
						t.Fatalf("held request ended before its body returned: %+v", e)
					}
				}
				if calls.Load() != 1 {
					t.Fatalf("executed %d bodies at a limit of one", calls.Load())
				}
				released.Do(func() { close(release) })
				if mode != "receiver-deadline" {
					outcome := ws.OutcomeCancelled
					if mode == "public-refusal" {
						outcome = ws.OutcomeErrored
					}
					if e := receive(t, ended); e.Outcome != outcome || e.ErrorCode != "cancelled" {
						t.Fatalf("withdrawal outcome = %+v", e)
					}
				}
				ready, stop := context.WithTimeout(context.Background(), 5*time.Second)
				defer stop()
				for {
					err := call(ready)
					if err == nil {
						break
					}
					var public *ws.PublicError
					if !errors.As(err, &public) || public.Code != "busy" {
						t.Fatal(err)
					}
					// Each retry produces an observation before its refusal. Drain
					// it so the test observer cannot block the next admission.
					if e := receive(t, ended); e.ErrorCode != "busy" {
						t.Fatalf("retry outcome = %+v", e)
					}
				}
			})
		}
	}
}

type wireVerifiedKey struct{}
type wireContextPropagator struct{ ws.Propagator }

func (p wireContextPropagator) Extract(ctx context.Context, trace ws.Trace) context.Context {
	return context.WithValue(p.Propagator.Extract(ctx, trace), wireVerifiedKey{}, true)
}

func TestWireKeepsReceivedContextWithoutForwardingApplicationMetadata(t *testing.T) {
	client, server := newPair(t, ws.Options{Propagator: wireContextPropagator{ws.DefaultPropagator}}, ws.Options{})
	clientBinding := testBinding(t, client.Wire())
	_, err := ws.HandleWire(clientBinding, []string{"reverse"}, func(ctx context.Context, _ json.RawMessage) (any, error) { return ws.MetaFrom(ctx), nil })
	if err != nil {
		t.Fatal(err)
	}
	serverBinding := testBinding(t, server.Wire())
	_, err = ws.HandleWire(serverBinding, []string{"check"}, func(ctx context.Context, _ json.RawMessage) (any, error) {
		var reverse ws.Meta
		if err := ws.CallWire(ctx, server.Wire(), []string{"reverse"}, nil, &reverse); err != nil {
			return nil, err
		}
		return map[string]any{"verified": ctx.Value(wireVerifiedKey{}) == true, "received": ws.MetaFrom(ctx), "reverse": reverse}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Verified bool
		Received ws.Meta
		Reverse  ws.Meta
	}
	if err := ws.CallWire(ws.WithMeta(context.Background(), ws.Meta{"credential": "one-call"}), client.Wire(), []string{"check"}, nil, &got); err != nil {
		t.Fatal(err)
	}
	if !got.Verified || got.Received["credential"] != "one-call" || len(got.Reverse) != 0 {
		t.Fatalf("wire request context = %+v", got)
	}
}

func TestWireHandlerPanicStaysPrivateAndObserved(t *testing.T) {
	observed := &recorder{}
	client, server := newPair(t, ws.Options{Observer: observed}, ws.Options{})
	serverBinding := testBinding(t, server.Wire())
	_, err := ws.HandleWire(serverBinding, []string{"panic"}, func(context.Context, json.RawMessage) (any, error) { panic("private failure") })
	if err != nil {
		t.Fatal(err)
	}
	err = ws.CallWire(context.Background(), client.Wire(), []string{"panic"}, nil, nil)
	var public *ws.PublicError
	if !errors.As(err, &public) || public.Code != "internal" || public.Message != "Internal error" {
		t.Fatalf("panic response = %v", err)
	}
	count := 0
	for _, event := range observed.all() {
		if failure, ok := event.(ws.HandlerPanic); ok {
			count++
			if failure.Value != "private failure" {
				t.Fatalf("panic observation = %+v", failure)
			}
		}
	}
	if count != 1 {
		t.Fatalf("panic observations = %d", count)
	}
	if server.Err() != nil {
		t.Fatalf("panic ended carrier: %v", server.Err())
	}
}

// A return capability refuses what it does not implement, as every addressed
// receiver in this profile does; this one carries outcomes and nothing else.
func (s *wireReplySink) Send(path []string, message duplex.Message) error {
	if len(path) != 0 {
		return errors.New("this return capability carries outcomes only")
	}
	s.replies <- message.Frame
	return nil
}

func TestWirePreservesRequestAndEventAdmissionOrder(t *testing.T) {
	observed := &recorder{}
	client, server := newPair(t, ws.Options{}, ws.Options{Observer: observed})
	sink := &wireReplySink{replies: make(chan duplex.ProfileFrame, 40)}
	address := &duplex.ReturnAddress{Wire: sink}
	wire := client.Wire()
	var want []string
	serverBinding := testBinding(t, server.Wire())
	for i := range 40 {
		path := []string{"ordered", fmt.Sprint(i)}
		_, err := ws.HandleWire(serverBinding, path, func(context.Context, json.RawMessage) (any, error) { return nil, nil })
		if err != nil {
			t.Fatal(err)
		}
		name, _ := duplex.EncodePath(path)
		for _, kind := range []duplex.ProfileKind{duplex.ProfileRequest, duplex.ProfileEvent} {
			frame := duplex.ProfileFrame{Version: 1, Kind: kind}
			if kind == duplex.ProfileRequest {
				frame.ID = fmt.Sprintf("c:%d", i+1)
				frame.Params = json.RawMessage("{}")
			} else {
				frame.Data = json.RawMessage("null")
			}
			if err := wire.Send(path, duplex.Message{Frame: frame, Return: address}); err != nil {
				t.Fatal(err)
			}
			want = append(want, string(kind)+" "+name)
		}
	}
	for range 40 {
		receive(t, sink.replies)
	}
	var got []string
	for _, event := range observed.all() {
		if sent, ok := event.(ws.FrameSent); ok && (sent.Kind == "request" || sent.Kind == "event") {
			got = append(got, sent.Kind+" "+sent.Name)
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("per-wire send order changed:\n got %v\nwant %v", got, want)
	}
}

func TestWirePreservesCancellationBeforeTheFollowingEvent(t *testing.T) {
	observed := &recorder{}
	client, server := newPair(t, ws.Options{}, ws.Options{Observer: observed})
	started := make(chan struct{})
	eventReceived := make(chan struct{})
	serverBinding := testBinding(t, server.Wire())
	if _, err := ws.RegisterWire(serverBinding, []string{"after"}, ws.WireHandlers{Event: func(context.Context, json.RawMessage) error { close(eventReceived); return nil }}); err != nil {
		t.Fatal(err)
	}
	_, err := ws.HandleWire(serverBinding, []string{"wait"}, func(ctx context.Context, _ json.RawMessage) (any, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	sink := &wireReplySink{replies: make(chan duplex.ProfileFrame, 1)}
	address := &duplex.ReturnAddress{Wire: sink}
	wire := client.Wire()
	if err := wire.Send([]string{"wait"}, duplex.Message{Frame: duplex.ProfileFrame{Version: 1, Kind: duplex.ProfileRequest, ID: "c:1", Params: json.RawMessage("{}")}, Return: address}); err != nil {
		t.Fatal(err)
	}
	receive(t, started)
	if err := wire.Send([]string{"wait"}, duplex.Message{Frame: duplex.ProfileFrame{Version: 1, Kind: duplex.ProfileCancel, ID: "c:1"}, Return: address}); err != nil {
		t.Fatal(err)
	}
	if err := ws.EmitWire(context.Background(), wire, []string{"after"}, nil); err != nil {
		t.Fatal(err)
	}
	receive(t, sink.replies)
	receive(t, eventReceived)
	var kinds []string
	for _, event := range observed.all() {
		if sent, ok := event.(ws.FrameSent); ok {
			kinds = append(kinds, sent.Kind)
		}
	}
	if !reflect.DeepEqual(kinds, []string{"request", "cancel", "event"}) {
		t.Fatalf("send order = %v", kinds)
	}
}

func TestMountedWireKeepsIndependentOriginsAndCancellation(t *testing.T) {
	client, server := newPair(t, ws.Options{}, ws.Options{})
	started := make(chan string, 2)
	finished := make(chan string, 2)
	allow := make(chan struct{})
	defer close(allow)
	serverBinding := testBinding(t, server.Wire())
	_, err := ws.HandleWire(serverBinding, []string{"worker", "run"}, func(ctx context.Context, raw json.RawMessage) (any, error) {
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
	// Both views select the same existing carrier. Each CallWire has its own
	// local return address, so cancelling one cannot cancel the other's id.
	wire := duplex.At(duplex.Mount(map[string]duplex.Endpoint{"service": client.Wire()}), []string{"service", "worker"})
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
			results <- ws.CallWire(call.ctx, wire, []string{"run"}, call.name, nil)
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
	_, err = ws.HandleWire(serverBinding, []string{"worker", "echo"}, func(_ context.Context, value json.RawMessage) (any, error) { return value, nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := ws.CallWire(context.Background(), wire, []string{"echo"}, "still open", &echoed); err != nil || echoed != "still open" {
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
	client, server := newPair(t, ws.Options{}, ws.Options{})
	serverBinding := testBinding(t, server.Wire())
	for _, route := range []struct {
		path []string
		want string
	}{{[]string{"a.b"}, "one segment"}, {[]string{"a", "b"}, "two segments"}, {[]string{""}, "empty segment"}} {
		_, err := ws.HandleWire(serverBinding, route.path, func(context.Context, json.RawMessage) (any, error) { return route.want, nil })
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, route := range []struct {
		path []string
		want string
	}{{[]string{"a.b"}, "one segment"}, {[]string{"a", "b"}, "two segments"}, {[]string{""}, "empty segment"}} {
		var got string
		if err := ws.CallWire(context.Background(), client.Wire(), route.path, nil, &got); err != nil || got != route.want {
			t.Fatalf("path %q = %q, %v; want %q", route.path, got, err, route.want)
		}
		if err := ws.CallWire(context.Background(), duplex.At(client.Wire(), route.path), nil, nil, &got); err != nil || got != route.want {
			t.Fatalf("selected leaf %q = %q, %v; want %q", route.path, got, err, route.want)
		}
	}
}
