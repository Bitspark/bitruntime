package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Bitspark/nightseam/duplex/go"
)

type localTestReturn struct{ send func(duplex.Message) error }

type localTestObserver func(ObserverEvent)

func (observe localTestObserver) Observe(event ObserverEvent) { observe(event) }

func (r *localTestReturn) Send(_ []string, m duplex.Message) error { return r.send(m) }

func testBinding(t *testing.T, endpoint duplex.Endpoint) *Dispatcher {
	t.Helper()
	binding, err := NewDispatcher(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = binding.Close(duplex.CodeNormal, "done") })
	return binding
}

func localPair(t *testing.T, options Options) (duplex.Endpoint, duplex.Endpoint) {
	t.Helper()
	a, b, err := NewWirePair(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close(duplex.CodeNormal, "done") })
	return a, b
}

func TestLocalWirePairRoundTripReverseAndIsolation(t *testing.T) {
	a, b := localPair(t, Options{})
	aBinding := testBinding(t, a)
	_, err := HandleWire(aBinding, []string{"reverse"}, func(_ context.Context, raw json.RawMessage) (any, error) { return string(raw), nil })
	if err != nil {
		t.Fatal(err)
	}
	bBinding := testBinding(t, b)
	_, err = HandleWire(bBinding, []string{"call"}, func(ctx context.Context, raw json.RawMessage) (any, error) {
		var result string
		err := CallWire(ctx, b, []string{"reverse"}, raw, &result)
		return result, err
	})
	if err != nil {
		t.Fatal(err)
	}
	var result string
	if err := CallWire(context.Background(), a, []string{"call"}, 7, &result); err != nil || result != "7" {
		t.Fatalf("reverse result %q: %v", result, err)
	}
	x, y := localPair(t, Options{})
	yBinding := testBinding(t, y)
	_, _ = HandleWire(yBinding, []string{"call"}, func(context.Context, json.RawMessage) (any, error) { return "independent", nil })
	_ = a.Close(duplex.CodeNormal, "first pair only")
	if err := CallWire(context.Background(), x, []string{"call"}, nil, &result); err != nil || result != "independent" {
		t.Fatalf("other pair %q: %v", result, err)
	}
}

func TestLocalWirePairRetainsPendingUntilResponse(t *testing.T) {
	a, b := localPair(t, Options{MaxPendingRequests: 1})
	started, release := make(chan struct{}), make(chan struct{})
	bBinding := testBinding(t, b)
	_, _ = HandleWire(bBinding, []string{"hold"}, func(context.Context, json.RawMessage) (any, error) { close(started); <-release; return "done", nil })
	first := make(chan error, 1)
	go func() { var result string; first <- CallWire(context.Background(), a, []string{"hold"}, nil, &result) }()
	<-started
	var result any
	err := CallWire(context.Background(), a, []string{"hold"}, nil, &result)
	var public *PublicError
	if !errors.As(err, &public) || public.Code != "busy" {
		t.Fatalf("pending budget: %v", err)
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	_, _ = HandleWire(bBinding, []string{"next"}, func(context.Context, json.RawMessage) (any, error) { return "reused", nil })
	if err := CallWire(context.Background(), a, []string{"next"}, nil, &result); err != nil {
		t.Fatal(err)
	}
}

func TestLocalWirePairOrderedEventsAndReservedCancel(t *testing.T) {
	a, b := localPair(t, Options{QueueCapacity: 1, MaxPendingRequests: 1})
	bBinding := testBinding(t, b)
	started, cancelled := make(chan struct{}), make(chan struct{})
	_, _ = HandleWire(bBinding, []string{"hold"}, func(ctx context.Context, _ json.RawMessage) (any, error) {
		close(started)
		<-ctx.Done()
		close(cancelled)
		return nil, ctx.Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	answer := make(chan error, 1)
	go func() { answer <- CallWire(ctx, a, []string{"hold"}, nil, nil) }()
	<-started
	entered, release, drained := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var mu sync.Mutex
	var seen []int
	_, _ = bBinding.Register([]string{"event"}, duplex.Receiver{Message: func(_ []string, m duplex.Message) {
		var n int
		_ = json.Unmarshal(m.Frame.Data, &n)
		mu.Lock()
		seen = append(seen, n)
		mu.Unlock()
		if n == 1 {
			close(entered)
			<-release
		} else {
			close(drained)
		}
	}})
	if err := EmitWire(context.Background(), a, []string{"event"}, 1); err != nil {
		t.Fatal(err)
	}
	<-entered
	if err := EmitWire(context.Background(), a, []string{"event"}, 2); err != nil {
		t.Fatal(err)
	}
	cancel()
	if !errors.Is(<-answer, context.Canceled) {
		t.Fatal("caller was not cancelled")
	}
	close(release)
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("reserved cancel did not arrive")
	}
	<-drained
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 || seen[0] != 1 || seen[1] != 2 {
		t.Fatalf("event order: %v", seen)
	}
}

func TestLocalWirePairOverflowClosesOnlyItsCarrier(t *testing.T) {
	a, b := localPair(t, Options{QueueCapacity: 1})
	bBinding := testBinding(t, b)
	entered, release, ended := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer close(release)
	_, _ = bBinding.Register([]string{"event"}, duplex.Receiver{Message: func([]string, duplex.Message) { close(entered); <-release }, Closed: func(duplex.Code, string) { close(ended) }})
	if err := EmitWire(context.Background(), a, []string{"event"}, nil); err != nil {
		t.Fatal(err)
	}
	<-entered
	if err := EmitWire(context.Background(), a, []string{"event"}, nil); err != nil {
		t.Fatal(err)
	}
	if err := EmitWire(context.Background(), a, []string{"event"}, nil); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("overflow: %v", err)
	}
	select {
	case <-ended:
	case <-time.After(time.Second):
		t.Fatal("blocked consumer hid closure")
	}
}

func TestLocalWirePairReturnMappingAndFailedResponseRetirement(t *testing.T) {
	a, b := localPair(t, Options{MaxPendingRequests: 1})
	bBinding := testBinding(t, b)
	received := make(chan duplex.Message, 2)
	_, _ = bBinding.Register([]string{"raw"}, duplex.Receiver{Message: func(_ []string, m duplex.Message) { received <- m }})
	failed := errors.New("return failed")
	original := &duplex.ReturnAddress{Wire: &localTestReturn{send: func(duplex.Message) error { return Unpublished(failed) }}}
	if err := a.Send([]string{"raw"}, duplex.Message{Frame: duplex.ProfileFrame{Version: 1, Kind: duplex.ProfileRequest, ID: "c:1", Params: json.RawMessage("null")}, Return: original}); err != nil {
		t.Fatal(err)
	}
	request := <-received
	if request.Return == original {
		t.Fatal("root did not map the return capability")
	}
	if err := a.Send([]string{"raw"}, duplex.Message{Frame: duplex.ProfileFrame{Version: 1, Kind: duplex.ProfileCancel, ID: "c:1"}, Return: original}); err != nil {
		t.Fatal(err)
	}
	if cancelled := <-received; cancelled.Return != request.Return {
		t.Fatal("cancellation used a different return capability")
	}
	err := request.Return.Wire.Send(nil, duplex.Message{Frame: duplex.ProfileFrame{Version: 1, Kind: duplex.ProfileResponse, ID: "c:1", Result: json.RawMessage("null")}})
	if !errors.Is(err, failed) {
		t.Fatalf("return failure lost: %v", err)
	}
	var unpublished *UnpublishedError
	if errors.As(err, &unpublished) {
		t.Fatal("return retained publication proof after dispatch")
	}
	_, _ = HandleWire(bBinding, []string{"next"}, func(context.Context, json.RawMessage) (any, error) { return "reused", nil })
	var result string
	if err := CallWire(context.Background(), a, []string{"next"}, nil, &result); err != nil || result != "reused" {
		t.Fatalf("next: %q, %v", result, err)
	}
}

func TestLocalWirePairPrivateDispatchContext(t *testing.T) {
	a, b := localPair(t, Options{})
	type verifiedKey struct{}
	verified := &struct{ identity string }{"verified locally"}
	dispatch := &wireDispatchContext{ctx: context.WithValue(context.Background(), verifiedKey{}, verified)}
	bBinding := testBinding(t, b)
	_, _ = HandleWire(bBinding, []string{"inspect"}, func(ctx context.Context, _ json.RawMessage) (any, error) {
		if ctx.Value(verifiedKey{}) != verified {
			return nil, errors.New("lost verified dispatch context")
		}
		return "observed", nil
	})
	var result string
	if err := callWire(context.Background(), a, []string{"inspect"}, nil, &result, dispatch); err != nil || result != "observed" {
		t.Fatalf("context: %q, %v", result, err)
	}
}

func TestLocalDispatcherExactAndPrefixRoutes(t *testing.T) {
	a, b := localPair(t, Options{})
	bBinding := testBinding(t, b)
	for _, path := range [][]string{nil, {"a"}} {
		label := "root"
		if len(path) > 0 {
			label = "a"
		}
		_, err := bBinding.RegisterPrefix(path, duplex.Receiver{Message: func(received []string, m duplex.Message) {
			if len(received) == 0 {
				t.Error("callback path lost its origin")
			}
			sendWireResponse(m, json.RawMessage(`"`+label+`"`), nil)
		}})
		if err != nil {
			t.Fatal(err)
		}
	}
	detach, _ := HandleWire(bBinding, []string{"a", "b"}, func(context.Context, json.RawMessage) (any, error) { return "exact", nil })
	var result string
	if err := CallWire(context.Background(), a, []string{"a", "b"}, nil, &result); err != nil || result != "exact" {
		t.Fatalf("exact: %q, %v", result, err)
	}
	detach()
	if err := CallWire(context.Background(), a, []string{"a", "b"}, nil, &result); err != nil || result != "a" {
		t.Fatalf("prefix: %q, %v", result, err)
	}
	if err := CallWire(context.Background(), a, []string{"other"}, nil, &result); err != nil || result != "root" {
		t.Fatalf("root: %q, %v", result, err)
	}
}

func TestLocalWirePairDeadlineRetainsNoncooperativeHandlerBudget(t *testing.T) {
	a, b := localPair(t, Options{RequestTimeout: 15 * time.Millisecond, MaxConcurrentHandlers: 1})
	started, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer close(release)
	bBinding := testBinding(t, b)
	_, _ = HandleWire(bBinding, []string{"hold"}, func(ctx context.Context, _ json.RawMessage) (any, error) {
		close(started)
		<-ctx.Done()
		close(cancelled)
		<-release
		return nil, nil
	})
	first := make(chan error, 1)
	go func() { first <- CallWire(context.Background(), a, []string{"hold"}, nil, nil) }()
	<-started
	var public *PublicError
	if err := <-first; !errors.As(err, &public) || public.Code != "cancelled" {
		t.Fatalf("deadline: %v", err)
	}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("deadline did not cancel handler")
	}
	err := CallWire(context.Background(), a, []string{"hold"}, nil, nil)
	if !errors.As(err, &public) || public.Code != "busy" {
		t.Fatalf("handler budget: %v", err)
	}
}

func TestLocalWirePairStalledEventDeadlineIsObserved(t *testing.T) {
	pressure := make(chan Backpressure, 1)
	a, b := localPair(t, Options{WriteTimeout: 15 * time.Millisecond, Observer: localTestObserver(func(event ObserverEvent) {
		if event, ok := event.(Backpressure); ok && event.Stalled {
			pressure <- event
		}
	})})
	bBinding := testBinding(t, b)
	release, closed := make(chan struct{}), make(chan struct{})
	defer close(release)
	_, _ = bBinding.Register([]string{"event"}, duplex.Receiver{Message: func([]string, duplex.Message) { <-release }, Closed: func(duplex.Code, string) { close(closed) }})
	if err := EmitWire(context.Background(), a, []string{"event"}, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-pressure:
		if event.Deadline != 15*time.Millisecond {
			t.Fatalf("deadline: %v", event.Deadline)
		}
	case <-time.After(time.Second):
		t.Fatal("stalled event was not observed")
	}
	<-closed
	if err := EmitWire(context.Background(), a, []string{"event"}, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed pair: %v", err)
	}
}

func TestLocalWirePairOversizedResponseUsesBoundedFallback(t *testing.T) {
	a, b := localPair(t, Options{MaxFrameBytes: 512})
	bBinding := testBinding(t, b)
	_, _ = HandleWire(bBinding, []string{"large"}, func(context.Context, json.RawMessage) (any, error) { return strings.Repeat("x", 2048), nil })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var result string
	err := CallWire(ctx, a, []string{"large"}, nil, &result)
	var public *PublicError
	if !errors.As(err, &public) || public.Code != "internal" {
		t.Fatalf("oversized response: %v", err)
	}
}
