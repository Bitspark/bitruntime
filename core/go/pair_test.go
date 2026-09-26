package core_test

// Ported from Nightseam v0.6.0 runtime/go/wire_pair_test.go (commit 5cc9723a).

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	core "github.com/Bitspark/bitruntime/core/go"
	dispatch "github.com/Bitspark/bitruntime/dispatch/go"
	"github.com/Bitspark/bitruntime/internal/delivery/go"
	"github.com/Bitspark/bitruntime/internal/request/go"
	transports "github.com/Bitspark/bitruntime/transports/go"
	wire "github.com/Bitspark/bitwire/wire/go"
)

type localTestReturn struct{ send func(wire.Message) error }

func (r *localTestReturn) Send(_ []string, m wire.Message) error { return r.send(m) }

func receive[T any](t *testing.T, channel <-chan T) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for peer activity")
		var zero T
		return zero
	}
}

func testBinding(t *testing.T, endpoint wire.Endpoint) *dispatch.Dispatcher {
	t.Helper()
	binding, err := dispatch.NewDispatcher(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = binding.Close(transports.CodeNormal, "done") })
	return binding
}

func localPair(t *testing.T, options core.PairOptions) (wire.Endpoint, wire.Endpoint) {
	t.Helper()
	a, b, err := core.NewPair(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close(transports.CodeNormal, "done") })
	return a, b
}

func TestLocalWirePairRoundTripReverseAndIsolation(t *testing.T) {
	a, b := localPair(t, core.PairOptions{})
	aBinding := testBinding(t, a)
	_, err := dispatch.Handle(aBinding, []string{"reverse"}, func(_ context.Context, raw json.RawMessage) (any, error) { return string(raw), nil })
	if err != nil {
		t.Fatal(err)
	}
	bBinding := testBinding(t, b)
	_, err = dispatch.Handle(bBinding, []string{"call"}, func(ctx context.Context, raw json.RawMessage) (any, error) {
		var result string
		err := dispatch.Call(ctx, b, []string{"reverse"}, raw, &result)
		return result, err
	})
	if err != nil {
		t.Fatal(err)
	}
	var result string
	if err := dispatch.Call(context.Background(), a, []string{"call"}, 7, &result); err != nil || result != "7" {
		t.Fatalf("reverse result %q: %v", result, err)
	}
	x, y := localPair(t, core.PairOptions{})
	yBinding := testBinding(t, y)
	_, _ = dispatch.Handle(yBinding, []string{"call"}, func(context.Context, json.RawMessage) (any, error) { return "independent", nil })
	_ = a.Close(transports.CodeNormal, "first pair only")
	if err := dispatch.Call(context.Background(), x, []string{"call"}, nil, &result); err != nil || result != "independent" {
		t.Fatalf("other pair %q: %v", result, err)
	}
}

func TestLocalWirePairRetainsPendingUntilResponse(t *testing.T) {
	a, b := localPair(t, core.PairOptions{MaxPendingRequests: 1})
	started, release := make(chan struct{}), make(chan struct{})
	bBinding := testBinding(t, b)
	_, _ = dispatch.Handle(bBinding, []string{"hold"}, func(context.Context, json.RawMessage) (any, error) { close(started); <-release; return "done", nil })
	first := make(chan error, 1)
	go func() {
		var result string
		first <- dispatch.Call(context.Background(), a, []string{"hold"}, nil, &result)
	}()
	<-started
	var result any
	err := dispatch.Call(context.Background(), a, []string{"hold"}, nil, &result)
	var public *core.PublicError
	if !errors.As(err, &public) || public.Code != "busy" {
		t.Fatalf("pending budget: %v", err)
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	_, _ = dispatch.Handle(bBinding, []string{"next"}, func(context.Context, json.RawMessage) (any, error) { return "reused", nil })
	if err := dispatch.Call(context.Background(), a, []string{"next"}, nil, &result); err != nil {
		t.Fatal(err)
	}
}

func TestLocalWirePairOrderedEventsAndReservedCancel(t *testing.T) {
	a, b := localPair(t, core.PairOptions{QueueCapacity: 1, MaxPendingRequests: 1})
	bBinding := testBinding(t, b)
	started, cancelled := make(chan struct{}), make(chan struct{})
	_, _ = dispatch.Handle(bBinding, []string{"hold"}, func(ctx context.Context, _ json.RawMessage) (any, error) {
		close(started)
		<-ctx.Done()
		close(cancelled)
		return nil, ctx.Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	answer := make(chan error, 1)
	go func() { answer <- dispatch.Call(ctx, a, []string{"hold"}, nil, nil) }()
	<-started
	entered, release, drained := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var mu sync.Mutex
	var seen []int
	_, _ = bBinding.Register([]string{"event"}, wire.Receiver{Message: func(_ []string, m wire.Message) {
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
	if err := dispatch.Emit(context.Background(), a, []string{"event"}, 1); err != nil {
		t.Fatal(err)
	}
	<-entered
	if err := dispatch.Emit(context.Background(), a, []string{"event"}, 2); err != nil {
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
	a, b := localPair(t, core.PairOptions{QueueCapacity: 1})
	bBinding := testBinding(t, b)
	entered, release, ended := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer close(release)
	_, _ = bBinding.Register([]string{"event"}, wire.Receiver{Message: func([]string, wire.Message) { close(entered); <-release }, Closed: func(wire.Code, string) { close(ended) }})
	if err := dispatch.Emit(context.Background(), a, []string{"event"}, nil); err != nil {
		t.Fatal(err)
	}
	<-entered
	if err := dispatch.Emit(context.Background(), a, []string{"event"}, nil); err != nil {
		t.Fatal(err)
	}
	if err := dispatch.Emit(context.Background(), a, []string{"event"}, nil); !errors.Is(err, core.ErrBackpressure) {
		t.Fatalf("overflow: %v", err)
	}
	select {
	case <-ended:
	case <-time.After(time.Second):
		t.Fatal("blocked consumer hid closure")
	}
}

func TestLocalWirePairReturnMappingAndFailedResponseRetirement(t *testing.T) {
	a, b := localPair(t, core.PairOptions{MaxPendingRequests: 1})
	bBinding := testBinding(t, b)
	received := make(chan wire.Message, 2)
	_, _ = bBinding.Register([]string{"raw"}, wire.Receiver{Message: func(_ []string, m wire.Message) { received <- m }})
	failed := errors.New("return failed")
	original := &wire.ReturnAddress{Wire: &localTestReturn{send: func(wire.Message) error { return core.Unpublished(failed) }}}
	if err := a.Send([]string{"raw"}, wire.Message{Frame: wire.ProfileFrame{Version: 1, Kind: wire.ProfileRequest, ID: "c:1", Params: json.RawMessage("null")}, Return: original}); err != nil {
		t.Fatal(err)
	}
	request := <-received
	if request.Return == original {
		t.Fatal("root did not map the return capability")
	}
	if err := a.Send([]string{"raw"}, wire.Message{Frame: wire.ProfileFrame{Version: 1, Kind: wire.ProfileCancel, ID: "c:1"}, Return: original}); err != nil {
		t.Fatal(err)
	}
	if cancelled := <-received; cancelled.Return != request.Return {
		t.Fatal("cancellation used a different return capability")
	}
	err := request.Return.Wire.Send(nil, wire.Message{Frame: wire.ProfileFrame{Version: 1, Kind: wire.ProfileResponse, ID: "c:1", Result: json.RawMessage("null")}})
	if !errors.Is(err, failed) {
		t.Fatalf("return failure lost: %v", err)
	}
	var unpublished *core.UnpublishedError
	if errors.As(err, &unpublished) {
		t.Fatal("return retained publication proof after dispatch")
	}
	_, _ = dispatch.Handle(bBinding, []string{"next"}, func(context.Context, json.RawMessage) (any, error) { return "reused", nil })
	var result string
	if err := dispatch.Call(context.Background(), a, []string{"next"}, nil, &result); err != nil || result != "reused" {
		t.Fatalf("next: %q, %v", result, err)
	}
}

// v0.6.0 constructed its unexported wireDispatchContext and called callWire;
// bitruntime keeps the same private association in internal/delivery and the
// same call primitive in internal/request.
func TestLocalWirePairPrivateDispatchContext(t *testing.T) {
	a, b := localPair(t, core.PairOptions{})
	type verifiedKey struct{}
	verified := &struct{ identity string }{"verified locally"}
	established := &delivery.Context{Ctx: context.WithValue(context.Background(), verifiedKey{}, verified)}
	bBinding := testBinding(t, b)
	_, _ = dispatch.Handle(bBinding, []string{"inspect"}, func(ctx context.Context, _ json.RawMessage) (any, error) {
		if ctx.Value(verifiedKey{}) != verified {
			return nil, errors.New("lost verified dispatch context")
		}
		return "observed", nil
	})
	var result string
	if err := request.Call(context.Background(), a, []string{"inspect"}, nil, &result, established, request.Options{}); err != nil || result != "observed" {
		t.Fatalf("context: %q, %v", result, err)
	}
}

func TestLocalDispatcherExactAndPrefixRoutes(t *testing.T) {
	a, b := localPair(t, core.PairOptions{})
	bBinding := testBinding(t, b)
	for _, path := range [][]string{nil, {"a"}} {
		label := "root"
		if len(path) > 0 {
			label = "a"
		}
		_, err := bBinding.RegisterPrefix(path, wire.Receiver{Message: func(received []string, m wire.Message) {
			if len(received) == 0 {
				t.Error("callback path lost its origin")
			}
			core.Respond(m, json.RawMessage(`"`+label+`"`), nil)
		}})
		if err != nil {
			t.Fatal(err)
		}
	}
	detach, _ := dispatch.Handle(bBinding, []string{"a", "b"}, func(context.Context, json.RawMessage) (any, error) { return "exact", nil })
	var result string
	if err := dispatch.Call(context.Background(), a, []string{"a", "b"}, nil, &result); err != nil || result != "exact" {
		t.Fatalf("exact: %q, %v", result, err)
	}
	detach()
	if err := dispatch.Call(context.Background(), a, []string{"a", "b"}, nil, &result); err != nil || result != "a" {
		t.Fatalf("prefix: %q, %v", result, err)
	}
	if err := dispatch.Call(context.Background(), a, []string{"other"}, nil, &result); err != nil || result != "root" {
		t.Fatalf("root: %q, %v", result, err)
	}
}

func TestLocalWirePairDeadlineRetainsNoncooperativeHandlerBudget(t *testing.T) {
	a, b := localPair(t, core.PairOptions{RequestTimeout: 15 * time.Millisecond, MaxConcurrentHandlers: 1})
	started, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer close(release)
	bBinding := testBinding(t, b)
	_, _ = dispatch.Handle(bBinding, []string{"hold"}, func(ctx context.Context, _ json.RawMessage) (any, error) {
		close(started)
		<-ctx.Done()
		close(cancelled)
		<-release
		return nil, nil
	})
	first := make(chan error, 1)
	go func() { first <- dispatch.Call(context.Background(), a, []string{"hold"}, nil, nil) }()
	<-started
	var public *core.PublicError
	if err := <-first; !errors.As(err, &public) || public.Code != "cancelled" {
		t.Fatalf("deadline: %v", err)
	}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("deadline did not cancel handler")
	}
	err := dispatch.Call(context.Background(), a, []string{"hold"}, nil, nil)
	if !errors.As(err, &public) || public.Code != "busy" {
		t.Fatalf("handler budget: %v", err)
	}
}

// v0.6.0 also held the stall to a Backpressure observation carrying its
// deadline; the observer hooks are not ported, so the stall is observed as
// the pair's ending alone.
func TestLocalWirePairStalledEventDeadlineIsObserved(t *testing.T) {
	a, b := localPair(t, core.PairOptions{WriteTimeout: 15 * time.Millisecond})
	bBinding := testBinding(t, b)
	release, closed := make(chan struct{}), make(chan struct{})
	defer close(release)
	_, _ = bBinding.Register([]string{"event"}, wire.Receiver{Message: func([]string, wire.Message) { <-release }, Closed: func(wire.Code, string) { close(closed) }})
	if err := dispatch.Emit(context.Background(), a, []string{"event"}, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("stalled event was not observed")
	}
	if err := dispatch.Emit(context.Background(), a, []string{"event"}, nil); !errors.Is(err, transports.ErrClosed) {
		t.Fatalf("closed pair: %v", err)
	}
}

func TestLocalWirePairOversizedResponseUsesBoundedFallback(t *testing.T) {
	a, b := localPair(t, core.PairOptions{MaxFrameBytes: 512})
	bBinding := testBinding(t, b)
	_, _ = dispatch.Handle(bBinding, []string{"large"}, func(context.Context, json.RawMessage) (any, error) { return strings.Repeat("x", 2048), nil })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var result string
	err := dispatch.Call(ctx, a, []string{"large"}, nil, &result)
	var public *core.PublicError
	if !errors.As(err, &public) || public.Code != "internal" {
		t.Fatalf("oversized response: %v", err)
	}
}
