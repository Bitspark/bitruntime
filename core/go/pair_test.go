package core_test

// Ported from nightseam v0.6.0 runtime/go/wire_pair_test.go (commit 5cc9723a),
// with regression tests for nightseam#722, nightseam#658 and research R26.

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

// nightseam#658: run this with -race and a high -count. The response frees its
// call's pending slot before the caller holds the answer, so the call issued
// right after it is never refused busy.
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

// nightseam#658, deterministically: the caller's return capability issues the
// next request while it is being handed the answer, before that hand-off has
// returned to the pair. At a pending limit of one it must find the slot free.
func TestPairResponseFreesPendingSlotBeforeCallerHoldsAnswer(t *testing.T) {
	a, b := localPair(t, core.PairOptions{MaxPendingRequests: 1})
	bBinding := testBinding(t, b)
	_, _ = dispatch.Handle(bBinding, []string{"next"}, func(context.Context, json.RawMessage) (any, error) { return "reused", nil })
	answers := make(chan wire.ProfileFrame, 2)
	request := func(id string, returning *wire.ReturnAddress) wire.Message {
		return wire.Message{Frame: wire.ProfileFrame{Version: 1, Kind: wire.ProfileRequest, ID: id, Params: json.RawMessage("null")}, Return: returning}
	}
	second := &wire.ReturnAddress{Wire: &localTestReturn{send: func(m wire.Message) error { answers <- m.Frame; return nil }}}
	first := &wire.ReturnAddress{Wire: &localTestReturn{send: func(m wire.Message) error {
		answers <- m.Frame
		return a.Send([]string{"next"}, request("c:2", second))
	}}}
	if err := a.Send([]string{"next"}, request("c:1", first)); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"c:1", "c:2"} {
		answer := receive(t, answers)
		if answer.ID != id || answer.Error != nil || string(answer.Result) != `"reused"` {
			t.Fatalf("answer to %s: %+v %+v", id, answer, answer.Error)
		}
	}
}

// nightseam#658, repeated: at a pending limit of one, each call is issued the
// moment the previous one returned, and none of them finds its slot taken.
func TestPairSequentialCallsAtThePendingLimitAreNeverBusy(t *testing.T) {
	a, b := localPair(t, core.PairOptions{MaxPendingRequests: 1})
	bBinding := testBinding(t, b)
	_, _ = dispatch.Handle(bBinding, []string{"next"}, func(context.Context, json.RawMessage) (any, error) { return "reused", nil })
	for i := range 500 {
		var result string
		if err := dispatch.Call(context.Background(), a, []string{"next"}, nil, &result); err != nil || result != "reused" {
			t.Fatalf("call %d after its predecessor returned: %q, %v", i, result, err)
		}
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

// nightseam#722: a pair that closes with a refusal still queued answers it
// with the refusal it was admitted with, instead of leaving its caller to its
// own deadline, and answers the call it still held disconnected.
func TestLocalWirePairCloseAnswersQueuedRefusals(t *testing.T) {
	for _, mode := range []string{"blocked-delivery", "immediate-close"} {
		t.Run(mode, func(t *testing.T) {
			a, b := localPair(t, core.PairOptions{MaxPendingRequests: 1})
			bBinding := testBinding(t, b)
			started, release := make(chan struct{}), make(chan struct{})
			var released sync.Once
			t.Cleanup(func() { released.Do(func() { close(release) }) })
			_, _ = dispatch.Handle(bBinding, []string{"hold"}, func(ctx context.Context, _ json.RawMessage) (any, error) {
				close(started)
				select {
				case <-release:
				case <-ctx.Done():
				}
				return "done", nil
			})
			entered, unblock := make(chan struct{}), make(chan struct{})
			var unblocked sync.Once
			t.Cleanup(func() { unblocked.Do(func() { close(unblock) }) })
			_, _ = bBinding.Register([]string{"block"}, wire.Receiver{Message: func([]string, wire.Message) { close(entered); <-unblock }})
			// Deadlines far past the test's own wait: an answer that depends
			// on them is the defect.
			long := dispatch.CallOptions{Timeout: time.Minute}
			held := make(chan error, 1)
			go func() { held <- dispatch.Call(context.Background(), a, []string{"hold"}, nil, nil, long) }()
			receive(t, started)
			if mode == "blocked-delivery" {
				// The receiving side's delivery is held, so nothing it has
				// queued drains before the pair closes.
				if err := dispatch.Emit(context.Background(), a, []string{"block"}, nil); err != nil {
					t.Fatal(err)
				}
				receive(t, entered)
			}
			queued := make(chan struct{})
			admitted := sendOnly(func(path []string, m wire.Message) error {
				err := a.Send(path, m)
				if m.Frame.Kind == wire.ProfileRequest {
					close(queued)
				}
				return err
			})
			refused := make(chan error, 1)
			go func() { refused <- dispatch.Call(context.Background(), admitted, []string{"hold"}, nil, nil, long) }()
			receive(t, queued)
			_ = a.Close(transports.CodeNormal, "closing with a queued refusal")
			var public *core.PublicError
			if err := receive(t, refused); !errors.As(err, &public) || public.Code != "busy" {
				t.Fatalf("queued refusal at close: %v", err)
			}
			if err := receive(t, held); !errors.As(err, &public) || public.Code != "disconnected" {
				t.Fatalf("held call at close: %v", err)
			}
		})
	}
}

// R26: a pair that overflowed is a closed carrier that keeps its cause. The
// send that overflowed reports both; the carrier is closed from then on.
func TestLocalWirePairOverflowIsAClosedCarrier(t *testing.T) {
	a, b := localPair(t, core.PairOptions{QueueCapacity: 1})
	bBinding := testBinding(t, b)
	entered, release, ended := make(chan struct{}), make(chan struct{}), make(chan wire.Code, 1)
	defer close(release)
	_, _ = bBinding.Register([]string{"event"}, wire.Receiver{Message: func([]string, wire.Message) { close(entered); <-release }, Closed: func(code wire.Code, _ string) { ended <- code }})
	event := wire.Message{Frame: wire.ProfileFrame{Version: 1, Kind: wire.ProfileEvent, Data: json.RawMessage("null")}}
	if err := a.Send([]string{"event"}, event); err != nil {
		t.Fatal(err)
	}
	receive(t, entered)
	if err := a.Send([]string{"event"}, event); err != nil {
		t.Fatal(err)
	}
	err := a.Send([]string{"event"}, event)
	if !errors.Is(err, core.ErrBackpressure) || !errors.Is(err, transports.ErrClosed) {
		t.Fatalf("overflowing send: %v", err)
	}
	if code := receive(t, ended); code != transports.CodeProtocol {
		t.Fatalf("overflow ended the pair with %d", code)
	}
	for _, end := range []wire.Endpoint{a, b} {
		if err := end.Send([]string{"event"}, event); !errors.Is(err, transports.ErrClosed) {
			t.Fatalf("send after overflow: %v", err)
		}
		if _, err := end.Receive(wire.Receiver{}); !errors.Is(err, transports.ErrClosed) {
			t.Fatalf("receive after overflow: %v", err)
		}
	}
}

// R26: a request that core.Forward hands to a pair it overflows is answered
// disconnected, not internal, through its own return capability.
func TestForwardedRequestToAnOverflowedPairIsAnsweredDisconnected(t *testing.T) {
	caller, inbound := localPair(t, core.PairOptions{})
	left, right := localPair(t, core.PairOptions{QueueCapacity: 1})
	stop, err := core.Forward(inbound, left)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stop)
	rightBinding := testBinding(t, right)
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	var once sync.Once
	_, _ = rightBinding.Register([]string{"event"}, wire.Receiver{Message: func([]string, wire.Message) {
		once.Do(func() { close(entered) })
		<-release
	}})
	reached := make(chan struct{}, 1)
	_, _ = dispatch.Handle(rightBinding, []string{"call"}, func(context.Context, json.RawMessage) (any, error) { reached <- struct{}{}; return nil, nil })
	if err := dispatch.Emit(context.Background(), caller, []string{"event"}, 1); err != nil {
		t.Fatal(err)
	}
	receive(t, entered)
	// The destination's queue now holds this one event and nothing drains it.
	if err := dispatch.Emit(context.Background(), caller, []string{"event"}, 2); err != nil {
		t.Fatal(err)
	}
	err = dispatch.Call(context.Background(), caller, []string{"call"}, nil, nil, dispatch.CallOptions{Timeout: time.Minute})
	var public *core.PublicError
	if !errors.As(err, &public) || public.Code != "disconnected" {
		t.Fatalf("request forwarded into an overflow: %v", err)
	}
	select {
	case <-reached:
		t.Fatal("the overflowing request reached its handler")
	default:
	}
}
