package dispatch_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	core "github.com/Bitspark/bitruntime/core/go"
	dispatch "github.com/Bitspark/bitruntime/dispatch/go"
	engine "github.com/Bitspark/bitruntime/engine/go"
	transports "github.com/Bitspark/bitruntime/transports/go"
	wire "github.com/Bitspark/bitwire/wire/go"
)

type eventVerifiedKey struct{}
type eventVerifiedPropagator struct{ verified any }

func (p eventVerifiedPropagator) Extract(ctx context.Context, trace core.Trace) context.Context {
	return context.WithValue(core.DefaultPropagator.Extract(ctx, trace), eventVerifiedKey{}, p.verified)
}
func (eventVerifiedPropagator) Inject(ctx context.Context) core.Trace {
	return core.DefaultPropagator.Inject(ctx)
}

func TestWireEventContextSurvivesPhysicalForwardLocalPairAndMount(t *testing.T) {
	verified := &struct{ source string }{"trusted context"}
	client, server := newPair(t, engine.Options{Propagator: eventVerifiedPropagator{verified}}, engine.Options{})
	access, binding, err := core.NewPair(core.PairOptions{MaxPendingRequests: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = access.Close(transports.CodeNormal, "done") })
	stop, err := core.Forward(server.Wire(), testBinding(t, core.Mount(map[string]wire.Endpoint{"local": access})).Select([]string{"local"}))
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	mountBinding := testBinding(t, core.Mount(map[string]wire.Endpoint{"model": binding}))
	model := mountBinding.Select([]string{"model", "events"})
	observed := make(chan context.Context, 1)
	effects := 0
	modelBinding := testBinding(t, model)
	_, err = dispatch.Register(modelBinding, []string{"change"}, dispatch.Handlers{Event: func(ctx context.Context, _ json.RawMessage) error {
		observed <- ctx
		if ctx.Value(eventVerifiedKey{}) != verified {
			return errors.New("unverified event")
		}
		effects++
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	meta := map[string]string{"tenant": "explicit", "verified": "cannot manufacture context"}
	if err := dispatch.Emit(core.WithMeta(context.Background(), meta), client.Wire(), []string{"events", "change"}, nil); err != nil {
		t.Fatal(err)
	}
	ctx := receive(t, observed)
	if ctx.Value(eventVerifiedKey{}) != verified || !reflect.DeepEqual(core.MetaFrom(ctx), meta) {
		t.Fatalf("lost received context: verified=%v meta=%v", ctx.Value(eventVerifiedKey{}), core.MetaFrom(ctx))
	}
	_, _ = dispatch.Handle(mountBinding, []string{"model", "barrier"}, func(context.Context, json.RawMessage) (any, error) { return effects, nil })
	var count int
	if err := dispatch.Call(context.Background(), access, []string{"barrier"}, nil, &count); err != nil || count != 1 {
		t.Fatalf("effect count=%d, err=%v", count, err)
	}

	// New outgoing events carry only explicitly supplied metadata. The private
	// received context ends at the next physical boundary.
	returned := make(chan context.Context, 2)
	clientBinding := testBinding(t, client.Wire())
	_, _ = dispatch.Register(clientBinding, []string{"outgoing"}, dispatch.Handlers{Event: func(ctx context.Context, _ json.RawMessage) error { returned <- ctx; return nil }})
	if err := dispatch.Emit(ctx, server.Wire(), []string{"outgoing"}, nil); err != nil {
		t.Fatal(err)
	}
	fresh := receive(t, returned)
	if fresh.Value(eventVerifiedKey{}) != nil || len(core.MetaFrom(fresh)) != 0 {
		t.Fatalf("ambient context crossed transport: %v %v", fresh.Value(eventVerifiedKey{}), core.MetaFrom(fresh))
	}
	if err := dispatch.Emit(core.WithMeta(ctx, core.MetaFrom(ctx)), server.Wire(), []string{"outgoing"}, nil); err != nil {
		t.Fatal(err)
	}
	if got := core.MetaFrom(receive(t, returned)); !reflect.DeepEqual(got, meta) {
		t.Fatalf("explicit outgoing metadata=%v", got)
	}
	_ = server.Close()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("event context lost its physical connection lifetime")
	}
}

func TestWireEventMetadataCannotSupplyVerifiedContext(t *testing.T) {
	client, server := newPair(t, engine.Options{}, engine.Options{})
	access, binding, err := core.NewPair(core.PairOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = access.Close(transports.CodeNormal, "done") })
	stop, err := core.Forward(server.Wire(), access)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	denied := make(chan bool, 1)
	modelBinding := testBinding(t, binding)
	_, _ = dispatch.Register(modelBinding, []string{"guard"}, dispatch.Handlers{Event: func(ctx context.Context, _ json.RawMessage) error {
		if ctx.Value(eventVerifiedKey{}) == nil {
			denied <- true
			return errors.New("event denied")
		}
		denied <- false
		return nil
	}})
	if err := dispatch.Emit(core.WithMeta(context.Background(), map[string]string{"verified": "yes"}), client.Wire(), []string{"guard"}, nil); err != nil {
		t.Fatal(err)
	}
	if !receive(t, denied) {
		t.Fatal("metadata bypassed the event guard")
	}
}

func TestWireEventContextStopsAtAnotherPhysicalBoundary(t *testing.T) {
	verified := &struct{}{}
	client, incoming := newPair(t, engine.Options{Propagator: eventVerifiedPropagator{verified}}, engine.Options{})
	outgoing, server := newPair(t, engine.Options{}, engine.Options{})
	stop, err := core.Forward(incoming.Wire(), outgoing.Wire())
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	observed := make(chan context.Context, 1)
	serverBinding := testBinding(t, server.Wire())
	_, _ = dispatch.Register(serverBinding, []string{"event"}, dispatch.Handlers{Event: func(ctx context.Context, _ json.RawMessage) error { observed <- ctx; return nil }})
	if err := dispatch.Emit(core.WithMeta(context.Background(), map[string]string{"explicit": "yes"}), client.Wire(), []string{"event"}, nil); err != nil {
		t.Fatal(err)
	}
	ctx := receive(t, observed)
	if ctx.Value(eventVerifiedKey{}) != nil || core.MetaFrom(ctx)["explicit"] != "yes" {
		t.Fatalf("physical boundary: private=%v meta=%v", ctx.Value(eventVerifiedKey{}), core.MetaFrom(ctx))
	}
}

func TestWireLocalEventsUseSuppliedPropagator(t *testing.T) {
	verified := &struct{}{}
	a, b, err := core.NewPair(core.PairOptions{Propagator: eventVerifiedPropagator{verified}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close(transports.CodeNormal, "done") })
	observed := make(chan context.Context, 1)
	bBinding := testBinding(t, b)
	_, _ = dispatch.Register(bBinding, []string{"event"}, dispatch.Handlers{Event: func(ctx context.Context, _ json.RawMessage) error { observed <- ctx; return nil }})
	if err := dispatch.Emit(context.Background(), a, []string{"event"}, nil); err != nil {
		t.Fatal(err)
	}
	if receive(t, observed).Value(eventVerifiedKey{}) != verified {
		t.Fatal("local event bypassed configured propagator")
	}
}
