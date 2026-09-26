package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Bitspark/nightseam/duplex/go"
	ws "github.com/Bitspark/nightseam/runtime/go"
)

type eventVerifiedKey struct{}
type eventVerifiedPropagator struct{ verified any }

func (p eventVerifiedPropagator) Extract(ctx context.Context, trace ws.Trace) context.Context {
	return context.WithValue(ws.DefaultPropagator.Extract(ctx, trace), eventVerifiedKey{}, p.verified)
}
func (eventVerifiedPropagator) Inject(ctx context.Context) ws.Trace {
	return ws.DefaultPropagator.Inject(ctx)
}

func TestWireEventContextSurvivesPhysicalForwardLocalPairAndMount(t *testing.T) {
	verified := &struct{ source string }{"trusted context"}
	client, server := newPair(t, ws.Options{Propagator: eventVerifiedPropagator{verified}}, ws.Options{})
	access, binding, err := ws.NewWirePair(ws.Options{MaxPendingRequests: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = access.Close(duplex.CodeNormal, "done") })
	stop, err := ws.ForwardWire(server.Wire(), testBinding(t, duplex.Mount(map[string]duplex.Endpoint{"local": access})).Select([]string{"local"}))
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	mountBinding := testBinding(t, duplex.Mount(map[string]duplex.Endpoint{"model": binding}))
	model := mountBinding.Select([]string{"model", "events"})
	observed := make(chan context.Context, 1)
	effects := 0
	modelBinding := testBinding(t, model)
	_, err = ws.RegisterWire(modelBinding, []string{"change"}, ws.WireHandlers{Event: func(ctx context.Context, _ json.RawMessage) error {
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
	if err := ws.EmitWire(ws.WithMeta(context.Background(), meta), client.Wire(), []string{"events", "change"}, nil); err != nil {
		t.Fatal(err)
	}
	ctx := receive(t, observed)
	if ctx.Value(eventVerifiedKey{}) != verified || !reflect.DeepEqual(ws.MetaFrom(ctx), meta) {
		t.Fatalf("lost received context: verified=%v meta=%v", ctx.Value(eventVerifiedKey{}), ws.MetaFrom(ctx))
	}
	_, _ = ws.HandleWire(mountBinding, []string{"model", "barrier"}, func(context.Context, json.RawMessage) (any, error) { return effects, nil })
	var count int
	if err := ws.CallWire(context.Background(), access, []string{"barrier"}, nil, &count); err != nil || count != 1 {
		t.Fatalf("effect count=%d, err=%v", count, err)
	}

	// New outgoing events carry only explicitly supplied metadata. The private
	// received context ends at the next physical boundary.
	returned := make(chan context.Context, 2)
	clientBinding := testBinding(t, client.Wire())
	_, _ = ws.RegisterWire(clientBinding, []string{"outgoing"}, ws.WireHandlers{Event: func(ctx context.Context, _ json.RawMessage) error { returned <- ctx; return nil }})
	if err := ws.EmitWire(ctx, server.Wire(), []string{"outgoing"}, nil); err != nil {
		t.Fatal(err)
	}
	fresh := receive(t, returned)
	if fresh.Value(eventVerifiedKey{}) != nil || len(ws.MetaFrom(fresh)) != 0 {
		t.Fatalf("ambient context crossed transport: %v %v", fresh.Value(eventVerifiedKey{}), ws.MetaFrom(fresh))
	}
	if err := ws.EmitWire(ws.WithMeta(ctx, ws.MetaFrom(ctx)), server.Wire(), []string{"outgoing"}, nil); err != nil {
		t.Fatal(err)
	}
	if got := ws.MetaFrom(receive(t, returned)); !reflect.DeepEqual(got, meta) {
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
	client, server := newPair(t, ws.Options{}, ws.Options{})
	access, binding, err := ws.NewWirePair(ws.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = access.Close(duplex.CodeNormal, "done") })
	stop, err := ws.ForwardWire(server.Wire(), access)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	denied := make(chan bool, 1)
	modelBinding := testBinding(t, binding)
	_, _ = ws.RegisterWire(modelBinding, []string{"guard"}, ws.WireHandlers{Event: func(ctx context.Context, _ json.RawMessage) error {
		if ctx.Value(eventVerifiedKey{}) == nil {
			denied <- true
			return errors.New("event denied")
		}
		denied <- false
		return nil
	}})
	if err := ws.EmitWire(ws.WithMeta(context.Background(), map[string]string{"verified": "yes"}), client.Wire(), []string{"guard"}, nil); err != nil {
		t.Fatal(err)
	}
	if !receive(t, denied) {
		t.Fatal("metadata bypassed the event guard")
	}
}

func TestWireEventContextStopsAtAnotherPhysicalBoundary(t *testing.T) {
	verified := &struct{}{}
	client, incoming := newPair(t, ws.Options{Propagator: eventVerifiedPropagator{verified}}, ws.Options{})
	outgoing, server := newPair(t, ws.Options{}, ws.Options{})
	stop, err := ws.ForwardWire(incoming.Wire(), outgoing.Wire())
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	observed := make(chan context.Context, 1)
	serverBinding := testBinding(t, server.Wire())
	_, _ = ws.RegisterWire(serverBinding, []string{"event"}, ws.WireHandlers{Event: func(ctx context.Context, _ json.RawMessage) error { observed <- ctx; return nil }})
	if err := ws.EmitWire(ws.WithMeta(context.Background(), map[string]string{"explicit": "yes"}), client.Wire(), []string{"event"}, nil); err != nil {
		t.Fatal(err)
	}
	ctx := receive(t, observed)
	if ctx.Value(eventVerifiedKey{}) != nil || ws.MetaFrom(ctx)["explicit"] != "yes" {
		t.Fatalf("physical boundary: private=%v meta=%v", ctx.Value(eventVerifiedKey{}), ws.MetaFrom(ctx))
	}
}

func TestWireLocalEventsUseSuppliedPropagator(t *testing.T) {
	verified := &struct{}{}
	a, b, err := ws.NewWirePair(ws.Options{Propagator: eventVerifiedPropagator{verified}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close(duplex.CodeNormal, "done") })
	observed := make(chan context.Context, 1)
	bBinding := testBinding(t, b)
	_, _ = ws.RegisterWire(bBinding, []string{"event"}, ws.WireHandlers{Event: func(ctx context.Context, _ json.RawMessage) error { observed <- ctx; return nil }})
	if err := ws.EmitWire(context.Background(), a, []string{"event"}, nil); err != nil {
		t.Fatal(err)
	}
	if receive(t, observed).Value(eventVerifiedKey{}) != verified {
		t.Fatal("local event bypassed configured propagator")
	}
}
