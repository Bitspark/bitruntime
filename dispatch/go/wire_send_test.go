package runtime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	ws "github.com/Bitspark/nightseam/runtime/go"
)

// A full destination's consumer remains held while the caller finishes. The
// write deadline is deliberately much longer than the caller's bound: waiting
// for that deadline would make fan-out run at its slowest destination's pace.
func TestWireSendEndsAFullCarrierWithoutWaitingForItsConsumer(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	control := &delayedWriteControl{started: make(chan struct{}), gate: release}
	observed := &recorder{}
	_, destination := delayedWritePair(t, control, ws.Options{
		QueueCapacity: 1,
		WriteTimeout:  5 * time.Second,
		Observer:      observed,
	}, ws.Options{})
	if err := destination.Emit(context.Background(), "first", 1); err != nil {
		t.Fatal(err)
	}
	receive(t, control.started)
	if err := destination.Emit(context.Background(), "second", 2); err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() { finished <- ws.EmitWire(context.Background(), destination.Wire(), []string{"overflow"}, 3) }()
	select {
	case err := <-finished:
		if err != nil && !errors.Is(err, ws.ErrBackpressure) {
			t.Fatalf("wire admission = %v, want admission or backpressure", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("send waited for a destination whose consumer is still held")
	}
	select {
	case <-destination.Done():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("wire dispatch waited for a destination whose consumer is still held")
	}
	if !errors.Is(destination.Err(), ws.ErrBackpressure) {
		t.Fatalf("full carrier remained open: %v", destination.Err())
	}
	stalls := 0
	for _, event := range observed.all() {
		if pressure, ok := event.(ws.Backpressure); ok && pressure.Stalled {
			stalls++
		}
	}
	if stalls != 1 {
		t.Fatalf("observer received %d terminal pressure events, want 1", stalls)
	}
}

func TestWireRequestEndsAFullCarrierWithoutWaitingForItsConsumer(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	control := &delayedWriteControl{started: make(chan struct{}), gate: release}
	_, destination := delayedWritePair(t, control, ws.Options{QueueCapacity: 1, WriteTimeout: 5 * time.Second}, ws.Options{})
	if err := destination.Emit(context.Background(), "first", 1); err != nil {
		t.Fatal(err)
	}
	receive(t, control.started)
	if err := destination.Emit(context.Background(), "second", 2); err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() {
		finished <- ws.CallWire(context.Background(), destination.Wire(), []string{"overflow"}, nil, nil)
	}()
	select {
	case err := <-finished:
		if err == nil {
			t.Fatal("wire request into a full carrier succeeded")
		}
		// Root wire admission already succeeded. A downstream carrier refusal
		// is an ordinary result, not proof of pre-admission non-publication.
		wantUnpublished(t, err, false)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("wire request waited for a destination whose consumer is still held")
	}
	if !errors.Is(destination.Err(), ws.ErrBackpressure) {
		t.Fatalf("full carrier remained open: %v", destination.Err())
	}
}

func TestOutputCancellationProofBelongsOnlyToTheUnadmittedCall(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	control := &delayedWriteControl{started: make(chan struct{}), gate: release}
	observed := &pacedOutputObserver{pressure: make(chan ws.Backpressure, 4)}
	_, destination := delayedWritePair(t, control, ws.Options{QueueCapacity: 1, WriteTimeout: 5 * time.Second, Observer: observed}, ws.Options{})
	accepted := make(chan error, 1)
	go func() { accepted <- destination.Call(context.Background(), "accepted", nil, nil) }()
	// The earlier call is already in its carrier's write. Hold it there, then
	// occupy the only queue slot before attempting the rejected call.
	receive(t, control.started)
	if err := destination.Emit(context.Background(), "queued", nil); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	refused := make(chan error, 1)
	go func() { refused <- destination.Call(ctx, "rejected", nil, nil) }()
	if pressure := receive(t, observed.pressure); pressure.Stalled {
		t.Fatalf("public call did not pace: %+v", pressure)
	}
	cancel()
	rejected := receive(t, refused)
	wantUnpublished(t, rejected, true)
	if !errors.Is(rejected, context.Canceled) {
		t.Fatalf("rejected call = %v", rejected)
	}
	// A subsequent immediate Wire dispatch ends this full carrier. Its failure
	// cannot make the earlier admitted call prove that it never left.
	if err := ws.EmitWire(context.Background(), destination.Wire(), []string{"overflow"}, nil); err != nil {
		t.Fatal(err)
	}
	earlier := receive(t, accepted)
	wantUnpublished(t, earlier, false)
	if !errors.Is(earlier, ws.ErrBackpressure) {
		t.Fatalf("earlier call = %v", earlier)
	}
}
