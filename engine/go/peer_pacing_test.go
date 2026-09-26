package engine_test

// Ported from Nightseam v0.6.0 runtime/go/peer_pacing_test.go
// (5cc9723a24646c40ed1861f892b2b23eb6d785d7).
//
// v0.6.0 held that the peer's public Emit and Call, finding the outgoing
// queue full, paced for one write deadline: the caller waited, could withdraw
// without ending the carrier, and resumed when the consumer drained; and that
// its observer was told of the pressure. Emit and Call are the removed raw
// API and observers are removed, so those assertions are dropped. What
// remains of the same rule is held here: a response — the one producer the
// peer still paces — waits for a full queue and resumes when it drains; and
// root traffic, which v0.6.0 also handed off immediately, never paces.

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	core "github.com/Bitspark/bitruntime/core/go"
	engine "github.com/Bitspark/bitruntime/engine/go"
	transports "github.com/Bitspark/bitruntime/transports/go"
)

// fullOutput is a server whose writer is held on its first write and whose
// outgoing queue of one is full behind it, and a client that serves items.
func fullOutput(t *testing.T, release <-chan struct{}, writeTimeout time.Duration, server serving) (client, destination *engine.Peer, items <-chan int) {
	t.Helper()
	control := newWriteGate(release)
	prefix := make(chan int, 4)
	client, destination = gatedPair(t, control, server.with(engine.Options{QueueCapacity: 1, WriteTimeout: writeTimeout}), serving{Events: map[string]eventHandler{
		"item": func(_ context.Context, _ *engine.Peer, data json.RawMessage) {
			var item int
			if err := json.Unmarshal(data, &item); err != nil {
				t.Error(err)
			}
			prefix <- item
		},
	}}.with(engine.Options{}))
	if err := emit(context.Background(), destination, "item", 1); err != nil {
		t.Fatal(err)
	}
	receive(t, control.started)
	if err := emit(context.Background(), destination, "item", 2); err != nil {
		t.Fatal(err)
	}
	return client, destination, prefix
}

func TestAResponseToAFullOutputIsPacedUntilTheConsumerDrains(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	allowWrites := func() { once.Do(func() { close(release) }) }
	defer allowWrites()
	answering := make(chan struct{}, 1)
	client, destination, prefix := fullOutput(t, release, 5*time.Second, serving{Handlers: map[string]handler{
		"echo": func(_ context.Context, _ *engine.Peer, data json.RawMessage) (any, error) {
			answering <- struct{}{}
			return data, nil
		},
	}})
	finished := make(chan error, 1)
	go func() {
		var result int
		err := call(context.Background(), client, "echo", 3, &result)
		if err == nil && result != 3 {
			t.Errorf("resumed response result = %d, want 3", result)
		}
		finished <- err
	}()
	receive(t, answering)
	// The response now waits behind the full queue: the carrier is whole and
	// the caller unanswered.
	time.Sleep(50 * time.Millisecond)
	if err := destination.Err(); err != nil {
		t.Fatalf("a transiently full queue ended the carrier: %v", err)
	}
	select {
	case err := <-finished:
		t.Fatalf("the call returned %v while its response could not be written", err)
	default:
	}
	allowWrites()
	if err := receive(t, finished); err != nil {
		t.Fatalf("resumed response = %v", err)
	}
	for want := 1; want <= 2; want++ {
		if got := receive(t, prefix); got != want {
			t.Fatalf("accepted prefix = %d, want %d", got, want)
		}
	}
	if err := destination.Err(); err != nil {
		t.Fatalf("a paced response ended its carrier: %v", err)
	}
}

// Root traffic is handed to the outgoing queue at once: a composition does
// not run at its slowest destination's pace. A full queue ends the carrier
// with ErrBackpressure without waiting for the write deadline, which is set
// far beyond the test's own wait here.
func TestRootTrafficToAFullOutputEndsTheCarrierAtOnce(t *testing.T) {
	for _, operation := range []string{"event", "request"} {
		t.Run(operation, func(t *testing.T) {
			release := make(chan struct{})
			defer close(release)
			_, destination, _ := fullOutput(t, release, time.Minute, serving{})
			finished := make(chan error, 1)
			go func() {
				if operation == "event" {
					finished <- emit(context.Background(), destination, "item", 3)
				} else {
					finished <- call(context.Background(), destination, "echo", 3, nil)
				}
			}()
			receive(t, destination.Done())
			if err := destination.Err(); !errors.Is(err, core.ErrBackpressure) || !errors.Is(err, transports.ErrClosed) {
				t.Fatalf("root traffic to a full queue ended the carrier with %v", err)
			}
			err := receive(t, finished)
			if operation == "event" {
				// The root took it, or refused it because its own queue was
				// still full: either way it is not delivered.
				if err != nil && !errors.Is(err, core.ErrBackpressure) {
					t.Fatalf("the event was refused with %v", err)
				}
				return
			}
			// A request the root admitted is answered disconnected once the
			// carrier ends (R26); one it refused at once carries the cause.
			var public *core.PublicError
			if !(errors.As(err, &public) && public.Code == "disconnected") && !errors.Is(err, core.ErrBackpressure) {
				t.Fatalf("the request was answered %v", err)
			}
		})
	}
}
