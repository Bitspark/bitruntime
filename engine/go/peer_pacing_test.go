package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	ws "github.com/Bitspark/nightseam/runtime/go"
)

type pacedOutputObserver struct {
	pressure chan ws.Backpressure
}

func (o *pacedOutputObserver) Observe(event ws.ObserverEvent) {
	if pressure, ok := event.(ws.Backpressure); ok {
		o.pressure <- pressure
	}
}

func TestPublicPeerFullOutputPacesUntilCallerCancellation(t *testing.T) {
	for _, operation := range []string{"event", "request"} {
		t.Run(operation, func(t *testing.T) {
			release := make(chan struct{})
			defer close(release)
			control := &delayedWriteControl{started: make(chan struct{}), gate: release}
			observed := &pacedOutputObserver{pressure: make(chan ws.Backpressure, 4)}
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
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			finished := make(chan error, 1)
			go func() {
				if operation == "event" {
					finished <- destination.Emit(ctx, "cancelled", 3)
				} else {
					finished <- destination.Call(ctx, "cancelled", 3, nil)
				}
			}()
			// Pressure is an admission barrier: the caller is waiting while the
			// accepted write and one queued frame remain held by the consumer.
			pressure := receive(t, observed.pressure)
			if pressure.Stalled || pressure.Queued != 1 {
				t.Fatalf("full public queue = %+v, want pacing at capacity", pressure)
			}
			cancel()
			err := receive(t, finished)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled producer = %v, want caller cancellation", err)
			}
			wantUnpublished(t, err, true)
			if err := destination.Err(); err != nil {
				t.Fatalf("cancelling an unadmitted producer ended its carrier: %v", err)
			}
		})
	}
}

func TestPublicPeerFullOutputResumesWhenConsumerDrains(t *testing.T) {
	for _, operation := range []string{"event", "request"} {
		t.Run(operation, func(t *testing.T) {
			release := make(chan struct{})
			var once sync.Once
			allowWrites := func() { once.Do(func() { close(release) }) }
			defer allowWrites()
			control := &delayedWriteControl{started: make(chan struct{}), gate: release}
			observed := &pacedOutputObserver{pressure: make(chan ws.Backpressure, 4)}
			prefix := make(chan int, 3)
			_, destination := delayedWritePair(t, control, ws.Options{
				QueueCapacity: 1,
				WriteTimeout:  5 * time.Second,
				Observer:      observed,
			}, ws.Options{
				Handlers: map[string]ws.Handler{
					"echo": func(_ context.Context, _ *ws.Peer, data json.RawMessage) (any, error) { return data, nil },
				},
				Events: map[string]ws.EventHandler{
					"item": func(_ context.Context, _ *ws.Peer, data json.RawMessage) {
						var item int
						if err := json.Unmarshal(data, &item); err != nil {
							t.Error(err)
						}
						prefix <- item
					},
				},
			})
			if err := destination.Emit(context.Background(), "item", 1); err != nil {
				t.Fatal(err)
			}
			receive(t, control.started)
			if err := destination.Emit(context.Background(), "item", 2); err != nil {
				t.Fatal(err)
			}
			finished := make(chan error, 1)
			go func() {
				if operation == "event" {
					finished <- destination.Emit(context.Background(), "item", 3)
				} else {
					var result int
					err := destination.Call(context.Background(), "echo", 3, &result)
					if err == nil && result != 3 {
						t.Errorf("resumed request result = %d, want 3", result)
					}
					finished <- err
				}
			}()
			if pressure := receive(t, observed.pressure); pressure.Stalled {
				t.Fatalf("transient full public queue was declared stalled: %+v", pressure)
			}
			allowWrites()
			if err := receive(t, finished); err != nil {
				t.Fatalf("resumed producer = %v", err)
			}
			count := 2
			if operation == "event" {
				count = 3
			}
			for want := 1; want <= count; want++ {
				if got := receive(t, prefix); got != want {
					t.Fatalf("accepted prefix = %d, want %d", got, want)
				}
			}
			if err := destination.Err(); err != nil {
				t.Fatalf("resumed producer ended its carrier: %v", err)
			}
		})
	}
}
