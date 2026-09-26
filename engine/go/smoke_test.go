package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	core "github.com/Bitspark/bitruntime/core/go"
	dispatch "github.com/Bitspark/bitruntime/dispatch/go"
	engine "github.com/Bitspark/bitruntime/engine/go"
	transports "github.com/Bitspark/bitruntime/transports/go"
)

// TestCallsEventsAndCancellationCrossAPipe: the path an adapter uses, end to
// end over the protocol engine — a dispatcher at the server's root, a call
// through the client's root, an event each way, and a withdrawn call whose
// cancellation reaches the handler.
func TestCallsEventsAndCancellationCrossAPipe(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	a, b := transports.Pipe(1 << 20)
	events := make(chan string, 4)
	cancelled := make(chan struct{})
	server, err := engine.NewPeer(ctx, b, engine.ServerRole, engine.Options{Prepare: func(p *engine.Peer) error {
		d, err := dispatch.NewDispatcher(p.Wire())
		if err != nil {
			return err
		}
		if _, err := dispatch.Handle(d, []string{"spaces", "a/b", "echo"}, func(ctx context.Context, raw json.RawMessage) (any, error) {
			return map[string]json.RawMessage{"echo": raw}, nil
		}); err != nil {
			return err
		}
		if _, err := dispatch.Handle(d, []string{"wait"}, func(ctx context.Context, raw json.RawMessage) (any, error) {
			<-ctx.Done()
			close(cancelled)
			return nil, ctx.Err()
		}); err != nil {
			return err
		}
		_, err = dispatch.Register(d, []string{"ping"}, dispatch.Handlers{Event: func(ctx context.Context, raw json.RawMessage) error {
			events <- "server:" + string(raw)
			return dispatch.Emit(ctx, p.Wire(), []string{"pong"}, "back")
		}})
		return err
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client, err := engine.NewPeer(ctx, a, engine.ClientRole, engine.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	d, err := dispatch.NewDispatcher(client.Wire())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dispatch.Register(d, []string{"pong"}, dispatch.Handlers{Event: func(ctx context.Context, raw json.RawMessage) error {
		events <- "client:" + string(raw)
		return nil
	}}); err != nil {
		t.Fatal(err)
	}

	var got map[string]json.RawMessage
	space := core.At(client.Wire(), []string{"spaces", "a/b"})
	if err := dispatch.Call(ctx, space, []string{"echo"}, map[string]int{"n": 1}, &got); err != nil {
		t.Fatal(err)
	}
	if string(got["echo"]) != `{"n":1}` {
		t.Fatalf("echo: %s", got["echo"])
	}
	var public *core.PublicError
	if err := dispatch.Call(ctx, client.Wire(), []string{"missing"}, nil, nil); !errors.As(err, &public) || public.Code != "method_not_found" {
		t.Fatalf("missing: %v", err)
	}

	if err := dispatch.Emit(ctx, client.Wire(), []string{"ping"}, 7); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"server:7", `client:"back"`} {
		select {
		case got := <-events:
			if got != want {
				t.Fatalf("event %q, want %q", got, want)
			}
		case <-ctx.Done():
			t.Fatalf("no event %q", want)
		}
	}

	withdrawn, withdraw := context.WithTimeout(ctx, 200*time.Millisecond)
	defer withdraw()
	if err := dispatch.Call(withdrawn, client.Wire(), []string{"wait"}, nil, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("withdrawn: %v", err)
	}
	select {
	case <-cancelled:
	case <-ctx.Done():
		t.Fatal("the cancellation never reached the handler")
	}

	_ = server.Close()
	<-client.Done()
	if err := dispatch.Call(ctx, client.Wire(), []string{"wait"}, nil, nil); !errors.Is(err, transports.ErrClosed) {
		t.Fatalf("after close: %v", err)
	}
}
