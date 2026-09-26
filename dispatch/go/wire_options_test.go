package dispatch_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	core "github.com/Bitspark/bitruntime/core/go"
	dispatch "github.com/Bitspark/bitruntime/dispatch/go"
	engine "github.com/Bitspark/bitruntime/engine/go"
	wire "github.com/Bitspark/bitwire/wire/go"
)

// Nightseam v0.6.0 called the raw method name "8:deadline" here; that is the
// wire name of the path ["deadline"], which the removed raw API no longer
// spells for a caller.
func TestWireForwardingRetainsTheAdmittedDeadline(t *testing.T) {
	client, server := newPair(t, engine.Options{RequestTimeout: time.Minute}, engine.Options{RequestTimeout: time.Minute})
	serverBinding := testBinding(t, server.Wire())
	_, err := dispatch.Handle(serverBinding, []string{"deadline"}, func(ctx context.Context, _ json.RawMessage) (any, error) {
		deadline, ok := ctx.Deadline()
		if !ok {
			return int64(0), nil
		}
		return int64(time.Until(deadline) / time.Millisecond), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var remaining int64
	if err := dispatch.Call(context.Background(), client.Wire(), []string{"deadline"}, nil, &remaining); err != nil {
		t.Fatal(err)
	}
	if remaining < 50000 {
		t.Fatalf("forwarding shortened the admitted one-minute deadline to %dms", remaining)
	}
}

type configuredWirePropagator struct {
	remaining time.Duration
	trace     core.Trace
}

func (p *configuredWirePropagator) Extract(ctx context.Context, _ core.Trace) context.Context {
	return ctx
}
func (p *configuredWirePropagator) Inject(ctx context.Context) core.Trace {
	if deadline, ok := ctx.Deadline(); ok {
		p.remaining = time.Until(deadline)
	}
	return p.trace
}

type optionWire struct {
	send func([]string, wire.Message) error
}

func (w optionWire) Send(path []string, m wire.Message) error { return w.send(path, m) }

func TestWireUsesTheConfiguredOutgoingPropagatorAndTimeout(t *testing.T) {
	want := core.Trace{Parent: "00-11111111111111111111111111111111-2222222222222222-01", State: "vendor=kept"}
	propagator := &configuredWirePropagator{trace: want}
	received := make(chan wire.ProfileFrame, 2)
	access := optionWire{send: func(_ []string, m wire.Message) error {
		received <- m.Frame
		if m.Frame.Kind == wire.ProfileRequest {
			return m.Return.Wire.Send(nil, wire.Message{Frame: wire.ProfileFrame{Version: 1, Kind: wire.ProfileResponse, ID: m.Frame.ID, Result: json.RawMessage(`null`)}})
		}
		return nil
	}}
	if err := dispatch.Call(context.Background(), access, []string{"call"}, nil, nil, dispatch.CallOptions{Propagator: propagator, Timeout: time.Minute}); err != nil {
		t.Fatal(err)
	}
	if propagator.remaining < 50*time.Second {
		t.Fatalf("configured minute shortened to %v", propagator.remaining)
	}
	if err := dispatch.Emit(context.Background(), access, []string{"event"}, nil, dispatch.EmitOptions{Propagator: propagator}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		frame := <-received
		if frame.Traceparent != want.Parent || frame.Tracestate != want.State {
			t.Fatalf("configured trace lost: %+v", frame)
		}
	}
}

func TestWireConfiguredTimeoutCancelsTheSameReturnCapability(t *testing.T) {
	messages := make(chan wire.Message, 2)
	access := optionWire{send: func(_ []string, m wire.Message) error { messages <- m; return nil }}
	started := time.Now()
	err := dispatch.Call(context.Background(), access, []string{"wait"}, nil, nil, dispatch.CallOptions{Timeout: 20 * time.Millisecond})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout result: %v", err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("configured timeout was ignored")
	}
	request, cancel := <-messages, <-messages
	if cancel.Frame.Kind != wire.ProfileCancel || cancel.Return != request.Return || cancel.Frame.ID != request.Frame.ID || cancel.Frame.Traceparent != request.Frame.Traceparent {
		t.Fatal("timeout changed request correlation")
	}
}
