package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/Bitspark/nightseam/duplex/go"
	ws "github.com/Bitspark/nightseam/runtime/go"
	"testing"
	"time"
)

func TestWireForwardingRetainsTheAdmittedDeadline(t *testing.T) {
	client, server := newPair(t, ws.Options{RequestTimeout: time.Minute}, ws.Options{RequestTimeout: time.Minute})
	serverBinding := testBinding(t, server.Wire())
	_, err := ws.HandleWire(serverBinding, []string{"deadline"}, func(ctx context.Context, _ json.RawMessage) (any, error) {
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
	if err := client.Call(context.Background(), "8:deadline", nil, &remaining); err != nil {
		t.Fatal(err)
	}
	if remaining < 50000 {
		t.Fatalf("forwarding shortened the admitted one-minute deadline to %dms", remaining)
	}
}

type configuredWirePropagator struct {
	remaining time.Duration
	trace     ws.Trace
}

func (p *configuredWirePropagator) Extract(ctx context.Context, _ ws.Trace) context.Context {
	return ctx
}
func (p *configuredWirePropagator) Inject(ctx context.Context) ws.Trace {
	if deadline, ok := ctx.Deadline(); ok {
		p.remaining = time.Until(deadline)
	}
	return p.trace
}

type optionWire struct {
	send func([]string, duplex.Message) error
}

func (w optionWire) Send(path []string, m duplex.Message) error { return w.send(path, m) }

func TestWireUsesTheConfiguredOutgoingPropagatorAndTimeout(t *testing.T) {
	want := ws.Trace{Parent: "00-11111111111111111111111111111111-2222222222222222-01", State: "vendor=kept"}
	propagator := &configuredWirePropagator{trace: want}
	received := make(chan duplex.ProfileFrame, 2)
	wire := optionWire{send: func(_ []string, m duplex.Message) error {
		received <- m.Frame
		if m.Frame.Kind == duplex.ProfileRequest {
			return m.Return.Wire.Send(nil, duplex.Message{Frame: duplex.ProfileFrame{Version: 1, Kind: duplex.ProfileResponse, ID: m.Frame.ID, Result: json.RawMessage(`null`)}})
		}
		return nil
	}}
	if err := ws.CallWire(context.Background(), wire, []string{"call"}, nil, nil, ws.WireCallOptions{Propagator: propagator, RequestTimeout: time.Minute}); err != nil {
		t.Fatal(err)
	}
	if propagator.remaining < 50*time.Second {
		t.Fatalf("configured minute shortened to %v", propagator.remaining)
	}
	if err := ws.EmitWire(context.Background(), wire, []string{"event"}, nil, ws.WireEmitOptions{Propagator: propagator}); err != nil {
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
	messages := make(chan duplex.Message, 2)
	wire := optionWire{send: func(_ []string, m duplex.Message) error { messages <- m; return nil }}
	started := time.Now()
	err := ws.CallWire(context.Background(), wire, []string{"wait"}, nil, nil, ws.WireCallOptions{RequestTimeout: 20 * time.Millisecond})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout result: %v", err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("configured timeout was ignored")
	}
	request, cancel := <-messages, <-messages
	if cancel.Frame.Kind != duplex.ProfileCancel || cancel.Return != request.Return || cancel.Frame.ID != request.Frame.ID || cancel.Frame.Traceparent != request.Frame.Traceparent {
		t.Fatal("timeout changed request correlation")
	}
}
