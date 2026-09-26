package dispatch_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	core "github.com/Bitspark/bitruntime/core/go"
	dispatch "github.com/Bitspark/bitruntime/dispatch/go"
	engine "github.com/Bitspark/bitruntime/engine/go"
	wire "github.com/Bitspark/bitwire/wire/go"
)

func TestWireRefusesMalformedFramesBeforeDispatch(t *testing.T) {
	client, server := newPair(t, engine.Options{}, engine.Options{MaxFrameBytes: 256})
	invoked := 0
	serverBinding := testBinding(t, server.Wire())
	_, err := dispatch.Handle(serverBinding, []string{"echo"}, func(_ context.Context, raw json.RawMessage) (any, error) {
		invoked++
		return raw, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sink := &wireReplySink{replies: make(chan wire.ProfileFrame, 30)}
	address := &wire.ReturnAddress{Wire: sink}
	valid := wire.ProfileFrame{Version: 1, Kind: wire.ProfileRequest, ID: "c:1", Params: json.RawMessage("{}")}
	for name, change := range map[string]func(*wire.ProfileFrame){
		"version":           func(f *wire.ProfileFrame) { f.Version = 0 },
		"id":                func(f *wire.ProfileFrame) { f.ID = "unscoped" },
		"zero id":           func(f *wire.ProfileFrame) { f.ID = "c:0" },
		"missing params":    func(f *wire.ProfileFrame) { f.Params = nil },
		"foreign member":    func(f *wire.ProfileFrame) { f.Result = json.RawMessage("null") },
		"trace":             func(f *wire.ProfileFrame) { f.Traceparent = "invalid" },
		"reserved metadata": func(f *wire.ProfileFrame) { f.Meta = map[string]string{"nightseam.future": "value"} },
		"oversize":          func(f *wire.ProfileFrame) { f.Params, _ = json.Marshal(strings.Repeat("x", 300)) },
	} {
		t.Run(name, func(t *testing.T) {
			frame := valid
			change(&frame)
			if err := client.Wire().Send([]string{"echo"}, wire.Message{Frame: frame, Return: address}); err == nil {
				t.Errorf("malformed frame admitted")
			}
		})
	}
	var result string
	if err := dispatch.Call(context.Background(), client.Wire(), []string{"echo"}, "still usable", &result); err != nil || result != "still usable" {
		t.Fatalf("healthy call = %q, %v", result, err)
	}
	if invoked != 1 {
		t.Fatalf("handler invoked %d times, want only the valid call", invoked)
	}
}

func TestWireSanitizesMalformedPublicErrors(t *testing.T) {
	client, server := newPair(t, engine.Options{}, engine.Options{})
	serverBinding := testBinding(t, server.Wire())
	for _, value := range []*core.PublicError{nil, {Code: ""}, {Code: "bad", Message: ""}} {
		detach, err := dispatch.Handle(serverBinding, []string{"fail"}, func(context.Context, json.RawMessage) (any, error) { return nil, value })
		if err != nil {
			t.Fatal(err)
		}
		err = dispatch.Call(context.Background(), client.Wire(), []string{"fail"}, nil, nil)
		var public *core.PublicError
		if !errors.As(err, &public) || public == nil || public.Code != "internal" {
			t.Errorf("malformed public error = %v", err)
		}
		detach()
	}
}
