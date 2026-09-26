package engine_test

// Ported from nightseam v0.6.0 runtime/go/unicode_peer_test.go
// (5cc9723a24646c40ed1861f892b2b23eb6d785d7).

import (
	"context"
	"encoding/json"
	"testing"

	core "github.com/Bitspark/bitruntime/core/go"
	engine "github.com/Bitspark/bitruntime/engine/go"
)

func TestPeerRefusesMalformedOutgoingUnicode(t *testing.T) {
	client, _ := newPair(t, serving{Handlers: map[string]handler{
		"echo": func(_ context.Context, _ *engine.Peer, raw json.RawMessage) (any, error) { return raw, nil },
		"bad":  func(context.Context, *engine.Peer, json.RawMessage) (any, error) { return string([]byte{0xff}), nil },
	}}.with(engine.Options{}), engine.Options{})
	ctx := context.Background()
	for _, value := range []any{string([]byte{0xff}), map[string]any{"x": string([]byte{0xff})}, json.RawMessage(`"\uD800"`)} {
		if err := emit(ctx, client, "probe", value); err == nil {
			t.Fatalf("emitted %T", value)
		}
		var result any
		if err := call(ctx, client, "echo", value, &result); err == nil {
			t.Fatalf("called with %T", value)
		}
	}
	if err := emit(ctx, client, string([]byte{0xff}), nil); err == nil {
		t.Fatal("emitted malformed event name")
	}
	if err := emit(core.WithMeta(ctx, map[string]string{"x": string([]byte{0xff})}), client, "probe", nil); err == nil {
		t.Fatal("emitted malformed metadata")
	}
	var result string
	if err := call(ctx, client, "bad", nil, &result); err == nil {
		t.Fatal("malformed response was silently replaced")
	}
	if err := call(ctx, client, "echo", "😀�", &result); err != nil || result != "😀�" {
		t.Fatalf("valid Unicode after refusal: %q, %v", result, err)
	}
}
