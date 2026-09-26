package runtime_test

import (
	"context"
	"encoding/json"
	"testing"

	ws "github.com/Bitspark/nightseam/runtime/go"
)

func TestPeerRefusesMalformedOutgoingUnicode(t *testing.T) {
	client, server := newPair(t, ws.Options{}, ws.Options{})
	server.Handle("echo", func(_ context.Context, _ *ws.Peer, raw json.RawMessage) (any, error) { return raw, nil })
	server.Handle("bad", func(context.Context, *ws.Peer, json.RawMessage) (any, error) { return string([]byte{0xff}), nil })
	ctx := context.Background()
	for _, value := range []any{string([]byte{0xff}), map[string]any{"x": string([]byte{0xff})}, json.RawMessage(`"\uD800"`)} {
		if err := client.Emit(ctx, "probe", value); err == nil {
			t.Fatalf("emitted %T", value)
		}
		var result any
		if err := client.Call(ctx, "echo", value, &result); err == nil {
			t.Fatalf("called with %T", value)
		}
	}
	if err := client.Emit(ctx, string([]byte{0xff}), nil); err == nil {
		t.Fatal("emitted malformed event name")
	}
	if err := client.Emit(ws.WithMeta(ctx, map[string]string{"x": string([]byte{0xff})}), "probe", nil); err == nil {
		t.Fatal("emitted malformed metadata")
	}
	var result string
	if err := client.Call(ctx, "bad", nil, &result); err == nil {
		t.Fatal("malformed response was silently replaced")
	}
	if err := client.Call(ctx, "echo", "😀�", &result); err != nil || result != "😀�" {
		t.Fatalf("valid Unicode after refusal: %q, %v", result, err)
	}
}
