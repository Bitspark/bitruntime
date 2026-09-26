package runtime_test

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	bitwire "github.com/Bitspark/bitwire/wire/go"
	"github.com/Bitspark/nightseam/duplex/go"
	runtime "github.com/Bitspark/nightseam/runtime/go"
	"github.com/Bitspark/nightseam/tunnel/go"
)

// These assignments cross the actual public package boundary. Independently
// named, structurally similar Message/Receiver/Code types do not satisfy it.
var _ bitwire.Wire = (duplex.Wire)(nil)
var _ duplex.Wire = (bitwire.Wire)(nil)
var _ bitwire.Endpoint = (duplex.Endpoint)(nil)
var _ duplex.Endpoint = (bitwire.Endpoint)(nil)
var _ bitwire.Endpoint = (*tunnel.Channel)(nil)

func TestPublishedBitwireTypesCarryNightseamCalls(t *testing.T) {
	left, right, err := runtime.NewWirePair(runtime.Options{})
	if err != nil {
		t.Fatal(err)
	}
	var client, server bitwire.Endpoint = left, right
	defer client.Close(duplex.CodeNormal, "done")
	detach, err := server.Receive(bitwire.Receiver{
		Message: func(path []string, request bitwire.Message) {
			if !slices.Equal(path, []string{"model", "read"}) {
				t.Errorf("shared endpoint path = %v", path)
			}
			if request.Frame.Kind != bitwire.ProfileRequest || request.Return == nil {
				t.Error("the shared receiver did not receive a request and return capability")
				return
			}
			err := request.Return.Wire.Send(nil, bitwire.Message{Frame: bitwire.ProfileFrame{
				Version: 1, Kind: bitwire.ProfileResponse, ID: request.Frame.ID, Result: request.Frame.Params,
			}})
			if err != nil {
				t.Error(err)
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer detach()
	selected := duplex.At(duplex.Mount(map[string]bitwire.Endpoint{"service": client}), []string{"service", "model"})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var result json.RawMessage
	if err := runtime.CallWire(ctx, selected, []string{"read"}, json.RawMessage(`{"shared":true}`), &result); err != nil {
		t.Fatal(err)
	}
	if string(result) != `{"shared":true}` {
		t.Fatalf("shared contract response: %s", result)
	}
}
