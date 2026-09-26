package dispatch_test

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	core "github.com/Bitspark/bitruntime/core/go"
	dispatch "github.com/Bitspark/bitruntime/dispatch/go"
	engine "github.com/Bitspark/bitruntime/engine/go"
	transports "github.com/Bitspark/bitruntime/transports/go"
	bitwire "github.com/Bitspark/bitwire/wire/go"
)

// These assignments cross the actual public package boundary. bitruntime
// declares no Message, Receiver or Code of its own: what it presents is
// bitwire's types.
var _ bitwire.Endpoint = (*dispatch.SelectedEndpoint)(nil)
var _ dispatch.Registry = (*dispatch.Dispatcher)(nil)
var _ bitwire.AddressedWire = (*dispatch.Dispatcher)(nil)
var _ func(*engine.Peer) bitwire.Endpoint = (*engine.Peer).Wire

func TestPublishedBitwireTypesCarryBitruntimeCalls(t *testing.T) {
	left, right, err := core.NewPair(core.PairOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var client, server bitwire.Endpoint = left, right
	defer client.Close(transports.CodeNormal, "done")
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
	selected := core.At(core.Mount(map[string]bitwire.Endpoint{"service": client}), []string{"service", "model"})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var result json.RawMessage
	if err := dispatch.Call(ctx, selected, []string{"read"}, json.RawMessage(`{"shared":true}`), &result); err != nil {
		t.Fatal(err)
	}
	if string(result) != `{"shared":true}` {
		t.Fatalf("shared contract response: %s", result)
	}
}
