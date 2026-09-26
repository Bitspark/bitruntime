package engine_test

// Ported from nightseam v0.6.0 runtime/go/serial_test.go
// (5cc9723a24646c40ed1861f892b2b23eb6d785d7).

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"testing"

	dispatch "github.com/Bitspark/bitruntime/dispatch/go"
	engine "github.com/Bitspark/bitruntime/engine/go"
)

// TestTheSerialTableIsHeldAsItJudges: every row of serials.json, held the way
// the peer holds what arrives — the first frame published, then the request
// that follows it — by a peer of each role, a row addressed to the server
// reaching a client with its identifiers mirrored. The rows name the raw
// method "echo", which no canonical path encodes, so an admitted request is
// answered method_not_found: still its response, as a v0.6.0 peer without
// that handler answered it.
func TestTheSerialTableIsHeldAsItJudges(t *testing.T) {
	rows := readVectors(t, "serials.json")
	for _, role := range []engine.Role{engine.ServerRole, engine.ClientRole} {
		t.Run(string(role), func(t *testing.T) {
			for _, row := range rowsFor(role, rows) {
				t.Run(row.Name, func(t *testing.T) {
					t.Parallel()
					if row.Valid {
						// The first frame the peer sends answers a request.
						peer, raw := rawPeer(t, role, echoing.with(engine.Options{}))
						raw.write(row.Before)
						raw.write(row.Frame)
						if members := raw.read(); string(members["kind"]) != `"response"` {
							t.Fatalf("frame was %s", members["kind"])
						}
						if peer.Err() != nil {
							t.Fatalf("an admissible serial ended the connection: %v", peer.Err())
						}
					}
					holdRow(t, role, row)
				})
			}
		})
	}
}

// Only a request advances the mark. A response answers a serial the receiver
// itself took, and a control names one it already admitted.
func TestOnlyRequestAdmissionAdvancesTheMark(t *testing.T) {
	peer, raw := rawPeer(t, engine.ServerRole, serving{Handlers: map[string]handler{
		"wait": func(ctx context.Context, _ *engine.Peer, _ json.RawMessage) (any, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}}.with(engine.Options{}))
	for _, frame := range []string{
		`{"version":1,"kind":"request","id":"c:4","method":"4:wait","params":null}`,
		`{"version":1,"kind":"cancel","id":"c:4"}`,
		`{"version":1,"kind":"response","id":"s:1","result":null}`,
		`{"version":1,"kind":"request","id":"c:5","method":"4:wait","params":null}`,
	} {
		raw.write(frame)
	}
	if members := raw.read(); string(members["id"]) != `"c:4"` {
		t.Fatalf("first answer was %s", members["id"])
	}
	if peer.Err() != nil {
		t.Fatalf("a control or a response advanced the mark: %v", peer.Err())
	}
}

// Serials are published in the order they were reserved, whatever order the
// callers that took them are scheduled in.
func TestConcurrentCallsPublishSerialsInOrder(t *testing.T) {
	peer, raw := rawPeer(t, engine.ServerRole, engine.Options{})
	calling, withdraw := context.WithCancel(context.Background())
	var wait sync.WaitGroup
	t.Cleanup(func() { withdraw(); wait.Wait() })
	for range 24 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_ = call(calling, peer, "probe", nil, nil)
		}()
	}
	previous := uint64(0)
	for range 24 {
		members := raw.read()
		if string(members["kind"]) != `"request"` {
			continue
		}
		var id string
		if err := json.Unmarshal(members["id"], &id); err != nil {
			t.Fatal(err)
		}
		serial, err := strconv.ParseUint(strings.TrimPrefix(id, "s:"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		if serial <= previous {
			t.Fatalf("published %d after %d", serial, previous)
		}
		previous = serial
	}
}

// Every carrier bridge mints its own serials on its own connection and maps
// replies back: an inner peer's ids are its own, whatever ids arrived.
func TestACarrierBridgeMintsItsOwnSerials(t *testing.T) {
	peer, raw := rawPeer(t, engine.ServerRole, engine.Options{})
	// The peer's root takes a request whose id is the sender's; publishing it
	// onward is the bridge's own request, with a serial of the bridge's.
	root := peer.Wire()
	go func() { _ = dispatch.Call(context.Background(), root, []string{"probe"}, nil, nil) }()
	members := raw.read()
	var id string
	if err := json.Unmarshal(members["id"], &id); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(id, "s:") {
		t.Fatalf("the bridge published %q rather than a serial of its own", id)
	}
	if serial, err := strconv.ParseUint(strings.TrimPrefix(id, "s:"), 10, 64); err != nil || serial == 0 {
		t.Fatalf("the bridge published %q", id)
	}
}
