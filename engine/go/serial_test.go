package runtime_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	ws "github.com/Bitspark/nightseam/runtime/go"
	"github.com/coder/websocket"
)

func echoOptions() ws.Options {
	return ws.Options{Handlers: map[string]ws.Handler{
		"echo": func(_ context.Context, _ *ws.Peer, data json.RawMessage) (any, error) { return data, nil },
	}}
}

// TestTheSerialTableIsHeldAsItJudges: every row of tables/serials.json, held
// the way the peer holds what arrives — the first frame published, then the
// request that follows it — so that the two runtimes and the suite read one
// description of the order, this one.
func TestTheSerialTableIsHeldAsItJudges(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "conformance", "tables", "serials.json"))
	if err != nil {
		t.Fatal(err)
	}
	var table struct {
		Rows []struct {
			Name          string
			Before, Frame string
			Valid         bool
		}
	}
	if err := json.Unmarshal(data, &table); err != nil {
		t.Fatal(err)
	}
	if len(table.Rows) == 0 {
		t.Fatal("the serials table has no rows")
	}
	for _, row := range table.Rows {
		t.Run(row.Name, func(t *testing.T) {
			peer, conn, ctx := rawPeer(t, echoOptions())
			for _, frame := range []string{row.Before, row.Frame} {
				if err := conn.Write(ctx, websocket.MessageText, []byte(frame)); err != nil {
					t.Fatal(err)
				}
			}
			if !row.Valid {
				receive(t, peer.Done())
				if peer.Err() == nil {
					t.Fatal("a serial that did not increase was admitted")
				}
				return
			}
			if members := readFrame(ctx, t, conn); string(members["kind"]) != `"response"` {
				t.Fatalf("frame was %s", members["kind"])
			}
			if peer.Err() != nil {
				t.Fatalf("an admissible serial ended the connection: %v", peer.Err())
			}
		})
	}
}

// Only a request advances the mark. A response answers a serial the receiver
// itself took, and a control names one it already admitted.
func TestOnlyRequestAdmissionAdvancesTheMark(t *testing.T) {
	peer, conn, ctx := rawPeer(t, ws.Options{Handlers: map[string]ws.Handler{
		"wait": func(ctx context.Context, _ *ws.Peer, _ json.RawMessage) (any, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}})
	for _, frame := range []string{
		`{"version":1,"kind":"request","id":"c:4","method":"wait","params":null}`,
		`{"version":1,"kind":"cancel","id":"c:4"}`,
		`{"version":1,"kind":"response","id":"s:1","result":null}`,
		`{"version":1,"kind":"request","id":"c:5","method":"wait","params":null}`,
	} {
		if err := conn.Write(ctx, websocket.MessageText, []byte(frame)); err != nil {
			t.Fatal(err)
		}
	}
	if members := readFrame(ctx, t, conn); string(members["id"]) != `"c:4"` {
		t.Fatalf("first answer was %s", members["id"])
	}
	if peer.Err() != nil {
		t.Fatalf("a control or a response advanced the mark: %v", peer.Err())
	}
}

// Serials are published in the order they were reserved, whatever order the
// callers that took them are scheduled in.
func TestConcurrentCallsPublishSerialsInOrder(t *testing.T) {
	peer, conn, ctx := rawPeer(t, ws.Options{})
	calling, withdraw := context.WithCancel(context.Background())
	var wait sync.WaitGroup
	t.Cleanup(func() { withdraw(); wait.Wait() })
	for range 24 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_ = peer.Call(calling, "probe", nil, nil)
		}()
	}
	previous := uint64(0)
	for range 24 {
		members := readFrame(ctx, t, conn)
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
	peer, conn, ctx := rawPeer(t, ws.Options{})
	// The peer's Wire takes a request whose id is the sender's; publishing it
	// onward is the bridge's own request, with a serial of the bridge's.
	wire := peer.Wire()
	go func() { _ = ws.CallWire(context.Background(), wire, []string{"probe"}, nil, nil) }()
	members := readFrame(ctx, t, conn)
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
