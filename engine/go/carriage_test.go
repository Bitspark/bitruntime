package engine_test

// Ported from nightseam v0.6.0 runtime/go/carriage_test.go
// (5cc9723a24646c40ed1861f892b2b23eb6d785d7). The carriage a request and an
// event may take: what is about the call rather than the call. The peer
// accepts it and keeps it on the decoded frame, and sends what WithMeta placed
// on the sending context — never one of its own.

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	core "github.com/Bitspark/bitruntime/core/go"
	engine "github.com/Bitspark/bitruntime/engine/go"
	"github.com/Bitspark/bitruntime/internal/delivery/go"
	"github.com/Bitspark/bitruntime/internal/profile/go"
	transports "github.com/Bitspark/bitruntime/transports/go"
)

// TestMetaIsKeptOnTheDecodedFrame: a frame of each kind that may carry meta
// decodes, and the member reaches the frame verbatim rather than being read
// and dropped.
func TestMetaIsKeptOnTheDecodedFrame(t *testing.T) {
	for _, test := range []struct {
		name  string
		frame string
		meta  map[string]string
	}{
		{"request", `{"version":1,"kind":"request","id":"c:1","method":"read","params":{},"meta":{"tenant":"acme","idempotency":"k-1"}}`,
			map[string]string{"tenant": "acme", "idempotency": "k-1"}},
		{"request with an empty carriage", `{"version":1,"kind":"request","id":"c:1","method":"read","params":{},"meta":{}}`,
			map[string]string{}},
		{"event", `{"version":1,"kind":"event","event":"updated","data":1,"meta":{"cause":"nightly"}}`,
			map[string]string{"cause": "nightly"}},
		{"event beside a trace", `{"version":1,"kind":"event","event":"updated","data":1,"meta":{"tenant":"acme"},` +
			`"traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}`,
			map[string]string{"tenant": "acme"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, err := profile.Decode([]byte(test.frame))
			if err != nil {
				t.Fatalf("a frame carrying meta was refused: %v", err)
			}
			if !reflect.DeepEqual(f.Meta, test.meta) {
				t.Fatalf("meta = %v, want %v", f.Meta, test.meta)
			}
		})
	}
	// A frame carrying none leaves the member absent rather than empty, so the
	// emitting half can tell a carriage with nothing in it from no carriage.
	f, err := profile.Decode([]byte(`{"version":1,"kind":"request","id":"c:1","method":"read","params":{}}`))
	if err != nil || f.Meta != nil {
		t.Fatalf("a frame carrying no meta = %v, %v", f.Meta, err)
	}
}

// TestMetaIsRefusedInEveryOtherForm: the kinds that may not carry it, the
// forms that are not an object of strings, and the keys the profile keeps.
func TestMetaIsRefusedInEveryOtherForm(t *testing.T) {
	for _, frame := range []string{
		// A response says what it says in its result; a cancel withdraws a call
		// rather than making one.
		`{"version":1,"kind":"response","id":"s:1","result":1,"meta":{"tenant":"acme"}}`,
		`{"version":1,"kind":"response","id":"s:1","error":{"code":"busy","message":"Try later"},"meta":{"tenant":"acme"}}`,
		`{"version":1,"kind":"cancel","id":"c:1","meta":{"tenant":"acme"}}`,
		// An object of strings, and nothing else.
		`{"version":1,"kind":"request","id":"c:1","method":"read","params":{},"meta":"acme"}`,
		`{"version":1,"kind":"request","id":"c:1","method":"read","params":{},"meta":["acme"]}`,
		`{"version":1,"kind":"request","id":"c:1","method":"read","params":{},"meta":7}`,
		`{"version":1,"kind":"request","id":"c:1","method":"read","params":{},"meta":null}`,
		`{"version":1,"kind":"request","id":"c:1","method":"read","params":{},"meta":{"attempt":2}}`,
		`{"version":1,"kind":"request","id":"c:1","method":"read","params":{},"meta":{"tenant":null}}`,
		`{"version":1,"kind":"event","event":"updated","data":1,"meta":{"live":true}}`,
		`{"version":1,"kind":"event","event":"updated","data":1,"meta":{"who":{"id":"u1"}}}`,
		// The namespace the profile keeps for itself, which it fills with
		// nothing in this version.
		`{"version":1,"kind":"request","id":"c:1","method":"read","params":{},"meta":{"nightseam.deadline":"2026-01-01T00:00:00Z"}}`,
		`{"version":1,"kind":"event","event":"updated","data":1,"meta":{"nightseam.cause":"nightly"}}`,
	} {
		t.Run(frame, func(t *testing.T) {
			if _, err := profile.Decode([]byte(frame)); err == nil {
				t.Fatal("a frame the profile does not admit was accepted")
			}
		})
	}
}

// vectorRow is one row of a bitwire/1 vector table: a frame, the frame before
// it where the table is of sequences, the role it is addressed to, and
// whether a peer of that role admits it.
type vectorRow struct {
	Name          string
	To            string
	Before, Frame string
	Valid         bool
}

func readVectors(t *testing.T, name string) []vectorRow {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "vectors", "bitwire-1", name))
	if err != nil {
		t.Fatal(err)
	}
	var table struct{ Rows []vectorRow }
	if err := json.Unmarshal(data, &table); err != nil {
		t.Fatal(err)
	}
	if len(table.Rows) == 0 {
		t.Fatalf("%s has no rows", name)
	}
	return table.Rows
}

// mirrored is a row addressed to a server spelled for a client: a request
// identifier's prefix names its sender's role, so the two prefixes swap and
// nothing else in the frame changes. A row addressed to either role is sent
// to both as it is.
var mirrored = strings.NewReplacer(`"id":"c:`, `"id":"s:`, `"id":"s:`, `"id":"c:`)

// rowsFor is every row as a peer of role receives it.
func rowsFor(role engine.Role, rows []vectorRow) []vectorRow {
	held := make([]vectorRow, 0, len(rows))
	for _, row := range rows {
		if row.To != "server" && row.To != "client" && row.To != "either" {
			panic("a vector row addressed to " + row.To)
		}
		if row.To != "either" && row.To != string(role) {
			row.Before, row.Frame = mirrored.Replace(row.Before), mirrored.Replace(row.Frame)
		}
		held = append(held, row)
	}
	return held
}

var echoing = serving{Handlers: map[string]handler{
	"echo": func(_ context.Context, _ *engine.Peer, data json.RawMessage) (any, error) { return data, nil },
}}

// holdRow sends a row's frames, as raw text over a pipe, to a fresh peer of
// role and holds that the peer ended with 4011 where the row is refused, or
// served on where it is admitted: a request sent after the row, with a serial
// above every row's, is answered.
func holdRow(t *testing.T, role engine.Role, row vectorRow) {
	t.Helper()
	peer, raw := rawPeer(t, role, echoing.with(engine.Options{}))
	for _, frame := range []string{row.Before, row.Frame} {
		if frame != "" {
			raw.write(frame)
		}
	}
	if !row.Valid {
		if closed := raw.closed(); closed.Code != transports.CodeProtocol {
			t.Fatalf("a refused frame ended the connection with %d %q, not 4011", int(closed.Code), closed.Reason)
		}
		receive(t, peer.Done())
		if err := peer.Err(); !errors.Is(err, transports.ErrClosed) {
			t.Fatalf("the peer ended with %v", err)
		}
		return
	}
	probe := `"c:1000000"`
	if role == engine.ClientRole {
		probe = `"s:1000000"`
	}
	raw.write(`{"version":1,"kind":"request","id":` + probe + `,"method":"4:echo","params":"served"}`)
	if response := raw.readUntil(member("id=" + probe)); string(response["result"]) != `"served"` {
		t.Fatalf("the request after an admitted frame was answered %v", response)
	}
	if err := peer.Err(); err != nil {
		t.Fatalf("an admitted frame ended the connection: %v", err)
	}
}

// TestTheConformanceTableIsJudgedAsItJudges: every row of frames.json, held
// the way the peer holds a frame it is handed — the envelope decoded and the
// id held to the prefix its kind carries — so that the two runtimes and the
// suite read one description of the wire, this one.
func TestTheConformanceTableIsJudgedAsItJudges(t *testing.T) {
	rows := readVectors(t, "frames.json")
	carriages := 0
	for _, row := range rows {
		// A row addressed to the server carries the client's ids and answers
		// the server's; one addressed to either is read as a server's.
		local, remote := "s:", "c:"
		if row.To == "client" {
			local, remote = "c:", "s:"
		}
		f, err := profile.Decode([]byte(row.Frame))
		accepted := err == nil
		if accepted && f.ID != "" {
			prefix := remote
			if f.Kind == "response" {
				prefix = local
			}
			accepted = profile.ValidID(f.ID, prefix)
		}
		if accepted != row.Valid {
			t.Errorf("%s: valid=%v, decode error %v", row.Name, row.Valid, err)
		}
		var members map[string]json.RawMessage
		if json.Unmarshal([]byte(row.Frame), &members) == nil {
			if _, carried := members["meta"]; carried {
				carriages++
			}
		}
	}
	if len(rows) < 70 || carriages < 12 {
		t.Fatalf("the table holds %d rows and names meta in %d; the wire is held by more than that", len(rows), carriages)
	}
}

// TestEveryConformanceRowIsHeldByAPeerOfEachRole: every row of frames.json
// sent to a running peer, as the table's harness describes, of each role —
// a row addressed to the server reaches a client with its identifiers
// mirrored — so that what the envelope decoder judges is what the connection
// does: a refused frame ends it with 4011, an admitted one does not.
func TestEveryConformanceRowIsHeldByAPeerOfEachRole(t *testing.T) {
	rows := readVectors(t, "frames.json")
	for _, role := range []engine.Role{engine.ServerRole, engine.ClientRole} {
		t.Run(string(role), func(t *testing.T) {
			for _, row := range rowsFor(role, rows) {
				t.Run(row.Name, func(t *testing.T) {
					t.Parallel()
					holdRow(t, role, row)
				})
			}
		})
	}
}

// TestAFrameWithARefusedMetaEndsTheConnection: the refusal is the profile's
// own close, 4011, as any malformed frame is. v0.6.0 held what its observer
// was told; observers are removed, and the far side reads the code instead.
func TestAFrameWithARefusedMetaEndsTheConnection(t *testing.T) {
	peer, raw := rawPeer(t, engine.ServerRole, engine.Options{})
	raw.write(`{"version":1,"kind":"request","id":"c:1","method":"read","params":{},"meta":{"nightseam.cause":"nightly"}}`)
	if closed := raw.closed(); closed.Code != transports.CodeProtocol {
		t.Fatalf("the connection closed with %d %q, want 4011", int(closed.Code), closed.Reason)
	}
	receive(t, peer.Done())
	if peer.Err() == nil {
		t.Fatal("the peer ended without an error")
	}
}

// TestMetaTravelsFromTheContextToTheFrame: what WithMeta said reaches the
// request and the event sent from that context, and a context that said
// nothing carries the member nowhere.
func TestMetaTravelsFromTheContextToTheFrame(t *testing.T) {
	peer, raw := rawPeer(t, engine.ClientRole, engine.Options{})
	ctx := raw.ctx
	carried := core.Meta{"tenant": "acme", "idempotency": "k-1"}
	// The call waits for a response nobody sends; the frame it sent is the
	// assertion, and the test's own context releases it at the end — cancelling
	// it here would put a cancel frame between the reads below.
	go func() { _ = call(core.WithMeta(ctx, carried), peer, "read", nil, nil) }()
	if meta := metaOf(t, raw.read()); !reflect.DeepEqual(meta, carried) {
		t.Fatalf("the request carried meta %v, want %v", meta, carried)
	}
	if err := emit(core.WithMeta(ctx, core.Meta{"cause": "nightly"}), peer, "updated", 1); err != nil {
		t.Fatal(err)
	}
	if meta := metaOf(t, raw.read()); !reflect.DeepEqual(meta, core.Meta{"cause": "nightly"}) {
		t.Fatalf("the event carried meta %v", meta)
	}
	// A context that said nothing sends the member nowhere: absent, not empty.
	if err := emit(ctx, peer, "updated", 1); err != nil {
		t.Fatal(err)
	}
	if meta := metaOf(t, raw.read()); meta != nil {
		t.Fatalf("an event from a bare context carried meta %v", meta)
	}
	// A key of the reserved prefix is the profile's; WithMeta drops it rather
	// than sending a frame the far peer would refuse.
	if err := emit(core.WithMeta(ctx, core.Meta{"nightseam.cause": "nightly", "tenant": "acme"}), peer, "updated", 1); err != nil {
		t.Fatal(err)
	}
	if meta := metaOf(t, raw.read()); !reflect.DeepEqual(meta, core.Meta{"tenant": "acme"}) {
		t.Fatalf("a reserved key reached the wire: %v", meta)
	}
}

// metaOf is the meta a frame carried, and nil where it carried none.
func metaOf(t *testing.T, members map[string]json.RawMessage) core.Meta {
	t.Helper()
	raw, carried := members["meta"]
	if !carried {
		return nil
	}
	var meta core.Meta
	if err := json.Unmarshal(raw, &meta); err != nil {
		t.Fatalf("decode meta %s: %v", raw, err)
	}
	return meta
}

// TestAHandlerReadsItsMetaAndForwardsNothingOfItself: a carriage reaches the
// handler of the frame that carried it, and goes no further on its own — a
// trace is the peer's to propagate and a credential is not, so a handler that
// means to forward one says WithMeta(ctx, MetaFrom(ctx)).
func TestAHandlerReadsItsMetaAndForwardsNothingOfItself(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	nested := make(chan core.Meta, 1)
	events := make(chan core.Meta, 1)
	client, _ := newPair(t, serving{
		Handlers: map[string]handler{
			// Reads its own meta, then calls back without saying to forward it.
			"read": func(ctx context.Context, p *engine.Peer, _ json.RawMessage) (any, error) {
				mine := core.MetaFrom(ctx)
				var back string
				if err := call(ctx, p, "reverse", nil, &back); err != nil {
					return nil, err
				}
				return mine, nil
			},
			// Reads its own meta, then forwards it as a handler must say to.
			"relay": func(ctx context.Context, p *engine.Peer, _ json.RawMessage) (any, error) {
				var back string
				return back, call(core.WithMeta(ctx, core.MetaFrom(ctx)), p, "reverse", nil, &back)
			},
		},
		Events: map[string]eventHandler{
			"updated": func(ctx context.Context, _ *engine.Peer, _ json.RawMessage) { events <- core.MetaFrom(ctx) },
		},
	}.with(engine.Options{}), serving{Handlers: map[string]handler{
		"reverse": func(ctx context.Context, _ *engine.Peer, _ json.RawMessage) (any, error) {
			nested <- core.MetaFrom(ctx)
			return "back", nil
		},
	}}.with(engine.Options{}))

	carried := core.Meta{"tenant": "acme"}
	var seen core.Meta
	if err := call(core.WithMeta(ctx, carried), client, "read", nil, &seen); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(seen, carried) {
		t.Fatalf("the handler read meta %v, want %v", seen, carried)
	}
	if forwarded := receive(t, nested); forwarded != nil {
		t.Fatalf("a call from the handler carried the caller's meta %v of its own accord", forwarded)
	}
	if err := call(core.WithMeta(ctx, carried), client, "relay", nil, nil); err != nil {
		t.Fatal(err)
	}
	if forwarded := receive(t, nested); !reflect.DeepEqual(forwarded, carried) {
		t.Fatalf("a handler that said to forward carried %v, want %v", forwarded, carried)
	}
	// An event's handler reads its event's carriage across the bounded queue.
	if err := emit(core.WithMeta(ctx, core.Meta{"cause": "nightly"}), client, "updated", 1); err != nil {
		t.Fatal(err)
	}
	if got := receive(t, events); !reflect.DeepEqual(got, core.Meta{"cause": "nightly"}) {
		t.Fatalf("the event handler read meta %v", got)
	}
	// A handler of a frame that carried none reads nil, not an empty carriage.
	if err := emit(ctx, client, "updated", 1); err != nil {
		t.Fatal(err)
	}
	if got := receive(t, events); got != nil {
		t.Fatalf("a handler of a bare event read meta %v", got)
	}
}

// TestMetaFromIsACopy: what a handler writes into what it read reaches no
// frame and no other handler.
func TestMetaFromIsACopy(t *testing.T) {
	carried := core.Meta{"tenant": "acme"}
	ctx := delivery.WithIncomingMeta(context.Background(), carried)
	mine := core.MetaFrom(ctx)
	mine["tenant"] = "other"
	if core.MetaFrom(ctx)["tenant"] != "acme" {
		t.Fatal("a handler's write reached the frame's carriage")
	}
	if core.MetaFrom(context.Background()) != nil {
		t.Fatal("a context no frame ran carries a carriage")
	}
}
