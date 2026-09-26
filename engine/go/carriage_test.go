package runtime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/Bitspark/nightseam/duplex/go"
)

// The carriage a request and an event may take: what is about the call rather
// than the call. The peer accepts it and keeps it on the decoded frame, and
// sends what WithMeta placed on the sending context — never one of its own.

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
			f, err := decodeFrame([]byte(test.frame))
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
	f, err := decodeFrame([]byte(`{"version":1,"kind":"request","id":"c:1","method":"read","params":{}}`))
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
			if _, err := decodeFrame([]byte(frame)); err == nil {
				t.Fatal("a frame the profile does not admit was accepted")
			}
		})
	}
}

// TestTheConformanceTableIsJudgedAsItJudges: every row of tables/frames.json,
// held the way the peer holds a frame it is handed — the envelope decoded and
// the id held to the prefix its kind carries — so that the two runtimes and
// the suite read one description of the wire, this one.
func TestTheConformanceTableIsJudgedAsItJudges(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "conformance", "tables", "frames.json"))
	if err != nil {
		t.Fatal(err)
	}
	var table struct {
		Rows []struct {
			Name  string
			To    string
			Frame string
			Valid bool
		}
	}
	if err := json.Unmarshal(data, &table); err != nil {
		t.Fatal(err)
	}
	carriages := 0
	for _, row := range table.Rows {
		// A row addressed to the server carries the client's ids and answers
		// the server's; one addressed to either is read as a server's.
		local, remote := "s:", "c:"
		if row.To == "client" {
			local, remote = "c:", "s:"
		}
		f, err := decodeFrame([]byte(row.Frame))
		accepted := err == nil
		if accepted && f.ID != "" {
			prefix := remote
			if f.Kind == "response" {
				prefix = local
			}
			accepted = validID(f.ID, prefix)
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
	if len(table.Rows) < 70 || carriages < 12 {
		t.Fatalf("the table holds %d rows and names meta in %d; the wire is held by more than that", len(table.Rows), carriages)
	}
}

// TestAFrameWithARefusedMetaEndsTheConnection: the refusal is the profile's
// own close, 4011, as any malformed frame is. The peer aborts rather than
// closing with a handshake, so the code this side decided on is what the
// observer is told, and the connection is dead either way.
func TestAFrameWithARefusedMetaEndsTheConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	far, near := duplex.Pipe(1 << 20)
	closes := make(chan ConnectionClosed, 1)
	peer, err := NewPeer(ctx, near, ServerRole, Options{Observer: observerFunc(func(event ObserverEvent) {
		if closed, ok := event.(ConnectionClosed); ok {
			closes <- closed
		}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = peer.Close() }()
	frame := `{"version":1,"kind":"request","id":"c:1","method":"read","params":{},"meta":{"nightseam.cause":"nightly"}}`
	if err := far.Send(ctx, duplex.Frame{Kind: duplex.Text, Data: []byte(frame)}); err != nil {
		t.Fatal(err)
	}
	select {
	case closed := <-closes:
		if closed.Code != int(duplex.CodeDuplex) || !closed.Local {
			t.Fatalf("the connection closed with %d local=%v, want %d local", closed.Code, closed.Local, int(duplex.CodeDuplex))
		}
	case <-ctx.Done():
		t.Fatal("a frame the profile does not admit left the connection open")
	}
	if peer.Err() == nil {
		t.Fatal("the peer ended without an error")
	}
}

// TestMetaTravelsFromTheContextToTheFrame: what WithMeta said reaches the
// request and the event sent from that context, and a context that said
// nothing carries the member nowhere.
func TestMetaTravelsFromTheContextToTheFrame(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	far, near := duplex.Pipe(1 << 20)
	peer, err := NewPeer(ctx, near, ClientRole, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = peer.Close() }()
	carried := Meta{"tenant": "acme", "idempotency": "k-1"}
	// The call waits for a response nobody sends; the frame it sent is the
	// assertion, and the test's own context releases it at the end — cancelling
	// it here would put a cancel frame between the reads below.
	go func() { _ = peer.Call(WithMeta(ctx, carried), "read", nil, nil) }()
	if meta := metaOfNextFrame(ctx, t, far); !reflect.DeepEqual(meta, carried) {
		t.Fatalf("the request carried meta %v, want %v", meta, carried)
	}
	if err := peer.Emit(WithMeta(ctx, Meta{"cause": "nightly"}), "updated", 1); err != nil {
		t.Fatal(err)
	}
	if meta := metaOfNextFrame(ctx, t, far); !reflect.DeepEqual(meta, Meta{"cause": "nightly"}) {
		t.Fatalf("the event carried meta %v", meta)
	}
	// A context that said nothing sends the member nowhere: absent, not empty.
	if err := peer.Emit(ctx, "updated", 1); err != nil {
		t.Fatal(err)
	}
	if meta := metaOfNextFrame(ctx, t, far); meta != nil {
		t.Fatalf("an event from a bare context carried meta %v", meta)
	}
	// A key of the reserved prefix is the profile's; WithMeta drops it rather
	// than sending a frame the far peer would refuse.
	if err := peer.Emit(WithMeta(ctx, Meta{"nightseam.cause": "nightly", "tenant": "acme"}), "updated", 1); err != nil {
		t.Fatal(err)
	}
	if meta := metaOfNextFrame(ctx, t, far); !reflect.DeepEqual(meta, Meta{"tenant": "acme"}) {
		t.Fatalf("a reserved key reached the wire: %v", meta)
	}
}

// metaOfNextFrame is the meta of the next frame the far side of a pipe reads,
// and nil where the frame carried none.
func metaOfNextFrame(ctx context.Context, t *testing.T, conn duplex.Conn) Meta {
	t.Helper()
	frame, err := conn.Receive(ctx)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	var members struct {
		Meta Meta `json:"meta"`
	}
	if err := json.Unmarshal(frame.Data, &members); err != nil {
		t.Fatalf("decode %s: %v", frame.Data, err)
	}
	return members.Meta
}

// TestAHandlerReadsItsMetaAndForwardsNothingOfItself: a carriage reaches the
// handler of the frame that carried it, and goes no further on its own — a
// trace is the peer's to propagate and a credential is not, so a handler that
// means to forward one says WithMeta(ctx, MetaFrom(ctx)).
func TestAHandlerReadsItsMetaAndForwardsNothingOfItself(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clientConn, serverConn := duplex.Pipe(1 << 20)
	nested := make(chan Meta, 1)
	events := make(chan Meta, 1)
	server, err := NewPeer(ctx, serverConn, ServerRole, Options{
		Handlers: map[string]Handler{
			// Reads its own meta, then calls back without saying to forward it.
			"read": func(ctx context.Context, p *Peer, _ json.RawMessage) (any, error) {
				mine := MetaFrom(ctx)
				var back string
				if err := p.Call(ctx, "reverse", nil, &back); err != nil {
					return nil, err
				}
				return mine, nil
			},
			// Reads its own meta, then forwards it as a handler must say to.
			"relay": func(ctx context.Context, p *Peer, _ json.RawMessage) (any, error) {
				var back string
				return back, p.Call(WithMeta(ctx, MetaFrom(ctx)), "reverse", nil, &back)
			},
		},
		Events: map[string]EventHandler{
			"updated": func(ctx context.Context, _ *Peer, _ json.RawMessage) { events <- MetaFrom(ctx) },
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()
	client, err := NewPeer(ctx, clientConn, ClientRole, Options{Handlers: map[string]Handler{
		"reverse": func(ctx context.Context, _ *Peer, _ json.RawMessage) (any, error) {
			nested <- MetaFrom(ctx)
			return "back", nil
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	carried := Meta{"tenant": "acme"}
	var seen Meta
	if err := client.Call(WithMeta(ctx, carried), "read", nil, &seen); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(seen, carried) {
		t.Fatalf("the handler read meta %v, want %v", seen, carried)
	}
	if forwarded := receiveMeta(ctx, t, nested); forwarded != nil {
		t.Fatalf("a call from the handler carried the caller's meta %v of its own accord", forwarded)
	}
	if err := client.Call(WithMeta(ctx, carried), "relay", nil, nil); err != nil {
		t.Fatal(err)
	}
	if forwarded := receiveMeta(ctx, t, nested); !reflect.DeepEqual(forwarded, carried) {
		t.Fatalf("a handler that said to forward carried %v, want %v", forwarded, carried)
	}
	// An event's handler reads its event's carriage across the bounded queue.
	if err := client.Emit(WithMeta(ctx, Meta{"cause": "nightly"}), "updated", 1); err != nil {
		t.Fatal(err)
	}
	if got := receiveMeta(ctx, t, events); !reflect.DeepEqual(got, Meta{"cause": "nightly"}) {
		t.Fatalf("the event handler read meta %v", got)
	}
	// A handler of a frame that carried none reads nil, not an empty carriage.
	if err := client.Emit(ctx, "updated", 1); err != nil {
		t.Fatal(err)
	}
	if got := receiveMeta(ctx, t, events); got != nil {
		t.Fatalf("a handler of a bare event read meta %v", got)
	}
}

func receiveMeta(ctx context.Context, t *testing.T, from <-chan Meta) Meta {
	t.Helper()
	select {
	case meta := <-from:
		return meta
	case <-ctx.Done():
		t.Fatal("nothing arrived")
		return nil
	}
}

// TestMetaFromIsACopy: what a handler writes into what it read reaches no
// frame and no other handler.
func TestMetaFromIsACopy(t *testing.T) {
	carried := Meta{"tenant": "acme"}
	ctx := withIncomingMeta(context.Background(), carried)
	mine := MetaFrom(ctx)
	mine["tenant"] = "other"
	if MetaFrom(ctx)["tenant"] != "acme" {
		t.Fatal("a handler's write reached the frame's carriage")
	}
	if MetaFrom(context.Background()) != nil {
		t.Fatal("a context no frame ran carries a carriage")
	}
}

type observerFunc func(ObserverEvent)

func (f observerFunc) Observe(event ObserverEvent) { f(event) }
