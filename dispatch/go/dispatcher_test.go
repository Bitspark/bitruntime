package runtime_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Bitspark/nightseam/duplex/go"
	ws "github.com/Bitspark/nightseam/runtime/go"
)

func TestDispatcherSharesOneAttachmentAndPreservesBorrowedEndpoint(t *testing.T) {
	left, right, err := ws.NewWirePair(ws.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = left.Close(duplex.CodeNormal, "") })
	dispatch, err := ws.NewDispatcher(right)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := right.Receive(duplex.Receiver{}); err == nil {
		t.Fatal("second owning attachment accepted")
	}
	for _, name := range []string{"a", "b"} {
		view := dispatch.Select([]string{name})
		binding, err := ws.NewDispatcher(view)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ws.HandleWire(binding, []string{"read"}, func(context.Context, json.RawMessage) (any, error) { return name, nil }); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"a", "b"} {
		var got string
		if err := ws.CallWire(context.Background(), left, []string{name, "read"}, nil, &got); err != nil || got != name {
			t.Fatalf("%s: %q, %v", name, got, err)
		}
	}
	if err := dispatch.Close(duplex.CodeNormal, ""); err != nil {
		t.Fatal(err)
	}
	rebound, err := ws.NewDispatcher(right)
	if err != nil {
		t.Fatalf("borrowed endpoint was closed: %v", err)
	}
	if _, err := ws.HandleWire(rebound, []string{"read"}, func(context.Context, json.RawMessage) (any, error) { return "rebound", nil }); err != nil {
		t.Fatal(err)
	}
	var got string
	if err := ws.CallWire(context.Background(), left, []string{"read"}, nil, &got); err != nil || got != "rebound" {
		t.Fatalf("rebound: %q, %v", got, err)
	}
}

func TestDispatcherClosesAnExplicitlyOwnedEndpoint(t *testing.T) {
	left, right, err := ws.NewWirePair(ws.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = left.Close(duplex.CodeNormal, "") })
	dispatch, err := ws.NewDispatcher(right, ws.DispatcherOptions{OwnEndpoint: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := dispatch.Close(duplex.CodeProtocolError, "wire event rejected"); err != nil {
		t.Fatal(err)
	}
	if _, err := right.Receive(duplex.Receiver{}); err == nil {
		t.Fatal("owned endpoint stayed open")
	}
	if err := dispatch.Close(duplex.CodeNormal, "again"); err != nil {
		t.Fatal(err)
	}
}

type unmanagedDispatchEndpoint struct{ receiver duplex.Receiver }

func (*unmanagedDispatchEndpoint) Send([]string, duplex.Message) error { return nil }
func (w *unmanagedDispatchEndpoint) Receive(receiver duplex.Receiver) (func(), error) {
	w.receiver = receiver
	return func() {}, nil
}
func (*unmanagedDispatchEndpoint) Close(duplex.Code, string) error { return nil }
