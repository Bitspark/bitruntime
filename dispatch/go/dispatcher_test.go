package dispatch_test

import (
	"context"
	"encoding/json"
	"testing"

	core "github.com/Bitspark/bitruntime/core/go"
	dispatch "github.com/Bitspark/bitruntime/dispatch/go"
	transports "github.com/Bitspark/bitruntime/transports/go"
	wire "github.com/Bitspark/bitwire/wire/go"
)

func TestDispatcherSharesOneAttachmentAndPreservesBorrowedEndpoint(t *testing.T) {
	left, right, err := core.NewPair(core.PairOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = left.Close(transports.CodeNormal, "") })
	shared, err := dispatch.NewDispatcher(right)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := right.Receive(wire.Receiver{}); err == nil {
		t.Fatal("second owning attachment accepted")
	}
	for _, name := range []string{"a", "b"} {
		view := shared.Select([]string{name})
		binding, err := dispatch.NewDispatcher(view)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := dispatch.Handle(binding, []string{"read"}, func(context.Context, json.RawMessage) (any, error) { return name, nil }); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"a", "b"} {
		var got string
		if err := dispatch.Call(context.Background(), left, []string{name, "read"}, nil, &got); err != nil || got != name {
			t.Fatalf("%s: %q, %v", name, got, err)
		}
	}
	if err := shared.Close(transports.CodeNormal, ""); err != nil {
		t.Fatal(err)
	}
	rebound, err := dispatch.NewDispatcher(right)
	if err != nil {
		t.Fatalf("borrowed endpoint was closed: %v", err)
	}
	if _, err := dispatch.Handle(rebound, []string{"read"}, func(context.Context, json.RawMessage) (any, error) { return "rebound", nil }); err != nil {
		t.Fatal(err)
	}
	var got string
	if err := dispatch.Call(context.Background(), left, []string{"read"}, nil, &got); err != nil || got != "rebound" {
		t.Fatalf("rebound: %q, %v", got, err)
	}
}

func TestDispatcherClosesAnExplicitlyOwnedEndpoint(t *testing.T) {
	left, right, err := core.NewPair(core.PairOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = left.Close(transports.CodeNormal, "") })
	owner, err := dispatch.NewDispatcher(right, dispatch.DispatcherOptions{OwnEndpoint: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(transports.CodeProtocolError, "wire event rejected"); err != nil {
		t.Fatal(err)
	}
	if _, err := right.Receive(wire.Receiver{}); err == nil {
		t.Fatal("owned endpoint stayed open")
	}
	if err := owner.Close(transports.CodeNormal, "again"); err != nil {
		t.Fatal(err)
	}
}
