package core_test

import (
	"errors"
	"slices"
	"testing"

	core "github.com/Bitspark/bitruntime/core/go"
	wire "github.com/Bitspark/bitwire/wire/go"
)

// Every node has an own value (bitwire decision 0012). A nil own was accepted
// by v0.2.0 and failed only when sent to (bitruntime#15).
func TestComposeRefusesAMissingOwn(t *testing.T) {
	if _, err := core.Compose[wire.Wire](nil, nil); !errors.Is(err, core.ErrInvalidTree) {
		t.Fatalf("a nil own Wire: %v", err)
	}
	var missing *struct{}
	if _, err := core.Compose(missing, nil); !errors.Is(err, core.ErrInvalidTree) {
		t.Fatalf("a nil pointer own: %v", err)
	}
	if _, err := core.Compose[map[string]int](nil, nil); !errors.Is(err, core.ErrInvalidTree) {
		t.Fatalf("a nil map own: %v", err)
	}
	// A zero value that is not nil is a value.
	if _, err := core.Compose(0, nil); err != nil {
		t.Fatalf("a zero own: %v", err)
	}
	if _, err := core.Compose("", nil); err != nil {
		t.Fatalf("an empty string own: %v", err)
	}
}

type recordingAccess struct {
	paths    [][]string
	messages []wire.Message
	refusal  error
}

func (a *recordingAccess) Send(path []string, message wire.Message) error {
	a.paths = append(a.paths, slices.Clone(path))
	a.messages = append(a.messages, message)
	// A callee may reuse the slice it was given.
	for i := range path {
		path[i] = "mutated"
	}
	return a.refusal
}

type returnWire struct{}

func (*returnWire) Send([]string, wire.Message) error { return nil }

func TestBindSendsAtItsFixedPath(t *testing.T) {
	access := &recordingAccess{}
	path := []string{"spaces", "a/b", "", "é"}
	bound := core.Bind(access, path)
	path[0] = "changed"
	back := &returnWire{}
	message := wire.Message{Frame: wire.ProfileFrame{Version: 1, Kind: wire.ProfileEvent}, Return: &wire.ReturnAddress{Wire: back}}
	for range 2 {
		if err := bound.Send(message); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{"spaces", "a/b", "", "é"}
	for i, got := range access.paths {
		if !slices.Equal(got, want) {
			t.Fatalf("send %d went to %q, want %q", i, got, want)
		}
	}
	if access.messages[0].Return != message.Return || access.messages[0].Return.Wire != back {
		t.Fatal("the return capability was not passed unchanged")
	}

	refusal := errors.New("refused")
	access.refusal = refusal
	if err := bound.Send(message); err != refusal {
		t.Fatalf("the refusal came back as %v", err)
	}

	if _, receives := bound.(interface {
		Receive(wire.Receiver) (func(), error)
	}); receives {
		t.Fatal("a bound Wire grants a receive attachment")
	}
	if _, closes := bound.(interface{ Close(wire.Code, string) error }); closes {
		t.Fatal("a bound Wire grants closure")
	}
}
