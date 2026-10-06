package core_test

import (
	"errors"
	core "github.com/Bitspark/bitruntime/core/go"
	ontos "github.com/Bitspark/bitwire/ontos/go/core"
	wire "github.com/Bitspark/bitwire/wire/go"
	"testing"
)

type sender func(ontos.Value) error

func (s sender) Send(v ontos.Value) error { return s(v) }

type addressedSender func(wire.Path, ontos.Value) error

func (s addressedSender) Send(p wire.Path, v ontos.Value) error { return s(p, v) }

func TestAddressedOwnershipAndInterpretation(t *testing.T) {
	a, b, _ := core.NewPair(core.PairOptions{})
	defer a.Close()
	left, right := core.Addressed(a), core.Addressed(b)
	probe, err := b.Receive(func(ontos.Value) {})
	if err != nil {
		t.Fatal("construction took receive ownership", err)
	}
	probe()
	if left.Closed() != a.Closed() || right.Closed() != b.Closed() {
		t.Fatal("new lifetime")
	}
	type observation struct {
		path  wire.Path
		value ontos.Value
	}
	got := make(chan observation, 3)
	detach, err := right.Receive(func(p wire.Path, v ontos.Value) { got <- observation{p, v} })
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.Receive(func(ontos.Value) {}); err == nil {
		t.Fatal("overlapping receive owners")
	}
	keys := wire.Path{ontos.NewAtom([]byte{255, 47}), ontos.NewAtom(nil)}
	value := ontos.NewTuple(ontos.NewAtom([]byte{128}), ontos.NewTuple())
	if err = left.Send(keys, value); err != nil {
		t.Fatal(err)
	}
	keys[0] = ontos.NewAtom([]byte{0})
	left.Send(nil, ontos.NewAtom(nil))
	left.Send(wire.Path{ontos.NewAtom(nil)}, ontos.NewAtom(nil))
	first, second, third := take(t, got), take(t, got), take(t, got)
	if len(first.path) != 2 || !first.path[0].Equal(ontos.NewAtom([]byte{255, 47})) || !first.path[1].Equal(ontos.NewAtom(nil)) || !first.value.Equal(value) {
		t.Fatal("path capture or message changed")
	}
	if len(second.path) != 0 || len(third.path) != 1 || !third.path[0].Equal(ontos.NewAtom(nil)) {
		t.Fatal("self and empty child merged")
	}
	detach()
	detach()
	left.Close()
	take(t, b.Closed())
	if err = left.Send(nil, value); err == nil {
		t.Fatal("admission after close")
	}
	c, d, _ := core.NewPair(core.PairOptions{})
	defer c.Close()
	core.Addressed(d)
	c.Send(ontos.NewTuple())
	core.Addressed(d).Receive(func(wire.Path, ontos.Value) { t.Error("malformed address dispatched") })
	take(t, d.Closed())
	if d.Termination().Kind != "failed" {
		t.Fatal("invalid addressed value did not fail interpretation")
	}
}

func TestPrefixCutsAndSendCapability(t *testing.T) {
	var seen []wire.Path
	target := addressedSender(func(p wire.Path, _ ontos.Value) error { seen = append(seen, p); return nil })
	p, q := wire.Path{ontos.NewAtom([]byte{255})}, wire.Path{ontos.NewAtom(nil)}
	selected := core.Bind(core.Under(target, p), q)
	p[0] = ontos.NewAtom([]byte{1})
	q[0] = ontos.NewAtom([]byte{2})
	if _, ok := selected.(wire.Endpoint); ok {
		t.Fatal("binding exposed endpoint ownership")
	}
	if len(seen) != 0 {
		t.Fatal("construction sent")
	}
	selected.Send(message())
	core.Under(core.Under(target, wire.Path{ontos.NewAtom([]byte{255})}), wire.Path{ontos.NewAtom(nil)}).Send(nil, message())
	target.Send(wire.Path{ontos.NewAtom([]byte{255}), ontos.NewAtom(nil)}, message())
	for _, path := range seen {
		if len(path) != 2 || !path[0].Equal(ontos.NewAtom([]byte{255})) || !path[1].Equal(ontos.NewAtom(nil)) {
			t.Fatal("prefix cut changed target")
		}
	}
	if len(seen) != 3 {
		t.Fatal("invocation count")
	}
}

func TestSenderTreeSelectionAndRefusal(t *testing.T) {
	calls := []string{}
	refused := errors.New("refused")
	var own wire.Wire = sender(func(ontos.Value) error { calls = append(calls, "root"); return nil })
	var child wire.Wire = sender(func(ontos.Value) error { calls = append(calls, "child"); return nil })
	var refusing wire.Wire = sender(func(ontos.Value) error { return refused })
	leaf, _ := core.Compose(child, nil)
	node, _ := core.Compose(refusing, []wire.Child[wire.Wire]{{Key: ontos.NewAtom([]byte{255}), Node: leaf}})
	tree, _ := core.Compose(own, []wire.Child[wire.Wire]{{Key: ontos.NewAtom(nil), Node: node}})
	parts := tree.Decompose()
	rebuilt, _ := core.Compose(parts.Own, parts.Children)
	if len(calls) != 0 {
		t.Fatal("structural operation invoked sender")
	}
	if selected, _ := rebuilt.At(wire.Path{ontos.NewAtom(nil)}); selected != node {
		t.Fatal("reconstruction changed child identity")
	}
	core.AsAddressed(rebuilt).Send(nil, message())
	core.AsAddressed(node).Send(wire.Path{ontos.NewAtom([]byte{255})}, message())
	core.AsAddressed(tree).Send(wire.Path{ontos.NewAtom(nil), ontos.NewAtom([]byte{255})}, message())
	if len(calls) != 3 || calls[0] != "root" || calls[1] != "child" || calls[2] != "child" {
		t.Fatal(calls)
	}
	if err := core.AsAddressed(tree).Send(wire.Path{ontos.NewAtom([]byte{1})}, message()); !errors.Is(err, core.ErrMissingPath) {
		t.Fatal("missing path", err)
	}
	if err := core.AsAddressed(tree).Send(wire.Path{ontos.NewAtom(nil)}, message()); err != refused {
		t.Fatal("refusal confused with absence", err)
	}
}
