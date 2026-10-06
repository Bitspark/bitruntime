package core_test

import (
	core "github.com/Bitspark/bitruntime/core/go"
	ontos "github.com/Bitspark/bitwire/ontos/go/core"
	wire "github.com/Bitspark/bitwire/wire/go"
	"testing"
	"time"
)

func message() ontos.Value {
	return ontos.NewAtom([]byte{255, 0})
}
func take[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("observation timed out")
		var v T
		return v
	}
}
func TestPairOwnershipAndAdmission(t *testing.T) {
	a, b, err := core.NewPair(core.PairOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	e := message()
	if err = a.Send(e); err != nil {
		t.Fatal(err)
	}
	if err = a.Send(message()); err != nil {
		t.Fatal(err)
	}
	got := make(chan ontos.Value, 4)
	detach, err := b.Receive(func(e ontos.Value) { got <- e })
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.Receive(func(ontos.Value) {}); err == nil {
		t.Fatal("second receiver accepted")
	}
	first, second := take(t, got), take(t, got)
	if !first.Equal(message()) || !first.Equal(second) {
		t.Fatal("opaque value or repeated admission lost")
	}
	detach()
	detach()
	if err = a.Send(message()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-got:
		t.Fatal("detached handler invoked")
	default:
	}
	_, err = b.Receive(func(e ontos.Value) { got <- e })
	if err != nil {
		t.Fatal(err)
	}
	detach()
	take(t, got)
	replies := make(chan ontos.Value, 1)
	a.Receive(func(e ontos.Value) { replies <- e })
	if err = b.Send(message()); err != nil {
		t.Fatal(err)
	}
	take(t, replies)
	a.Close()
	take(t, b.Closed())
	if b.Termination().Kind != "closed" {
		t.Fatal(b.Termination())
	}
	if err = b.Send(message()); err == nil {
		t.Fatal("send after close")
	}
}
func TestPairBoundsAndExecution(t *testing.T) {
	a, b, _ := core.NewPair(core.PairOptions{MaxQueuedMessages: 1})
	a.Send(message())
	if err := a.Send(message()); err == nil {
		t.Fatal("overflow admitted")
	}
	take(t, b.Closed())
	if b.Termination().Kind != "failed" {
		t.Fatal(b.Termination())
	}
	c, d, _ := core.NewPair(core.PairOptions{MaxMessageBytes: 80})
	defer c.Close()
	bad := ontos.NewAtom(make([]byte, 100))
	if err := c.Send(bad); err == nil {
		t.Fatal("oversize admitted")
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	d.Receive(func(ontos.Value) { close(entered); <-release; close(done) })
	c.Send(message())
	take(t, entered)
	c.Close()
	take(t, d.Closed())
	close(release)
	take(t, done)
	e, f, _ := core.NewPair(core.PairOptions{})
	f.Receive(func(ontos.Value) { panic("provider") })
	e.Send(message())
	take(t, e.Closed())
	if f.Termination().Kind != "failed" {
		t.Fatal(f.Termination())
	}
}
func TestByteTrees(t *testing.T) {
	leaf, _ := core.Compose(7, nil)
	key := ontos.NewAtom([]byte("a/b"))
	children := []wire.Child[int]{{Key: key, Node: leaf}, {Key: ontos.NewAtom(nil), Node: leaf}}
	tree, err := core.Compose(3, children)
	if err != nil {
		t.Fatal(err)
	}
	children[0].Node = nil
	if n, ok := tree.At(wire.Path{key}); !ok || n != leaf {
		t.Fatal("child identity lost")
	}
	if n, ok := tree.At(nil); !ok || n != tree {
		t.Fatal("self selection")
	}
	if _, ok := tree.At(wire.Path{ontos.NewAtom([]byte("a")), ontos.NewAtom([]byte("b"))}); ok {
		t.Fatal("path parsed")
	}
	copy := tree.Children()
	copy[0].Node = nil
	if tree.Children()[0].Node == nil {
		t.Fatal("children mutable")
	}
	if _, err = core.Compose(0, []wire.Child[int]{{Key: key, Node: leaf}, {Key: key, Node: leaf}}); err == nil {
		t.Fatal("duplicate key")
	}
}

type foreignNode []wire.Child[int]

func (n foreignNode) Own() int                                  { return 0 }
func (n foreignNode) Children() []wire.Child[int]               { return n }
func (n foreignNode) At(wire.Path) (wire.DeixisNode[int], bool) { return n, true }
func (n foreignNode) Decompose() wire.Parts[int]                { return wire.Parts[int]{Children: n} }
func TestForeignTreeValidation(t *testing.T) {
	leaf := foreignNode{}
	key := ontos.NewAtom(nil)
	if _, err := core.Compose(0, []wire.Child[int]{{Key: key, Node: leaf}}); err != nil {
		t.Fatal(err)
	}
	invalid := foreignNode{{Key: key, Node: leaf}, {Key: key, Node: leaf}}
	if _, err := core.Compose(0, []wire.Child[int]{{Key: key, Node: invalid}}); err == nil {
		t.Fatal("duplicate descendant keys")
	}
	var nilNode *core.Node[int]
	if _, err := core.Compose(0, []wire.Child[int]{{Key: key, Node: nilNode}}); err == nil {
		t.Fatal("typed nil descendant")
	}
}
