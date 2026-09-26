package core_test

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	core "github.com/Bitspark/bitruntime/core/go"
	wire "github.com/Bitspark/bitwire/wire/go"
)

func mustCompose[T any](t *testing.T, own T, children ...wire.Child[T]) wire.DeixisNode[T] {
	t.Helper()
	tree, err := core.Compose(own, children)
	if err != nil {
		t.Fatal(err)
	}
	return tree
}

func TestStructureIdentityAndDefensiveCopies(t *testing.T) {
	own := &struct{ name string }{"root"}
	leaf := mustCompose(t, &struct{ name string }{"leaf"})
	key := []byte{0, 255}
	children := []wire.Child[*struct{ name string }]{
		{Key: key, Tree: leaf}, {Key: []byte{}, Tree: leaf},
	}
	tree := mustCompose(t, own, children...)
	key[0] = 23
	children[0].Tree = nil
	children[1].Key = []byte("changed")
	for _, path := range []wire.TreePath{{{0, 255}}, {{}}} {
		selected, ok := tree.At(path)
		if !ok || selected != leaf {
			t.Fatal("construction did not retain a copied key and the original child identity")
		}
	}
	if selected, ok := tree.At(nil); !ok || selected != tree {
		t.Fatal("empty path must select self")
	}
	if selected, ok := tree.At(wire.TreePath{{0}}); ok || selected != nil {
		t.Fatal("missing path fabricated a node")
	}
	if selected, ok := tree.At(wire.TreePath{{0, 255}, {}}); ok || selected != nil {
		t.Fatal("partial match fell back to an ancestor")
	}
	partsOwn, partsChildren := tree.Decompose()
	if partsOwn != own || len(partsChildren) != 2 || partsChildren[0].Tree != leaf {
		t.Fatal("decomposition must preserve own and child identity")
	}
	rebuilt := mustCompose(t, partsOwn, partsChildren...)
	partsChildren[0].Key[0] = 14
	partsChildren[0].Tree = nil
	exposed := tree.Children()
	exposed[0].Key[0] = 42
	exposed[1].Tree = nil
	for _, check := range []wire.DeixisNode[*struct{ name string }]{tree, rebuilt} {
		got := check.Children()
		if !bytes.Equal(got[0].Key, []byte{0, 255}) || got[0].Tree != leaf || got[1].Tree != leaf || check.Own() != own {
			t.Fatal("returned children mutated the represented structure")
		}
	}
}

// foreign is an independent node. Compose must retain it, not replace it with a
// local wrapper. Mutation below is used only to supply invalid construction cases.
type foreign[T any] struct {
	own      T
	children []wire.Child[T]
	visits   int
}

func (n *foreign[T]) Own() T { return n.own }
func (n *foreign[T]) Children() []wire.Child[T] {
	n.visits++
	return n.children
}
func (n *foreign[T]) At(path wire.TreePath) (wire.DeixisNode[T], bool) {
	return core.Select[T](n, path)
}
func (n *foreign[T]) Decompose() (T, []wire.Child[T]) { return n.own, n.Children() }

type valueNode struct {
	own      []byte
	children []wire.Child[[]byte]
}

func (n valueNode) Own() []byte                    { return n.own }
func (n valueNode) Children() []wire.Child[[]byte] { return n.children }
func (n valueNode) At(path wire.TreePath) (wire.DeixisNode[[]byte], bool) {
	return core.Select[[]byte](n, path)
}
func (n valueNode) Decompose() ([]byte, []wire.Child[[]byte]) { return n.own, n.children }

func TestConstructionRejectsInvalidStructure(t *testing.T) {
	leaf := mustCompose(t, 1)
	var typedNil *foreign[int]
	cycle := &foreign[int]{own: 1}
	other := &foreign[int]{own: 2, children: []wire.Child[int]{{Key: []byte("back"), Tree: cycle}}}
	cycle.children = []wire.Child[int]{{Key: []byte("next"), Tree: other}}
	duplicate := &foreign[int]{children: []wire.Child[int]{{Tree: leaf}, {Key: []byte{}, Tree: leaf}}}
	cases := []struct {
		name     string
		children []wire.Child[int]
		want     error
	}{
		{"duplicate binary key", []wire.Child[int]{{Key: []byte{255}, Tree: leaf}, {Key: []byte{255}, Tree: leaf}}, core.ErrDuplicateKey},
		{"nil and empty keys are equal", []wire.Child[int]{{Tree: leaf}, {Key: []byte{}, Tree: leaf}}, core.ErrDuplicateKey},
		{"nil child", []wire.Child[int]{{}}, core.ErrInvalidTree},
		{"typed nil child", []wire.Child[int]{{Tree: typedNil}}, core.ErrInvalidTree},
		{"cycle", []wire.Child[int]{{Tree: cycle}}, core.ErrCycle},
		{"external duplicate", []wire.Child[int]{{Tree: duplicate}}, core.ErrDuplicateKey},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got, err := core.Compose(0, tt.children)
			if got != nil || !errors.Is(err, tt.want) {
				t.Fatalf("Compose = %v, %v; want nil, %v", got, err, tt.want)
			}
		})
	}
}

func TestNonComparableValueImplementationIsPreserved(t *testing.T) {
	leaf := valueNode{own: []byte("leaf")}
	branch := valueNode{own: []byte("branch"), children: []wire.Child[[]byte]{{Key: []byte{255}, Tree: leaf}}}
	tree := mustCompose(t, []byte("root"), wire.Child[[]byte]{Key: []byte("value"), Tree: branch})
	selected, ok := tree.At(wire.TreePath{[]byte("value")})
	if !ok {
		t.Fatal("non-comparable node was not retained")
	}
	if _, sameImplementation := selected.(valueNode); !sameImplementation || &selected.Own()[0] != &branch.own[0] {
		t.Fatal("non-comparable child was wrapped or its own payload copied")
	}
	nested, ok := selected.At(wire.TreePath{{255}})
	if !ok || &nested.Own()[0] != &leaf.own[0] {
		t.Fatal("non-comparable descendant selection lost the original payload")
	}
	partsOwn, partsChildren := tree.Decompose()
	rebuilt := mustCompose(t, partsOwn, partsChildren...)
	got, ok := rebuilt.At(wire.TreePath{[]byte("value"), {255}})
	if !ok || &got.Own()[0] != &leaf.own[0] {
		t.Fatal("recomposition failed for a lawful non-comparable implementation")
	}
}

func TestIndependentNodesSharedChildrenAndDeepValidation(t *testing.T) {
	leaf := &foreign[[]byte]{own: []byte("non-comparable payload")}
	tree := mustCompose(t, []byte("root"),
		wire.Child[[]byte]{Key: []byte("a"), Tree: leaf},
		wire.Child[[]byte]{Key: []byte("b"), Tree: leaf})
	if leaf.visits != 1 {
		t.Fatalf("shared foreign child traversed %d times; want once", leaf.visits)
	}
	if a, ok := tree.At(wire.TreePath{[]byte("a")}); !ok || a != leaf {
		t.Fatal("foreign child identity was not retained")
	}
	var deep wire.DeixisNode[int] = &foreign[int]{own: 10000}
	path := make(wire.TreePath, 10000)
	for i := len(path) - 1; i >= 0; i-- {
		deep = &foreign[int]{own: i, children: []wire.Child[int]{{Tree: deep}}}
	}
	root := mustCompose(t, -1, wire.Child[int]{Tree: deep})
	selected, ok := root.At(append(wire.TreePath{{}}, path...))
	if !ok || selected.Own() != 10000 {
		t.Fatal("deep tree selection failed")
	}
}

type recorder struct {
	messages []wire.Message
	refusal  error
}

func (r *recorder) Send(message wire.Message) error {
	if r.refusal != nil {
		return r.refusal
	}
	r.messages = append(r.messages, message)
	return nil
}

func TestSendSelectsExactlyPreservesMessageAndRefusal(t *testing.T) {
	rootWire, childWire := &recorder{}, &recorder{}
	refusal := errors.New("own primitive refused")
	refusing := mustCompose[wire.Wire](t, &recorder{refusal: refusal})
	child := mustCompose[wire.Wire](t, childWire)
	tree := mustCompose[wire.Wire](t, rootWire,
		wire.Child[wire.Wire]{Key: []byte{255}, Tree: child},
		wire.Child[wire.Wire]{Key: []byte("refusing"), Tree: refusing})
	message := wire.Message{
		Frame:  wire.ProfileFrame{Version: 1, Kind: wire.ProfileEvent, Data: []byte(`{"value":9007199254740993}`)},
		Return: &wire.ReturnAddress{Wire: core.AsAddressed(tree)},
	}
	if err := core.Send(tree, wire.TreePath{{255}}, message); err != nil {
		t.Fatal(err)
	}
	if len(rootWire.messages) != 0 || len(childWire.messages) != 1 || !reflect.DeepEqual(childWire.messages[0], message) || childWire.messages[0].Return != message.Return {
		t.Fatal("send did not preserve the selected primitive and complete message")
	}
	if err := core.Send(tree, wire.TreePath{{255}, {}}, message); !errors.Is(err, core.ErrMissingPath) {
		t.Fatalf("missing selection = %v", err)
	}
	if err := core.Send(tree, wire.TreePath{[]byte("refusing")}, message); err != refusal {
		t.Fatal("primitive refusal was not preserved")
	}
	if len(rootWire.messages) != 0 || len(childWire.messages) != 1 {
		t.Fatal("missing descendant fell back to an own primitive")
	}
	if err := core.Send(tree, nil, message); err != nil || len(rootWire.messages) != 1 {
		t.Fatal("empty path did not use root own wire")
	}
	var typedNil *recorder
	if err := core.Send(mustCompose[wire.Wire](t, typedNil), nil, message); !errors.Is(err, core.ErrInvalidWire) {
		t.Fatalf("typed nil primitive = %v", err)
	}
	if err := core.Send(nil, nil, message); !errors.Is(err, core.ErrMissingPath) {
		t.Fatalf("nil tree = %v", err)
	}
}

func TestAddressedBridgeExactUTF8AndValidation(t *testing.T) {
	keys := []string{"", "a/b", ".", "..", "é", "e\u0301", "\x00", "😀"}
	primitives := make([]*recorder, len(keys))
	children := make([]wire.Child[wire.Wire], len(keys))
	for i, key := range keys {
		primitives[i] = &recorder{}
		children[i] = wire.Child[wire.Wire]{Key: []byte(key), Tree: mustCompose[wire.Wire](t, primitives[i])}
	}
	binary := &recorder{}
	children = append(children, wire.Child[wire.Wire]{Key: []byte{255}, Tree: mustCompose[wire.Wire](t, binary)})
	rootWire := &recorder{}
	tree := mustCompose[wire.Wire](t, rootWire, children...)
	access := core.AsAddressed(tree)
	for _, key := range keys {
		if err := access.Send([]string{key}, wire.Message{}); err != nil {
			t.Fatalf("literal key %q: %v", key, err)
		}
	}
	for i, primitive := range primitives {
		if len(primitive.messages) != 1 {
			t.Fatalf("key %q received %d messages", keys[i], len(primitive.messages))
		}
	}
	for _, path := range [][]string{{"a", "b"}, {"absent"}, {"a/b", "absent"}} {
		if err := access.Send(path, wire.Message{}); !errors.Is(err, core.ErrMissingPath) {
			t.Fatalf("missing path %q = %v", path, err)
		}
	}
	for _, segment := range []string{"\xff", "\xed\xa0\x80", "\xc0\x80"} {
		// Invalid path is rejected in full, even beyond an already missing edge.
		if err := access.Send([]string{"absent", segment}, wire.Message{}); !errors.Is(err, core.ErrInvalidPath) {
			t.Fatalf("invalid Unicode path = %v", err)
		}
	}
	if len(binary.messages) != 0 || len(rootWire.messages) != 0 {
		t.Fatal("bridge dispatched a malformed or missing path")
	}
	if err := core.Send(tree, wire.TreePath{{255}}, wire.Message{}); err != nil || len(binary.messages) != 1 {
		t.Fatal("binary key must remain usable through the native tree")
	}
	if err := access.Send(nil, wire.Message{}); err != nil || len(rootWire.messages) != 1 {
		t.Fatal("empty addressed path did not select the root")
	}
}
