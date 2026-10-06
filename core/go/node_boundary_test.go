package core_test

import (
	"reflect"
	"testing"

	core "github.com/Bitspark/bitruntime/core/go"
	ontos "github.com/Bitspark/bitwire/ontos/go/core"
	wire "github.com/Bitspark/bitwire/wire/go"
	deixis "github.com/bitspark/deixis/core/go"
)

type boundarySlot struct {
	name  string
	count int
}
type boundaryFixture struct {
	selectCut func([][]byte, int) *boundarySlot
	slots     map[string]*boundarySlot
}

func nodeBoundaryFixture(t *testing.T, bytesKeys bool) boundaryFixture {
	t.Helper()
	slots := map[string]*boundarySlot{}
	for _, name := range []string{"shared", "empty", "binary", "slash"} {
		slots[name] = &boundarySlot{name: name}
	}
	key := []byte{255, 0, 47}
	if bytesKeys {
		makeNode := func(own *boundarySlot, children []deixis.Child[*boundarySlot]) deixis.DeixisNode[*boundarySlot] {
			n, err := deixis.ComposeTree(own, children)
			if err != nil {
				t.Fatal(err)
			}
			return n
		}
		alias := makeNode(slots["shared"], nil)
		branch := makeNode(slots["binary"], []deixis.Child[*boundarySlot]{{Key: nil, Tree: alias}})
		root := makeNode(slots["shared"], []deixis.Child[*boundarySlot]{
			{Key: key, Tree: branch}, {Key: nil, Tree: makeNode(slots["empty"], nil)},
			{Key: []byte{97, 47, 98}, Tree: makeNode(slots["slash"], nil)},
		})
		own, children := root.Decompose()
		root = makeNode(own, children)
		if own != slots["shared"] || len(root.Children()) != 3 {
			t.Fatal("incomplete decomposition")
		}
		key[0] = 9
		for _, child := range root.Children() {
			for i := range child.Key {
				child.Key[i] = 8
			}
		}
		return boundaryFixture{slots: slots, selectCut: func(path [][]byte, cut int) *boundarySlot {
			selected, ok := root.At(path[:cut])
			if !ok {
				return nil
			}
			selected, ok = selected.At(path[cut:])
			if !ok {
				return nil
			}
			return selected.Own()
		}}
	}
	makeNode := func(own *boundarySlot, children []wire.Child[*boundarySlot]) wire.DeixisNode[*boundarySlot] {
		n, err := core.Compose(own, children)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	alias := makeNode(slots["shared"], nil)
	branch := makeNode(slots["binary"], []wire.Child[*boundarySlot]{{Key: ontos.NewAtom(nil), Node: alias}})
	root := makeNode(slots["shared"], []wire.Child[*boundarySlot]{
		{Key: ontos.NewAtom(key), Node: branch}, {Key: ontos.NewAtom(nil), Node: makeNode(slots["empty"], nil)},
		{Key: ontos.NewAtom([]byte{97, 47, 98}), Node: makeNode(slots["slash"], nil)},
	})
	parts := root.Decompose()
	root = makeNode(parts.Own, parts.Children)
	if parts.Own != slots["shared"] || len(root.Children()) != 3 {
		t.Fatal("incomplete decomposition")
	}
	key[0] = 9
	for _, child := range root.Children() {
		b := child.Key.Bytes()
		for i := range b {
			b[i] = 8
		}
	}
	return boundaryFixture{slots: slots, selectCut: func(path [][]byte, cut int) *boundarySlot {
		keys := make(wire.Path, len(path))
		for i, k := range path {
			keys[i] = ontos.NewAtom(k)
		}
		selected, ok := root.At(keys[:cut])
		if !ok {
			return nil
		}
		selected, ok = selected.At(keys[cut:])
		if !ok {
			return nil
		}
		return selected.Own()
	}}
}

func TestIndependentNodeKeyBoundaries(t *testing.T) {
	cases := []struct {
		path [][]byte
		own  string
	}{
		{nil, "shared"}, {[][]byte{nil}, "empty"}, {[][]byte{{255, 0, 47}}, "binary"},
		{[][]byte{{255, 0, 47}, nil}, "shared"}, {[][]byte{{97, 47, 98}}, "slash"},
		{[][]byte{{97}, {98}}, ""}, {[][]byte{{255, 0, 47}, {1}}, ""},
	}
	for _, bytesKeys := range []bool{true, false} {
		name := "bitwire Atom / bitruntime node"
		if bytesKeys {
			name = "deixis bytes"
		}
		t.Run(name, func(t *testing.T) {
			for _, c := range cases {
				for cut := 0; cut <= len(c.path); cut++ {
					direct, split := nodeBoundaryFixture(t, bytesKeys), nodeBoundaryFixture(t, bytesKeys)
					d, s := direct.selectCut(c.path, 0), split.selectCut(c.path, cut)
					if d != direct.slots[c.own] || s != split.slots[c.own] {
						t.Fatalf("wrong target for %x cut %d", c.path, cut)
					}
					if d != nil {
						d.count++
						s.count++
					}
					if !reflect.DeepEqual(direct.slots, split.slots) {
						t.Fatal("corresponding executions differ")
					}
				}
			}
			f := nodeBoundaryFixture(t, bytesKeys)
			f.selectCut(nil, 0).count++
			f.selectCut([][]byte{{255, 0, 47}, nil}, 1).count++
			f.selectCut([][]byte{nil}, 0).count++
			if f.slots["shared"].count != 2 || f.slots["empty"].count != 1 {
				t.Fatal("lost capability aliasing")
			}
		})
	}
}

func TestRemoteAbsenceIsNotLocalAdmissionFailure(t *testing.T) {
	a, b, err := core.NewPair(core.PairOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	results := make(chan bool, 2)
	node, err := core.Compose(func(ontos.Value) {}, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = core.Addressed(b).Receive(func(p wire.Path, v ontos.Value) { results <- core.Route(node, p, v) })
	if err != nil {
		t.Fatal(err)
	}
	left := core.Addressed(a)
	if err = left.Send(wire.Path{ontos.NewAtom([]byte{255})}, message()); err != nil {
		t.Fatal("local admission failed", err)
	}
	if err = left.Send(nil, message()); err != nil {
		t.Fatal(err)
	}
	if take(t, results) || !take(t, results) {
		t.Fatal("missing route fell back or blocked a valid route")
	}
}
