package core_test

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"sort"
	"testing"

	core "github.com/Bitspark/bitruntime/core/go"
	wire "github.com/Bitspark/bitwire/wire/go"
)

type oraclePrimitive struct {
	name       string
	admissions *[]string
	refuse     bool
}

func (p *oraclePrimitive) Send(message wire.Message) error {
	if p.refuse {
		return errors.New("refused")
	}
	*p.admissions = append(*p.admissions, p.name+":"+string(message.Frame.Kind))
	return nil
}

// This adapter observes the production runtime against Bitwire's independently
// written oracle. It does not contain a reference tree implementation.
func TestBitwireStructuralOracle(t *testing.T) {
	admissions := []string{}
	makeNode := func(name string, children ...wire.Child[wire.Wire]) wire.WireTree {
		return mustCompose[wire.Wire](t, &oraclePrimitive{name: name, admissions: &admissions}, children...)
	}
	leaf := makeNode("leaf")
	refusing := mustCompose[wire.Wire](t, &oraclePrimitive{refuse: true})
	tree := makeNode("root",
		wire.Child[wire.Wire]{Key: []byte{}, Tree: makeNode("empty")},
		wire.Child[wire.Wire]{Key: []byte{255}, Tree: makeNode("binary")},
		wire.Child[wire.Wire]{Key: []byte("a/b"), Tree: refusing},
		wire.Child[wire.Wire]{Key: []byte("a"), Tree: makeNode("branch", wire.Child[wire.Wire]{Key: []byte("b"), Tree: leaf})})
	at := func(path wire.TreePath) wire.WireTree {
		n, ok := core.Select[wire.Wire](tree, path)
		if !ok {
			t.Fatalf("missing oracle path %x", path)
		}
		return n
	}
	label := func(path wire.TreePath) string { return at(path).Own().(*oraclePrimitive).name }
	own, children := tree.Decompose()
	rebuilt := mustCompose(t, own, children...)
	exposed := tree.Children()
	exposed[1].Key[0] = 0
	if err := core.Send(tree, wire.TreePath{{255}}, wire.Message{Frame: wire.ProfileFrame{Version: 1, Kind: wire.ProfileEvent}}); err != nil {
		t.Fatal(err)
	}
	refusal := core.Send(tree, wire.TreePath{[]byte("a/b")}, wire.Message{})
	self, _ := tree.At(nil)
	_, missing := tree.At(wire.TreePath{{0}})
	nested, _ := at(wire.TreePath{[]byte("a")}).At(wire.TreePath{[]byte("b")})
	reconstructed, _ := rebuilt.At(wire.TreePath{[]byte("a"), []byte("b")})
	keys := []string{}
	for _, c := range tree.Children() {
		keys = append(keys, hex.EncodeToString(c.Key))
	}
	sort.Strings(keys)
	_, exists := tree.At(wire.TreePath{[]byte("a/b")})
	observations := map[string]any{
		"self": self == tree, "binary": label(wire.TreePath{{255}}), "emptyKey": label(wire.TreePath{{}}), "missing": !missing,
		"nested": label(wire.TreePath{[]byte("a"), []byte("b")}), "nestedLaw": nested == at(wire.TreePath{[]byte("a"), []byte("b")}),
		"children": keys, "slashIsLiteral": at(wire.TreePath{[]byte("a/b")}) != at(wire.TreePath{[]byte("a"), []byte("b")}),
		"partsIdentity": own == tree.Own() && children[1].Tree == at(wire.TreePath{{255}}), "rebuildIdentity": reconstructed.Own() == leaf.Own(),
		"keyCopy": label(wire.TreePath{{255}}) == "binary", "refusingExists": exists, "refusingSend": refusal != nil, "admissions": admissions,
	}
	wantBytes, err := os.ReadFile("testdata/tree-observations.json")
	if err != nil {
		t.Fatal(err)
	}
	gotBytes, err := json.Marshal(observations)
	if err != nil {
		t.Fatal(err)
	}
	var got, want any
	if err := json.Unmarshal(gotBytes, &got); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(wantBytes, &want); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Bitwire structural observations differ\ngot: %s\nwant: %s", gotBytes, wantBytes)
	}
}
