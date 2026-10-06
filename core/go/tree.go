package core

import (
	"errors"
	ontos "github.com/Bitspark/bitwire/ontos/go/core"
	wire "github.com/Bitspark/bitwire/wire/go"
	"reflect"
)

type Node[T any] struct {
	own      T
	children []wire.Child[T]
}

func Compose[T any](own T, children []wire.Child[T]) (*Node[T], error) {
	captured := append([]wire.Child[T]{}, children...)
	for i, c := range captured {
		if c.Node == nil {
			return nil, errors.New("nil child")
		}
		for j := 0; j < i; j++ {
			if c.Key.Equal(captured[j].Key) {
				return nil, errors.New("duplicate byte key")
			}
		}
	}
	// Validate supplied descendants too. Interface implementations can be values
	// containing slices; never use those noncomparable values as map keys.
	active := map[wire.DeixisNode[T]]bool{}
	complete := map[wire.DeixisNode[T]]bool{}
	var validate func(wire.DeixisNode[T], int) error
	validate = func(n wire.DeixisNode[T], depth int) error {
		if n == nil {
			return errors.New("nil child")
		}
		value := reflect.ValueOf(n)
		switch value.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Map, reflect.Func, reflect.Chan:
			if value.IsNil() {
				return errors.New("nil child")
			}
		}
		if depth > 4096 {
			return errors.New("tree validation depth limit exceeded")
		}
		comparable := value.Comparable()
		if comparable {
			if active[n] {
				return errors.New("cyclic tree")
			}
			if complete[n] {
				return nil
			}
			active[n] = true
		}
		children := n.Children()
		keys := make(map[string]bool, len(children))
		for _, c := range children {
			key := string(c.Key.Bytes())
			if keys[key] {
				return errors.New("duplicate byte key")
			}
			keys[key] = true
			if err := validate(c.Node, depth+1); err != nil {
				return err
			}
		}
		if comparable {
			delete(active, n)
			complete[n] = true
		}
		return nil
	}
	for _, c := range captured {
		if err := validate(c.Node, 1); err != nil {
			return nil, err
		}
	}
	return &Node[T]{own: own, children: captured}, nil
}
func (n *Node[T]) Own() T                    { return n.own }
func (n *Node[T]) Children() []wire.Child[T] { return append([]wire.Child[T]{}, n.children...) }
func (n *Node[T]) At(path wire.Path) (wire.DeixisNode[T], bool) {
	var selected wire.DeixisNode[T] = n
	for _, key := range path {
		var next wire.DeixisNode[T]
		for _, c := range selected.Children() {
			if c.Key.Equal(key) {
				next = c.Node
				break
			}
		}
		if next == nil {
			return nil, false
		}
		selected = next
	}
	return selected, true
}
func (n *Node[T]) Decompose() wire.Parts[T] { return wire.Parts[T]{Own: n.own, Children: n.Children()} }
func Select[T any](n wire.DeixisNode[T], path wire.Path) (wire.DeixisNode[T], bool) {
	return n.At(path)
}
func Route(n wire.DeixisNode[func(ontos.Value)], path wire.Path, message ontos.Value) bool {
	target, ok := n.At(path)
	if !ok {
		return false
	}
	target.Own()(message)
	return true
}
