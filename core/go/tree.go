// Package core constructs full Deixis trees and derives sending from selection.
// It implements Bitwire's structural contract without a carrier or dispatcher.
package core

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"unicode/utf8"

	wire "github.com/Bitspark/bitwire/wire/go"
)

var (
	ErrDuplicateKey = errors.New("bitruntime: duplicate child key")
	ErrInvalidTree  = errors.New("bitruntime: invalid tree")
	ErrCycle        = errors.New("bitruntime: structural cycle")
	ErrMissingPath  = errors.New("bitruntime: missing tree path")
	ErrInvalidPath  = errors.New("bitruntime: invalid addressed path")
	ErrInvalidWire  = errors.New("bitruntime: missing own wire")
)

// node is immutable in topology and own-value association. The generic payload
// need not itself be immutable: a Wire is a capability, not a copied value.
type node[T any] struct {
	own      T
	children []wire.Child[T]
	byKey    map[string]wire.DeixisNode[T]
}

// Compose constructs a full node. It copies the child collection and all keys,
// preserves own and child capability identity, and rejects duplicate keys, nil
// children, and cycles identifiable by Go node identity. Empty and arbitrary
// binary keys are valid.
//
// Independently implemented children must satisfy DeixisNode's finite, stable
// topology contract. Compose validates their complete Children graph; continued
// stability is their implementation's responsibility. Comparable node identities
// permit cycle detection and shared-node deduplication. Value implementations
// without comparable identity are also supported under the same finite-tree
// precondition; no generic traversal can guarantee termination for a foreign
// implementation that violates it. Payloads need not be comparable.
// Construction and validation do not call Own, At, or any payload operation on
// children. There is no transformation from an opaque AddressedWire to a tree.
func Compose[T any](own T, children []wire.Child[T]) (wire.DeixisNode[T], error) {
	copied, err := copyChildren(children)
	if err != nil {
		return nil, err
	}
	if err := validate(copied); err != nil {
		return nil, err
	}
	n := &node[T]{own: own, children: copied, byKey: make(map[string]wire.DeixisNode[T], len(copied))}
	for _, child := range copied {
		n.byKey[string(child.Key)] = child.Tree
	}
	return n, nil
}

func copyChildren[T any](children []wire.Child[T]) ([]wire.Child[T], error) {
	result := make([]wire.Child[T], len(children))
	keys := make(map[string]struct{}, len(children))
	for i, child := range children {
		key := string(child.Key)
		if _, exists := keys[key]; exists {
			return nil, fmt.Errorf("%w: %x", ErrDuplicateKey, child.Key)
		}
		if isNil(child.Tree) {
			return nil, fmt.Errorf("%w: nil child at key %x", ErrInvalidTree, child.Key)
		}
		keys[key] = struct{}{}
		result[i] = wire.Child[T]{Key: bytes.Clone(child.Key), Tree: child.Tree}
	}
	return result, nil
}

// validate uses an explicit DFS stack, so finite deep trees do not consume the
// call stack. Active and completed nodes are separate: DAG sharing is allowed.
func validate[T any](children []wire.Child[T]) error {
	type visit struct {
		tree wire.DeixisNode[T]
		exit bool
	}
	stack := make([]visit, 0, len(children))
	for _, child := range children {
		stack = append(stack, visit{tree: child.Tree})
	}
	active := make(map[wire.DeixisNode[T]]bool)
	done := make(map[wire.DeixisNode[T]]bool)
	for len(stack) != 0 {
		v := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if _, trusted := v.tree.(*node[T]); trusted {
			// Already validated; its retained children must remain conforming.
			continue
		}
		if isNil(v.tree) {
			return fmt.Errorf("%w: nil child", ErrInvalidTree)
		}
		if v.exit {
			delete(active, v.tree)
			done[v.tree] = true
			continue
		}
		// Non-comparable value implementations remain lawful nodes. A map can
		// track only comparable, reflexive identities (unlike a value with NaN).
		identified := reflect.ValueOf(v.tree).Comparable() && v.tree == v.tree
		if identified {
			if active[v.tree] {
				return ErrCycle
			}
			if done[v.tree] {
				continue
			}
		}
		parts, err := copyChildren(v.tree.Children())
		if err != nil {
			return err
		}
		if identified {
			active[v.tree] = true
			stack = append(stack, visit{tree: v.tree, exit: true})
		}
		for _, part := range parts {
			stack = append(stack, visit{tree: part.Tree})
		}
	}
	return nil
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

func (n *node[T]) Own() T { return n.own }

func (n *node[T]) Children() []wire.Child[T] {
	result := make([]wire.Child[T], len(n.children))
	for i, child := range n.children {
		result[i] = wire.Child[T]{Key: bytes.Clone(child.Key), Tree: child.Tree}
	}
	return result
}

func (n *node[T]) At(path wire.TreePath) (wire.DeixisNode[T], bool) {
	return Select[T](n, path)
}

func (n *node[T]) Decompose() (T, []wire.Child[T]) { return n.own, n.Children() }

// Select follows exact child keys. An empty path selects tree itself; a missing
// edge returns (nil, false). It never manufactures a view or uses own as fallback.
// The tree must satisfy the full, stable DeixisNode contract.
func Select[T any](tree wire.DeixisNode[T], path wire.TreePath) (wire.DeixisNode[T], bool) {
	if isNil(tree) {
		return nil, false
	}
	current := tree
	for _, key := range path {
		var next wire.DeixisNode[T]
		if local, ok := current.(*node[T]); ok {
			next = local.byKey[string(key)]
		} else {
			for _, child := range current.Children() {
				if bytes.Equal(child.Key, key) {
					next = child.Tree
					break
				}
			}
		}
		if isNil(next) {
			return nil, false
		}
		current = next
	}
	return current, true
}

// Send is exactly selection followed by the selected own Wire.Send. Missing
// selection is ErrMissingPath; a selected primitive's refusal is returned intact.
// Message and return capability identity are preserved. This operation does not
// attach receivers, dispatch handlers, or assume ownership of an endpoint.
func Send(tree wire.WireTree, path wire.TreePath, message wire.Message) error {
	selected, ok := Select[wire.Wire](tree, path)
	if !ok {
		return ErrMissingPath
	}
	own := selected.Own()
	if isNil(own) {
		return ErrInvalidWire
	}
	return own.Send(message)
}

type addressed struct{ tree wire.WireTree }

// AsAddressed exposes a full tree through the existing bitwire/1 addressed
// access contract. Each Unicode-scalar string segment maps to its exact UTF-8
// byte key; malformed UTF-8 is refused before any primitive is called. Empty
// strings, slashes, dots, and Unicode normalization differences stay literal.
//
// Binary tree keys outside the UTF-8 image remain valid tree keys but cannot be
// selected through this facade. This adapter grants no receiver or lifecycle
// authority and provides no inverse conversion from an opaque AddressedWire.
func AsAddressed(tree wire.WireTree) wire.AddressedWire { return addressed{tree: tree} }

func (a addressed) Send(path []string, message wire.Message) error {
	keys := make(wire.TreePath, len(path))
	for i, segment := range path {
		if !utf8.ValidString(segment) {
			return fmt.Errorf("%w: segment %d is not Unicode-scalar UTF-8", ErrInvalidPath, i)
		}
		keys[i] = []byte(segment)
	}
	return Send(a.tree, keys, message)
}
