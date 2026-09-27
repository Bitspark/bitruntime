package dispatch

import (
	"bytes"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"unicode/utf8"

	core "github.com/Bitspark/bitruntime/core/go"
	wire "github.com/Bitspark/bitwire/wire/go"
)

// Served is a WireTree served on a dispatcher beneath a prefix.
type Served struct {
	prefix      []string
	routes      *RouteSet
	mu          sync.Mutex
	unreachable []wire.TreePath
}

// Serve routes each position of tree through one exact route beneath prefix,
// bound to that position's own Wire, so what a peer sends at prefix ++ path
// reaches select(tree, path).own(). A node shared by two positions is served
// at both: a carrier path names a position, not a node.
//
// A child whose key is not UTF-8 cannot be named by a bitwire/1 path; it and
// its whole subtree are left unserved and listed by Unreachable. A request its
// node's own refuses is answered with that refusal, as internal unless it is a
// PublicError. A refused event is dropped, as an event nothing handles is; it
// never ends the carrier. Cancellation is not routed here: the dispatcher hands
// it to the route that admitted its request.
//
// The prefix is nonempty, since bitwire/1 names no request or event at the
// empty path. Serve validates the tree's complete child graph and refuses a
// missing own Wire. Neither Serve nor Close closes the dispatcher or its
// endpoint.
func Serve(d *Dispatcher, prefix []string, tree wire.WireTree) (*Served, error) {
	if len(prefix) == 0 {
		return nil, fmt.Errorf("%w: a tree is served beneath a nonempty prefix", core.ErrInvalidPath)
	}
	s := &Served{prefix: slices.Clone(prefix), routes: d.RouteSet()}
	if err := s.Update(tree); err != nil {
		s.routes.Close()
		return nil, err
	}
	return s, nil
}

// Update serves tree in place of the tree served now, in one step: each
// delivery is routed by the old tree or by the new one, never by neither, and
// a request admitted before the update keeps the node that admitted it, its
// cancellation included. A refused update leaves the served tree unchanged.
func (s *Served) Update(tree wire.WireTree) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	routes, unreachable, err := positions(s.prefix, tree)
	if err != nil {
		return err
	}
	if err := s.routes.Set(routes); err != nil {
		return err
	}
	s.unreachable = unreachable
	return nil
}

// Unreachable lists, relative to the tree, the positions of children whose key
// is not UTF-8 and which are therefore not served, with their subtrees.
func (s *Served) Unreachable() []wire.TreePath {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]wire.TreePath, len(s.unreachable))
	for i, path := range s.unreachable {
		result[i] = make(wire.TreePath, len(path))
		for j, key := range path {
			result[i][j] = bytes.Clone(key)
		}
	}
	return result
}

// Close stops serving. Requests already admitted still reach their nodes.
func (s *Served) Close() { s.routes.Close() }

type position struct {
	node wire.WireTree
	path []string
	keys wire.TreePath
}

func positions(prefix []string, tree wire.WireTree) ([]Route, []wire.TreePath, error) {
	if isNilValue(tree) {
		return nil, nil, fmt.Errorf("%w: nil tree", core.ErrInvalidTree)
	}
	// Composing validates a foreign tree's complete child graph: duplicate
	// keys, nil children and cycles, as well as the root's own Wire.
	root, err := core.Compose(tree.Own(), tree.Children())
	if err != nil {
		return nil, nil, err
	}
	var routes []Route
	var unreachable []wire.TreePath
	stack := []position{{node: root}}
	for len(stack) != 0 {
		p := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		own := p.node.Own()
		if isNilValue(own) {
			return nil, nil, fmt.Errorf("%w at %x", core.ErrInvalidWire, p.keys)
		}
		routes = append(routes, Route{Path: append(slices.Clone(prefix), p.path...), Receiver: serving(own)})
		children := p.node.Children()
		for _, child := range children {
			if !utf8.Valid(child.Key) {
				unreachable = append(unreachable, append(slices.Clone(p.keys), bytes.Clone(child.Key)))
			}
		}
		for i := len(children) - 1; i >= 0; i-- {
			child := children[i]
			if utf8.Valid(child.Key) {
				stack = append(stack, position{
					node: child.Tree,
					path: append(slices.Clone(p.path), string(child.Key)),
					keys: append(slices.Clone(p.keys), bytes.Clone(child.Key)),
				})
			}
		}
	}
	return routes, unreachable, nil
}

func serving(own wire.Wire) wire.Receiver {
	return wire.Receiver{Message: func(_ []string, message wire.Message) {
		if err := own.Send(message); err != nil && message.Frame.Kind == wire.ProfileRequest {
			_ = core.Respond(message, nil, err)
		}
	}}
}

func isNilValue(value any) bool {
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
