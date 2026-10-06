package core

import (
	"errors"
	ontos "github.com/Bitspark/bitwire/ontos/go/core"
	wire "github.com/Bitspark/bitwire/wire/go"
)

// Addressed is an inert facade over the same endpoint and its receive ownership.
func Addressed(endpoint wire.Endpoint) wire.AddressedEndpoint {
	return &addressedEndpoint{endpoint}
}

type addressedEndpoint struct{ endpoint wire.Endpoint }

func (a *addressedEndpoint) Send(path wire.Path, message ontos.Value) error {
	value, err := wire.PackAddressed(path, message)
	if err != nil {
		return err
	}
	return a.endpoint.Send(value)
}
func (a *addressedEndpoint) Receive(handler func(wire.Path, ontos.Value)) (func(), error) {
	if handler == nil {
		return nil, errors.New("addressed handler must be nonnil")
	}
	return a.endpoint.Receive(func(value ontos.Value) {
		path, message, err := wire.UnpackAddressed(value)
		if err != nil {
			panic(err)
		} // The endpoint's handler-failure contract terminates it.
		handler(path, message)
	})
}
func (a *addressedEndpoint) Closed() <-chan struct{}       { return a.endpoint.Closed() }
func (a *addressedEndpoint) Termination() wire.Termination { return a.endpoint.Termination() }
func (a *addressedEndpoint) Close() error                  { return a.endpoint.Close() }

type boundWire struct {
	target wire.AddressedWire
	path   wire.Path
}

func Bind(target wire.AddressedWire, path wire.Path) wire.Wire {
	return &boundWire{target, append(wire.Path{}, path...)}
}
func (b *boundWire) Send(message ontos.Value) error {
	return b.target.Send(append(wire.Path{}, b.path...), message)
}

type prefixWire struct {
	target wire.AddressedWire
	prefix wire.Path
}

func Under(target wire.AddressedWire, prefix wire.Path) wire.AddressedWire {
	return &prefixWire{target, append(wire.Path{}, prefix...)}
}
func (b *prefixWire) Send(path wire.Path, message ontos.Value) error {
	full := make(wire.Path, 0, len(b.prefix)+len(path))
	full = append(full, b.prefix...)
	full = append(full, path...)
	return b.target.Send(full, message)
}

var ErrMissingPath = errors.New("wire tree path is absent")

type treeWire struct{ tree wire.WireNode }

func AsAddressed(tree wire.WireNode) wire.AddressedWire { return &treeWire{tree} }
func (t *treeWire) Send(path wire.Path, message ontos.Value) error {
	node, ok := t.tree.At(append(wire.Path{}, path...))
	if !ok {
		return ErrMissingPath
	}
	return node.Own().Send(message)
}
