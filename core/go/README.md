# Go structural core

Package `github.com/Bitspark/bitruntime/core/go` implements the full structural
contract declared by bitwire 0.3.0. It uses Go 1.26, matching that dependency.

```go
import (
    core "github.com/Bitspark/bitruntime/core/go"
    wire "github.com/Bitspark/bitwire/wire/go"
)

func service(root, echo wire.Wire) (wire.WireTree, error) {
    leaf, err := core.Compose[wire.Wire](echo, nil)
    if err != nil {
        return nil, err
    }
    return core.Compose[wire.Wire](root, []wire.Child[wire.Wire]{
        {Key: []byte("echo"), Tree: leaf},
    })
}
```

`Wire.Send(message)` is addressless. `WireTree` is exactly
`wire.DeixisNode[wire.Wire]`: every node has an own primitive and a complete
finite map of exact byte keys to child nodes. The generic constructor also
accepts other payload types; it never invokes a payload while building a tree.

## Operations

```go
tree, err := core.Compose(own, children)
selected, exists := core.Select(tree, path)
selected, exists = tree.At(path)
own, children = tree.Decompose()
rebuilt, err := core.Compose(own, children)

err = core.Send(wireTree, path, message)
addressed := core.AsAddressed(wireTree)
```

- `Own` returns the retained own value. `Children` and `Decompose` return the
  complete child collection, copying keys and the collection itself.
- `Compose` preserves own and child identity, snapshots keys, and rejects nil
  children and duplicate byte keys. Empty keys and arbitrary binary keys work.
- `Select` and `At` follow exact byte keys. An empty path returns the original
  node. An absent edge returns `(nil, false)`, including when a prefix exists.
- Reconstruction preserves structure, own capability identity, and retained
  child identity. Shared child instances are allowed under different keys.
- `Send` selects a node and calls its own `Wire.Send` exactly once. It returns
  `ErrMissingPath` for absent paths, without falling back to an ancestor. A
  selected primitive's refusal is returned unchanged. The complete message and
  return capability pass through unchanged.

Collection order has no routing meaning. The current implementation preserves
construction order; consumers use exact keys rather than positional meaning.

## Independent nodes

Children may come from another implementation of `wire.DeixisNode[T]`,
including Go value implementations with non-comparable slice payloads. The
constructor retains the actual child implementation rather than replacing it
with a wrapper. Such children must meet the same finite, acyclic, stable topology
contract. Their continued stability is their implementation's responsibility.

Validation walks complete child maps iteratively, rejects duplicates and nil
children throughout, and detects cycles where Go node identities can be tracked.
Comparable, reflexive identities permit deduplication of shared nodes. Nodes
without such an identity are traversed under the finite-tree precondition.
The runtime does not claim it can prove termination of arbitrary foreign
`Children` implementations. Locally constructed nodes already enforce their
structure and need no repeated traversal.

bitstore's Go declarations have the same semantics but define their own
recursive `DeixisNode[T]` and `Child[T]` types. They are not directly assignable
to bitwire's Go interfaces: `At`, `Children`, and `Decompose` name different
return types. Interoperation requires an explicit adapter preserving own
capability identity, complete children, exact keys, and selection/reconstruction
laws. The TypeScript presentations are structurally assignable; that does not
create Go type aliases or add a bitstore dependency to this package.

## Addressed access

`AsAddressed` is a one-way bridge from a full `WireTree` to the existing
`wire.AddressedWire` carrier access contract. It maps every Unicode-scalar
string segment to its exact UTF-8 bytes. It validates the entire path before
sending and returns `ErrInvalidPath` for malformed UTF-8, including encoded
surrogates. Empty strings, dots, slashes, and Unicode normalization differences
retain their literal meaning. Binary keys outside the UTF-8 image remain usable
through the native tree but are unreachable through this unchanged profile.

An opaque addressed router cannot be converted to a full tree without supplying
the missing structural information. The bridge supplies no receiving, closing,
dispatch, or carrier lifecycle implementation.

## Verification

From the repository root:

```console
go test ./core/go
go vet ./core/go
go test -race ./core/go
```

The tests cover exact binary and empty keys, defensive copies, child and own
identity, decomposition/reconstruction, partial selection, shared nodes,
independent value implementations, identifiable cycles, deep foreign trees,
derived sending, unchanged refusals, and Unicode-safe addressed access.
`TestBitwireStructuralOracle` runs production operations against bitwire's
independent [observations](testdata/README.md).
