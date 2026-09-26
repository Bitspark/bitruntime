# Data / DataTree and Wire / WireTree

**Status: maintainer-selected contract, 2026-09-26; structural core implemented,
carrier/runtime migration pending.** [Bitwire decision 0012](https://github.com/Bitspark/bitwire/blob/main/docs/decisions/0012-explicit-data-and-wire-trees.md)
records the names and full structural obligation:

```ts
interface Data { read(): Promise<Bytes>; }
interface Wire { send(message: Message): void; }

type DataTree = DeixisNode<Data>;
type WireTree = DeixisNode<Wire>;
```

The earlier `End` primitive, preservation of addressed `Wire`, `ByteSource`
naming, and `Bitdata = Deixis[Bytes]` as the public access model are superseded.
They remain history in earlier discussions, including draft
[Bitwire PR #49](https://github.com/Bitspark/bitwire/pull/49). New code must use
the names above. [Bitwire #42](https://github.com/Bitspark/bitwire/issues/42)
tracks interaction delivery; the shared structure is coordinated through
[Deixis #49](https://github.com/Bitspark/deixis/issues/49).

## One complete structural contract

Deixis defines `Node[T] = T × FiniteMap[Bytes, Node[T]]`. Both tree types have
the same obligations, irrespective of whether an own primitive reads or sends:

- an own value at every node, including nodes which also have children;
- a complete finite map of children with exact byte keys;
- `own()`, `children()`, partial `at(path)` and `decompose()`;
- reconstruction from the own value and complete child parts;
- a finite acyclic structure, with byte keys compared by content;
- duplicate-byte-key rejection and protection against key/child-map mutation;
- empty-path selection is self, and a missing child is absence;
- an empty key is a real child key, distinct from the empty path;
- selection composition and decompose/recompose round trips preserve structure.

Construction and reconstruction preserve own-capability and retained-child
identity. Shared child instances under different names are permitted; structural
cycles are not.

The shared TypeScript shape is:

```ts
type Key = Uint8Array;
type TreePath = readonly Key[];
type Child<T> = readonly [Key, DeixisNode<T>];

interface DeixisNode<T> {
  own(): T;
  children(): ReadonlyArray<Child<T>>;
  at(path: TreePath): DeixisNode<T> | undefined;
  decompose(): Readonly<{ own: T; children: ReadonlyArray<Child<T>> }>;
}

// Runtime construction, governed by the common structure and laws:
declare function compose<T>(
  own: T, children: Iterable<Child<T>>
): DeixisNode<T>;
```

Child enumeration cannot silently be optional on either full tree type. Each
tree operation has the same meaning in both lanes. Mutability of a payload's
behavior does not change the structural contract; neither payload grants
receiver attachment or ownership of another endpoint's lifetime.

These are native presentations of the same semantic contract, not permission
to add a private checkout dependency. A shared library dependency must be
public, released, versioned and consistent with the affected charters.

TypeScript's structural types let the generic core consume either family's
matching node interface directly. Go's recursive return types retain package
identity: Bitstore's `DeixisNode[Data]` needs an explicit adapter to Bitwire's
`DeixisNode[Data]` before using this runtime's generic operators. This release
does not claim direct Go assignment or introduce a storage-to-interaction
production dependency. The adapter must preserve the common structural laws.

## Derived reading and sending

For an existing path, the operations are exactly:

```text
read(tree, path)          = select(tree, path).own().read()
send(tree, path, message) = select(tree, path).own().send(message)
```

`select` is structural selection (`at` in the native presentation), not a
prefix-binding wrapper around an arbitrary router. Missing selection returns
absence; a derived operation must report that absence according to its public
contract without invoking a primitive. It must distinguish a missing node from
a present node whose primitive refuses or fails.

For present paths, selection composes:

```text
select(select(tree, p), q) = select(tree, p ++ q)
```

`Data.read()` is asynchronous because the bytes can come from memory, disk or a
remote store. A fixed-content data capability returns the same bytes on
successful reads. Taking a materialized snapshot reads those capabilities and
produces `DeixisNode<Bytes>` for the byte codec. The capability tree and its
snapshot are different types; serializing the structure does not serialize an
arbitrary reading function.

`Wire.send(message)` is addressless and reports admission or refusal, not
application completion. Message/profile content, return capabilities and
received-context evidence keep their own contracts. The rename does not remove
correlation, cancellation or ownership rules from the runtime.

## Full trees and addressed carrier access

The old path-taking `Wire` interface becomes **`AddressedWire`**:

```ts
interface AddressedWire {
  send(path: Path, message: Message): void;
}
```

This is an explicit access/transport boundary. `Endpoint` extends
`AddressedWire` and owns receiver attachment and closure. `ReturnAddress` holds
`AddressedWire` access because revision 1 profiles can address controls beneath
a return origin. New primitive `Wire` supplies neither of those ownership
facilities. The native breaking rename leaves immutable `bitwire/1` unchanged.

Full trees use byte-keyed paths. The existing addressed profile uses exact
Unicode string segments, embedded as UTF-8 keys. An adapter must state and check
that boundary; it must not normalize keys, reinterpret slashes/dots, or silently
decode arbitrary binary keys with replacement characters. Native tree paths
must support arbitrary bytes even where an older carrier profile cannot.

A `WireTree` can provide addressed sending through structural selection. An
arbitrary `AddressedWire` may discover descendants dynamically or hide them
entirely. It cannot be cast to `WireTree`, or wrapped with an invented empty
child map, because sending at known paths does not reveal the complete tree.
Exposing such access as a full tree requires an explicit, complete admitted
structure and its primitive bindings. A prefix-bound addressed view stays an
addressed view; it is not successful structural selection.

This also distinguishes two absent-looking behaviors: a missing child and a
present child whose `Wire` refuses every send. Their sending observations may
coincide, but their structural observations differ. Independent cases must
reject adapters that collapse them.

## Ownership and protocol boundaries

Bitwire owns `Wire`, `WireTree`, `AddressedWire`, their laws, native
presentations and independent interaction cases. Bitstore owns `Data`,
`DataTree` and persistence APIs. Deixis owns the generic structure, laws and
codec. bitruntime implements the Bitwire contract; no new primitive repository
is needed. Existing `wire/go/` and `wire/ts/` presentations can hold the new
types without inventing a module per type.

Historical [decision 0006](https://github.com/Bitspark/bitwire/blob/main/docs/decisions/0006-declared-composites-realize-deixis-nodes.md)
provided declared origins and opaque addressed child accesses. That subset
mapping explains the old API, but opaque accesses are not the new complete
`WireTree` children. The runtime migration must preserve the explicit boundary.

Structural selection and reconstruction do not settle execution policy. An
accepted invocation needs its captured receiver, cancellation, actual body
completion, control drain and retirement. A late cancellation must not reach a
replacement handler. A borrowed view must not close every child. Pure local
selection preserves primitive/return-capability identity and received context;
carrier crossing establishes its own bound capabilities and evidence.

No revision-2 handshake is inserted into `bitwire/1`. Accepting a frame into a
transport queue does not prove a remote path exists or a mutation completed.
The declared acceptance boundary, carrier bounds and refusal observations need
independent evidence across the explicit adapters.

## What this means for Bit System

A fully represented space access tree has its own addressless `Wire` for that
space's operations and byte-keyed child `WireTree` nodes. A space's persistent
ID, facts, parent relation and name-to-child mapping remain durable data;
live sending primitives are bound from that model, not persisted as endpoints.

Operation names in an RPC profile and model child names remain distinct
namespace decisions. A child need not know its parent's prefix. Selecting and
remounting a complete subtree preserves its structure and own primitive. The
browser/server and parent/child boundaries can share the same model once their
bindings actually preserve those guarantees. An opaque browser carrier alone
does not prove complete remote-tree access.

## Runtime delivery still required

The Go/TypeScript core now provides composition, selection, complete parts,
derived sending and a local AddressedWire facade. It is independently checked
against Bitwire's structural oracle. This is not a networking release.
The [kickoff](bitsystem3-migration-kickoff.md) still requires remaining runtime
implementation, independent carrier conformance, remote bridges, admission
and lifecycle evidence, interoperable released packages, and the complete
bitsystem3 adoption. The wider services work remains tracked in
[deixis-svc #1](https://github.com/Bitspark/deixis-svc/issues/1),
[bitwire-svc #9](https://github.com/Bitspark/bitwire-svc/issues/9) and
[bitstore-svc #13](https://github.com/Bitspark/bitstore-svc/issues/13).
