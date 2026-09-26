# bitruntime

The Go and TypeScript implementation of the
[Bitwire](https://github.com/Bitspark/bitwire) contract.

**Status: structural core 0.1.0 implemented.** It provides
full generic tree construction and selection, decomposition/reconstruction,
derived sending and an explicit addressed-access adapter. See
[Go](core/go/README.md) and [TypeScript](core/ts/README.md).

Transports, carriers, the protocol engine, invocation lifecycle, dispatch,
live references and tunnels remain planned under
[Bitwire decision 0010](https://github.com/Bitspark/bitwire/blob/main/docs/decisions/0010-bitwire-holds-the-contract-and-bitruntime-implements-it.md).
Consumer networking still uses frozen Nightseam v0.6.0 until that migration is
delivered; the structural core is not a replacement carrier runtime.

## Where it sits

```text
bitruntime  →  Bitwire (the contract, the protocol and carrier specifications, conformance)
```

- **Bitwire** specifies addressless `Wire`, structural `WireTree`, and the
  separate `AddressedWire` carrier access contract. bitruntime implements them.
- **Bitwire's conformance cases** judge bitruntime as an external implementation,
  written from the specification and never recorded from this code.
- **What bitruntime depends on.** Bitwire, and in separate modules, the libraries a
  transport needs.
- **What it does not depend on.** The contract language (bittype), the adapters
  (Bitlink), or Nightseam.

## The primitive and tree contract

The maintainer's accepted naming is symmetric across interaction and storage:

```ts
interface Wire { send(message: Message): void; }
interface Data { read(): Promise<Bytes>; }

type WireTree = DeixisNode<Wire>;
type DataTree = DeixisNode<Data>;
```

Both trees have the same full Deixis structure: an own primitive, complete
children keyed by exact bytes, partial path selection, decomposition and
reconstruction. For a present path:

```text
send(tree, path, message) = select(tree, path).own().send(message)
read(tree, path)          = select(tree, path).own().read()
```

`Wire` belongs to Bitwire; `Data` belongs to Bitstore. A `Data` is a reading
capability. A materialized `DeixisNode<Bytes>` remains the codec snapshot, not
the definition of `DataTree`.

The old path-taking interface is explicitly `AddressedWire`. It can be an
opaque router and does not provide a `WireTree`'s structural guarantees.
`Endpoint` extends that access contract and owns receiving/closing; the
`bitwire/1` profile and return capabilities retain addressed access. The runtime
must provide explicit bridges without inventing children for opaque peers.

See [the contract and migration boundaries](docs/wire-under-bitwire.md).
The structural core implements these tree obligations. It does not infer a
tree from an opaque addressed endpoint, queue invocations, or own endpoint
lifetime. Derived sending preserves the selected primitive's admission/refusal
and message/return-capability identity.

## Packages

Go uses module `github.com/Bitspark/bitruntime` and package `core/go`.
TypeScript uses `@bitspark/bitruntime-core` from `core/ts`. Both depend on the
public Bitwire 0.3.0 contract. The initial release process publishes a root Go
tag and a TypeScript tarball with checksums on GitHub; npm registry publication
is not configured. Read [RELEASING.md](RELEASING.md) for validation and delivery.

The generic core uses Bitwire's native node declarations. TypeScript accepts
Bitstore's matching structural node type directly; Go requires an explicit
adapter between the two packages' recursive node types. The shared semantic
contract does not imply direct Go assignability.

## Read first

- [Charter](CHARTER.md): what this repository owns, promises and is checked by.
- [Release scope](docs/RELEASE.md) and [release process](RELEASING.md).
- [Working here as an agent](AGENTS.md).
- [Repository layout](LAYOUT.md) and the
  [interactive kickoff for the bitsystem3 migration](docs/bitsystem3-migration-kickoff.md).
- [Bitwire's carrier specification](https://github.com/Bitspark/bitwire/blob/main/docs/wire/carriers.md)
  and [the contract](https://github.com/Bitspark/bitwire/blob/main/docs/wire/contract.md).

## Source layout

Read [LAYOUT.md](LAYOUT.md) for the component-first, two-letter language
directory convention and this repository's adoption notes.
