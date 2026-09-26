# bitruntime

The Go and TypeScript implementation of the
[Bitwire](https://github.com/Bitspark/bitwire) contract.

**Status: the runtime path hand-written adapters use is implemented** (the next
release after the structural core 0.1.0):

- **core:** full tree construction, selection, decomposition and derived
  sending; the addressed operators `At`, `Mount` and `Forward`; the local pair;
  the invocation lifecycle.
- **transports:** the frame transport seam, the in-memory pipe and WebSocket.
- **engine:** the `bitwire/1` protocol engine and WebSocket connection setup.
- **dispatch:** the dispatcher and the `Call`, `Emit`, `Handle` and `Register`
  helpers.

The runtime is ported from Nightseam v0.6.0 with its provenance in `NOTICE`, and
fixes Nightseam's recorded defects in this path; see
[the port record](docs/port-from-nightseam.md). The engine interoperates with
Nightseam v0.6.0 peers in both roles and both languages and sends the same
bytes (`node scripts/interop.mjs`). Live references, tunnels, the framed byte
stream, telemetry and authentication integration remain planned under
[Bitwire decision 0010](https://github.com/Bitspark/bitwire/blob/main/docs/decisions/0010-bitwire-holds-the-contract-and-bitruntime-implements-it.md).

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

Go uses one module, `github.com/Bitspark/bitruntime`, released by root tags:

| Package | Holds |
| --- | --- |
| `core/go` | Trees, `At`, `Mount`, `Forward`, `NewPair`, the invocation lifecycle, `Respond`, `PublicError` |
| `transports/go` | The seam, `Pipe`, close codes and `Sendable`, the closed classification `ErrClosed` |
| `transports/websocket/go` | The WebSocket transport |
| `engine/go` | The `bitwire/1` `Peer` over any transport |
| `engine/websocket/go` | `Accept`, `NewHandler` and `Dial` over WebSockets |
| `dispatch/go` | `NewDispatcher`, `Call`, `Emit`, `Handle`, `Register` |

A program links only the packages it imports; `coder/websocket` and `net/http`
enter only through the WebSocket packages. TypeScript uses one package,
`@bitspark/bitruntime`, with the subpaths `./core`, `./transports`, `./engine`
and `./dispatch`, so the received context its components share stays private
to the package. Both depend on the public Bitwire 0.3.0 contract. Releases
publish a root Go tag and a TypeScript tarball with checksums on GitHub; npm
registry publication is not configured. Read [RELEASING.md](RELEASING.md).

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
