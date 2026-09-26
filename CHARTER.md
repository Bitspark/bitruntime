# Charter

[bitwire decision 0010](https://github.com/Bitspark/bitwire/blob/main/docs/decisions/0010-bitwire-holds-the-contract-and-bitruntime-implements-it.md)
requires every new repository to open with answers to four questions. A
repository owns an independently useful compatibility commitment. A module owns a
coherent semantic decision.

## 1. What decisions does it own?

How the bitwire contract is implemented in Go and TypeScript, including the
maintainer's `Wire` / `WireTree` naming decision:

- module layout and package coordinates;
- concurrency, buffering and backpressure strategy, and resource bounds within the
  carrier contract;
- the engine's public hooks for context, observation and tracing. These are
  designed together with bitwire's received-context contract change.
- which transports ship, following bitwire decision 0009:
  - the in-memory pipe;
  - WebSocket;
  - `bitwire-stream/1` over stdio, TCP and Unix sockets.
- the live-reference mechanism and tunnels. They depend on stated capabilities (an
  Endpoint plus lifetime and scope), not on a concrete peer.
- concrete full `WireTree` construction, partial selection, decomposition and
  reconstruction, using deixis's generic byte-keyed structural contract;
- derived sending through `select(tree, path).own().send(message)` and explicit
  adapters to the separate `AddressedWire` carrier access contract. An opaque
  router does not become a full tree merely by being wrapped or renamed.

It does **not** own:

- the contract, the protocol, the carrier contract or the conformance expectations,
  which are bitwire's;
- declaration semantics or identity, which are bittype's;
- adapters or the identity check, which are bitlink's;
- validation, which is bitschema's;
- authority, which stays with its consumers.

The naming across the two families is `WireTree = DeixisNode<Wire>` and
`DataTree = DeixisNode<Data>`. `Wire.send(message)` is addressless;
`Data.read()` reads bytes. bitstore owns `Data` and `DataTree`, and deixis owns
the common structure and laws. Materialized `DeixisNode<Bytes>` values remain
the storage codec's snapshots. This charter does not move storage implementation
into bitruntime.

The full tree contract grants complete child enumeration. Restricted opaque
access remains explicitly `AddressedWire`; it is not a structural tree type.
Tree access alone grants neither receiver attachment nor endpoint closure.
The `bitwire/1` addressed profile, `Endpoint` ownership and addressed return
capabilities remain separate from the native primitive rename.

## 2. What does it promise consumers, and how is that versioned?

Each release states:

- which bitwire contract version it implements;
- which protocol revisions it implements (`bitwire/1`, …);
- which conformance suite revision it passes.

Modules are versioned independently. The runtime path's components — core,
transports, engine and dispatch — form one release unit: one root Go module,
`github.com/Bitspark/bitruntime`, whose packages are `core/go`,
`transports/go`, `transports/websocket/go`, `engine/go`, `engine/websocket/go`
and `dispatch/go`, and one TypeScript package, `@bitspark/bitruntime`, with a
subpath per component. Root tags version them together; `v0.1.0` released the
structural core alone as `@bitspark/bitruntime-core`. They move together because
the engine and the pair create the invocations and received context that
dispatch reads, which the TypeScript package keeps private to itself. Later
components (live references, tunnels, telemetry, authentication integration)
record their module boundaries before joining this unit or publishing
separately. Before 1.0 there is no compatibility promise. There are no aliases
or re-exports of nightseam.

The initial release process publishes Go through its source tag and TypeScript
as a GitHub release tarball with checksums. Registry publication is separately
configured and cannot be inferred from the presence of a tarball. See
[RELEASING.md](RELEASING.md).

## 3. What independently written evidence checks the promise?

- bitwire's conformance cases, run against released bitruntime from a test-only
  module in bitwire.
- The initial core runs its actual implementations against bitwire's independent
  structural oracle as well as native edge cases. These observations do not
  stand in for the carrier/runtime suites, which bitwire runs from its own
  test-only module.
- nightseam v0.6.0's `bitwire/1` tables, vendored byte for byte in
  `vectors/bitwire-1`, and the byte-level transcripts of `scripts/interop.mjs`.
- The portable byte vectors for `bitwire-stream/1`.
- Interoperability runs against nightseam v0.6.0 peers, until the last consumer
  moves.
- Deliberately unlawful implementations, which check that the cases reject
  violations.
- bittheory's cross-repository integration checks against released versions.

## 4. Which real change becomes easier or safer because it is separate?

- Runtime fixes and implementations of new protocol revisions ship without
  touching the published contract, which decision 0008 keeps immutable.
- Contract changes and implementation fixes stay visibly different.
- The handoff becomes explicit: "this implementation satisfies this contract
  revision under this suite revision".

## Planned modules

These are modules, not repositories, until an independent commitment justifies
more:

| Module | Contents |
| --- | --- |
| core | Full WireTree construction/selection/decomposition, derived sending, explicit AddressedWire bridges, mounting/forwarding, the invocation lifecycle, the in-process pair |
| transports | The frame transport interface, the in-memory pipe, WebSocket, `bitwire-stream/1` |
| engine | The protocol engine (the peer), connection setup |
| dispatch | The dispatcher and the request/response helpers |
| live | Scopes, bindings, owners and release for live references |
| tunnel | Many channels over one connection |
| telemetry (optional) | Observation and tracing adapters |
| auth-integration (optional) | Wire-level authentication integration above archon |

## The first milestone

bitsystem3 is the first consumer to move off nightseam. It needs the path its
hand-written adapters use: carriers, dispatch, helpers, selection and connection
setup.

The structural core implements the primitive/tree construction and derived-send
obligations plus a local AddressedWire facade. Carrier/runtime delivery,
received-context evidence, lifecycle gaps and remote/profile bridges remain
work to implement and independently verify. See the
[migration kickoff](docs/bitsystem3-migration-kickoff.md).
