# Changelog

## Unreleased

- Name npm 12's `allow-remote=root` in the install instructions: npm 12 refuses
  a tarball-URL dependency, the form a release is installed in, unless the
  consuming project allows it.

## 0.2.0 (26 September 2026)

- Implement the path hand-written adapters use, ported from nightseam v0.6.0
  (`5cc9723`) with provenance in `NOTICE`: the transport seam, in-memory pipe
  and WebSocket (`transports`); `At`, `Mount`, `Forward`, the local pair and the
  invocation lifecycle (`core`); the dispatcher and the `Call`, `Emit`,
  `Handle` and `Register` helpers (`dispatch`); and the `bitwire/1` protocol
  engine with WebSocket connection setup (`engine`). The engine sends, accepts
  and refuses exactly what v0.6.0 does and presents the protocol only through
  its root Endpoint.
- Fix rather than port nightseam's recorded defects in this path: a closing
  pair answers its queued refusals (nightseam#722), a pair response frees its
  call's slot before the caller holds it (nightseam#658), every ended carrier
  classifies as one closed error that forwarding answers as `disconnected`,
  observe-only close codes are never sent, and forwarding fails only a refused
  message. See [the port record](docs/port-from-nightseam.md).
- Hold the engine to nightseam v0.6.0 peers over real WebSockets in both roles
  and both languages, and to its bytes (`scripts/interop.mjs`).

## 0.1.0 (26 September 2026)

- Implement the first Go/TypeScript structural core: full generic tree
  construction/selection/decomposition, derived sending, and an explicit
  addressed facade with exact UTF-8 path conversion. Validate with independent
  bitwire structural observations, native edge cases and package consumers.
  Establish source-tag and GitHub tarball delivery for this bounded core;
  carriers and the wider runtime/consumer migration remain pending.

- Adopt the maintainer's final primitive/tree names: addressless `Wire` and
  reading `Data`, with full `WireTree = DeixisNode<Wire>` and
  `DataTree = DeixisNode<Data>`. Document the common exact-byte-keyed structure,
  partial selection, decomposition/reconstruction and derived send/read laws.
  Mark earlier End/Bitdata naming proposals as superseded history.

- Name the old opaque addressed access `AddressedWire` and keep the
  Endpoint/return-capability boundary explicit. Update the charter, agent
  instructions and migration kickoff to follow bitwire decision 0012 without
  claiming the runtime or consumer networking migration is implemented.

- Document the family component-first layout with two-letter language directories,
  command paths and explicit adoption notes for existing source. Add the interactive
  kickoff for the first runtime and bitsystem3 migration.

- Charter the repository (bitwire decision 0010): what it owns, what it promises and how that is versioned, what independent evidence checks it, and which change its separation makes easier.
