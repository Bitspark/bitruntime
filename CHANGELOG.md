# Changelog

## 0.6.0

- Restore addressless Wire sending and Endpoint ownership under bitwire 0.5.0.
- Implement addressed access once above all carriers, with exact byte paths,
  captured bindings and prefixes, and complete WireNode selection.
- Remove the generic envelope API and decoder. WebSocket now uses
  `bitwire.ontos.v2`; services own their exchange fields and conventions.
- Validate raw and addressed Go/TypeScript interoperability and inert structure.
- Check the published deixis byte-keyed and runtime Atom-keyed node boundaries,
  including every path cut, capture, mutable slot aliases and remote absence.
- Enforce Go WebSocket close deadlines by interrupting the owned stream even
  when the carrier's handshake has taken over the read context.

## 0.5.0

- Replace the generic boundary with bitwire 0.4.0's duplex envelope Wire.
- Implement bounded local pairs and binary WebSocket carriers in Go and TypeScript; keep complete byte-keyed trees separate.
- Remove the RPC engine, dispatch helpers, transport seam, compatibility exports and protocol selection.
- Verify independent contract observations, byte vectors, TLS/lifecycle cases, cross-language peers and fresh consumer installs.

## Earlier development

- TypeScript refuses a close reason it cannot send as it is: not valid UTF-8
  (an unpaired surrogate), or over 123 bytes. The adapter, the pipe and a
  peer's root endpoint throw `RangeError` before anything changes, and
  `validCloseReason` is exported. Before, a `ws` socket given such a reason was
  left in CLOSING with nothing sent (bitruntime#31), and the browser API's
  refusal was hidden by a close without a code.

## 0.4.2 (27 September 2026)

- The TypeScript `pipe(limit)` takes a receive limit, as Go's `Pipe(limit)`
  does: a frame over it ends the pipe with 1009 on both ends (bitruntime#26).
- A Go transport that refuses a frame over its receive limit now reports that
  refusal to its own side as a `*CloseError` with code 1009 and the new
  `Local` field set, still a closed carrier. Before, the receiver got a bare
  closed error, so an observer of its side read 1006. The transport
  conformance suite requires this of every transport.
- The testees use both: the TypeScript testee's `conn.pipe` accepts a `limit`,
  and the Go testee reads its side's close from the transport alone.

## 0.4.1 (27 September 2026)

- A peer's own deadline for a call is its caller's (bitruntime#23). In Go an
  application's own call now fails with `context.DeadlineExceeded`, not a public
  `cancelled`. In TypeScript a call the root forwards is now answered
  `cancelled` across the wire, not `request_timeout`, which bitwire/1 never
  carries in a frame.
- The TypeScript testee reports 1009 when its WebSocket refuses a frame over
  the peer's limit, for dialled, accepted and `peer.over` peers, as the Go
  testee does, instead of the 1006 its socket's close reports when the remote
  does not answer the close. Both testees now support the core claim of
  bitwire's conformance contract, 345 of 345 cases in every pairing.
- Name both asset URLs when installing the TypeScript testee under npm 12,
  whose `allow-remote=root` refuses the runtime URL as the testee's own
  dependency.

## 0.4.0 (27 September 2026)

- Ship driver-1 testees for bitwire's `bitwire/1` conformance contract
  (bitruntime#20), ported from nightseam v0.6.0 (`5cc9723`) onto bitruntime's
  public API: the Go command `cmd/bitwire-testee/go` and the TypeScript release
  asset `bitspark-bitruntime-testee-<version>.tgz`. They claim the core layers,
  `seam` and `peer`.
- A peer handed a frame over its own frame limit ends the connection with 1009,
  in Go and TypeScript, which bitwire/1 binds for a frame over the receiver's
  limit. v0.6.0, and bitruntime until now, closed 4011 there.

## 0.3.0 (27 September 2026)

- Carry a `WireTree` across a carrier (bitruntime#15): `Bind`/`bind` sends at
  one fixed addressed path, and `Serve`/`serve` routes each UTF-8 position of a
  tree through its own exact dispatcher route, skipping binary-keyed subtrees
  (`Unreachable`), answering a refused request and dropping a refused event.
- Add `RouteSet` to the dispatcher (`Dispatcher.RouteSet`/`routeSet`): a group
  of routes replaced as a whole, atomically, so a delivery racing a
  replacement is never refused `method_not_found`, and an admitted request's
  cancellation stays with the route that admitted it. `Served.Update` uses it.
- `Compose`/`compose` refuse a missing own value (bitwire decision 0012: every
  node has one). v0.2.0 accepted it, and sending there failed later.
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
