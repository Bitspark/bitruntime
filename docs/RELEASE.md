# bitruntime v0.2.0

The runtime path that hand-written adapters use, in Go and TypeScript, over
the public **bitwire 0.3.0** contract, speaking the protocol revision
**`bitwire/1`**:

- **core:** the structural core of v0.1.0 (trees, selection, decomposition,
  derived sending); the addressed operators `At`/`at`, `Mount`/`mount` and
  `Forward`/`forward`; the local pair; the public invocation lifecycle.
- **transports:** the frame transport seam, the in-memory pipe and WebSocket,
  with one closed classification and a sendable/observe-only split of close
  codes.
- **engine:** the `bitwire/1` peer over any transport and WebSocket connection
  setup (`Accept`, `NewHandler`, `Dial`; `Peer.connect`, `Peer.attach`).
- **dispatch:** the dispatcher and the `Call`, `Emit`, `Handle` and `Register`
  helpers (`call`, `emit`, `handle`, `register`, `onEvent`).

It is ported from nightseam v0.6.0 (`5cc9723`), with provenance in `NOTICE` and
every change in [the port record](port-from-nightseam.md). nightseam's recorded
defects in this path are fixed rather than ported: queued refusals are answered
when a pair closes (nightseam#722), a pair's pending slot is free before its
caller holds the answer (nightseam#658), ended carriers answer `disconnected`,
observe-only close codes are never sent, a forwarder fails only a refused
message, and a closing peer's code reaches the far side.

**Evidence**

- `bitwire/1` is unchanged: bitruntime interoperates with nightseam v0.6.0 in
  every pairing of Go and TypeScript over real WebSockets, and sends the same
  bytes (`node scripts/interop.mjs`). The envelope tables of v0.6.0 are held by
  a running peer of each role.
- bitwire's independent cases, run from bitwire's own test-only module against
  this release: lifecycle, composition groups, declared composites (reference
  realization; production gaps recorded) and trees, over the local pair and
  WebSockets in both directions.
- The v0.6.0 Go and TypeScript test suites, ported with their expectations.

**Coordinates.** Go: module `github.com/Bitspark/bitruntime` at the `v0.2.0`
tag, packages `core/go`, `transports/go`, `transports/websocket/go`,
`engine/go`, `engine/websocket/go` and `dispatch/go`. TypeScript: the package
`@bitspark/bitruntime` 0.2.0 with the subpaths `/core`, `/transports`,
`/engine` and `/dispatch`, published as this release's tarball with a SHA-256
checksum; it replaces `@bitspark/bitruntime-core` and is not published to an
npm registry.

Not in this release: live references, tunnels, the framed byte stream
`bitwire-stream/1`, observation and tracing hooks, authentication integration,
and received-context evidence as a contract field, which awaits a later
bitwire revision.
