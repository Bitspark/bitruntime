# bitruntime v0.4.0

Driver-1 testees for bitwire's **`bitwire/1` conformance contract**, edition 1,
in Go and TypeScript, over the public **bitwire 0.3.0** contract
([bitruntime#20](https://github.com/Bitspark/bitruntime/issues/20)). A testee
is one bitruntime peer under remote control: bitwire's runner drives two of
them over WebSockets and judges what they answer. The testees are test
tooling, never a runtime dependency.

- **Go:** the command `cmd/bitwire-testee/go` in this module, at this tag.
- **TypeScript:** the release asset `bitspark-bitruntime-testee-0.4.0.tgz`,
  whose `bin` is `bitwire-testee`. It depends on this release's package asset,
  so no testee code ships inside `@bitspark/bitruntime`.

Both are ported from nightseam v0.6.0's testees (`5cc9723`) onto bitruntime's
public API, with provenance in `NOTICE` and the differences in
[the port record](port-from-nightseam.md#the-driver-1-testees). They claim the
core layers, `seam` and `peer`, with the features `listen`, `pipe`, `lazy` and
`propagator`. Tunnels answer `unsupported` until bitruntime implements them.

**Fixed:** a peer handed a frame over its own frame limit now ends the
connection with **1009**, which bitwire/1 binds for a frame over the receiver's
limit. v0.6.0, and bitruntime until now, closed 4011 there. Its transports
already refused with 1009 at their own limits; this is the peer's backstop when
a connection's limit is laxer than the peer's.

**Evidence**

- bitwire's runner (bitwire#62 at `19d4081`, edition 1) over WebSockets, core
  scope. The pairings were Go/Go, TypeScript/TypeScript, and Go with
  TypeScript in both orders; the Go testee was built with `-race`.
  - Per implementation, 321 of 327 required cases pass.
  - The six others are one scenario, as written and mirrored, in each pairing.
    Its raw frame names a plain method (`echo`), which bitruntime, serving only
    canonical path names, answers `method_not_found`. That is a contract
    question raised in bitwire; the core claim waits on it.
- `scripts/testee-smoke.mjs` drives both testees across languages, from
  source and from the packed tarballs, in CI and before release.
- Go (`-race`) and TypeScript suites (234 tests), with a regression test for
  the 1009 close in each language that fails without the fix. Interop with
  nightseam v0.6.0 is unchanged.

**Coordinates.** Go: module `github.com/Bitspark/bitruntime` at the `v0.4.0`
tag, packages `core/go`, `transports/go`, `transports/websocket/go`,
`engine/go`, `engine/websocket/go`, `dispatch/go` and the command
`cmd/bitwire-testee/go`. TypeScript: the package `@bitspark/bitruntime` 0.4.0
with the subpaths `/core`, `/transports`, `/engine` and `/dispatch`, and the
testee `@bitspark/bitruntime-testee` 0.4.0. Both are published as this
release's tarballs with SHA-256 checksums, not to an npm registry. Under
npm 12, a consuming project allows their URLs with `allow-remote=root`.

Not in this release: live references, tunnels, the framed byte stream
`bitwire-stream/1`, observation and tracing hooks, authentication integration,
and received-context evidence as a contract field.
