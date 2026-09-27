# bitruntime v0.4.2

A patch release over the public **bitwire 0.3.0** contract, speaking
**`bitwire/1`**. It closes two asymmetries between the Go and TypeScript
transports ([bitruntime#26](https://github.com/Bitspark/bitruntime/issues/26)):

- **The TypeScript pipe takes a receive limit.** `pipe(limit)` refuses a frame
  over `limit` bytes (UTF-8 for text) before delivery and ends the pipe with
  **1009** on both ends, as Go's `Pipe(limit)` and a WebSocket do. `pipe()`
  with no limit is unchanged.
- **A Go transport's receiver reports its own refusal.** When the pipe or the
  WebSocket transport refuses a frame over its receive limit, the receiver's
  error is now a `*CloseError` with code **1009** and the new `Local` field
  set. It is still a closed carrier (`errors.Is(err, ErrClosed)`). Before, the
  receiver got a bare closed error, and an observer of its side read 1006. The
  transport conformance suite (`transporttest`) now requires this of every
  transport.
- **The testees use both.** The TypeScript testee's `conn.pipe` accepts a
  `limit`, and the Go testee reads its side's close from the transport alone.

Not changed: whether TypeScript's `FrameConnection` gains an `abort()` is an
interface decision, which stays open on #26 beside bitwire's carrier
guarantees.

**Evidence**

- bitwire's runner and evidence at bitwire `3b237aa`, core scope, over
  WebSockets, pairings Go/Go (`-race`), TypeScript/TypeScript, and Go with
  TypeScript in both orders.
  - **bitruntime (Go): core claim supported, 345 of 345.**
  - **bitruntime (TypeScript): core claim supported, 345 of 345.**
- Go (`-race`) and TypeScript suites (237 tests); the new conformance subtest
  fails against the old pipe. The cross-language testee smoke passes.

**Coordinates.** Go: module `github.com/Bitspark/bitruntime` at the `v0.4.2`
tag, with the command `cmd/bitwire-testee/go`. TypeScript: the package
`@bitspark/bitruntime` 0.4.2 and the testee `@bitspark/bitruntime-testee`
0.4.2, both published as this release's tarballs with SHA-256 checksums, not to
an npm registry. Under npm 12, a consuming project allows their URLs with
`allow-remote=root` and names both.

Not in this release: live references, tunnels, the framed byte stream
`bitwire-stream/1`, observation and tracing hooks, authentication integration,
and received-context evidence as a contract field.
