# bitruntime v0.4.1

A patch release over the public **bitwire 0.3.0** contract, speaking
**`bitwire/1`**. With it, both driver-1 testees support the core claim of
bitwire's `bitwire/1` conformance contract, edition 1.

- **A peer's deadline is its caller's**
  ([bitruntime#23](https://github.com/Bitspark/bitruntime/issues/23)). bitwire/1
  fails a call locally when its caller's deadline passes and sends a `cancel`;
  `request_timeout` is the caller's own error, never a frame.
  - **Go:** an application's own call that outlives its peer's
    `RequestTimeout` now fails with `context.DeadlineExceeded`, the error its
    own deadline gives, instead of a public `cancelled`.
  - **TypeScript:** a request the root forwards for another carrier is now
    answered `cancelled` across the wire, instead of `request_timeout`.
  - In both languages a `cancel` still goes to the remote. A test in each
    language fails without the fix.
- **The TypeScript testee reports 1009** when its WebSocket refuses a frame over
  the peer's limit, for dialled, accepted and `peer.over` peers, as the Go
  testee does. Before, it reported the 1006 its socket's close leaves when the
  remote does not answer the close.
- The testee's install instructions name both asset URLs for npm 12.

**Evidence**

- bitwire's runner and evidence at bitwire `c3440df` (bitwire#62, #63), core
  scope, over WebSockets, in every pairing: Go/Go (the Go testee under
  `-race`), TypeScript/TypeScript, and Go with TypeScript in both orders.
  - **bitruntime (Go): the core claim is supported, 345 of 345 required
    cases.**
  - **bitruntime (TypeScript): the core claim is supported, 345 of 345.**
- Go (`-race`) and TypeScript suites (236 tests), and the cross-language testee
  smoke from source and from the packed tarballs.

**Coordinates.** Go: module `github.com/Bitspark/bitruntime` at the `v0.4.1`
tag, with the command `cmd/bitwire-testee/go`. TypeScript: the package
`@bitspark/bitruntime` 0.4.1 and the testee `@bitspark/bitruntime-testee`
0.4.1, both published as this release's tarballs with SHA-256 checksums, not to
an npm registry. Under npm 12, a consuming project allows their URLs with
`allow-remote=root` and names both.

Not in this release: live references, tunnels, the framed byte stream
`bitwire-stream/1`, observation and tracing hooks, authentication integration,
and received-context evidence as a contract field.
