# bitruntime v0.3.0

Carries a `WireTree` across a carrier, in Go and TypeScript, over the public
**bitwire 0.3.0** contract. The protocol revision **`bitwire/1`** is unchanged.

A carrier path names a position, not a node. The two bridges are therefore
explicit, and neither infers structure from an opaque endpoint
([bitruntime#15](https://github.com/Bitspark/bitruntime/issues/15)):

- **`Bind`/`bind` (core):** an addressless `Wire` that sends at one fixed
  addressed path. `Bind(a, p).Send(m)` is `a.Send(p, m)`, with the message,
  the return capability and any refusal unchanged. The path is copied, and
  the result grants nothing but sending. A near tree binds each far position
  it names.
- **`Serve`/`serve` (dispatch):** serves a tree beneath a nonempty prefix.
  - Each position whose keys are UTF-8 gets one exact dispatcher route, bound
    to that position's own `Wire`. A node shared by two positions is served
    at both.
  - A binary-keyed child and its whole subtree are not served, and
    `Unreachable` lists them.
  - A request the node refuses is answered (`internal`, or the refusal's
    public code). A refused event is dropped and never ends the carrier.
  - `Update` replaces the served tree atomically. A request admitted before
    the update keeps the node that admitted it, its cancellation included.
  - `Close` releases only the routes, never the dispatcher or its endpoint.
- **`RouteSet` (dispatch):** a group of dispatcher routes its owner replaces as
  a whole, atomically. `Serve` is built on it.
  - A delivery racing a replacement is routed by the old set or the new one,
    never refused `method_not_found`. Replacing by detaching then registering
    leaves that gap.
  - An admitted request's cancellation stays with the route that admitted it.
  - A refused `Set` changes nothing.
- **`Compose`/`compose`** refuse a missing own value (bitwire decision 0012:
  every node has one). v0.2.0 accepted one, and sending to it failed later.

**Evidence**

- Go (under `-race`) and TypeScript tests for each item:
  - a concurrent swap test, which fails 3 of 3 runs if the swap detaches and
    then registers;
  - captured cancellation across replacement;
  - binary-keyed subtrees and a key with a leading U+FEFF;
  - refused requests and events over a real WebSocket;
  - an empty prefix, conflicting routes and a missing own.
- bitwire's independent `conformance/wiretree` cases (bitwire#57) pass with
  bitwire's test-only adapters replaced by `Bind`, `Serve` and `Update`, in Go
  (`-race`) and TypeScript. Before release they were run from a scratch copy
  of bitwire's drivers:
  - structure 19/19 and bridge 1/1;
  - carrier 7/7 over the local pair and WebSockets in both directions;
  - every unlawful mutant still rejected, `retargeting-serve` by
    `carrier-cancel-across-replacement`.

  No case or expectation was changed. The other runtime families are
  unchanged, trees 14/14 included.
- The v0.2.0 suites and the nightseam v0.6.0 interoperability run
  (`node scripts/interop.mjs`) unchanged.

**Coordinates.** Go: module `github.com/Bitspark/bitruntime` at the `v0.3.0`
tag, packages `core/go`, `transports/go`, `transports/websocket/go`,
`engine/go`, `engine/websocket/go` and `dispatch/go`. TypeScript: the package
`@bitspark/bitruntime` 0.3.0 with the subpaths `/core`, `/transports`,
`/engine` and `/dispatch`, published as this release's tarball with a SHA-256
checksum. It is not published to an npm registry. Under npm 12, a consuming
project allows its URL with `allow-remote=root` in `.npmrc`.

Not in this release: live references, tunnels, the framed byte stream
`bitwire-stream/1`, observation and tracing hooks, authentication integration,
and received-context evidence as a contract field.
