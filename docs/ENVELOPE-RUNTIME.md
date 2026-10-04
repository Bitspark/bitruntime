# Generic envelope runtime replacement

Defined before runtime code, 4 October 2026. Owner-approved clean cut.
The contract is bitwire decision 0014 and docs/wire/{contract,carriers}.md,
committed before dependent code at e490e7bfc7f2. The target public dependency is
bitwire 0.4.0. The cube adoption boundary is system/WIRE-SHARED.md at model
f0bfda81316664ba5a22cfb2d3f3e15b25c6b16f. That adoption does not make a cube or
a service dependency part of this runtime.

## Realization and observations

Implement one generic Wire in Go and TypeScript. Local pairs and WebSocket use
that same interface directly. A pair captures headers and measures the current
canonical encoding before admission, delivers asynchronously in order, allows
one receive owner, buffers while detached and closes both ends on handler/queue
failure. Overflow refuses the overflowing admission and fails the connection;
previously admitted outcomes may be unknown. Reject outbound invalid/oversized
input before admission. No request/response vocabulary, dedup ID set, invocation
table, implicit execution deadline or payload inspection belongs in the runtime.

Endpoint defaults are 16 MiB envelope, 64 MiB queued encoded bytes and 1024
queued envelopes. Queue bounds describe undelivered traffic, not total process
memory or application-retained values. Full finite DeixisNode<T> construction,
exact partial selection and decomposition remain generic. A complete node's own
value is independent of its children. Duplicate byte keys and structural cycles
are rejected. Exact routing to a leaf never falls back to an ancestor. A router
handler does not acquire discovery merely by matching a path.

WebSocket encodes bitwire/envelope/1 and negotiates only bitwire.ontos.v1. Binary
messages carry whole envelopes; fragmented messages are reassembled by the
carrier. Text/wrong-protocol/malformed/oversized traffic fails. Go and TypeScript
use the same canonical fixtures. Dial supports normal verified TLS and bounded
establishment; no service timeout is hidden in a wire. Close releases owned
resources before terminal observation, with a five-second closing bound followed
by forced release. Caller-owned HTTP/HTTPS servers remain caller-owned. A
convenience listener owns its listening handle. Establishment authorization
never makes a claimed source path an authenticated identity.

Expected observations: independent bitwire pair cases; snapshot/duplex/detach/
close/overflow/handler-failure cases; byte-path tree selection; exact independent
codec vectors; real binary WebSocket peers, TLS, malformed traffic and shutdown;
Go/TypeScript cross-language exchanges carrying unknown payloads; and a consumer
installed outside both checkouts. Service semantics are tested by system2's
four adapters, not embedded in this generic runtime.

## Bounded migration and delivery

Retire the active RPC engine, dispatch helpers, addressed/return interfaces,
legacy exports, protocol autodetection, testees and nightseam interop suite.
Reuse useful host-effect and resource-release code from the system2 experiment
at a866f502933a43d4e24eda7bc01e4aabf38073b8, documenting provenance. Reuse the
abstract structural laws rather than retain a second sending API. Preserve old
immutable releases/history and unrelated working-tree state.

No old protocol or compatibility export may ship in 0.5.0. A temporary local
bitwire artifact may be used only during development before its public release;
replace it with the exact public 0.4.0 dependency before packaging/integration.
Exit: current generic Go/TypeScript endpoints pass independent cases, race/lifecycle
and real-peer checks; package and public Go consumers pass; reviewed green PR
lands on main; immutable 0.5.0 tag produces the established GitHub package asset
and checksums, which are verified by fresh consumers. Registry publication of
bitruntime is not inferred from an npm-compatible tarball.

Evidence and full integration review are pending; old release test results are
not reused as evidence for this breaking replacement.
