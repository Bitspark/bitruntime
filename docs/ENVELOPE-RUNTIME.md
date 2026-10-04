# Generic envelope runtime replacement

Defined before runtime code, 4 October 2026. Owner-approved clean cut.
The contract is bitwire decision 0014 and docs/wire/{contract,carriers}.md,
committed before dependent code at cf0647efa4513b98010398b18d73422024eaf5ac. The target public dependency is
bitwire 0.4.0. The cube adoption boundary is system/WIRE-SHARED.md at model
55fd1641c4bf7065e62083efa31a46559f47ee5e. That adoption does not make a cube or
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
are rejected; tree validation is bounded at depth 4096. Exact routing to a leaf never falls back to an ancestor. A router
handler does not acquire discovery merely by matching a path.

WebSocket encodes bitwire/envelope/1 and negotiates only bitwire.ontos.v1. Binary
messages carry whole envelopes; fragmented messages are reassembled by the
carrier. Text/wrong-protocol/malformed/oversized traffic fails. Go and TypeScript
use the same canonical fixtures. Dial supports normal verified TLS and bounded
establishment; no service timeout is hidden in a wire. Close releases owned
resources before terminal observation, with a five-second closing bound followed
by forced release. Caller-owned HTTP/HTTPS servers remain caller-owned. A
convenience listener owns its listening handle. Establishment authorization
never makes a claimed source path an authenticated identity. Servers accept absent Origin or matching authority by default; explicit native origin allowlists can widen that policy.

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

## Implementation review and evidence

The complete replacement diff removes the old engine/dispatch/transport code,
exports, driver testees and interop fixtures. Both implementations import
bitwire 0.4.0 directly; the final package lock resolves its public npm tarball,
and Go has no local module replacement.

Local evidence: strict TypeScript, 13 native TypeScript tests, bitwire-owned
pair observations, Go vet and native tests, Go/TypeScript exchanges in both
connection roles, and a fresh installed package exercising local delivery,
structural selection and WebSocket. Independent bytes cover an empty envelope;
other tests cover binary/slash/empty paths, unknown embeddings, duplicate IDs,
TLS, origin refusal, detached queue overflow, admission refusal and resource
ownership. A stalled peer exposed that coder/websocket CloseNow waits for an
already-started handshake; the force-close timer now cancels carrier I/O and
awaits the timer callback before reporting release. This regression passes.

Review also found and corrected outgoing byte accounting (count pending encoded
bytes, excluding carrier framing), supplied-child validation, and npm 12's
package-keyed pack output in the fresh-consumer checker. The local Windows Go
race build fails in runtime/cgo; Linux CI must pass the race gate before landing.
No historical test result stands in for validation of this replacement. The PR
and immutable release workflows record the integration and public-consumer
results required by the exit criteria above.
