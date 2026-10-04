# bitruntime 0.5.0

This breaking release implements bitwire 0.4.0's generic duplex envelope Wire directly in Go and TypeScript. Local pairs and WebSocket carry byte paths, byte IDs and opaque ground ontos values. Complete byte-keyed trees remain a separate facility.

The active package contains `/core` and `/websocket`. The former RPC engine, dispatch helpers, transport seam, addressed aliases, return-address machinery, protocol selection and interoperability compatibility code are removed. Services define any invocation/error/stream conventions above the shared Wire.

Admission and receiving ownership are explicit and queues are bounded. Incoming overflow or handler failure terminates endpoints. Close releases owned carrier resources while leaving already dispatched application work and caller-owned HTTP servers under their owners' control. WebSocket uses `bitwire.ontos.v1` and binary `bitwire/envelope/1` only.

Validation includes bitwire-owned local-pair cases, independent encoded bytes, Go race tests, TypeScript tests, TLS and origin checks, Go/TypeScript WebSocket exchanges and fresh installed package/module consumers. TypeScript ships as this release's tarball with SHA256SUMS; Go ships through the root version tag.
