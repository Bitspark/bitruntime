# bitruntime 0.6.0

This breaking release implements bitwire 0.5.0's addressless Wire and Endpoint in Go and TypeScript. Local pairs and WebSocket carry opaque ground ontos values without mandatory IDs, paths or request-response fields.

One addressed layer works over either carrier. bind captures a path into a send-only Wire; under composes prefixes; asAddressed derives access from a complete WireNode. Selection and reconstruction preserve own capabilities without invoking them. Missing paths remain distinct from refused operations.

The generic envelope API and decoder are removed. WebSocket negotiates bitwire.ontos.v2 and carries ontos-codec-v1 values. Addressed access uses bitwire/addressed/1 above that raw boundary. Consumers own their service exchange fields.

Existing local admission, ordering, bounded queues, exclusive detachable receiving and resource-release guarantees remain. Facades retain the underlying endpoint's ownership and lifetime.

Validation covers contract-owned raw and addressed observations, exact byte paths, independent bytes, Go race tests, TLS/origin checks, both Go/TypeScript connection roles and fresh installed consumers. TypeScript ships as the release tarball with SHA256SUMS; Go ships through the root tag.
