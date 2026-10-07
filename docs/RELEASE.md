# bitruntime 0.7.1

0.7.1 publishes what 0.7.0 tagged: its library code is unchanged. The v0.7.0
release run failed in its Go consumer smoke on Linux after the tag, so v0.7.0
has a public Go module but no GitHub release or tarball. The smoke is fixed and
CI now runs it against each pushed commit.

## The hydrated layer, introduced in 0.7.0

This release adds the hydrated wire layer, realizing bitwire decision 0019 over
bitwire 0.6.0's public hydrated declarations and shared pure codec. A message may
carry live Wires inside its structure. A Wire travels as a reference that its
receiver turns back into a usable send-only Wire. A reference returning to its
owner restores the original Wire. Routers between participants see only ground
values.

`hydrated/go` and the TypeScript subpath `/hydrated` provide:
- **Scopes.** One per participant incarnation, with a random token. Export ids
  are 16 unpredictable octets, so one reference never reveals another.
- **Endpoints.** Their sending faces are the only exportable local Wires. An
  export ends when its endpoint closes, a receiver failure terminates the
  endpoint, and termination is reported as bitwire's `Termination`.
- **Proxies.** Recognized throughout the namespace, so forwarding never wraps a
  proxy in another.
- **Liveness and context.** Liveness is judged on send. The composition's
  received context is delivered beside each value, never inside it.
- **Bounds.** Nodes, depth and bytes are counted one way in both directions.
  Conversion refuses before allocating, and incoming frames count whole.

Validation covers decision 0019's observations in Go and TypeScript, the
independent vectors, the composition conformance suite, Go race tests,
Go/TypeScript hydrated interoperability in both roles and fresh installed
consumers. The core and WebSocket guarantees of 0.6.0 are unchanged. TypeScript
ships as the release tarball with SHA256SUMS; Go ships through the root tag.
