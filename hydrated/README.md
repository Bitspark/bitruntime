# Hydrated wires

**Status, 7 October 2026:** the realization of bitwire decision 0019, the hydrated
wire protocol's first edition, accepted at bitwire `5b1de82`, for bitruntime
0.7.0. It implements bitwire 0.6.0's public hydrated declarations
(`HydratedValue`, `HydratedWire`, `HydratedEndpoint`, `ReceivedContext`) and
encodes and decodes through bitwire's shared pure hydrated codec, which owns the
grammar and its bounds. No private copy of either remains here.

[The Go package](go/hydrated.go) and [the TypeScript module](ts/src/index.ts)
hold the stateful half of the protocol:
- scopes with random tokens and 16-octet unpredictable export ids;
- frames sent to owner paths;
- proxies recognized throughout the namespace;
- exports bound to a local Endpoint's lifetime;
- liveness judged on send;
- received context delivered beside the value;
- one counting domain for bounds, in both directions;
- Endpoint termination, reported as bitwire's `Termination`.

[The Go tests](go/hydrated_test.go) run the record's observations 1-15 and 17
around an opaque middle router, over local pairs and WebSocket where the carrier
matters. The [TypeScript tests](ts/test/hydrated.test.mjs) repeat observations
1-4, 6-10, 15 and 17. [scripts/hydrated-interop.mjs](../scripts/hydrated-interop.mjs)
runs observation 16 over a real WebSocket in both roles: Go serving TypeScript,
and TypeScript serving Go. The peers build from this repository; published
packages are qualified at release.

[The composition suite](ts/test/COMPOSITION.md) is Fiber Composition's
consumer conformance, accepted at `4217f1b`. It covers Cell, Text and Counter
adapters, nested cells, lazy reads, both write directions, failures and bounded
reply lifetimes, over both carriers.

The record's vectors are copied from bitwire `5b1de82` into
[testdata](go/testdata/hydrated-vectors.json), pinned by sha256; both languages
replay them.
