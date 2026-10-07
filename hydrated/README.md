# Hydrated wires

**Status, 7 October 2026:** the realization of bitwire decision 0019, the hydrated
wire protocol's first edition, accepted at bitwire `5b1de82`, for bitruntime
0.7.1 (first implemented in 0.7.0). It implements bitwire 0.6.0's public hydrated
declarations
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

## Public release evidence

[bitruntime v0.7.1](https://github.com/Bitspark/bitruntime/releases/tag/v0.7.1)
is published at `583174d1b2e3cc3916c363dee9fb9d1cbbeedf55`, using public
bitwire 0.6.0. [The release run](https://github.com/Bitspark/bitruntime/actions/runs/37691354522)
passed, including the fresh public Go module consumer. Its TypeScript package
is the GitHub release asset `bitspark-bitruntime-0.7.1.tgz`.

An independent download matched the published `SHA256SUMS` entry:
`78e0ce694162cecc5b6f0ff503df91631dadb12ea97915af2848fd1637db5f0d`.
A fresh consumer installed that asset and public bitwire 0.6.0 and passed the
local-pair, tree, WebSocket and hydrated-reply checks
([receipt](https://github.com/Bitspark/bitruntime/pull/45#issuecomment-6047570121)).
The independent reviewer also ran all 15 composition cases against those actual
public packages ([receipt](https://github.com/Bitspark/bitruntime/pull/45#issuecomment-6047513997)).

The 0.7.0 tag remains historical: its Go module is public, but its release
consumer check failed before a GitHub release or TypeScript asset was published.
Version 0.7.1 repairs the Linux smoke-program output path; the hydrated library
implementation is unchanged. This evidence establishes the shared hydration
layer over local pairs and WebSocket, including the test router used by its
composition cases. It does not establish a deployed bitnode network.
