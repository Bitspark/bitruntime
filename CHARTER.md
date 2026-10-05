# Charter

bitruntime implements the [bitwire layered contract](https://github.com/Bitspark/bitwire/blob/v0.5.0/docs/wire/contract.md). Carriers provide addressless Endpoints; one carrier-independent layer adds AddressedWire access. Wire grants sending, Endpoint adds receive and close ownership, and WireTree supplies complete structural access. These are distinct capabilities, not compatibility spellings.

bitwire owns interface laws, canonical raw/addressed formats and independent conformance expectations. deixis owns complete-tree selection and reconstruction laws for any opaque own type. bitruntime owns concrete concurrency, queues, resource bounds, carrier setup and packaging. Consumers own services, authorization and message conventions. An implementation convenience may not silently amend those upstream boundaries.

The current release unit contains `core/{go,ts}` and `websocket/{go,ts}`. One root Go module and one TypeScript package share a version. Complete trees are a separate facility; opaque endpoint paths do not grant tree discovery.

Each release states the implemented bitwire version, wire format and evidence. Independent bitwire raw and addressed observations and encoded vectors, native edge cases, Go race tests, Go/TypeScript WebSocket exchanges and fresh installed consumers check the promise. No compatibility implementation is carried across a breaking replacement. Historic tags and commits retain previous versions.

Selection, composition and decomposition preserve own capabilities without invoking them. Sender trees and receiver trees are both valid instances. Path-cut associativity does not establish transport relay safety or progress; carrier guarantees require their own evidence. Revisions must name the affected upstream invariant, the counterexample or motivation, alternatives and consumer consequences before implementation. See [RELEASING.md](RELEASING.md) and [the current realization](docs/WIRE-RUNTIME.md).
