# Charter

bitruntime owns the concrete implementation of the [bitwire generic envelope contract](https://github.com/Bitspark/bitwire/blob/v0.4.0/docs/wire/contract.md). It owns runtime concurrency, resource bounds, carrier setup and release packaging. bitwire owns interface laws, canonical envelope bytes and independent conformance expectations. Consumers own services, authorization and message conventions.

The current release unit contains `core/{go,ts}` and `websocket/{go,ts}`. One root Go module and one TypeScript package share a version. Complete trees are a separate facility; opaque endpoint paths do not grant tree discovery.

Each release states the implemented bitwire version, wire format and evidence. Independent bitwire pair observations and encoded vectors, native edge cases, Go race tests, Go/TypeScript WebSocket exchanges and fresh installed consumers check the promise. No compatibility implementation is carried across a breaking replacement. Historic tags and commits retain previous versions.

Separating runtime from contract permits carrier and resource-management fixes without changing message laws or adding domain vocabulary. See [RELEASING.md](RELEASING.md) and [the current realization](docs/ENVELOPE-RUNTIME.md).
