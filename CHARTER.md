# Charter

[Bitwire decision 0010](https://github.com/Bitspark/bitwire/blob/main/docs/decisions/0010-bitwire-holds-the-contract-and-bitruntime-implements-it.md)
requires every new repository to open with answers to four questions. A
repository owns an independently useful compatibility commitment. A module owns a
coherent semantic decision.

## 1. What decisions does it own?

How the Bitwire contract is implemented in Go and TypeScript:

- module layout and package coordinates;
- concurrency, buffering and backpressure strategy, and resource bounds within the
  carrier contract;
- the engine's public hooks for context, observation and tracing. These are
  designed together with Bitwire's received-context contract change.
- which transports ship, following Bitwire decision 0009:
  - the in-memory pipe;
  - WebSocket;
  - `bitwire-stream/1` over stdio, TCP and Unix sockets.
- the live-reference mechanism and tunnels. They depend on stated capabilities (an
  Endpoint plus lifetime and scope), not on a concrete peer.

It does **not** own:

- the contract, the protocol, the carrier contract or the conformance expectations,
  which are Bitwire's;
- declaration semantics or identity, which are bittype's;
- adapters or the identity check, which are Bitlink's;
- validation, which is bitschema's;
- authority, which stays with its consumers.

## 2. What does it promise consumers, and how is that versioned?

Each release states:

- which Bitwire contract version it implements;
- which protocol revisions it implements (`bitwire/1`, …);
- which conformance suite revision it passes.

Modules are versioned independently. Before 1.0 there is no compatibility
promise. There are no aliases or re-exports of Nightseam.

## 3. What independently written evidence checks the promise?

- Bitwire's conformance cases, run against released bitruntime from a test-only
  module in Bitwire.
- The portable byte vectors for `bitwire-stream/1`.
- Interoperability runs against Nightseam v0.6.0 peers, until the last consumer
  moves.
- Deliberately unlawful implementations, which check that the cases reject
  violations.
- bittheory's cross-repository integration checks against released versions.

## 4. Which real change becomes easier or safer because it is separate?

- Runtime fixes and implementations of new protocol revisions ship without
  touching the published contract, which decision 0008 keeps immutable.
- Contract changes and implementation fixes stay visibly different.
- The handoff becomes explicit: "this implementation satisfies this contract
  revision under this suite revision".

## Planned modules

These are modules, not repositories, until an independent commitment justifies
more:

| Module | Contents |
| --- | --- |
| core | Operators (selection, mounting, forwarding, declared composition), the invocation lifecycle, the in-process pair |
| transports | The frame transport interface, the in-memory pipe, WebSocket, `bitwire-stream/1` |
| engine | The protocol engine (the peer), connection setup |
| dispatch | The dispatcher and the request/response helpers |
| live | Scopes, bindings, owners and release for live references |
| tunnel | Many channels over one connection |
| telemetry (optional) | Observation and tracing adapters |
| auth-integration (optional) | Wire-level authentication integration above Archon |

## The first milestone

bitsystem3 is the first consumer to move off Nightseam. It needs the path its
hand-written adapters use: carriers, dispatch, helpers, selection and connection
setup.
