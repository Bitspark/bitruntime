# Runtime composition plan

**Status, 7 October 2026:** implementation plan, with no new runtime code or API.
The released baseline is bitruntime 0.6.0 with bitwire 0.5.0. The current
[realization record](WIRE-RUNTIME.md) describes that implementation. The
[bitwire composition architecture](https://github.com/Bitspark/bitwire/blob/main/docs/wire/composition.md)
owns the target semantics and acceptance obligations; this page owns how
bitruntime should realize them. Future protocol records must settle the pending
details before implementation. An assigned component is not an implemented API.

## Intended result

A host should be able to create communication runtime instances, explicitly
connect and mount them in a logical routing tree, send opaque messages through
several instances, and export existing Wire capabilities through connections.
The host supplies topology and admission policy; the runtime supplies reusable
connection, routing and lifetime machinery.

Addressing chooses a destination for a message. Multiplexing creates and manages
logical connections sharing a carrying Endpoint. Wire export associates a
send-only capability with a scoped reference and constructs a forwarding proxy
at the peer. These are cooperating facilities, not aliases for one operation.

## Component ownership

| Component | Runtime responsibility | Specification dependency |
| --- | --- | --- |
| Existing `core/{go,ts}` | Local pairs, addressed facade, prefix binding, complete local trees and exact selection. | Released bitwire contract and deixis structural laws. |
| Existing `websocket/{go,ts}` | WebSocket establishment, raw Endpoint behavior and owned transport release. | Released carrier binding. |
| Proposed stream component | Read/write framing, decode bounds, shutdown and concrete stream bindings. | A new bitwire stream-framing contract. |
| Proposed routing component | Explicit mount registration, dispatch/forwarding, binding generations, return-route bindings and bounded route state. | A specified routing/mount profile; no implicit changes to DeixisNode selection. |
| Proposed multiplexing component | Open/accept/refuse logical connections, own the carrier receive attachment, manage channel state/queues and release. | A specified bitwire multiplexing protocol. |
| Proposed reference component | Export/import Wire references, forward sends, reject stale references and release registry entries. | A specified bitwire wire-export protocol. |

Names and public signatures for proposed components remain to be selected.
Follow component-first paths and the two runtime languages when code is added.
Reuse endpoint machinery where semantics match; do not copy an old RPC engine,
envelope interface or second generic Wire. bitwire supplies meaning, canonical
codecs and independent observations; portable production code still belongs here.

An executable host is separate from a runtime instance. The
[bitnode draft](https://github.com/Bitspark/bitnode/blob/6f2e77314a1e0ffd203b2079b11fdfc6a19a7c38/docs/DESIGN.md)
proposes an application with a tree location, parent/child attachments,
configuration and service outlets. Reusable mechanisms should come from
bitruntime; a host chooses the concrete routing profile, identities, enrollment
and deployment policy. This plan does not implement that executable or adopt
its older mandatory-envelope assumptions.

The [bitwire-svc design](https://github.com/Bitspark/bitwire-svc/blob/37ad781cf63e51dfc0c1956282fda17c7f01510d/README.md)
owns its proposed durable relay allocations, owner management and attachment
credentials. Runtime channel/export tables are live, scoped state and require
no such service or persistent store. A durable allocation is not a resurrected
endpoint. This division is based on consumers' own sources; a service table in
deixis is not authority over either implementation.

## Composition and ownership

### Runtime trees

Local route bindings may delegate a prefix to another running instance without
enumerating its subtree. Keep those bindings separate from complete WireNode
structure. Declare each routing root and whether forwarding retains absolute
coordinates or delegates a suffix. Mixed roots need explicit mapping, including
reply routes; raw carriers and `under` do not rewrite opaque payload names.

An admitted attachment has a generation distinct from its path. Replacement and
cleanup must not silently switch an in-flight association to a replacement peer.
Missing routes, forbidden attachments, loops, closed links and forwarding
refusal need declared outcomes. No implicit ancestor fallback, replay or service
cancellation is introduced.

The router's receive owner dispatches to its registered bindings. A selected
send-only Wire does not acquire that receiver or permission to close a shared
connection. The host separately owns listeners and routes under the binding
contract. Operation authorization stays with the relevant service.

### Multiplexing and wire export

The multiplexer takes the carrying Endpoint's receive slot and manages ordinary
logical Endpoints. Healthy siblings remain usable when one logical connection
closes. Carrier failure ends dependent endpoints and invalidates their live
references. Error isolation, control allocation, overflow behavior and progress
assumptions must be specified before the implementation claims them. Channels
share carrier capacity and ultimate failure.

Export associates a Wire with a reference and forwards incoming sends to that
registered target. Releasing the reference must not close the borrowed target.
A proxy grants Wire.send, not Endpoint ownership. Duplicate exports, re-export
through another instance and release/send races require stated outcomes. A
channel per exported Wire is optional; examine addressed reference routing first.

### Both composition orders

| Composition | Mechanism | Additional requirement |
| --- | --- | --- |
| Addressing inside multiplexed connection | Apply the existing addressed facade to a logical Endpoint. | Per-channel namespace and ordinary endpoint ownership. |
| Multiplexer at addressed destination | A router dispatches protocol traffic into that multiplexer. | An explicit bidirectional binding: send destination, incoming dispatch, reply route, lifetime and release. |

`bind(addressedWire,path)` returns a send-only Wire and cannot supply the second
construction by itself. The shared root endpoint still has one receive owner.
Do not add a second receiver behind a wrapper or grant a bound path permission
to close the carrier. Nested addressing and channel selection are not implicitly
equivalent to one concatenated path.

## Delivery stages

| Stage | Work and completion evidence |
| --- | --- |
| 1. Protocol records | bitwire records routing/mounts, multiplexing, wire export and composition choices, expected outcomes and independent vectors. Each implementation follows its own settled record. |
| 2. Routing realization | Implement explicit bindings and routing in Go and TypeScript; demonstrate local then multi-process traversal, cross-branch replies under a declared service convention, refusal and stale-binding isolation. |
| 3. Multiplexing and exports | Implement logical endpoints and export/import against the protocols. Exercise endpoint observations plus allocation, authority, aliasing, release and aggregate resource cases. |
| 4. Additional carrier | Define stream framing in bitwire and implement a TCP/TLS binding. Verify arbitrary chunks, EOF/truncation, limits and release. This can proceed alongside stages 2 and 3 after its own contract exists. |
| 5. Combined consumer | Run the same machinery over local, WebSocket and the new stream carrier, in both language roles and fresh packaged consumers. Then a host or `Pages<Wire>` consumer can adopt it explicitly. |

Stage 1 must settle identifier scope/reuse, establishment and refusal,
receive/close ownership, roots and return mapping, ordering/admission, authority
context, release/replacement races, malformed input, failure propagation and
finite resource bounds. APIs, frame grammar, credit policy and timeout values
remain undecided. Implementation must not establish these rules accidentally.

## Validation plan and honest evidence

The acceptance corpus belongs in bitwire; derive it from the agreed contracts,
not runtime output. Retain existing endpoint, addressed and tree observations.
Add these separately reported families:

- Channel establishment, isolated close, carrier loss, exact messages,
  per-direction order, exclusive receiving and endpoint bounds.
- Export to the intended send-only target, duplicate/aliased exports, re-export,
  stale references, generation isolation and release/send races; no unintended
  receive/close authority or release of a borrowed endpoint.
- A service reached through two runtime boundaries, a cross-branch reply and
  exact binary/empty/slash-containing path atoms; payloads stay opaque.
- Both composition orders, including actual ownership of the addressed binding.
- Route absence versus target refusal; no fallback, wrong-target invocation,
  replay or claim that local admission proves remote execution.
- Bounded pending opens/exports, route tables, data/control queues, connections
  and total retained bytes; cleanup and declared progress under saturation.
- Stream framing across arbitrary chunks, terminal truncation, malformed and
  oversized records, TLS/establishment failure and actual resource release.
- Native Go/TypeScript observations, Go race checks, cross-language and
  multi-process runs, and independently installed public package consumers.

These new families are unrun. Existing 0.6.0 endpoint checks do not establish
multi-hop safety, multiplexing or capability transfer. A documentation CI pass
does not change that status. Each delivered stage must name its exact source,
protocol version, environments, observations and remaining limits here or in its
linked realization record before a release claims it.

## Scope of this documentation change

This plan records the owner's request to document the intended composition goal.
It changes no implementation, dependency pin, release artifact or interface. It
does not assign deployment or durable allocation ownership by reference to a
foundational deixis document. Published releases stay immutable; future changes
use the normal reviewed contract and runtime process.
