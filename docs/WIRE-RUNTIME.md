# Layered wire realization

**Target decided 2026-10-05; implementation and validation in progress.** This
realizes bitwire decision 0015 and deixis's structural identity under the owner's cross-repository
correction. It replaces the generic envelope realization without retaining its
API or decoder. The target is specified before these runtime changes.

## Ownership and behavior

Local pairs and WebSocket return addressless Endpoints carrying ground ontos
Values. The existing local-admission, ordered dispatch, detachable receive owner,
finite queue, termination and carrier-release semantics remain. IDs and source or
reply addresses are opaque consumer payloads, not endpoint fields.

`addressed(endpoint)` / `Addressed(endpoint)` presents AddressedEndpoint through
one carrier-independent pack/unpack layer. Construction attaches no receiver,
opens no resource and sends nothing. Receive attaches one decoding handler to
the existing endpoint. Detach, close and termination use that same lifetime;
the raw endpoint and facade cannot have concurrent receive owners. Malformed
addressed data fails that endpoint via its handler-failure rule. Raw delivery
itself does not interpret the addressed grammar.

`bind(addressedWire,path)` captures a relative path and exposes only Wire.send.
`under(addressedWire,prefix)` captures a prefix and concatenates later paths.
Neither proves remote membership or exposes receive/close authority. Neither has
a queue, registry, timer, retry or connection-opening behavior.

The addressed receiver delivers the full path relative to its stable local
connection root. under and bind expose sending only; they do not strip incoming
prefixes or rewrite source fields in opaque messages. Consumers define reply
paths relative to that same stable root, as system2/service/2 does.

`asAddressed(tree)` derives addressed access from a complete WireTree by exact
selection followed by one own-value send. It rejects a missing path with
MissingPathError / ErrMissingPath; refusal by a selected sender propagates.
`route(tree,path,message)` selects and invokes a receiver handler, returning false
only when the path is absent. The generic tree remains usable with any own type.

Complete tree construction, selection and decomposition preserve own capabilities
and children without invoking, binding or disposing of them. Prefix composition
and tree selection share exact byte-segment concatenation. Opaque access does
not claim child enumeration. No generic RPC, discovery, multiplexing or wire
allocation protocol is introduced.

## Expected observations

- Contract-owned raw endpoint observations pass unchanged in meaning: bare and
  unfamiliar Values, repeated admissions, detach/order, duplex sends, close,
  handler failure, outgoing refusal and bounded incoming queues.
- One addressed implementation works on a local pair and WebSocket, in Go and
  TypeScript; independent bitwire bytes match native and cross-language peers.
- Binary paths, empty self/empty-key child and segment boundaries survive.
  Capturing a prefix or send path prevents later caller mutation.
- Prefix cuts agree. Complete tree reconstruction retains own capabilities;
  structural operations invoke none. Missing routes and sender refusal differ.
- Construction is inert, receive ownership remains exclusive, malformed addressed
  data fails only after the addressed layer is attached, and close releases the
  same owned socket/server resources as raw use.

## Delivery boundary

Candidate bitruntime 0.6.0 requires the actual published bitwire 0.5.0, with no
local module replacement or checkout dependency in delivery. Both runtime
languages, package consumers and interop must pass before landing/release.
system2 then adopts the same addressless endpoints and addressed facade while
owning its service exchange fields. Existing release tags remain historical
evidence, not current conformance. The shared build service is stopped after its
5 October allowance; documented fallback checks retain every required gate.

At the candidate source review, TypeScript checking and all 18 runtime tests,
Go tests and vet, both raw/addressed Go/TypeScript connection roles, and all 76
system2 tests pass against local candidate packages. This is development evidence,
not a released dependency check. Public lock files and release validation remain
pending bitwire 0.5.0 publication; the draft PR cannot land before those gates pass.
