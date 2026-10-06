# Layered wire realization

**Target decided 2026-10-05; implemented, release validation in progress.** This
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

The send-only prefix helper deliberately does not restore the old symmetric
origin operation. It confines destination paths under its prefix when the supplied
addressed sender obeys that contract; it does not prove comprehensive authority
attenuation. It captures the sender and path, not a remotely selected participant.
Shared exchange semantics are deferred under bitwire decision 0015's explicit
R3.1-R3.7 obligations and trigger. No cancellation or relay-composition guarantee
is inferred from prefix concatenation.

`asAddressed(tree)` derives addressed access from a complete WireNode by exact
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

Research 0006 R25b is tracked against the capabilities actually supplied here.
The node-boundary batteries exercise the published deixis 0.6.0 byte-keyed cores
and the independently declared bitwire Atom-keyed interfaces in TypeScript and
Go. Hand-written expected paths cover binary and empty keys, separator bytes,
every path cut, capture of caller/returned key buffers, complete reconstruction
and live-slot aliasing. Mutable execution comparisons use separate corresponding
fixtures. Remote unknown paths are admitted locally, then report structural
absence only at the receiver; they neither fall back nor poison valid traffic.

These are the two implemented runtime languages. The other bitwire presentations
declare interfaces and check package consumption; no concrete tree implementation
or cross-package runtime conformance is claimed for them. Their first concrete
implementations must run the same structural observations. Multi-hop relays,
mount-crossing exchanges, cancellation and exported live wires remain absent:
their R25b lifecycle/relay families are required when their separately specified
protocols are introduced, under decision 0015's triggers. Existing endpoint
queue saturation, close and disconnect checks are not evidence for a relay chain.

## Delivery boundary

Candidate bitruntime 0.6.0 requires the actual published bitwire 0.5.0, with no
local module replacement or checkout dependency in delivery. Both runtime
languages, package consumers and interop must pass before landing/release.
system2 then adopts the same addressless endpoints and addressed facade while
owning its service exchange fields. Existing release tags remain historical
evidence, not current conformance. Shared build service admission and qualification
are checked before submission; while this repository's full gate is unqualified,
the documented local/hosted fallback retains every required check.

The 6 October check exposed an intermittent forced-close race in Go. The
WebSocket library's closing handshake can acquire its own read context, so
cancelling the endpoint context alone cannot enforce CloseTimeout. Dial now
retains the acquired upgraded response stream, and Accept retains its hijacked
socket; the deadline closes that endpoint-owned stream directly. Caller-owned
HTTP transports and servers remain open. The unchanged closure contract requires
this fix. Both server-side and repeated client-side silent-peer tests pass.

At dependency commit fb8c7d5, the lock files resolve published bitwire 0.5.0 and
deixis 0.6.0 from public npm and Go sources. A fresh npm installation, TypeScript
checking, all 21 runtime tests, Go tests and vet, the fresh packed consumer and
both raw/addressed Go/TypeScript connection roles pass without replacements.
The Linux race check and exact final CI revision remain integration gates.
Earlier 76-test system2 evidence used candidate packages; system2 must still
validate the actual runtime release before its own local integration.
