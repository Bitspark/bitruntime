# Recursive live-Wire hydration spike

**Status, 7 October 2026:** disposable TypeScript experiment for
[issue 39](https://github.com/Bitspark/bitruntime/issues/39). This is evidence for
the contract owners, not a released API, a production bitnode router, or an
accepted protocol. The root package exports and ontos value definitions are
unchanged. Production work still follows the
[composition plan](../../docs/COMPOSITION.md).

## Question and owner direction

The owner requested a spike of this fiber path: a programmatic API has a
domain-specific adapter to a hydrated Wire, whose messages may recursively
contain live Wires. Shared machinery turns those messages into addressed ground
ontos data. The bitnode network routes that data, and the destination reverses
the transformations. Domain adapters must not allocate references, maintain
export tables, declare reference positions or implement hydration themselves.

```text
Echo API                                             Echo provider
   | domain adapter                                     | domain adapter
live Wire + recursive live values                    live Wire + live values
   | shared hydration                                   | shared hydration
addressed ground ontos ---- opaque middle router ---- addressed ground ontos
   | addressless Endpoint                               | addressless Endpoint
   +-------------- local pair or WebSocket --------------+
```

There are two physical hops, caller to middle node and middle node to provider.
The same domain and hydration code runs for both carrier arrangements.
[The domain fixture](ts/src/echo.ts) imports only the live-value representation
and ground Atom constructors; it knows nothing about routing or references.
[Hydration](ts/src/hydration.ts) accepts an AddressedWire and incoming dispatch.
The [test composition](ts/test/harness.mjs) owns Endpoint reception, routing and
scope lifetime. Every hop uses the released addressed facade over a raw Endpoint.

## What the experiment establishes

The caller adapter places a reply Wire inside a nested tuple. The provider
receives a usable live proxy and replies through it, including another Wire
in its response. That continuation then carries a third Wire in the opposite
direction. All four traversals use the same hydration machinery. The middle
node forwards a destination and opaque ground value; it creates no reference
entries and inspects no application data.

A locally exported Wire returned to its originating runtime hydrates to the
original object. Repeated occurrences hydrate to the same imported proxy.
Forwarding an imported proxy preserves its reference rather than registering
a proxy of that proxy. Its representation is send-only: it grants neither
Endpoint receiving nor carrier close ownership.

The 14 observations in [the hydration tests](ts/test/hydration.test.mjs) cover this
recursive exchange over local pairs and actual loopback WebSockets, data that
resembles a reference, aliasing, exact empty/binary/slash-containing path atoms,
replacement at the same route, withdrawal, carrier-bound scope cleanup, failed
encoding/decoding and finite value, registry and active-work limits. The stale
reference test deliberately reuses an export ID at the same route in a new
scope; it observes refusal and no call to the replacement target.

Local Windows validation passed all 16 hydration and composition observations
with Node 24.21.0 after type checking. CI repeats both suites on Linux and
Windows. Exact reviewed commits and CI runs belong in the pull request; this source-level account does not imply
that a later revision has passed. Existing repository CI remains required.

## Composing generic API adapters

The owner's follow-up asks whether an adapter for `API3<Wire>` can be reused for
`API3<T>` by composing it with the adapter for `T`. The necessary step is to
lift the entity adapters through API3's operations:

```text
adapter3 = adapter3_ composed with lift_API3(adapterT)
```

[The Slot fixture](ts/src/slot.ts) tests a stronger case than a read-only list:
`Slot<T>` both returns T from `read()` and accepts T in `write(T)`. A produced T
uses `T -> LiveWire`; an accepted T needs `LiveWire -> T`. Its `adaptSlot` lifts
both directions. `slotToWire` only knows `Slot<LiveWire>`, and is composed with
the independent entity adapters by `slotOfToWire`.

[Two composition observations](ts/test/composition.test.mjs) use a separate Text
API as T, through the same two-hop tree over both carriers. Reading returns a
remotely callable Text. Writing passes a caller-provided Text to the provider,
which can call it and return it again. Neither adapter knows the other's
operation protocol or the hydration machinery.

This is evidence for composition preserving the API's observations, not a proof
for every programmatic type. A covariant API such as a read-only collection
needs only the forward mapping; mixed input/output APIs need both directions.
Generic type syntax alone does not supply those mappings. Object identity and
byte-for-byte encoding equality are not implied by behavioral equivalence.

## Experimental choices, not new upstream laws

`LiveValue` is an extension **above** ontos: Atom, Tuple, a recursive LiveTuple,
or a LiveWire. A Wire is never an ontos ground Value. `LiveWire` is a deliberately
small nominal class for this experiment, not a decision about public types or
an additional production Wire interface. A future contract could express the
relationship using parameterized types; that deserves an upstream decision.

Every live value is encoded as a tagged data node. Atoms and tuples are tagged
as data too, so a data tuple cannot accidentally become a capability because
of its contents. The disposable grammar is:

```text
message       = ("hydration-spike/1", destinationScope, destinationID, liveData)
liveData      = (0x00, atom)
              | (0x01, (liveData ...))
              | (0x02, ((ownerPath ...), ownerScope, exportID))
```

All fields are ground ontos values; byte tags are one-byte atoms. The outer
destination is passed separately to AddressedWire.send. The addressed facade
then supplies its already-released data framing. These experimental bytes are
not proposed canonical conformance vectors.

The production owner's alternative, ground data plus an explicit table of
reference positions, can also separate data from capabilities. It avoids a tag
around each ordinary data node but needs rules for duplicate/overlapping
positions and placeholders. This spike chooses the fully tagged form to make
the separation easy to inspect, not to settle that encoding tradeoff. Neither
form requires a new kind in ontos or scanning ordinary payload tuples for
reference-shaped values.

A reference names an owner at an absolute path in one admitted tree, its
random 16-byte incarnation scope, and a never-reused ID within that scope.
The owner retains the target. All holders can route to that owner without
intermediate export tables. This is a concrete counterexample to requiring
per-hop re-export for every transit node. It supports end-to-end hydration
within the chosen namespace, not transparent rewriting across different
namespaces or live re-rooting. A gateway between namespaces needs an explicit
composition; the experiment does not pick its contract.

## Lifetime and admission meaning

The composition owns the hydration scope and closes it with its carrying
attachment. Closing the scope clears its imports and exports and makes its
proxies unusable. The export owner can withdraw a target without closing that
target; a future export gets a fresh ID. Work already admitted into a local
target is allowed to finish. Closing a scope does not cancel a domain operation.

Imports are interned and retained until scope close; there is no remote release
message. A holder cannot safely revoke other holders just because it discards
its own proxy. Likewise, a lost intermediate connection does not by itself
prove that every holder of an end-to-end reference vanished. This implementation
uses bounded retention and explicit owner withdrawal as the experiment's
policy. Distributed release/collection for long-lived scopes remains a concrete
production requirement. It must be derived from ownership and delegation, not
hidden in the domain adapter or guessed from JavaScript garbage collection.

**Observed limit:** with the fixture's default limits, one long-lived caller
using one callback completes 63 Echo calls; call 64 rejects with `export limit`.
Both ends then retain 64 exports and 64 imports. Every completed call has left
its reply and continuation associations in those scopes. The adapters have no
reference bookkeeping with which to end a finished reply. The regression
observation records this limit explicitly: bounded retention demonstrates the
construction but is not adequate reclamation for a long-running service.
The production contract must derive a usable target lifetime and import policy
while retaining the domain adapter boundary.

Encoding and decoding stage registry changes until the entire value validates.
Exports are registered before bytes are admitted so a fast reply can find them.
A carrier send failure leaves those exports retained until owner cleanup; there
is no assumption that the provider has or has not acted. Registry sizes and
active sends/receives are finite, and each value has node, depth and byte limits.
The test router's traces and diagnostics are test observations, not production
queues or evidence of aggregate deployment resource bounds.

`send` resolving means local admission under the carrying Wire contract. A
remote stale-reference refusal is a host diagnostic, not an automatically
correlated response to that Promise. The Echo adapter chooses a result message
and its own fixture timeout. Invocation, outcomes, cancellation and streaming
still belong to domain conventions above the communication layer.

## What should change in the owning concepts

| Owner | Finding and proposed follow-up |
| --- | --- |
| bitwire | Specify the recursive live-value/Wire boundary and its relation to ground Wire/AddressedWire, including unambiguous data encoding and reference lifetime. A manual export table alone does not fulfill the owner's fiber layering. Keep raw/addressed separation, exact byte paths and ontos ground meanings intact. |
| bitruntime | Realize generic hydration together with registries, imported send-only proxies, bounds and cleanup. Carriers remain raw Endpoints; reuse the addressed facade. Production Go and TypeScript, independent upstream cases and interoperability remain necessary. |
| bitnode | Compose those runtime facilities with the selected tree namespace, attachments and admission. Opaque transit routing does not require domain decoding or re-export at every hop. Its concrete routing/binding realization still needs qualification. |
| Domain consumers | Adapt typed APIs to live messages and choose operation/reply semantics. A nested reply Wire replaces manually assigned reply-path/correlation machinery for this fixture. No registry or serialized-reference API is required in that adapter. |

This revises the incomplete export-only construction, not deixis's tree laws.
Addressing routes a message; hydration carries a live send capability as data
and reconstructs a local proxy. Multiplexed Endpoints remain a separate facility
when consumers need independent receive/close lifetimes on shared carriers.
This example needs no logical Endpoint per exported Wire.

The tree in this experiment is explicitly admitted test composition. Neither
knowledge of a path nor a random scope authenticates an actor or authorizes a
domain operation. Production attachment/authority integration stays with
bitnode and native archon/thesmos; this test does not introduce another identity
or permission model. It also does not establish browser support, network
partitions, cross-process/cross-language hydration or production throughput.

## Reproduce and inspected inputs

From `spikes/hydration/ts`, with Node 24:

```sh
npm run fetch
npm ci --ignore-scripts
npm run check
npm test
```

The private package pins bitwire 0.5.0 and downloads the public bitruntime
0.6.0 release artifact, checking both its pinned SHA-256 and release checksum
list. It imports no sibling checkout source and is excluded from the runtime's
published package. The download helper was written for this experiment.

- bitwire 0.5.0 source: `5bf2737b16fa6ae8c1f5c955cdba658d5adce364`.
- bitruntime 0.6.0 source: `b19458f6f741a0f78729e89ef099bfd2869900a2`;
  artifact SHA-256 `da651aee6f14cc5a1116af55ab1bd575650a562ea6efc08f14dc840cff8d9c19`.
- bitruntime composition baseline: `0bc971fd1512457d0591ace825e2ffd9dd0573cd`.
- The production owner's connection-scoped implementation was inspected at
  `a383393` on `lane/02-3b-export`. Its table/importer work is useful lower-level
  evidence but lacks recursive hydration. No code from that implementation is
  copied here and its pending protocol is not adopted.
- Owner clarification and scope coordination are preserved in platform-board
  topic record `40a333f0-cac3-4346-9e43-4136b0fd91f0` and owner response
  `b810d7a2-b875-464f-ac22-2565072652f9`. The independent assessment supplied by
  the owner agrees with this intended boundary; it is not runtime evidence.
