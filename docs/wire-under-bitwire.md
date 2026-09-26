# Exploration: Wire underneath Bitwire

**Status: design exploration, 2026-09-26; not an accepted API decision.**
The maintainer asked whether the direction could be stated as
`Bitwire = Deixis[Wire]`, with an addressless Wire primitive underneath Bitwire.

Yes. This is a viable interpretation of the A0/A1 direction in
[Bitwire #42](https://github.com/Bitspark/bitwire/issues/42), and is explicitly
close to [Deixis #49](https://github.com/Bitspark/deixis/issues/49).
It separates the names and responsibilities more clearly than calling both
levels Wire. It still needs a precise contract and crossing evidence.

## The decomposition

```text
Wire                         addressless sending access at one origin
Bitwire = Deixis[Wire]        own Wire plus keyed child Bitwires
RPC / typed adapters         operations, correlation, outcomes, domain types
```

Deixis contributes structure, not execution policy:
`Node[T] = T × FinMap[Bytes, Node[T]]`. For a fully declared local tree,
instantiate T with an addressless sending capability. Every node has its own
Wire, including a refusing Wire where no operation is offered at that position.
The node can also have named children.

Conceptually, and without committing to public names:

```text
Wire.send(message)                         -> admission or refusal
Bitwire.at(path)                           -> selected Bitwire access
Bitwire.origin()                           -> addressless Wire access
Bitwire.send(path, message)                -> derived convenience operation
```

For an admitted, statically bound tree and a present path:

```text
send(tree, path, message) =
    origin(select(tree, path)).send(message)
```

This equation is semantic; it does not require an RPC per segment, expose owner
parts, or require materializing a remote tree. A selected access can retain a
prefix and delegate on use. Missing-path selection needs a specified refusal
behavior; Deixis's partial navigation and the current total selected-view syntax
are different surfaces.

The current public `Wire.Send(path, message)` could therefore remain as an
addressed Bitwire operation implemented through the structural layer. It would
cease to be the primitive. Retaining a useful derived API is different from
fixing it as an axiom that the new primitive must obey.

## What already exists

[Decision 0006](https://github.com/Bitspark/bitwire/blob/main/docs/decisions/0006-declared-composites-realize-deixis-nodes.md)
defines `Origin = Message → Admission` and
`Parts = Origin × FinMap[Segment, Access]`. An origin never sees a path;
composition consumes one child segment and delegates the unchanged message.
This gives the proposed addressless primitive a concrete predecessor.

That decision's children are complete addressed access, including opaque
children. It realizes Deixis for admitted declarations; it does not assert that
every behavior accessible by a path is an enumerable tree.
[#42](https://github.com/Bitspark/bitwire/issues/42) reopens the API allocation,
while [#49](https://github.com/Bitspark/deixis/issues/49) asks for precise own-value
representations, binding contexts and opaque-child boundaries for Wire and Bytes.

Thus the decomposition has already been explored structurally. The explicit
public naming `Wire` / `Bitwire`, a minimal addressless API, and the complete
migration contract have not been delivered by those documents.

## Consequences that need decisions

### A tree value is not the same thing as access to it

For fully declared structures, `Deixis[Wire]` is literal. Public Bitwire access
is a capability to use such a structure, potentially remote or opaque. Sending
access must not grant enumeration, decomposing parts, reassembly authority,
receiver attachment or closure ownership.

A finite tree of raw origin Wires alone cannot represent every current opaque
path-dispatching endpoint, such as one whose descendants are discovered on use.
The contract must either state an admitted structural subset or provide explicit
opaque subtree access and its binding laws. Treat that as an interpretation of
the structural model; do not silently redefine a finite Deixis node to mean an
arbitrary router.

A parent owns/retains named child Bitwire capabilities. It need not own the
child's endpoint lifetime or be able to inspect the child's structure.

### Addressless does not necessarily mean raw bytes

Removing the path parameter yields an addressless message capability. It does
not by itself settle whether the message contains bytes, typed envelopes,
return capabilities or context evidence. Today's Bitwire Message is tied to a
four-kind RPC profile; copying it unchanged would keep that profile commitment.

Specify the minimal delivery/message contract explicitly. Request IDs,
response/error interpretation and cancellation belong to the profile that needs
them. A generic relay carries their representation without interpreting
application operations. The Wire primitive need not become a TCP socket.

### Replies and cancellation can be addressed capabilities too

An ordinary reply can use an addressless Wire. A returned object with methods
can expose Bitwire access. A profile that currently sends control operations to
paths beneath a return origin must explicitly retain an addressed return
capability, derive distinct raw capabilities for those operations, or define an
appropriate control-message profile. Merely deleting the path argument is not
a complete migration.

Keep return-capability identity and received context intact through pure local
selection/composition. Carrier crossing establishes its own bound capabilities
and receiving evidence. Do not reconstruct a wrapper around every local reply
just to make the new types fit.

### Deixis does not decide asynchronous lifetime

Selection, mounting and reconstruction are structural. Admission, route capture,
queued delivery, cancellation, actual executing-body completion and retirement
still need owners and public facilities in the runtime/profile.

If a selected subtree or handler is replaced, a previously captured invocation
must continue to address its original receiver. A late cancellation must not
reach the replacement. Closing a borrowed parent view must not close every
child. A runtime must bound both admitted work and retained routing/control
state. None of these follows simply from `Deixis[Wire]`.

### Native APIs and the wire protocol can change independently

A derived addressed adapter can retain the current bitwire/1 envelope and path
encoding while using the new primitive locally. That is a possible migration
strategy, not interoperability proof. A path carried inside an opaque frame is
interpreted by the addressed adapter, not by the raw Wire relay.

Define where refusal occurs: accepting a frame into a transport queue is not
proof that a remote path exists or an application mutation completed. Preserve
the selected profile's admission/capture observations in the migration tests.

Do not insert a revision-2 handshake into bitwire/1. Changing native types,
changing protocol bytes and changing the meaning of an immutable profile are
separate decisions.

### The structural layer can be shared with Bytes

The same Deixis operations can lift Wire into addressed sending access and Bytes
into addressed byte content. This shares key/path laws, own values, selection
and reconstruction. It does not equate wire lifetime with storage durability,
make live endpoints serializable, or identify arbitrary blob hashes as Deixis
roots. Those bindings remain explicit and independently tested.

## What this means for Bit System

A space can have its own message endpoint for model operations and named child
space accesses:

```text
space
  own Wire: messages about this space's ID, facts and relationships
  children[name]: Bitwire access to that child space
```

Whether operation names themselves occupy structural children or live in an
operation profile is a separate namespace decision. Avoid accidentally mixing
model child names with reserved API method names.

A child's implementation does not need its parent's prefix. Selecting and
remounting its Bitwire access preserves the child's identity and local behavior.
The browser can use the same addressed abstraction as a parent uses for a child.
This removes a separate navigation model at those boundaries, provided both
bindings preserve the same laws.

The database still stores IDs, facts, parent relations and child-name mappings.
Live Wires are bound from that durable model; the structural analogy does not
persist endpoints or deliver cross-host transactions.

## Recommendation to take into the kickoff

Evaluate `Wire` as A0 and `Bitwire` as the addressed A1 API explicitly before
choosing names or package coordinates. Keep convenient addressed sending as a
derived operation where useful. Use existing origin/composition observations as
evidence, then specify the opaque subtree, reply/control and lifecycle boundaries
with independent cases.

This does not by itself justify another repository. The primitive and addressed
contract can be components of Bitwire's specification repository; bitruntime
implements them, and the structural core remains Deixis's. Use component/language
paths for any resulting components.

Using a shared Deixis implementation may introduce a library dependency that the
current charters do not name. Record the resulting dependency graph and any
charter amendment explicitly. The mathematical construction alone neither
requires a particular package dependency nor authorizes adding one silently.

The decision record should compare this proposal with making the public Bitwire
surface itself addressless. State the public names, admitted structures, ownership,
message/profile allocation, required version transitions and consumer mapping.
Do not treat the exploration as approval of either exact API.
