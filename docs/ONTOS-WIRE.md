# Shared native value migration

The owner's 4 November 2026 migration moves Go and TypeScript mixed tuple
structure to the optional `ontos/wire` package. The runtime imports public ontos
0.16.0 and bitwire 0.7.0; all raw and hydrated ground values use that same family.
`hydrated.NewTuple` and `hydratedTuple` compose the shared factory with this
runtime's leaf recognition and endpoint-to-sending-face projection. They no
longer implement or own a separate tuple class. The old runtime tuple types are
removed; inspect structure with `Items` / `items`, or the ontos native tuple type.

The factory is private. A tuple constructed under another policy is refused on
local delivery and before remote export staging. Qualified immutable children
retain their sharing; local admission does not unfold them. Ground-only tuples
still collapse to ground ontos tuples. Closed/stale capabilities remain structural
leaves; liveness is checked at the same operations as before.

The runtime retains queues and dispatch, received context, namespaces, scopes,
random export identifiers, staged whole-value admission, aliases, proxies,
ownership and lifetime. Its D8 limits still count node occurrences, depth,
atom/reference bytes and encoded frames. The separate limits of explicit
ontos/deixis conversion are not imposed on native construction or sending.

No transport bytes or negotiated protocol names change. Raw carriers and bitnode
routing still carry ground ontos frames. The pure hydrated reference/frame codec
remains in bitwire. The optional value package has no knowledge of transports,
Function, Stream, Cell or their behavior.

Qualification retains every existing vector, ownership, alias, scope, lifetime,
deep/shared value, local/network and Go/TypeScript interoperability observation.
Additional tests reject tuples from foreign qualification factories and prove
that forged containers cannot conceal endpoint ownership. There are no legacy
decoders, duplicate value implementations or compatibility tuple aliases.
