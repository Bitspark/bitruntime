# @bitspark/bitruntime/core

The TypeScript structural runtime for Bitwire 0.3.0, the `core` subpath of the
`@bitspark/bitruntime` package (v0.1.0 shipped it as `@bitspark/bitruntime-core`).
The same subpath also exports the addressed operators (`at`, `mount`,
`forward`), the local `pair`, the invocation lifecycle, `PublicError`,
`respond` and trace propagation, which docs/port-from-nightseam.md describes. It depends only on the
public Bitwire contract and implements the common finite, acyclic, byte-keyed
Deixis structure. It does not implement carriers, RPC, or receive/close ownership.

```ts
import type { Message, Wire } from '@bitspark/bitwire';
import { asAddressed, compose, select, send } from '@bitspark/bitruntime/core';

const destination: Wire = { send(message: Message) { /* admit the message */ } };
const key = new TextEncoder().encode('child');
const tree = compose(destination, [[key, compose(destination)]]);

select(tree, []) === tree;
select(tree, [key])?.own() === destination;
send(tree, [key], { frame: { version: 1, kind: 'event', data: null } });
asAddressed(tree).send(['child'], { frame: { version: 1, kind: 'event', data: null } });
```

`compose<T>(own, children)` implements `DeixisNode<T>`; `WireTree` is
`DeixisNode<Wire>`. Every node exposes `own()`, complete `children()`, partial
`at(path)`, and complete `decompose()`. `select(tree, path)` follows exactly the
same structural edges. Empty path selects self; an absent edge returns
`undefined`. Empty keys and arbitrary binary keys are valid and never normalized.
The child map is complete; child enumeration order is not part of the contract.

Composition preserves own-value and child object identity, copies mutable input
keys and child collections, and protects output keys with copies. Duplicate keys
and structural cycles throw `TypeError`. A shared subtree under multiple names
is allowed. Foreign `DeixisNode` implementations are validated and retained;
they remain responsible for the contract's finite, immutable, stable topology.
The constructor cannot freeze external implementations. Validation and selection
use iterative traversal, allowing deep finite trees without recursive stack use.

Derived sending is exactly `select(tree, path).own().send(message)`. Missing
selection throws `MissingPathError` without calling a primitive or falling back
to an ancestor. Existing primitive refusals propagate unchanged. Message and
return-capability identity are preserved. A supplied primitive implements
Bitwire's admission and asynchronous dispatch rules; this structural operator
does not create a dispatch queue or await an application result.

`asAddressed(tree)` grants the separate `AddressedWire` access interface.
Unicode-scalar string segments map to exact UTF-8 byte keys. Empty segments,
slashes, dots and distinct Unicode normalizations are preserved. Unpaired
surrogates throw `InvalidPathError` before delivery. Non-UTF-8 tree keys remain
usable through structural paths but cannot be named by this unchanged carrier
profile. The adapter exposes no receiver or lifecycle access. There is no inverse
conversion from an opaque addressed router to a complete tree.

Run `npm ci`, `npm run check`, and `npm test` at the repository root. The tests include an observation
driver against Bitwire's independently maintained full-tree oracle from
`conformance/trees/expected.json` in contract 0.3.0, plus construction, identity,
cycle, byte-key, deep traversal, refusal, and Unicode bridge checks.
