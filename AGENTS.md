# Working here as an agent

Read [CHARTER.md](CHARTER.md), [the current realization](docs/WIRE-RUNTIME.md), and
bitwire decision 0015 with its wire/carrier contracts at the pinned dependency.

- Implement addressless Endpoints in carriers and one reusable addressed layer above them. Wire grants sending; Endpoint adds receive/close ownership; WireTree adds complete structure. Change missing or incorrect laws in bitwire first.
- Keep service calls, errors, cancellation and streaming conventions in consumers. Do not introduce competing generic interfaces, RPC profiles, string-path aliases or compatibility implementations. The addressed layer connects different abstraction levels under bitwire decision 0015.
- Preserve deixis's generic identity: sender and receiver trees are equally valid instances. Runtime convenience cannot silently amend its structural laws or bitwire's charter boundaries.
- Preserve the distinction between opaque routing and complete byte-keyed trees.
- Test against independent bitwire observations and vectors; never infer expected bytes from this implementation.
- Use component-first paths with two-letter language directories; see [LAYOUT.md](LAYOUT.md).
- Record source attribution in NOTICE when reusing code.
- Use a branch/worktree and pull request. Review the complete change, pass required checks and squash onto main.
- Follow [RELEASING.md](RELEASING.md); no private checkout dependencies or credentials may enter a release.
- Spell Bitspark project names exactly as their repositories. Run `node scripts/naming.mjs`.
