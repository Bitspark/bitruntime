# Working here as an agent

Read [CHARTER.md](CHARTER.md), [the current realization](docs/ENVELOPE-RUNTIME.md), bitwire [decision 0014](https://github.com/Bitspark/bitwire/blob/v0.4.0/docs/decisions/0014-generic-envelope-wire.md) and its [carrier contract](https://github.com/Bitspark/bitwire/blob/v0.4.0/docs/wire/carriers.md).

- Implement the shared generic Wire directly. Change missing or incorrect laws in bitwire first.
- Keep service calls, errors, cancellation and streaming conventions in consumers. Do not introduce generic adapters, RPC profiles, string-path aliases or compatibility implementations.
- Preserve the distinction between opaque routing and complete byte-keyed trees.
- Test against independent bitwire observations and vectors; never infer expected bytes from this implementation.
- Use component-first paths with two-letter language directories; see [LAYOUT.md](LAYOUT.md).
- Record source attribution in NOTICE when reusing code.
- Use a branch/worktree and pull request. Review the complete change, pass required checks and squash onto main.
- Follow [RELEASING.md](RELEASING.md); no private checkout dependencies or credentials may enter a release.
- Spell Bitspark project names exactly as their repositories. Run `node scripts/naming.mjs`.
