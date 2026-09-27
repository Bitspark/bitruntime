# Working here as an agent

Read the [charter](CHARTER.md), and in bitwire read
[decision 0007](https://github.com/Bitspark/bitwire/blob/main/docs/decisions/0007-using-bitwire-never-requires-nightseam.md),
[decision 0010](https://github.com/Bitspark/bitwire/blob/main/docs/decisions/0010-bitwire-holds-the-contract-and-bitruntime-implements-it.md),
[decision 0012](https://github.com/Bitspark/bitwire/blob/main/docs/decisions/0012-explicit-data-and-wire-trees.md)
and the [carrier specification](https://github.com/Bitspark/bitwire/blob/main/docs/wire/carriers.md),
before changing this tree.

- Implement the bitwire contract; never redefine it. If the contract or a
  specification looks wrong, raise it in bitwire, don't work around it here.
- Use `Wire.send(message)` and full `WireTree = DeixisNode<Wire>`, symmetric
  with bitstore's `Data.read()` / `DataTree = DeixisNode<Data>`. The old
  path-taking access is explicitly `AddressedWire`. Never disguise an opaque
  router as a full tree or implement the superseded `End` naming proposal.
- Depend on bitwire and, in their own modules, on transport libraries. Never
  depend on nightseam, bittype or bitlink.
- When porting from nightseam:
  - port from an identified commit;
  - keep the released v0.6.0 apart from its unreleased commits;
  - record the provenance in `NOTICE`;
  - add no aliases or re-exports.
- Test against bitwire's independent cases and vectors. Never make an
  expectation match what the code happens to do.
- Treat nightseam's recorded defects (nightseam#720–#724) as acceptance criteria
  for the ported code.
- After the initial bootstrap, use a branch or worktree and a pull request.
  Squash a green change onto `main`.
- Do not add private checkout dependencies, local orchestration state or
  credentials.

## Naming

Write every Bitspark project exactly as its repository is named, in prose,
headings, comments and messages alike: `bitwire`, `nightseam`, `bitruntime`,
`bitsystem3` (the org's
[naming rule](https://github.com/Bitspark/.github/blob/master/NAMING.md)). CI
runs `node scripts/naming.mjs`; refresh its name list with
`node scripts/naming.mjs --update`.

## Repository layout

Use component-first source paths with two-letter language directories:
`<component>/<lang>/` and `cmd/<command>/<lang>/`. Read [LAYOUT.md](LAYOUT.md)
for the shared codes, current paths and migration boundaries. Apply it to new
components and ports; an existing path moves only with its imports, manifests,
tests and tooling. Preserve the repository's ownership and release rules.
