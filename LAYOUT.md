# Repository layout

Use **component first, language second**, following the family
[layout policy](https://github.com/Bitspark/bitwire/blob/main/LAYOUT.md).
This rule applies to new source and to deliberate source-layout migrations.

```text
<repo>/<component>/<lang>/...
<repo>/cmd/<command>/<lang>/...
```

A component can have a semantic hierarchy, such as `core/node/go/` or
`providers/tree/memory/go/`. Choose that hierarchy before the language
directory. Do not organize implementations as repository-root `go/` or `ts/`,
`go/<component>/`, or a language-first `packages/` tree.

Use exactly two lowercase letters: `go`, `ts`, `py`, `rs`, `hs`, `cc`,
`jv`, `sw`; other assigned codes include `rb`, `kt`, `cs` and `sh`.
Bitwire already uses `hs` for Haskell. Service SDKs and their source manifests
also follow their own language registry; register a code there before using it.

Source, native tests and language-specific package metadata belong with the
language implementation. Shared specifications, schemas, vectors, documentation,
assets and deployment configuration stay language-neutral. Repository-root
workspace/build manifests and maintenance scripts may remain at their normal
tooling locations. A source directory does not automatically require its own
module, package publication or repository.

Create a language directory only when it contains a real implementation or
contract presentation. Keep module boundaries, dependencies and release rules
consistent with the repository's charter. When moving existing source, update
imports, manifests, generators, tests, CI and documentation together; never
rewrite an immutable published release or bypass a frozen-foundation policy.

## Adoption in this repository

The runtime path is delivered in `core/{go,ts}`, `transports/{go,ts}`,
`transports/websocket/go`, `engine/{go,ts}`, `engine/websocket/go` and
`dispatch/{go,ts}`. The Go module manifest and the TypeScript package manifest
both stay at the repository root, as build manifests of one release unit;
sources, native tests and the packages' contents stay under each component's
language directory. Shared, non-public machinery lives in
`internal/<name>/go` and `core/ts/src/internal`. Language-neutral test data
lives in `vectors/`, and test-only interoperability programs in
`conformance/interop/<implementation>/<lang>`.

Later modules use the same shape: `live/{go,ts}`, `tunnel/{go,ts}`,
`telemetry/{go,ts}` and `auth-integration/{go,ts}`. Those are intended paths,
not delivered packages. Do not create empty language packages. Record a later
module's coordinates in [RELEASING.md](RELEASING.md) before it is released.

Start the first consumer migration with the
[kickoff prompt](docs/bitsystem3-migration-kickoff.md).
