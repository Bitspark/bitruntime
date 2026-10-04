# Repository layout

Use component first, language second: `<component>/<lang>/`, with two-letter codes such as `go` and `ts`. The current implementation is `core/{go,ts}` and `websocket/{go,ts}`. TypeScript source and tests use `src/` and `test/` within each component; Go tests sit beside their packages.

The root `go.mod` and `package.json` describe one release unit. Shared documentation stays under `docs/`. The generic interoperability peer is test-only at `conformance/interop/go`; its runner is `scripts/interop.mjs`. Test fixtures live with the tests that use them. Do not add empty packages for planned components.

Move imports, tests, manifests, build/release tooling and documentation together whenever a component moves. See [RELEASING.md](RELEASING.md) for the exported coordinates.
