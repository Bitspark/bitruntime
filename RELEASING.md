# Releasing

One root `vX.Y.Z` tag versions the Go module and TypeScript package together. bitruntime 0.6.0 implements bitwire 0.5.0: raw ontos-codec-v1 messages, the optional `bitwire/addressed/1` layer and WebSocket `bitwire.ontos.v2`.

- Go module: `github.com/Bitspark/bitruntime`, packages `core/go` and `websocket/go`.
- TypeScript package: `@bitspark/bitruntime`, subpaths `/core` and `/websocket`.
- TypeScript distribution: GitHub release asset `bitspark-bitruntime-<version>.tgz`, covered by `SHA256SUMS`. This is an npm-compatible tarball, not a registry publication. URL installs with npm 12 require `--allow-remote=root`.

Before tagging, land a reviewed green PR on main. CI requires Go formatting/vet/race tests, TypeScript checking/build/tests, bitwire conformance observations, a fresh installed package consumer and Go/TypeScript WebSocket interoperability. Run the same checks locally where supported. Confirm bitwire's exact dependency release is publicly installable, with no file dependencies, module replacements or private registries. Run `node scripts/go-smoke.mjs <pushed-commit>` before tagging.

Keep package.json, package-lock.json and docs/RELEASE.md consistent. Create and push an annotated version tag on the verified main commit. The tag workflow repeats checks, verifies a fresh public Go consumer, packs the TypeScript artifact and publishes the GitHub release. Verify the workflow, remote commit/tag, downloaded artifact checksum and fresh installed consumers before reporting completion.

Never move a published tag or overwrite release assets. Corrections receive a new version.
