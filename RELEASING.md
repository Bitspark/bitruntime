# Releasing

One root `vX.Y.Z` tag versions the Go module and TypeScript package together. bitruntime 0.7.1 implements bitwire 0.6.0: raw ontos-codec-v1 messages, the optional `bitwire/addressed/1` layer, WebSocket `bitwire.ontos.v2` and the hydrated layer `bitwire/hydrated/1` (decision 0019).

- Go module: `github.com/Bitspark/bitruntime`, packages `core/go`, `websocket/go` and `hydrated/go`.
- TypeScript package: `@bitspark/bitruntime`, subpaths `/core`, `/websocket` and `/hydrated`.
- TypeScript distribution: GitHub release asset `bitspark-bitruntime-<version>.tgz`, covered by `SHA256SUMS`. This is an npm-compatible tarball, not a registry publication. URL installs with npm 12 require `--allow-remote=root`.
- Private-scope mirror: the released asset is also mirrored, byte for byte, into the private `@bitspark` scope on GitHub Packages, for Bitspark's private consumers whose `@bitspark` scope resolves there (`.github/workflows/mirror-private-scope.yml`). The public distribution above is unchanged. The mirror package is private: its first publication from this public repository was public on GitHub Packages despite `--access=restricted`, and was set private before any consumer was granted Read. Later versions keep the package's settings.

Before tagging, land a reviewed green PR on main. CI requires Go formatting/vet/race tests, TypeScript checking/build/tests, bitwire conformance observations, a fresh installed package consumer, and Go/TypeScript WebSocket and hydrated interoperability. Run the same checks locally where supported. Confirm bitwire's exact dependency release is publicly installable, with no file dependencies, module replacements or private registries. Run `node scripts/go-smoke.mjs <pushed-commit>` before tagging.

Keep package.json, package-lock.json and docs/RELEASE.md consistent. Create and push an annotated version tag on the verified main commit. The tag workflow repeats checks, verifies a fresh public Go consumer, packs the TypeScript artifact and publishes the GitHub release. Verify the workflow, remote commit/tag, downloaded artifact checksum and fresh installed consumers before reporting completion.

Never move a published tag or overwrite release assets. Corrections receive a new version.
