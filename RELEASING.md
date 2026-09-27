# Releasing

A release is one root tag that versions the Go module and the TypeScript
package together. Each release states the bitwire contract version it
implements, the protocol revisions it speaks and the conformance evidence it
passed (charter §2); `docs/RELEASE.md` is the release's notes.

Coordinates:

- Go module `github.com/Bitspark/bitruntime`, distributed by its root `vX.Y.Z`
  tags, with the packages `core/go`, `transports/go`, `transports/websocket/go`,
  `engine/go`, `engine/websocket/go` and `dispatch/go`.
- TypeScript package `@bitspark/bitruntime`, whose manifest and lockfile are at
  the repository root. Its sources stay under `<component>/ts/src`, and it
  exports the subpaths `/core`, `/transports`, `/engine` and `/dispatch`.
  v0.1.0 shipped the structural core alone as `@bitspark/bitruntime-core` from
  `core/ts`.
- The driver-1 testees of bitwire's `bitwire/1` conformance contract
  (`cmd/bitwire-testee`). The Go testee is the module's package
  `cmd/bitwire-testee/go`, released by the same tag. The TypeScript testee is
  its own release asset, `bitspark-bitruntime-testee-<version>.tgz`, packed by
  `scripts/testee-package.mjs`; it depends on the package by this release's
  asset URL, so no testee code ships in the package. `SHA256SUMS` covers both
  tarballs, and bitwire pins them as it pins the module and the package.
- A later component records its module boundary here before it is released.

Both implementations use the public bitwire 0.3.0 contract. No dependency on a
private deixis checkout, local replacement, or nightseam may enter the release.
The nightseam v0.6.0 interoperability programs are test-only: their Go module is
nested under `conformance/interop` and their npm package is never packed.

Before tagging, land a reviewed green PR on main. The CI workflow checks Go
formatting/vet/race tests, TypeScript checking/build/tests, the independent
bitwire structural oracle, a fresh installed TypeScript tarball consumer, the
two testees across languages from source and from their packed tarballs
(`scripts/testee-smoke.mjs`), and interoperability with nightseam v0.6.0 peers
in every Go/TypeScript pairing, byte for byte. Run bitwire's conformance runner
against the testees in every Go/TypeScript pairing before a release that
changes the engine or the testees.
Confirm the actual bitwire dependency release is publicly installable and run
the same checks locally where supported. Validate a fresh Go consumer against
the pushed commit, then against the final tag.

Keep the root `package.json`, its lockfile and `docs/RELEASE.md` consistent. Create
an annotated root version tag on the verified main commit and push it. The tag
workflow repeats validation, packages TypeScript, and publishes the tarball plus
`SHA256SUMS` on a GitHub release. The Go module is published through the Git tag.
Verify the run, artifact checksum, fresh consumer installs and remote tag before
reporting a release.

The tarball is an npm-compatible package, not an npm registry publication.
This process does not claim `npm install @bitspark/bitruntime` (or
`@bitspark/bitruntime-core`) works until registry publication is separately
configured and verified. Use
the GitHub release artifact URL for that package in the meantime. npm 12
installs a URL dependency only when the consuming project allows it, so the
README's install instructions name `allow-remote=root`; verify the fresh
tarball install with npm 12 as well as with the npm bundled beside node.

Never move an existing release tag or overwrite release assets to repair a
published version. A correction gets a new version.
