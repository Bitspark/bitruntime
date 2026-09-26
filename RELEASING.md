# Releasing the structural core

The first release contains only the complete structural core. It does not
deliver transports, an invocation runtime, dispatch, an RPC engine, or the
Nightseam consumer migration.

Coordinates:

- Go module `github.com/Bitspark/bitruntime`, package `core/go`.
- TypeScript package `@bitspark/bitruntime-core`, source and manifest `core/ts`.
- The root `v0.1.0` tag versions this initial module pair together. Further
  runtime components require an explicit module/versioning decision.

Both implementations use the public Bitwire 0.3.0 contract. No dependency on a
private Deixis checkout, local replacement, or Nightseam may enter the release.

Before tagging, land a reviewed green PR on main. The CI workflow checks Go
formatting/vet/race tests, TypeScript checking/build/tests, the independent
Bitwire structural oracle, and a fresh installed TypeScript tarball consumer.
Confirm the actual Bitwire dependency release is publicly installable and run
the same checks locally where supported. Validate a fresh Go consumer against
the pushed commit, then against the final tag.

Keep `core/ts/package.json`, its lockfile and `docs/RELEASE.md` consistent. Create
an annotated root version tag on the verified main commit and push it. The tag
workflow repeats validation, packages TypeScript, and publishes the tarball plus
`SHA256SUMS` on a GitHub release. The Go module is published through the Git tag.
Verify the run, artifact checksum, fresh consumer installs and remote tag before
reporting a release.

The tarball is an npm-compatible package, not an npm registry publication.
This initial process does not claim `npm install @bitspark/bitruntime-core`
works until registry publication is separately configured and verified. Use
the GitHub release artifact URL for that package in the meantime.

Never move an existing release tag or overwrite release assets to repair a
published version. A correction gets a new version.
