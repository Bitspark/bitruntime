# bitruntime v0.1.0

The first release implements the shared structural core over the public
Bitwire 0.3.0 contract in Go and TypeScript:

- Full generic Deixis tree construction, own values, complete exact-byte-keyed
  children, partial selection, decomposition and reconstruction.
- `WireTree = DeixisNode<Wire>` and derived sending through the selected
  node's own addressless `Wire.send(message)`.
- Explicit addressed access over a complete tree for the existing UTF-8
  string-path carrier surface, with missing-path refusal and no lifetime
  ownership transfer.
- Independent Bitwire structural observations plus native edge-case tests.

This release does not include carriers, protocol engines, dispatch, live
references, tunnels or a completed bitsystem3/Nightseam migration. An opaque
`AddressedWire` cannot be converted into a full tree without a complete
declaration. Data reading and storage remain Bitstore's responsibility.

Go is available through the `v0.1.0` module tag. The TypeScript package is
published as a GitHub release tarball with a SHA-256 checksum; it is not yet
published to an npm registry.

## Unreleased: one TypeScript package

The next TypeScript version, 0.2.0, is one package, `@bitspark/bitruntime`,
built from the repository root. It replaces `@bitspark/bitruntime-core` and
exports four subpaths: `@bitspark/bitruntime/core` (the structural core, the
addressed operators, the local pair and the invocation lifecycle),
`/transports`, `/engine` (the `bitwire/1` peer) and `/dispatch`. It is not
released yet.
