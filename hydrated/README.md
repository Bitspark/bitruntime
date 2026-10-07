# Hydrated wires (evidence for bitwire decision 0019)

**Status, 7 October 2026:** evidence for the proposed
[bitwire decision 0019](https://github.com/Bitspark/bitwire/pull/83), the first
edition of the hydrated wire protocol. This is not a released API, and this
branch is not proposed for merge until the record is accepted. The
[package](go/hydrated.go) realizes the record's rules in Go. [Its tests](go/hydrated_test.go)
run the record's observations 1-15 and 17 around an opaque middle router, over local
pairs and WebSocket where the carrier matters.

The [TypeScript mirror](ts/src/index.ts) repeats observations 1-4 and 6-9
([tests](ts/test/hydrated.test.mjs)), with 2, 3, 4 and 7 over both carriers.
[scripts/hydrated-interop.mjs](../scripts/hydrated-interop.mjs) runs observation
14 over a real WebSocket in both roles, Go serving TypeScript and TypeScript
serving Go. Both peers build from this repository, not from fresh published
dependencies, so they show the grammar interoperates, not package delivery.

The record's vectors are copied from bitwire#83 (revision 2) into
[testdata](go/testdata/hydrated-vectors.json), pinned by sha256; both languages
replay them.
