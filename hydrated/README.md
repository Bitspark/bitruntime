# Hydrated wires (evidence for bitwire decision 0019)

**Status, 7 October 2026:** evidence for the proposed
[bitwire decision 0019](https://github.com/Bitspark/bitwire/pull/83), the first
edition of the hydrated wire protocol. This is not a released API, and this
branch is not proposed for merge until the record is accepted. The
[package](go/hydrated.go) realizes the record's rules in Go. [Its tests](go/hydrated_test.go)
run the record's observations 1-13 around an opaque middle router, over local
pairs and WebSocket where the carrier matters.

The record's vectors are copied from bitwire#83 at `20a6b6b` into
[testdata](go/testdata/hydrated-vectors.json), pinned by sha256. Observation 14
(Go and TypeScript peers with fresh published dependencies) needs a TypeScript
realization and is not run here.
