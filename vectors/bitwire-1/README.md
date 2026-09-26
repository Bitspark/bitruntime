# bitwire/1 envelope vectors

Byte-identical copies of Nightseam v0.6.0's profile tables, taken from
`conformance/tables/` at `5cc9723a24646c40ed1861f892b2b23eb6d785d7` (tag
`v0.6.0`). Bitwire decision 0008 defines `bitwire/1` as that release's
behavior, so these rows state what a `bitwire/1` peer accepts and refuses.

| File | Holds |
| --- | --- |
| `frames.json` | Envelopes a peer of each role accepts or refuses |
| `serials.json` | Request-serial sequences a receiver admits or refuses |
| `unicode.json` | JSON texts whose strings are, or are not, Unicode scalar values |

The Go and TypeScript native tests read these files. They are implementation
evidence, not Bitwire's independent conformance: Bitwire publishes the
normative tables and manifest of `bitwire/1` under its issue #39, and its
cases judge released bitruntime from a test-only module. When that
publication exists, these copies are replaced by it rather than edited.
