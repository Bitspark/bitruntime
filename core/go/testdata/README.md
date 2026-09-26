# Structural oracle provenance

`tree-observations.json` is copied unchanged from Bitwire's independent
[`conformance/trees/expected.json`](https://github.com/Bitspark/bitwire/blob/v0.3.0/conformance/trees/expected.json).
`TestBitwireStructuralOracle` makes the observations against this runtime's
production `Compose`, `Select`, and `Send`, rather than the Bitwire test-only
interpreter. Expected observations must change in Bitwire first, with the
corresponding contract decision; they are never regenerated from this runtime.
