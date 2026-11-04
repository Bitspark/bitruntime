# bitruntime 0.8.0

This release uses bitwire 0.7.0 and public ontos 0.16.0. The optional ontos/wire
package owns the Go and TypeScript native mixed-value structure. The runtime
supplies its private capability-recognition and sending-face policy to that
factory. See the [migration contract](ONTOS-WIRE.md).

The runtime's duplicate tuple implementation and exported tuple class/type are
removed. Use `hydratedTuple` / `NewTuple` for runtime-qualified construction and
`items` / `Items` for observation. Ground values come directly from the public
ontos packages; the old bitwire ground subpaths and re-exports are removed.

Transport bytes and protocol names remain unchanged: ontos-codec-v1,
bitwire/addressed/1, bitwire.ontos.v2 and bitwire/hydrated/1. Export/import,
namespace, ownership, lifetime, liveness, context, queue, dispatch and D8 counting
rules remain runtime responsibilities. Foreign-factory tuples cannot hide owner
handles from local or remote admission. Deep and shared native values retain
their existing operational behavior.

The complete release gate retains Go race tests, TypeScript checks and tests,
independent vectors, composition conformance, bidirectional Go/TypeScript
interoperability, and fresh installed consumers. Go is distributed through the
root tag; the TypeScript archive is a GitHub release asset with SHA256SUMS.

This is a coordinated source/dependency cutover. It does not deploy existing
services or add Function, Stream or Cell semantics to the runtime.
