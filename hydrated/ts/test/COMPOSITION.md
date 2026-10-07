# Consumer adapter composition observations

**Candidate acceptance, 7 October 2026.** This package tests the composition
proposal in bitwire PR85 against the proposed decision 0019 implementation.
The starting runtime is `3adb89cae4daef71501b2b2ee88228b39bb42235`; the proposed
contract is bitwire PR83 at `20a6b6b6c0b26fe16c8e57a4f88241207119880c`.
Neither being testable nor passing these observations adopts that contract.

## Boundary and expected observations

The fixtures define two independent domain APIs: Text with a string-producing
operation, and Counter with stateful numeric operations and a declared failure.
One Cell-of-Wire convention supplies get/set. Its bidirectional lifting composes
either inner adapter, then composes itself for Cell<Cell<Text>>. Test direct and
adapted executions from corresponding initial state using expected results and
provider traces; construction must invoke no provider operation.

Run each transport observation using the existing opaque-middle harness over
both local pairs and real loopback WebSockets. Domain fixtures import only the
hydrated endpoint/value API and ground atoms, never Scope, references or routes.

1. Reuse the same outer Cell adapter for Text and Counter. Reads observe values
   replaced after construction, and writes carry caller-provided services to the
   provider and back. Preserve results, effects, aliases and invocation counts.
2. Compose Cell<Cell<Text>> without an extra protocol. Read and replace both the
   inner service and inner cell, including capabilities sent in either direction.
3. Compare identity lifting, successive coherent bidirectional conversions and
   their composition. Compare provider failures and writes, not just read values.
4. Malformed domain requests with a valid reply capability produce a domain
   protocol error and invoke no provider. Declared Counter failures remain
   distinguishable from admission refusal and unknown outcomes after loss.
5. Over 200 complete Cell<Text> reads and Text calls, close each reply endpoint
   and keep live export counts bounded by the open service endpoints. Closing an
   adapter owner ends its exports, invalidates service use, settles pending local
   calls and does not close the carrier or borrowed endpoints.
6. Lose the caller's attachment after a provider operation starts. The caller
   settles with outcome unknown; do not infer rollback or cancel provider work.

## Domain lifetime and failure convention

An explicit adapter owner captures the service lifetime. Materialization caches
one owned endpoint per domain object per adapter in that lifetime; proxies map
back to their original sending wire. This is local adapter ownership, with no
transport reference IDs or registry management. A production domain can choose
different end events, but must still name them. Shared runtime close performs
export withdrawal. Tests must not raise the runtime's default limits.

The test-only request convention carries an operation, argument tuple and owned
reply face. Each invocation closes its reply endpoint on every completion path.
An explicit owner-close signal and a finite fixture deadline settle incomplete
local calls with outcome unknown. They do not cancel remote operations. A valid
reply conveys success, a declared domain failure, or a domain-protocol error.
These conventions belong to these fixtures, not the generic protocol stack.

## Scope and evidence

Only new files in `hydrated/ts/test` belong to this package. The shared harness,
runtime source, Go realization, wire format and existing observation suite stay
with their current owners. A failing runtime law is reported as a counterexample
to the owning contract/implementation, not hidden by fixture fallback behavior.

This is bounded evidence for these APIs, schedules and carriers. It does not
establish a law for every generic type, arbitrary partitions, distributed
collection, cross-namespace gateways or cross-language domain protocols. The
earlier hydration spike's call-64 retention counterexample (bitruntime `4ead83f`,
since removed) remains historical evidence. The owning production change still requires its full Go/TypeScript,
package and interoperability gates and independent review.

Run `npm ci --ignore-scripts`, `npm run check`, and `npm test` at the repository
root. Record exact reviewed revisions and CI results in the pull request; this
document does not assert that an untested later revision has passed.
