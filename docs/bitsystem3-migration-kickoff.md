# Kickoff: bitruntime and the first complete consumer migration

Use this as the opening instruction for an interactive code-agent session started
from the bitruntime checkout. Read this entire document, then execute the work.

## Outcome

Implement and land the first usable Go/TypeScript bitruntime milestone and move
bitsystem3 completely off Nightseam. Carry the work through design resolution,
implementation, independent validation, release where needed, consumer adoption,
green pull requests and verified integration on the affected repositories' main
branches. The main delivery issues are
[bitruntime #1](https://github.com/Bitspark/bitruntime/issues/1) and
[bitsystem3 #7](https://github.com/Bitspark/bitsystem3/issues/7).
Do not equate a scaffold, a merged runtime PR or an enabled auto-merge with a
completed consumer migration.

This is interactive: bring me consequential unresolved API choices with the
evidence, alternatives and a recommendation. Continue independent baseline and
test work while a decision is pending. Do not ask again about settled ownership,
routine implementation details, tests, branches, commits, PRs or delivery steps.
Follow each repository's actual review and protection rules.

The bounded milestone is the hand-written adapter path used by bitsystem3.
Complete the supporting Bitwire contract/conformance work it needs. Later
generated consumers, live/tunnel migrations and the rest of Nightseam's
retirement remain separately tracked; do not silently expand this session to
all of them.

## Read and reconcile before fixing the public API

Inspect current local and remote state, uncommitted work, active branches, open
PRs, issue comments and dependency versions. Read each repository's AGENTS.md,
contribution/release guidance and local LAYOUT.md before touching it.

Read this repository's CHARTER.md and, in Bitwire:
- decisions 0006, 0007, 0008, 0009 and especially 0010;
- the current Wire, profile, composition and carrier specifications;
- [#39](https://github.com/Bitspark/bitwire/issues/39),
  [#42](https://github.com/Bitspark/bitwire/issues/42) and
  [#20](https://github.com/Bitspark/bitwire/issues/20).

Also read the coordination issues
[bitruntime #2](https://github.com/Bitspark/bitruntime/issues/2),
[Bitwire #47](https://github.com/Bitspark/bitwire/issues/47),
[Nightseam #725](https://github.com/Bitspark/nightseam/issues/725) and
[bitsystem3 #8](https://github.com/Bitspark/bitsystem3/issues/8).
Review bitsystem3's actual adapters, CLI, browser client, docs/bitwire.md and the
applied decisions at the end of research-docs/0001-carrier-stack-home.md.
Earlier research advice and Bitverse's historical architecture are evidence, not
authority overriding decision 0010 and current charters.

The architecture is decided: **Bitwire specifies and independently checks;
bitruntime implements.** Bitwire must not gain a production dependency on
bitruntime or Nightseam. bittype owns the new wire-independent language;
bitschema owns validation; Bitlink owns adapters and their generation.

The API is not fully settled.

Read the maintainer-requested [Wire underneath Bitwire exploration](wire-under-bitwire.md).
Use `End = A0` and `Bitwire = Deixis[End] = A1` as the target decomposition: addressed
`Send(path, message)` may survive as a derived convenience API without remaining
the primitive. The maintainer chose the name `End` on 2026-09-26, after this
kickoff first said `Wire = A0`. `bitwire.Wire` stays the addressed interface and
keeps its name in every language. Preserve the distinction between a declared tree and opaque access
to one. The exact API remains open.

The maintainer selected the counterpart name on 2026-09-26:
**`Bitdata = Deixis[Bytes]`**. Use Bitwire for structured interaction, Bitdata for
structured byte content, and Bitstore for persistence. The current Store backend
holds raw content-addressed blobs; the data model has Bytes at each node. Carry
these names into the design and coordination documents. The storage work remains
with its existing workstreams and does not enlarge this networking milestone.

Keep both contracts in `github.com/Bitspark/bitwire`: `End` is the addressless
primitive and `Bitwire = Deixis[End]` is the target addressed construction, presented
natively as `bitwire.Wire`.
Their interfaces, laws and independent conformance belong in that repository;
bitruntime implements both. Do not create another repository for the primitive.
The existing `wire/go/` and `wire/ts/` presentations may hold both native types;
the layout rule does not require a module per type.

The Bitwire session (bitwire-12) has claimed this design deliverable on
[Bitwire #42](https://github.com/Bitspark/bitwire/issues/42#issuecomment-5843652866)
as a proposed decision 0011. Review that draft, run small experiments against it
and report findings on #42 or its pull request; do not write a competing decision.
The deliverable must resolve, or explicitly defer with a documented version
boundary:

- The exact interfaces for the addressless End (A0) and addressed Bitwire (A1,
  the `Wire` type), and the migration from today's addressed Wire. Do not copy the current addressed
  Send signature and assume it constrains the primitive.
- Which layer owns message representation, return capabilities, correlation,
  cancellation, invocation lifetime, path selection and endpoint ownership.
- Own values, segment-to-byte-key mapping, opaque child boundaries, missing-path
  behavior and retained-parts authority. Reconcile
  [Deixis #49](https://github.com/Bitspark/deixis/issues/49),
  [deixis-svc #1](https://github.com/Bitspark/deixis-svc/issues/1),
  [bitwire-svc #9](https://github.com/Bitspark/bitwire-svc/issues/9) and
  [bitstore-svc #13](https://github.com/Bitspark/bitstore-svc/issues/13).
  End and Bytes must use the same structural contract; a generic relay does not
  acquire application interpretation.
  Record any shared Deixis library dependency and reconcile it with the charters.
- Public lifecycle facilities: admission, capture, cancellation, actual body
  completion, control drain and retirement are distinct. A timeout does not
  retire executing work. Do not require concrete-peer access or a shared private
  ledger to prove cross-endpoint behavior.
- Unforgeable received-context evidence and the engine's context/observation
  hooks, designed together with the relevant Bitwire contract revision.
- Carrier close/error classification, sendable versus observation-only close
  codes, buffering/backpressure bounds and exact package/module coordinates.

Write down the proposed public Go/TS surface, ownership, version transition,
dependency graph and independent acceptance cases. Ask for decisions that remain
mine; never invent an approval or mark another coordinator's work complete.
Do not block all useful work on those decisions: provenance, baseline capture and
independently specified regression cases can proceed.

## Required source layout

Use component first, then an exactly two-letter language directory:

```text
bitruntime/
  core/go/                 core/ts/
  transports/go/           transports/ts/
  engine/go/               engine/ts/
  dispatch/go/             dispatch/ts/
  conformance/go/          conformance/ts/   # native implementation test drivers
  cmd/<command>/go/                         # only when a command is needed
  docs/
```

Shared vectors and specifications may remain language-neutral. Bitwire owns
independent expected behavior; implementation tests here do not replace it.
Optional later modules follow the same rule: live, tunnel, telemetry and
auth-integration each have go/ and ts/ implementations when delivered.

Do not use repository-root go/ or ts/, go/core/, packages/go/, or a command with
Go files directly under cmd/<command>/. Semantic nesting is allowed before the
language, such as providers/tree/memory/go/. Root workspace/build manifests and
maintenance scripts may stay at their normal tooling locations. Native source,
tests and package metadata go with their language implementation. Create no
empty future packages. Independent versioning follows coherent modules; the
directory rule does not require a module per directory.

In bitsystem3 retain cmd/bs-server/go, cmd/bs-cli/go, kernel/go, store/go,
api/{go,ts} and ui/ts. A TypeScript kernel would be kernel/ts only if one is
actually implemented. Apply the same policy to every family repository touched.
Existing long language names are historical paths; use the assigned two-letter
codes for newly ported components. Read LAYOUT.md for the code registry and
recorded exceptions.

## Implementation and evidence

Start from identified Nightseam source commits. The accepted v0.6.0 protocol
baseline is commit 5cc9723a24646c40ed1861f892b2b23eb6d785d7; verify its tag
and provenance. Distinguish it from unreleased Nightseam improvements. Record
ported source and modifications in NOTICE. Add no compatibility aliases,
re-exports, local replace directives or sibling-checkout build dependencies.

Implement core selection/composition/forwarding/local pairs, frame transports
and WebSocket, the bitwire/1 peer and dial/accept setup, and dispatch/request/
response/event helpers in Go and TypeScript. Port only what the milestone needs
and fix the recorded defects in that path, especially
[Nightseam #722](https://github.com/Bitspark/nightseam/issues/722):
queued refusals must not disappear when a pair closes.
Review the lifecycle observations in #658 as well. Keep other #720–#724 defects
attached to their relevant successor modules; do not claim a tunnel or live
issue fixed without its own evidence.

Hold these distinctions throughout:
- The native contract version, wire protocol revision, suite revision and runtime
  module versions are separate identities, recorded in test and release output.
- bitwire/1 keeps the accepted revision-1 wire behavior. Do not insert a new
  handshake into it or silently tighten immutable normative artifacts.
  Revision-2 negotiation of roles/extensions/receive limits is later work.
- Contract changes update their native presentations and independent cases,
  including the other delivered languages where meaning changes; Go/TS runtime
  delivery alone is not eight-language runtime conformance.
- Independent expected observations come from the specification. Run Bitwire's
  cases against released bitruntime in a separate test-only module. Include
  unlawful implementations to show the cases reject real violations.
- Exercise local pairs and WebSocket across Go/Go, TS/TS and both cross-language
  directions, plus interoperability with actual Nightseam v0.6.0 peers.
- Verify path composition/remounting, retained child access, identity, local
  context/received evidence, reverse calls, cancellation and bounded overload.
  Where lifecycle acceptance requires two independent endpoint implementations
  and an opaque wrapper, use only the public contract in that evidence.

Publish the smallest complete module set through the configured release process.
Check module coordinates, version tags, provenance and fresh external installs;
pin released dependencies in consumers. A workspace-only green build is not
release evidence. Keep any historical Nightseam interop dependency isolated to
test-only fixtures, outside published production dependencies.

## Move bitsystem3 and prove it remains the same live model

Update all Go and TypeScript imports, manifests and lockfiles in one coherent
consumer adoption. Cover the server, CLI, api/ts and browser conformance fixtures.
Audit go.mod/go.sum and package.json/package-lock.json for remaining Nightseam
dependencies or alias shims.

Preserve the space model: persistent ID, local facts, parent and name-to-child
access. A child wire is an access capability; storage persists identities and
relationships, not live wire objects. Browser/server and parent/child access
should share the compositional abstraction. Preserve identity and local scope
when selecting, mounting, remounting or retaining a child. Document what the
migration actually enables; using a common abstraction does not by itself
provide distributed storage, cross-host transactions or authority propagation.

Keep the frontend/backend/PostgreSQL Docker setup and Logos DB persistence.
Planning, goals and inference remain outside this milestone. PostgreSQL keeps
its normal database protocol. Preserve persist-before-acknowledge/publish and
the handling of uncertain commits; never replay a mutation merely because a
connection dropped.

Retain the 4 MiB frame budget until a supported negotiated limit replaces it.
Re-evaluate the cached-pair eviction/replacement workaround against the repaired
runtime semantics; remove it only with evidence covering closed and overloaded
pairs, concurrent access and safe retry before admission.

Run the appropriate tests in every Go module, including vet and race testing,
and all TypeScript checks/builds/tests. For bitsystem3 run go test -race ./...,
go vet ./..., npm run check, npm run build, browser end-to-end tests against
Docker and the persistence smoke test. Cover large CLI watch snapshots,
two-browser live updates, reconnects, nested access, uncertain-write recovery,
duplicate facts at capacity, subscriber snapshot isolation and dialog focus/draft
retention. Inspect the actual app in the browser.

Use an isolated Compose project and disposable test database. Never reset or
reuse the user's live volume for destructive tests. Respect shared CI-runner
guidance; report fleet faults to the runner repository rather than patching a
runner or inventing a per-repository workaround.

## Land and report

Keep README, API/protocol docs, AGENTS, source layout, release notes and version
evidence current. Use each repository's PR/review/merge process, resolve findings,
and verify the actual remote main commits and installed artifacts. Preserve
unrelated work and immutable releases. Do not touch original bitsystem's frozen
foundation as a side effect of this migration.

Update the milestone and coordination issues with exact commits, dependency
versions, checks and remaining scope. Close only issues whose acceptance is
demonstrated. Do not close the wider Nightseam-retirement or A0/A1 epics just
because the bitsystem3 milestone passes.

Finish with the landed commits/PRs, published module versions, conformance
versions and results, consumer test evidence, and any concrete blocker or
separately tracked follow-up. Start by reporting the current state and the first
unresolved design choice, then continue the work.
