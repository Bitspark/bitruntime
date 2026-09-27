# Advice for consult
**Research doc:** doc_9a7e16b9-e38f-4af4-97e2-f0f0af78041b.md
**Document:** doc_9a7e16b9-e38f-4af4-97e2-f0f0af78041b
**Advisor:** ChatGPT GPT-6 Pro
**Session:** nsess_d465abbc-e5d8-4ce2-946d-dc1e4a36f86c
**Run:** run_68a80ec1-7907-4c5f-b28c-d1c351b77066
**Chat:** https://chatgpt.com/c/6ab994ea-6738-83ed-8fb7-11b6e71a7c74
**Verify:** nightfall consult verify run_68a80ec1-7907-4c5f-b28c-d1c351b77066 --output <this file>
**Citations:** 35 source marker(s) linked from the answer page by the controller

## Recommendation

Proceed with edition 1, but change two of your current leans.

First, **do not describe the browser combination simply as “`bitwire/1` only, gap documented.”** The quoted revision already requires 1009 for an over-limit frame. Declining edition 1 does not resolve that underlying incompatibility. Pending bitwire’s decision, describe it as a compatibility implementation with an explicit deviation, not an unqualified conformance claim. [file: research.md]

Second, **separate stopping the engine, ending the connection, and finalizing the termination record.** They are related milestones, not one event. That distinction resolves much of the API and lifecycle design—and makes the remaining platform limitations visible rather than concealing them.

The recommendations below use your excerpts and probes as the baseline for bitruntime. I also inspected the pinned upstream library sources; two findings materially qualify the document’s assumptions. I have not run bitruntime’s repository or reproduced its probes.

## 1. The browser combination: an explicit compatibility mode, not an implicit fallback

### What to promise before connecting

Offer two deliberate policies:

| Policy | Admission rule | Promise |
|---|---|---|
| Required conformance | Reject a combination lacking capabilities required by the requested claim. | No silent downgrade. |
| Explicit browser compatibility | Allow the combination only through an explicit option. | Compatible message exchange, with named termination and receive-limit limitations; no unqualified conformance claim. |

Evaluate the policy where **engine, connection capabilities, configuration, and revision meet**. The adapter declares capabilities; it does not certify the engine. The combination’s assessment belongs on the peer and in release/conformance metadata.

For `peer.connect(url)`, the browser factory’s limitations are knowable before creating the native socket. Reject an incompatible requested claim there. For an already-created connection supplied by a caller, assessment failure should not silently close that caller’s resource.

A runtime assessment is useful, but distinguish it from published test evidence:

```ts
interface ClaimAssessment {
  readonly revision: 'bitwire/1';
  readonly requestedCarrierEdition: 1 | null;
  readonly acceptedClaims: readonly string[];
  readonly gaps: readonly {
    capability: string;
    consequence: string;
    deviationId: string;
  }[];
}
```

This is local metadata. Do not add it to protocol messages or negotiation merely to expose it.

### What the engine should do when the browser gap occurs

My recommendation for the explicit compatibility mode is **application-level containment without pretending that transport termination succeeded**.

On an over-limit message, refuse delivery to the protocol decoder and receivers, stop admitting application work, stop the engine’s writers, cancel incoming work, and fail pending calls with a local closed error whose original cause is the receive-limit failure. Record separately that the required action was `close(1009, …)` and that the adapter could not perform it.

On a write failure, take the same application-level containment steps, but record the unavailable required action as abort. Do not enqueue another close behind the failed output.

In neither case should the adapter substitute 1000, a private code, an empty close, or a simulated abort. The WHATWG API restricts script-selected codes and explicitly places the closing handshake after previously queued messages; it exposes no abort operation. [WebSockets Specification: WebSockets Standard](https://websockets.spec.whatwg.org/)

Crucially, **a rejected close must not create a fictitious accepted local close in the connection’s record**. The engine may retain “required action: 1009” in its failure diagnostics, while the adapter’s accepted-close state remains unchanged. That preserves R9’s no-state-change guarantee. [file: research.md]

Keep a minimal adapter-owned close/error observer alive, but discard further application frames. Until an actual terminal observation arrives:

- The engine is unusable.
- The socket may still be open.
- The connection’s observed termination code is unknown.
- No physical abort has occurred.

Do not manufacture `Closed(1006, "")` merely because the engine stopped. For a network-backed root, I would defer that notification until its R17 projection is available. Calls and local lifecycle observers should learn immediately that the engine is unavailable; they need not wait for `Closed`. In browser compatibility mode, final transport notification may remain unavailable indefinitely. That is an additional lifecycle limitation to document, not something a timeout can magically repair.

This policy is not fully conforming. It is the least misleading containment policy under your chosen “never substitute” constraint. Given the development-only deployment described in the document, it is a reasonable temporary mode—but an unsuitable basis for promising bounded resource cleanup. [file: research.md]

There is also a useful distinction in your “before delivery” framing: an adapter can check the completed browser message **before delivering it across the transport seam**. It cannot thereby enforce a limit before the browser receives or allocates that message. Moving the check downward improves the seam guarantee, but does not solve mandatory 1009.

### What to take to bitwire

I would propose this decision:

> Under the quoted frozen revision, a browser-API combination unable to perform the required receive-limit closure must not make an unqualified `bitwire/1` claim. A browser-compatible alternative requires an explicitly identified, owner-approved scope or binding; a carrier edition cannot silently weaken the existing revision.

Ask bitwire to settle the precise delivery boundary and the name of that alternative—not to approve an implementation-specific remapping.

The useful gRPC-Web precedent is **explicit protocol differentiation**. Its specification identifies a different browser-compatible protocol, preserves common framing where practical, and defines translation to native gRPC. It does not pretend that the browser exposes native transport capabilities. Borrow that clarity of scope; do not introduce a proxy or new browser wire protocol in this first implementation step. [GitHub: grpc/doc/PROTOCOL-WEB.md at master · grpc/grpc · GitHub](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-WEB.md?utm_source=chatgpt.com)

## 2. API shape: required capabilities, one local closed kind, and an observable ending

### Make capabilities required and per connection

Your lean toward a required seam member is sound. Use an immutable per-connection value, not optional discovery. The document identifies few implementers and specifically notes the optional-method forwarding problem with Go embedding. [file: research.md] [file: research.md]

The following are interface sketches; existing `Frame`, `Code`, and handler types are assumed.

```go
type Capabilities struct {
    CannotSend []Code // defensive copy; frozen edition-1 valid set
    Abort      bool   // includes interrupting an in-progress close
    Role       string // "client", "server", or "none"
}

type Ending interface {
    Snapshot() Termination // independent, immutable-by-contract snapshot
    Done() <-chan struct{} // closed when final observations are published
}

type Conn interface {
    Send(context.Context, Frame) error
    Receive(context.Context) (Frame, error)

    Capabilities() Capabilities
    Close(context.Context, Code, string) error
    Abort() error
    Ending() Ending // nil until an ending begins
}
```

```ts
interface Capabilities {
  readonly cannotSend: readonly number[];
  readonly abort: boolean;
  readonly role: 'client' | 'server' | 'none';
}

interface Ending {
  snapshot(): Termination;
  readonly done: Promise<Termination>; // resolves, rather than rejects
}

interface FrameConnection {
  readonly state: ConnectionState;
  readonly buffered: number;
  readonly ending: Ending | undefined;

  capabilities(): Capabilities;
  send(frame: Frame): void;

  // Ordinary methods: validation failures throw synchronously.
  close(code?: number, reason?: string): Ending;
  abort(): Ending;

  listen(handlers: ConnectionHandlers): () => void;
}
```

Returning an `Ending` from TypeScript `close` provides something to join without changing the seam into a blocking or promise-only API. **Do not make `close` an `async` function:** that would turn argument exceptions into rejected promises.

For a supported request, repeated TypeScript closes return the same ending handle. Go callers join through `Close(ctx, …)` and can also inspect `Ending()`.

A required browser `abort()` throws `UnsupportedCapabilityError` without changing state. Its existence is not evidence that abort is supported.

Retain one underlying WebSocket implementation, but provide trusted construction paths such as `browserWebSocketConnection` and `nodeWsConnection`. The latter receives the actual termination operation and role. Do not infer full capability merely from an object having a property named `terminate`, or probe capability by attempting a close.

Keep transport-role permission explicit. For example, RFC 6455 specifies 1010 for client use, not server use; a library’s willingness to encode it does not settle R7. [RFC Editor: www.rfc-editor.org](https://www.rfc-editor.org/rfc/rfc6455.html)

### Keep the closed error separate from its record and its original cause

I would modify your termination-record lean: **make the record accessible through the local closed error, rather than inserting the record into the cause chain in place of the cause.**

Use a small dependency-neutral package/module for classification, records, and validation. Re-export the same identities from existing packages where useful.

```go
var ErrClosed = errors.New("bitruntime: closed")

type CloseArgumentError struct {
    Field, Rule, Detail string
}

func (e *CloseArgumentError) Error() string {
    return "invalid close " + e.Field + ": " + e.Detail
}

type UnsupportedCapabilityError struct {
    Capability, Rule, Detail string
}

func (e *UnsupportedCapabilityError) Error() string {
    return "unsupported " + e.Capability + ": " + e.Detail
}

type ClosedError struct {
    Termination Ending
    Cause       error
}

func (e *ClosedError) Error() string { return "bitruntime: closed" }
func (e *ClosedError) Unwrap() error { return e.Cause }
func (e *ClosedError) Is(target error) bool {
    return target == ErrClosed
}
```

The two request-error types must not match `ErrClosed`. A failed close request is not evidence of an ended carrier.

In TypeScript, use a distinct local class:

```ts
export class ClosedError extends Error {
  declare readonly termination: Ending;
  declare readonly cause: unknown;

  constructor(termination: Ending, cause: unknown) {
    super('Carrier ended');
    Object.defineProperties(this, {
      termination: { value: termination },
      cause: { value: cause },
    });
  }
}

export function isClosed(error: unknown): error is ClosedError {
  return error instanceof ClosedError;
}
```

A received `PublicError('disconnected', …)` remains a `PublicError`; it does not become this class. Public serialization explicitly projects a `ClosedError` to `disconnected`, without copying its termination, cause, or arbitrary properties. This addresses the TypeScript conflation identified in your current `ended()` implementation. [file: research.md]

At the root boundary, construct a root-specific error that retains the connection error. Do not preserve `core.Ended`’s current “already closed, therefore return unchanged” behavior across resource boundaries: that would leave the root unnamed. The contract explicitly requires a root’s error to identify the root and retain the connection’s error. [file: research.md]

When a root initiates an operational failure, preserve both its initiating cause and the connection-ending error. Go can use an error tree, such as `errors.Join(connectionError, trigger)`, so `errors.Is` can still find the trigger. TypeScript can retain the connection error in `.cause` and the initiating trigger in the root’s termination snapshot. Avoid causal cycles.

### Model knowledge explicitly

“Unknown,” “known absent,” and “known false” are different states.

```ts
type Fact<T> =
  | { readonly state: 'unknown' }
  | {
      readonly state: 'known';
      readonly value: T;
      readonly evidence: 'direct' | 'library' | 'derived';
    };

type CloseFrame = Readonly<{
  code: number | null; // null: a received/sent Close frame without a status
  reason: string;
}>;

type WriteProgress = 'not-started' | 'partial' | 'complete';

interface Termination {
  readonly resource: Readonly<{
    kind: 'root' | 'pair' | 'connection';
    id: string;
  }>;
  readonly cause: unknown;
  readonly final: boolean;

  readonly localClose: Fact<null | Readonly<{
    frame: CloseFrame;
    origin: 'caller' | 'engine' | 'adapter';
    written: Fact<WriteProgress>;
  }>>;

  readonly peerClose: Fact<CloseFrame | null>;
  readonly observedCode: Fact<number>;
  readonly drainComplete: Fact<boolean>;
  readonly handshakeComplete: Fact<boolean>;
}
```

The Go equivalent can use:

```go
type Fact[T any] struct {
    Known    bool
    Value    T
    Evidence string // "direct", "library", or "derived"
}
```

For example, `Fact[*CloseFrame]{Known: true, Value: nil}` means that no peer close was received; `Known: false` means that fact is unavailable.

Notice the extra distinction: a received empty Close frame is **not** an absent peer close. It has no encoded status, and projects to 1005. An ended connection with no received Close frame projects to 1006. [RFC Editor: www.rfc-editor.org](https://www.rfc-editor.org/rfc/rfc6455.html)

Errors retain a stable `Ending` handle. Each snapshot is immutable; later snapshots may gain evidence. Finalization freezes the record. This lets a pending call fail immediately without permanently losing a close observation that arrives later.

There is an owner-level issue here: your quoted contract explicitly permits unknown drain completion, but does not spell out the admissible unknown states for every other member. Obtain agreement on that evidence model. **Honestly reporting unknown is necessary, but does not automatically satisfy a contract that might require the fact to be observable.** [file: research.md]

### Preserve the fixed endpoint interfaces

Keep bitwire’s `Endpoint` and `Receiver` unchanged. Expose ending access and owner-only abort through bitruntime’s peer/concrete handles or companion helpers.

Go `Endpoint.Close` returns validation failures and uses the peer’s configured close policy. TypeScript `Endpoint.close` remains `void`: it validates synchronously, starts or joins the ending, and discards the returned handle. The companion accessor provides observation.

Every endpoint close boundary must validate before detaching receivers, rejecting calls, cancelling contexts, or otherwise changing lifecycle state.

## 3. Ending is a process, and the adapter owns transport evidence

### Separate three dimensions

Maintain these dimensions separately:

| Dimension | States |
|---|---|
| Application usability | accepting work / no longer accepting work |
| Transport lifecycle | open / closing / aborting / closed |
| Termination evidence | provisional / final |

A normal close stops new work, selects one close request, and starts the transport ending. An abort stops new work and interrupts transport activity. An observed remote close enters the same controller without pretending that the engine initiated it.

Replace the numeric `codeAborted = 0` convention with an explicit internal action:

```ts
type EndAction =
  | { kind: 'close'; code: number; reason: string }
  | { kind: 'abort' }
  | { kind: 'observe' };
```

Your Go loop currently routes all failed reads, including remote closes, through `fail`; the new observer path should retain an already-observed close instead of reflexively reclassifying it as a fresh abort. [file: research.md]

**Use `sync.Once` only for genuinely one-time publication or cleanup.** Do not hold it—or a mutex—while performing blocking close I/O. The abort path must remain independently reachable while close is waiting. In TypeScript, commit lifecycle state before calling a library operation that may invoke callbacks synchronously.

### Validation and joining need a defined order

I recommend:

`argument validation → capability/role validation → join-or-start ending`

Validate every request, including requests made while closing or closed. A rejected second request must not replace the selected close or turn into an abort.

R9 versus R10 leaves one detail worth confirming with bitwire: whether a numerically valid but unsupported repeated request is rejected or joins. I recommend rejection, consistently with validation-first behavior; do not let two language implementations choose different interpretations.

For Go contexts, give the first accepted close ownership of the operation’s deadline. Later callers can stop waiting when their own contexts expire, but cannot replace or extend the selected close deadline. Distinguish “this waiter timed out” from “the connection’s close operation failed.”

For TypeScript, let the connection’s configured close deadline govern the operation; callers await the shared handle separately. On capable transports, deadline expiry triggers the real abort path.

### Stop work promptly; publish `Closed` only when its projection is known

At the start of an ending, reject new sends, settle pending calls, invalidate queued deliveries, and cancel application handlers. Retain transport observation and any control processing necessary to complete the close.

Publish the finalized record before delivering `Closed(code, reason)`. Then detach remaining observation machinery.

Do not make transport completion wait for arbitrary application callbacks. Otherwise a receiver that calls `Close` can deadlock the operation that is waiting for that receiver.

For R19, finality must be enforced per attachment. Queued tasks need an attachment/generation check when they execute, not merely when queued. A detached attachment, a replacement attachment, and a reconnected peer must not share stale delivery permission. Your adopted rule is specifically about the finality of each attachment. [file: research.md]

### coder/websocket: require a genuine hard-close path

The existing `New(*websocket.Conn, limit)` surface cannot guarantee an interrupting abort just by wrapping `CloseNow`. Your probes already establish the blocking case. [file: research.md]

I would make bitruntime own the WebSocket construction paths and capture a vetted hard-close operation at dial/accept time. For an externally supplied connection, require that operation explicitly to enable the full abort capability. A legacy wrapper without it must declare the limitation.

This is feasible without accessing private fields: the pinned dial path obtains an `io.ReadWriteCloser` from the HTTP response body, while the accept path obtains the hijacked connection. Wrapping those construction boundaries can retain the corresponding closer. Preserve the original handshake validation and HTTP behavior when doing so. [GitHub (+1 more source)](https://raw.githubusercontent.com/coder/websocket/v1.8.15/dial.go)

Do not close an unrelated parent connection, and do not use cancellation of “some pending read” as the sole public abort guarantee. That workaround depends on a read actually being pending at the correct moment. It can remain a version-pinned experiment, not the declared invariant.

**One upstream-source correction to the document:** coder/websocket’s public `Close` returning nil is not universally proof of a completed handshake. Its public method normalizes errors matching `net.ErrClosed` to nil. The narrower `closeHandshake` success path is not the entire public result space. Conversely, receiving a different valid peer code need not mean that both close frames were not exchanged. [GitHub](https://raw.githubusercontent.com/coder/websocket/v1.8.15/close.go)

Therefore retain actual peer-close evidence, not just the final `Close` error.

For the automatic 1009 path, keep the library-generated reason separate from the engine’s explanatory cause. Do not record `"frame exceeds the receive limit"` as the transmitted reason when the library selected `"read limited at 1025 bytes"`. Your probe directly establishes that mismatch. [file: research.md]

A version-pinned derivation can identify the selected library reason, with provenance. It does not prove successful transmission: the library’s `writeError` discards the close-write result. [GitHub](https://raw.githubusercontent.com/coder/websocket/v1.8.15/write.go)

Hard-close access solves the abort problem, not every observability problem. Missing close-frame evidence may still require supported hooks, instrumentation, or a documented claim limitation.

### `ws`: retain the evidence it already exposes

Validate before entering `ws.close`, eliminating the early-state-change failure identified in your probe. Expose `terminate()` through the trusted Node construction path; the library documents it as a forced close using socket destruction. [file: research.md] [GitHub](https://raw.githubusercontent.com/websockets/ws/8.21.3/doc/ws.md)

**A useful additional source finding:** the pinned `ws` EventTarget implementation constructs `CloseEvent.wasClean` from its sent-and-received close-frame flags. Your existing browser-shaped interface can receive that evidence; the adapter currently drops it. Retaining the event field avoids reading private fields yourself. [GitHub](https://raw.githubusercontent.com/websockets/ws/8.21.3/lib/event-target.js)

Do not, however, turn a successful return from `socket.close()` into “close frame completely written.” It establishes that the request was accepted, not that the output completed.

For browser events, preserve `code`, `reason`, and `wasClean`. Treat affirmative cleanliness as evidence supporting handshake completion; avoid universally treating a false cleanliness value as an exact account of which frame was missing. [WebSockets Specification: WebSockets Standard](https://websockets.spec.whatwg.org/)

### Preserve history when abort interrupts close

Abort cannot undo an already-written close frame. If a local close was complete before abort, its progress remains complete. If a valid peer close was already received, retain it and its R17 projection.

Consequently, remove unconditional API promises that abort makes the far side observe 1006. That observation depends on what the far side had already received. The contract itself gives priority to the first valid received close. [file: research.md]

Where a library hides write progress, use unknown—not “partial because an error occurred” or “complete because no exception occurred.”

### Who may end a shared carrier?

The connection owner and engine may terminate the transport. A borrowing dispatcher should not acquire that authority merely because a handler failed.

For a request handler failure, fail that request through the existing error mechanism. For an event handler failure, report a local diagnostic or end the affected owned attachment according to policy. An event has no response to fail. Do not automatically send 1002 on a shared connection for either case.

Ownership transfer should confer permission to end the connection, not mandate doing so after every handler error. Preserve genuinely carrier-wide failures such as the full root queue; your source explicitly identifies that as an ending condition. [file: research.md] [file: research.md]

For local pairs, validate public close requests under R8 even though no bytes are sent. R18 is not permission to accept arbitrary close arguments. I would add an owner-side logical abort through bitruntime’s companion API and propose an explicit logical-abort projection to bitwire, rather than inventing an unrestricted private numeric namespace.

## 4. Interim evidence: portable behavioral scenarios, not implementation snapshots

Build a small scenario catalog independently of either implementation. Each scenario should specify its source rule, preconditions, actor actions, local observations, forbidden effects, and permitted unknowns.

Pin it to the contract bundle/hash. Your R-ids are useful traceability labels, but should not become permanent public error identifiers: the document explicitly identifies them as local labels. [file: research.md]

A practical coverage matrix is:

| Area | Essential scenarios |
|---|---|
| R1–R9: validation and permission | Every numeric boundary; observe-only and reserved codes; invalid UTF-8/unpaired surrogates; 123 versus 124 bytes; unsupported valid codes; role restrictions; rejection while open, closing, and closed. |
| R10–R11: lifecycle | Different repeated close arguments; joined completion; abort during a stalled close; repeated abort; close after abort; first caller versus joining caller cancellation. |
| R12–R16: classification | Connection → root → dispatcher → pending and later calls; preserved cause and identity; received public `disconnected` while the local connection remains usable; serialization excludes local records. |
| R17–R18: evidence | Peer-first and simultaneous close; differing local/remote codes and reasons; empty peer Close frame; drop without one; automatic 1009; local pair logical ending; unavailable facts remain unknown. |
| R19: finality | Queued delivery versus close; callback reentrancy; detach/re-attach; late asynchronous callbacks; reconnect generation isolation. |
| R20–R23: engine policy | Each protocol refusal remains 4011; over-limit handling selects 1009; failed writes abort; operational paths follow the chosen abort policy; no accidental new code assignment. |

R22 is a preference. A test can pin bitruntime’s selected abort policy without claiming that every other revision-permitted operational close violates the rule.

### Separate three evidence levels

**Portable behavioral tests** should run against both languages’ pipes and deterministic fake drivers. Normalize local traces and compare guarantees, not scheduling or error text.

**Adapter tests** should exercise the pinned libraries with controlled I/O. Use explicit barriers such as “close write started,” “waiting for peer close,” and “abort requested.” A test that sleeps and hopes to hit the closing window is not adequate evidence.

For partial writes, inject failure at a lower I/O boundary whose accepted byte count is observable. Test zero bytes, some bytes, and a complete write followed by loss of the peer response. Do not require an atomic in-memory pipe to simulate a genuine partial WebSocket write.

**Platform tests** should cover actual browser engines, Node’s global WebSocket, and `ws` separately. Playwright provides Chromium, Firefox, and WebKit projects; those are useful real-browser coverage, while your mock and Node stand-in remain separate evidence categories. [Playwright: Browsers | Playwright](https://playwright.dev/docs/browsers)

Record platform and dependency versions in results. Where a real browser cannot expose a fact, test that the adapter reports it as unknown—not that the mock’s richer introspection appears in production.

### Assert the actor’s behavior at the right boundary

Your testee’s current latch records a close code before transport success. That can establish **intent**, but not completed emission. [file: research.md]

Use distinguishable trace events:

```text
close-request-accepted(code, reason)
close-write-progress(progress, evidence)
peer-close-observed(code-or-none, reason)
abort-invoked
transport-ended
record-finalized
attachment-closed(code, reason)
```

A controlled remote endpoint can help exercise and diagnose the adapter. Its observation should not be the normative definition of the local actor’s action. This follows bitwire’s stated vector principle. [file: research.md]

Add mutation checks: deliberately reintroduce code substitution, late validation, cause loss, premature detachment, or blanket 1006 projection. The corresponding tests should fail. That is stronger evidence that the suite tests the contract rather than merely mirrors the implementation.

When bitwire publishes scenarios, map each interim scenario to an owner scenario, a retained library-regression test, or a remaining deviation. Replace overlapping normative expectations; retain deterministic library-fault regressions.

## 5. Release sequence, remaining risks, and scope limits

### Ship one deliberate seam break

For the release after the v0.4.2 baseline, I would make one intentional breaking minor release, developed in this order:

1. Introduce common validation, local error identity, termination primitives, and scenario fixtures.
2. Change the required seam members and implement adapter lifecycle/evidence handling.
3. Convert engines, roots, pairs, dispatchers, helpers, and testees; then publish per-combination claims and deviations.

Do not advertise edition 1 merely because the new methods exist. Publish the claim only for combinations that satisfy the adopted behavior and the agreed evidence requirements. A documented deviation makes a release transparent; it does not make an incompatible combination conforming.

The bitsystem3 migration should be short: select the appropriate WebSocket factory, explicitly opt its browser client into compatibility mode, update local-closed checks, and replace code-latching workarounds with termination access. Its existing valid `Close(1000, "")` calls should remain valid. The document’s small, controlled consumer set makes this a good point for a deliberate break rather than a prolonged optional-interface transition. [file: research.md] [file: research.md]

### Resolve these semantic traps before claiming completion

**A failed write can coincide with a required refusal.** Ask bitwire to confirm precedence when an over-limit or protocol-invalid input is discovered after the write path has failed. My proposed rule is that a known failed writer is not reused to attempt the refusal. That follows R21’s purpose, but the overlapping case deserves an explicit owner scenario.

**Validate received-close evidence too.** “First valid peer close” requires more than accepting whichever error the library returns. The pinned coder/websocket close-payload parser checks numeric validity but does not itself check the reason’s UTF-8 validity. Add a malformed-reason probe and determine whether an adapter hook is needed before automatic response behavior. [GitHub](https://raw.githubusercontent.com/coder/websocket/v1.8.15/close.go)

**Capability declarations depend on ownership and configuration.** They are unreliable if callers can continue manipulating the native socket or change validation settings beneath the adapter. In particular, `ws` exposes an option that disables UTF-8 checking for text and close messages. A claiming factory must control or constrain such configuration. [GitHub](https://raw.githubusercontent.com/websockets/ws/8.21.3/doc/ws.md)

**Termination is not publication evidence.** Neither an empty output buffer nor a close handshake proves that an application call was not executed. Do not add retry safety, “unpublished” conclusions, or delivery guarantees while implementing the termination record. Those belong with D1.

**Generated reasons should be bounded constants.** Keep detailed local errors in `cause`; choose an independently valid wire reason for internal refusals. Removing `closeReason` must not replace silent truncation with an internal refusal that fails validation because `err.Error()` is too long.

**Mechanical type migration requires semantic agreement.** A neutral module and re-exports reduce later changes, but cannot reconcile different definitions of completion, unknown evidence, repeated-close validation, or logical abort. Take those milestones to bitwire now.

### What to leave out

Leave out D1 publication guarantees, D2’s full ownership/admission contract, D3 draining, stream framing, tunnels, a new operational close code, and a browser-specific protocol redesign.

Prepare internally for an admission barrier and writer seal, but report drain completion as unknown until you actually implement the adopted successor semantics. D3 can later insert a draining phase between stopping admission and sealing the writer without replacing the ending handle or error model. The document explicitly places these drafts outside the present adopted scope. [file: research.md]

**The central invariant should be: once the engine decides it cannot continue, application work stops exactly once; thereafter, the termination record gains only evidence the owning layer can support.** A close request, a completed close write, a received peer close, and an ended socket must never stand in for one another.