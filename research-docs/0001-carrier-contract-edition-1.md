# Research: Carrying bitwire's carrier contract, edition 1, into bitruntime's Go and TypeScript APIs

**ID:** 0001
**Date:** 28 September 2026
**Status:** advised
**Run-ID:** run_68a80ec1-7907-4c5f-b28c-d1c351b77066 (attempt 2; attempt 1, run_1e55330d, failed at the service with nothing sent)
**Reviewed:** https://github.com/Bitspark/bitruntime/issues/29#issuecomment-5860240132
**Owner:** bitruntime's maintainers (prepared by the bitruntime coding-agent session)
**Issue:** [Bitspark/bitruntime#29](https://github.com/Bitspark/bitruntime/issues/29)

## Question

We maintain **bitruntime**, a Go and TypeScript runtime for a small JSON messaging protocol called **`bitwire/1`**. The contract that runtime implements has just gained a *carrier contract* (edition 1). It is a set of rules for:
- which close codes a connection may send;
- how a close request is validated;
- how every layer reports a connection that has ended, including a local *termination record* of how it ended;
- what a protocol engine does when its transport fails.

Our code breaks many of those rules today. Most of them we can fix. One we cannot fix as written:
- A browser's `WebSocket` API can send only close codes 1000 and 3000–4999, and has no abort.
- `bitwire/1` binds 1009 for a frame over the receiver's size limit.
- The carrier contract says a peer whose write path failed aborts.
- So over the browser API, once the contract's "declare the gap, never substitute a code" rules are applied, **the engine has no conforming way to end the connection after an over-limit frame or a write failure.**

We need to decide five things, and we will check each piece of advice against our code before acting on it. Concrete type sketches in Go and TypeScript help most. The decisions:
1. What the engine does, and promises, in a combination of adapter, engine and platform that cannot do what `bitwire/1` binds, and what we propose to bitwire about it.
2. The API shape and release order for four additions: capability declarations, distinct argument and unsupported-capability errors, a termination record, and a TypeScript abort.
3. How to model an ending as a process, and fill the record honestly, over WebSocket libraries that act early, block or hide facts.
4. How to test all of this in two languages before the contract's owner has written its own test scenarios.
5. Anything our framing misses.

Our current lean on (1): the browser combination does not claim edition 1, and its gap is documented. We do not know what the engine should *do* when the case arises.

## Context

### Terms used throughout

- **bitwire** is the repository that owns the contract. **bitruntime** implements it. Project names are lowercase, even at the start of a sentence.
- A **carrier** is anything that carries protocol messages: a transport connection, a protocol peer, an in-process local pair, or a tunnel channel. bitruntime implements no tunnels, so channels are out of scope here.
- The **transport seam** is the interface between a transport and the protocol engine above it: Go's `transports.Conn` and TypeScript's `FrameConnection`. An **adapter** is one implementation of that seam over a concrete socket or library.
- A **claim** is a conformance statement: "`bitwire/1`", or "`bitwire/1` and carrier contract, edition 1". Today a claim lives in bitruntime's documentation and release notes, and in the report of bitwire's conformance runner, which exercises bitruntime's released artifacts. There is no run-time value that states a claim. Whether there should be one is part of question 1.
- The **engine** (called **peer** in the code) speaks `bitwire/1` over one transport connection. The **peer close** in the termination record means the *remote* side's close.
- An **endpoint** is bitwire's messaging interface: send a message at a path, attach one **receiver**, close. A receiver gets messages and one final `Closed(code, reason)`. The engine exposes each connection as one **root endpoint**, at the empty path. The **root queue** holds inbound deliveries waiting for the root's receiver; under `bitwire/1`, a full root queue ends the carrier.
- A **call** is a request whose response settles it. The **call helpers** are Go's `dispatch.Call` and TypeScript's `call()`. A **dispatcher** is a receiver that routes messages to handlers by path; its routing table is its **registry**.
- An **error code** is a string in a public error object, such as `disconnected`, `busy` or `invalid_message`. bitwire calls this object `ProfileError`, and bitruntime implements it as `PublicError` in both languages. A **close code** is a number from the WebSocket registry, sent in a close. The two namespaces are unrelated.
- **Local** means in-process, never sent. In "local close" and Go's `CloseError.Local`, it means "chosen by this side".
- Bitruntime's own **conformance testee** is a small program that bitwire's test runner drives in order to check bitruntime from outside. bitwire's **drivers** are its test-only harnesses per language.

**Go idioms used below:**
- `errors.Is(err, target)` tests whether an error, or anything it wraps, is `target`. `errors.As(err, &x)` extracts a wrapped error of `x`'s type.
- A `context.Context` carries a deadline and cancellation.
- `sync.Once` runs a function at most once.
- A struct that **embeds** an interface forwards that interface's methods automatically. So adding a method to an interface does not break embedders at compile time; they silently forward the new method to the embedded value. An *optional* method, discovered by a type assertion such as `conn.(interface{ Subprotocol() string })`, is **not** forwarded by embedding.

### System overview

**`bitwire/1`** is a JSON protocol over a message-oriented connection, usually a WebSocket. Each WebSocket message is one JSON envelope: a request, a response, an event, or a cancel. The revision is frozen: a hash-identified bundle of documents defines exactly what crosses the network. It is defined as the network behaviour of **nightseam** v0.6.0, the project bitruntime was ported from. bitruntime reproduces it byte for byte, and CI checks this against nightseam in every pairing of the two languages.

**Consumers.** bitruntime's releases (currently v0.4.2) are a Go module and an npm-compatible tarball, pre-1.0. They are used by:
- **bitsystem3**, the only application: a Go server, a Go command-line client, and a TypeScript browser client;
- bitwire's conformance drivers;
- bitruntime's own testees.

We know of no bitsystem3 deployment serving outside users; its developers run it as a local Docker stack. Both ends of every connection are ours, configured with the same frame limit (4 MiB), so an over-limit frame means a bug or a misconfiguration, not routine traffic. The team is small and there is no external deadline.

**Layers**, in both languages:

| Layer | What it is | Go | TypeScript |
| --- | --- | --- | --- |
| core | bitwire's endpoint model, plus an in-process **local pair**: two endpoints joined in memory, with no transport and no protocol | `core.NewPair` | `pair()` |
| transport | Moves whole frames (text or binary) between two ends; closes with a code and a reason. Adapters: an in-memory **pipe** and a **WebSocket** adapter | `transports.Conn` | `FrameConnection` |
| engine | Speaks `bitwire/1` over one transport: envelopes, correlation, pacing, refusal of protocol violations; exposes the root endpoint | `engine.Peer` | `Peer` |
| dispatch | Routes deliveries to handlers | `dispatch.Dispatcher` | `Dispatcher` |

bitwire's endpoint types (v0.3.0) are **fixed for us**. Changing them means a new bitwire contract release, not a bitruntime change:

```go
// bitwire v0.3.0, wire/go/wire.go
type Code int // a close code
type Message struct { Frame ProfileFrame; Return *ReturnAddress } // one envelope's content, and where replies go

type Receiver struct {
	Message func(path []string, message Message)
	Closed  func(code Code, reason string)
}
type Endpoint interface {
	Send(path []string, message Message) error
	Receive(receiver Receiver) (detach func(), err error)
	Close(code Code, reason string) error
}
```

In TypeScript, `Endpoint.close(code?: number, reason?: string): void` returns nothing.

**Close-code constants** used in the excerpts below. Note the trap: `CodeProtocol` is 4011, but `CodeProtocolError` is 1002.

| Go | TypeScript | Code | Meaning here |
| --- | --- | --- | --- |
| `CodeNormal` | `CODE_NORMAL` | 1000 | a chosen close |
| `CodeGoingAway` | `CODE_GOING_AWAY` | 1001 | a side going away |
| `CodeProtocolError` | `CODE_PROTOCOL_ERROR` | 1002 | protocol error (WebSocket's) |
| `CodeNoStatus` | `NO_STATUS` | 1005 | observe-only: a close without a code |
| `CodeAbnormalClosure` | `CODE_ABNORMAL_CLOSURE` | 1006 | observe-only: no close at all (an abort or a drop) |
| `CodeTooLarge` | `CODE_TOO_LARGE` | 1009 | a frame over the receiver's limit |
| `CodeTLSHandshake` | `CODE_TLS_HANDSHAKE` | 1015 | observe-only: TLS failed |
| `CodeProtocol` | `CODE_PROTOCOL` | 4011 | `bitwire/1`: the other side broke the protocol |

### How an ending travels today

In both languages:
1. The **transport** notices first: a remote close, a dropped socket, a frame over its receive limit, or a local `Close` or `Abort`.
2. The **engine** notices through its reader or writer loop, or makes its own decision: refuse a violation, or give up.
3. The engine then **ends**, once:
   - it records an error with a cause;
   - it closes or aborts the transport;
   - it fails every pending call;
   - it tells the root endpoint's receiver `Closed(…)`.
4. The **dispatcher** on the root gets `Closed` and tells each route's receiver `Closed(…)`.
5. **Pending and later calls** fail with the *closed classification*:
   - In Go, `errors.Is(err, transports.ErrClosed)` holds, and the cause is wrapped.
   - In TypeScript, a `PublicError` with error code `disconnected`, keeping the cause.

At each step, "what close did this side send, and what did it observe" is either lost or replaced by a constant. The contract asks for a record that keeps both, through every layer.

### The contract being implemented

**Layering** (bitwire's decision 0013, one of its numbered decision records):

> A separately identified and versioned carrier contract may add local API guarantees. It may also constrain the implementations that claim it to behaviors that a named protocol revision already permits. It does not change that revision's standalone conformance requirements, defaults or normative test suite. […] A conformance claim therefore names what it covers: "`bitwire/1`", or "`bitwire/1` and carrier contract, edition N". A peer that conforms to `bitwire/1` alone is not made nonconforming by a carrier contract.

So edition 1 cannot make a `bitwire/1` peer nonconforming. Whether the browser client, which cannot send the 1009 that `bitwire/1` binds, conforms to `bitwire/1` at all is a separate question (question 1).

**What `bitwire/1` binds for over-limit frames** (its scope document, section "The connection beneath"):

> A frame larger than the receiver's limit is refused before delivery. The receiver ends the connection with **1009** ("too large" in the close codes this section lists).

bitwire's carrier document also lists what **every transport** provides, including these two properties:

> - **A receive limit.** A frame over the receiver's limit is not delivered, and the connection ends with 1009.
> - **Explicit closing.** `Close` sends a code and a reason that the other side sees. It waits for an acknowledgement where the transport has one. `Abort` ends the connection at once and sends nothing. […] 1006 is what a side sees when the other aborted or the transport dropped.

**The adopted rules** of edition 1, from bitwire's `docs/wire/carriers.md`, section "Adopted: close codes and the closed classification":
- The text has no MUST/SHOULD keywords. Every declarative rule binds a carrier that claims the contract; "prefers" (R22) is the one soft rule.
- The ids are ours.
- Topic headings are in the left column. Our notes are in italics.

| Id | Topic | Rule (verbatim) |
| --- | --- | --- |
| R1 | Is the number valid in an encoded close? | "The valid set is frozen at 1000–1003, 1007–1014 and 3000–4999. A later change to the WebSocket registry does not change it." |
| R2 | | "1005, 1006 and 1015 are *observe-only*: they describe what a side observed, and are never sent." |
| R3 | | "1004 and every other number are reserved or unassigned, and never sent either." |
| R4 | | "Valid does not mean assigned. 4000–4999 is for private use." |
| R5 | Can this adapter emit it? | "An adapter declares the valid codes it cannot send. The browser WebSocket API, for one, sends only 1000 and 3000–4999 and has no abort. An adapter that lacks a capability a claim needs does not make that claim; it does not remap codes to hide the gap." |
| R6 | Does the protocol permit it for this condition? | "The protocol revision decides." *(R20–R23 below)* |
| R7 | | "Numeric validity does not override a transport's own rules. A WebSocket's role-specific restrictions still apply." |
| R8 | A close request | "A close request is validated before it changes any state: A code outside the valid set, or a reason that is not valid UTF-8 or is longer than 123 bytes, is an argument error. Nothing is sent, the connection stays as it was, and the reason is never truncated or repaired." |
| R9 | | "A valid code that the adapter cannot emit is an unsupported-capability error, with the same effect. The adapter never substitutes another code, never omits the code, and never aborts instead." |
| R10 | | "A repeated valid close joins the ending already under way. It neither changes its code and reason nor sends a second close." |
| R11 | | "`Abort` is idempotent, and may interrupt a close in progress." |
| R12 | One closed classification | "Every endpoint, operator and helper reports a closed or ended carrier as one local error kind, whatever layer noticed it. The error carries a termination record" *(below; an "operator" is a combinator over endpoints, such as mounting or forwarding)* |
| R13 | | "The record is local. It is never serialized into a `ProfileError`." |
| R14 | | "The public projection of a closed carrier stays the code `disconnected`." *(the projection is what a closed carrier becomes when reported as a public error)* |
| R15 | | "A received public error named `disconnected` is never evidence that this side's carrier closed." *(such an error arrives in a response when a carrier further along closed, for example behind a forwarder)* |
| R16 | | "A call that the carrier's end cuts off reports the local kind with its cause, through every call helper, not only the public projection." |
| R17 | `Closed(code, reason)` | "On a network transport it reports the first valid peer close when one arrived. Otherwise it reports what the transport observed without one: 1006, or another observe-only code such as a WebSocket's 1015. The code this side selected stays available separately, in the termination record." |
| R18 | | "A local pair reports its logical ending directly." |
| R19 | | "A `Closed` notification is final for its attachment: no message is delivered after it." *(an attachment is one `Receive` registration)* |
| R20 | Under `bitwire/1` | "The revision binds 4011 for a protocol violation and 1009 for an over-limit frame, and a carrier claiming this contract keeps both." |
| R21 | | "When its own write path has failed, or a partly written record cannot be completed safely, it aborts and sends nothing more." *("record" here is a byte-stream framing record, not the termination record; bitruntime has no byte-stream transport yet)* |
| R22 | | "For other operational failures, such as overload, a stalled consumer or a full root queue, it prefers an abort. It stops using 4011 as a catch-all for them. The revision permits other codes here, so a peer that sends 4011 is not made nonconforming to `bitwire/1`." |
| R23 | | "A dedicated operational-failure code waits for a later protocol revision, or for another explicitly versioned claim like this contract." |

We leave out one adopted rule: the revision-1 tunnel's exception for sending 1006. bitruntime has no tunnel. Edition 1 also adopts `bitwire-stream/1`, a length-framed format for byte-stream transports, which is separate work.

**The termination record** (R12), verbatim:

| Member | Holds |
| --- | --- |
| resource | What closed, by kind and identity: a root, a pair, a channel or a connection. A root's error names the root and keeps the connection's as its cause. |
| cause | The original local cause, such as backpressure, a remote close or a context's end. |
| local close | The code and reason this side selected, if any, and how far its close was written: not started, partial or complete. |
| peer close | The first complete, valid close the other side sent, if any. |
| observed code | What this side observed the connection end under. |
| drain complete | Whether the queued work covered by a drain reached the transport (D3). Until D3 is adopted, a carrier that does not track it reports it as unknown. |
| handshake complete | Whether both close records, or both close frames, were exchanged. |

**Drafts that will follow as a later edition**, not adopted and not in scope now, one line each:
- **D1, publication:** what counts as evidence that a send was, or was not, published to the network.
- **D2, ownership:** the caller keeps its inputs stable during a send; before admission the carrier owns a validated copy.
- **D3, closing:** a queued-work drain. New work stops being admitted, admitted work is flushed within one close deadline, and then the writer is sealed.

bitwire has published no types for any of this yet, and no date for them. It intends to declare the transport seam, the close codes and the closed classification in its own language bindings, stated as language-neutral milestones that each binding expresses idiomatically.

bitwire's API-boundary test scenarios for these rules are not written yet. Until they are, bitwire's instruction to us (on bitruntime#29) is: "record the behavior in bitruntime's own tests, and list any deviation you keep as a documented deviation". Its conformance principle for published vectors is that they "assert what a side does, never what the far side observes".

**Already settled, and not asked here.** An earlier consultation, bitwire's research 0004, produced this contract. It settled:
- which code each condition gets;
- that numeric validity, adapter capability and protocol permission are three separate questions;
- what the record holds;
- how `Closed` is projected (R17, R18);
- that a browser's gap is declared, not hidden.

It left the browser case as "the mandatory 1009 problem":

> The browser API permits script-requested close codes only at 1000 and 3000–4999. Its `close()` also does not discard previously queued messages before closing, and its interface exposes no immediate abort operation. Therefore a wrapper around that API cannot simply promise a genuine bounded, immediate transport abort or reliable script-requested 1009 at a custom receive limit. A private operational code does not fix the mandatory 1009 problem. Publish an explicit capability distinction. An adapter missing a required capability must not claim full conformance for a combination that requires it. Do not conceal the gap by remapping codes, and do not weaken the full seam to match the least capable API.

### The central conflict, step by step

The client is bitsystem3's browser client: a TypeScript peer over the browser `WebSocket` API, created with `new Peer({ maxFrameBytes })` and `peer.connect(url)`.

1. The server sends a frame larger than `maxFrameBytes`. The browser has no receive limit, so it accepts the whole message and delivers it to the engine.
2. The engine sees the frame is over its limit, and ends: it tears down its state, detaches its listeners, and asks the adapter to `close(1009, 'frame exceeds the receive limit')`.
3. The platform refuses 1009 with `InvalidAccessError`. The adapter's fallback calls `socket.close()` with no arguments.
4. The server observes **1005**. `bitwire/1` binds 1009, and nothing tells the local user that 1009 was not sent.

The same happens to 1002, which the dispatcher sends when an application's event handler fails, and to any code a caller asks for in 1001–1003 or 1007–1014. 4011 works, because it lies in 3000–4999.

A **write failure** (a send that throws, or output that never drains) should abort (R21). The browser API has no abort, so today the engine closes with 4011 instead. On the browser that close frame even queues behind the data that did not drain.

Once the adapter follows R5 and R9 (declare "cannot send 1001–1003 or 1007–1014" and "no abort", then refuse such a close as unsupported rather than falling back), the engine is left with no conforming action in either case.

### The adapters, and what each can do

| Adapter | Valid codes it cannot send | Abort reachable through the seam | Claim we lean to |
| --- | --- | --- | --- |
| Go pipe (in memory) | none | yes | edition 1 |
| Go WebSocket adapter (coder/websocket) | none | yes, but slow and not idempotent (below) | edition 1, after fixes |
| TypeScript pipe (in memory) | none | **no**: the seam has no abort | edition 1, once the seam has one |
| TypeScript WebSocket adapter over a Node `ws` socket (the server side) | none | **no**: `ws` has `terminate()`, but the adapter's socket type does not expose it | edition 1, once it does |
| TypeScript WebSocket adapter over the browser or Node's global `WebSocket` (clients) | 1001–1003, 1007–1014 | **no**: the platform has none | `bitwire/1` only, gap documented |

There is **one** TypeScript WebSocket adapter. It wraps any object with the browser API's shape (`WebSocketLike`), whether that object is a `ws` socket or a browser socket. So its capabilities depend on which socket it was given, and are not fixed per adapter type.

### Where each rule stands today

| Rule | Go | TypeScript |
| --- | --- | --- |
| R1–R3 valid set | Met at the transports (`Sendable`). The local pair and the endpoint layers accept any integer. | Met at the seam (`sendable`). The local pair accepts any integer. |
| R8 argument errors (code) | Transports refuse with `ErrUnsendableCode`, one error for both invalid and observe-only codes. The peer's root endpoint **aborts** instead, and returns nil. | The seam throws `RangeError`. The peer **substitutes 1000**. |
| R8 argument errors (reason) | Not checked. The peer **truncates** silently. The WebSocket adapter turns an over-long reason into an **abort**. | Not checked. The browser and `ws` **repair** unpaired surrogates to U+FFFD. The browser throws for over 123 bytes. `ws` **hangs in CLOSING** (below). |
| R9 unsupported capability | No such error; both Go adapters can send every valid code. | No such error. The adapter **omits** the code (falls back to `close()`). |
| R10 repeated close joins | Peer: yes, by `sync.Once`, but returns nil. Pipe: a silent no-op. WebSocket: returns `ErrClosed` at once, without waiting. | A no-op; `close` returns nothing to join. |
| R11 abort idempotent, may interrupt a close | Pipe: yes. WebSocket: a second abort errors, and an abort during a close blocks 4.9 s. | No abort. |
| R12, R16 one kind, with a record | One kind (`ErrClosed`) with a cause. No record; only a transport-limit refusal carries a code. | One kind (`disconnected`) with a cause, but sometimes none (below). No record. |
| R15 a received `disconnected` is not local evidence | Met: a received error is a `*core.PublicError`, which does not match `ErrClosed`. | **Not met**: a received `disconnected` is the same `PublicError('disconnected')` class and code as the local classification. |
| R17, R18 `Closed` projection | The peer's root always reports 1001, "peer ended". The pair reports the caller's code, unvalidated. | Same. |
| R20 4011 and 1009 | Met. | Met, except over the browser API (1009 → 1005). |
| R21 write failure aborts | Met. | **Not met**: closes 4011. No abort exists. |
| R22 operational failures prefer abort | Met in the engine. The local pair uses 4011; the dispatcher uses 1002 for an application failure. | **Not met**: the engine closes 4011. The pair and dispatcher behave as in Go. |

### Relevant code: Go

Excerpts are from bitruntime v0.4.2. The WebSocket adapter wraps `github.com/coder/websocket` v1.8.15. Lines marked **[probed]** were checked by running a test against v0.4.2; a probe's remote "not reading" means it never reads, and so never answers a close.

**The seam** (`transports/go/transport.go`):

```go
type Kind int                                // Text or Binary
type Frame struct{ Kind Kind; Data []byte }  // one whole message
type Code = wire.Code                        // bitwire's close-code type

func Sendable(code Code) bool {
	switch {
	case code == CodeNoStatus, code == CodeAbnormalClosure, code == CodeTLSHandshake, code == 1004:
		return false
	case code >= 1000 && code <= 1014:
		return true
	case code >= 3000 && code <= 4999:
		return true
	}
	return false
}

// ErrClosed is the one classification of a closed carrier. […] every bitruntime
// endpoint, operator and helper reports a closed or ended carrier as an error
// for which errors.Is(err, ErrClosed) holds, whatever layer noticed it.
var ErrClosed = errors.New("bitruntime: closed")

// ErrUnsendableCode refuses a Close whose code may only be observed. Nothing
// is sent and the connection stays as it was; Abort ends it without a code.
var ErrUnsendableCode = errors.New("bitruntime: close code may only be observed")

// CloseError is what Receive returns once the remote side closed: the code and
// the reason it gave. It is also a closed carrier. Local marks a close this side
// chose, as when a receiver refuses a frame over its limit with 1009.
type CloseError struct {
	Code   Code
	Reason string
	Local  bool
}
func (e *CloseError) Is(target error) bool { return target == ErrClosed }

type Conn interface {
	// Send writes one frame; it blocks while the transport cannot take more, until ctx ends.
	Send(ctx context.Context, frame Frame) error
	// Receive returns the next frame. A frame over the receive limit is not
	// delivered: Receive returns an error and the connection ends with CodeTooLarge.
	Receive(ctx context.Context) (Frame, error)
	// Close ends the connection with a code and a reason the remote side will see,
	// waiting for its acknowledgement until ctx ends where the transport has one.
	// A code that is not Sendable is refused with ErrUnsendableCode and nothing is sent.
	Close(ctx context.Context, code Code, reason string) error
	// Abort ends the connection at once, with nothing sent, and releases every
	// blocked Send and Receive. The remote side observes CodeAbnormalClosure.
	Abort() error
}
```

After a local `Close` or `Abort`, the local side's `Send` and `Receive` return the bare `ErrClosed`, with no record of what this side did.

**The pipe** (`transports/go/pipe.go`):

```go
func (e *pipeEnd) Close(ctx context.Context, code Code, reason string) error {
	if !Sendable(code) {
		return ErrUnsendableCode
	}
	e.end(&CloseError{Code: code, Reason: reason}) // guarded by a sync.Once: a repeated Close is a silent no-op
	return nil
}
func (e *pipeEnd) Abort() error { e.end(nil); return nil } // the remote then reads CloseError{Code: 1006}
```

**[probed]** A 501-byte reason that is not valid UTF-8 is accepted and reaches the remote unchanged.

**The WebSocket adapter** (`transports/websocket/go/websocket.go`). `New` receives a coder/websocket connection that is already built:

```go
func New(conn *websocket.Conn, limit int64) transports.Conn {
	conn.SetReadLimit(limit) // the library adds one byte internally
	return &connection{conn: conn}
}
func (c *connection) Receive(ctx context.Context) (transports.Frame, error) {
	t, data, err := c.conn.Read(ctx)
	if err != nil {
		if errors.Is(err, websocket.ErrMessageTooBig) { // the library has already sent 1009 with its own reason
			c.end()
			_ = c.conn.CloseNow()
			return transports.Frame{}, fmt.Errorf("%w: %w",
				&transports.CloseError{Code: transports.CodeTooLarge, Reason: "frame exceeds the receive limit", Local: true}, err)
		}
		return transports.Frame{}, c.translate(ctx, err) // a remote close becomes CloseError{code, reason}
	}
	…
}
func (c *connection) Close(ctx context.Context, code transports.Code, reason string) error {
	if !transports.Sendable(code) {
		return transports.ErrUnsendableCode
	}
	if c.closed() {
		return transports.ErrClosed
	}
	c.end()                                                // mark this side done
	return c.conn.Close(websocket.StatusCode(code), reason) // ctx is not used
}
func (c *connection) Abort() error {
	c.end()
	return c.conn.CloseNow()
}
```

The library underneath (coder/websocket v1.8.15). Its own `CloseError` type is written `websocket.CloseError` here, to keep it apart from bitruntime's:

```go
// Close: "The connection can only be closed once. Additional calls to Close are no-ops."
func (c *Conn) Close(code StatusCode, reason string) (err error) {
	if c.casClosing() {          // someone is already closing: wait for the library's
		err = c.waitGoroutines() // goroutines, then report net.ErrClosed
		…
		return net.ErrClosed
	}
	err = c.closeHandshake(code, reason)
	err2 := c.close()            // internal and idempotent: drops the socket
	…
}
// CloseNow (the abort) has the same "already closing" branch: it waits instead of dropping.

func (c *Conn) closeHandshake(code StatusCode, reason string) error {
	err := c.writeClose(code, reason) // write the close frame (its own 5 s timeout)
	if err != nil {
		return err
	}
	err = c.waitCloseHandshake()      // wait up to 5 s for the remote's close
	if CloseStatus(err) != code {
		return err                    // nil only when the remote echoed the same code
	}
	return nil
}
func (c *Conn) writeClose(code StatusCode, reason string) error {
	…
	p, err = ce.bytes()               // encodes code + reason
	if err != nil {
		return err                    // returns before writing anything
	}
	…
}
const maxControlPayload = 125
const maxCloseReason = maxControlPayload - 2 // 123 bytes; no UTF-8 check
func (ce websocket.CloseError) bytesErr() ([]byte, error) {
	if len(ce.Reason) > maxCloseReason {
		return nil, fmt.Errorf("reason string max is %v …", maxCloseReason, …)
	}
	…
}
// A pending Read or Write whose context ends drops the socket:
//   context.AfterFunc(ctx, func() { …; c.close() })
```

The library tracks privately whether it sent and received close frames. It also echoes a remote close automatically. Its public surface is `Close`, `CloseNow`, `Read`, `Reader`, `Write`, `Writer`, `Ping`, `CloseRead`, `SetReadLimit` and `Subprotocol`. The underlying `net.Conn` is private: reaching it would mean changing `New`'s signature, and capturing it at dial and accept time. The only public route to the internal, immediate drop is to cancel the context of a pending `Read` or `Write`.

What the probes found:
- **[probed]** `Close(1000, a 124-byte reason)` returns an error. But the adapter has already marked itself done, and the library drops the socket without writing a close frame. So the remote observes **1006**: an invalid argument became an abort nobody asked for (against R8).
- **[probed]** A second `Close` returns `ErrClosed` at once, without waiting for the first close's handshake (against R10).
- **[probed]** An `Abort` during a `Close` whose remote is not reading blocks for **4.9 s**, until the library's 5 s handshake wait gives up (against R11).
- **[probed]** A second `Abort` returns an error wrapping `net.ErrClosed` (against R11).
- **[probed]** When the receive limit (1024) trips, the library sends 1009 with its own reason, `"read limited at 1025 bytes"`. The adapter's local `CloseError` says `"frame exceeds the receive limit"`. So the reason recorded locally is not the reason sent.
- `Close`'s return value already tells whether the handshake completed with the same code. bitruntime discards it.

**The engine's ending** (`engine/go/peer.go`):

```go
// Close ends the peer, closing the connection with 1000. It is safe to call more than once.
func (p *Peer) Close() error { p.end(transports.ErrClosed, transports.CodeNormal, ""); return nil }

// fail ends the peer on a transport there is nothing to say over: a write that
// failed, the context ending, a consumer that stalled past its deadline. The
// connection is aborted and the far side reads an abnormal closure.
func (p *Peer) fail(err error) { p.end(err, codeAborted, "") }

// refuse ends the peer on a frame the protocol does not admit […] with 4011 and a reason.
func (p *Peer) refuse(err error) { p.end(err, transports.CodeProtocol, err.Error()) }

const codeAborted transports.Code = 0 // "no close at all"

func (p *Peer) end(err error, code transports.Code, reason string) {
	p.once.Do(func() {
		err = core.Ended(err) // the closed classification, keeping the cause (below)
		p.mu.Lock(); p.err = err; p.mu.Unlock()
		close(p.done)
		// The connection is closed before the peer's context is cancelled. The
		// reader receives under that context, and a WebSocket whose pending read
		// is cancelled drops the socket, so the far side would observe 1006.
		if code == codeAborted || !transports.Sendable(code) {
			_ = p.conn.Abort()        // ANY invalid code, not only observe-only ones, becomes an abort
		} else {
			ctx, cancel := context.WithTimeout(context.Background(), p.options.WriteTimeout) // default 10 s
			_ = p.conn.Close(ctx, code, closeReason(reason))                                // result discarded
			cancel()
		}
		p.cancel() // cancels the peer's own context, which its reader and writer run under
	})
}

func closeReason(reason string) string {
	const limit = 123
	if len(reason) <= limit {
		return reason // a short reason that is not valid UTF-8 passes unchanged
	}
	reason = reason[:limit]
	for len(reason) > 0 && !utf8.ValidString(reason) {
		reason = reason[:len(reason)-1] // an early invalid byte trims the reason to nothing
	}
	return reason
}

// core.Ended classifies a cause as a closed carrier while keeping it:
func Ended(cause error) error {
	if cause == nil || errors.Is(cause, transports.ErrClosed) {
		if cause == nil { return transports.ErrClosed }
		return cause // e.g. a remote *CloseError passes through unchanged
	}
	return &endedError{cause: cause} // errors.Is(…, ErrClosed) and errors.Is(…, cause) both hold
}
```

The engine's loops already follow R20–R22:
- A failed write calls `fail`, which aborts.
- A failed read, including a remote close, calls `fail`.
- The five `refuse` (4011) sites are all protocol violations by the far side: a binary frame, an undecodable envelope, a bad identifier, a request serial that did not increase, and a duplicate request id.
- A frame over the peer's own limit ends with 1009.
- Every operational failure aborts: backpressure, a stalled consumer, a full queue, the context ending, request serials exhausted, and an event receiver that panicked.

What a user of the peer can observe:
- `Peer.Err()` satisfies `errors.Is(err, ErrClosed)`.
- After a remote close, it holds the remote's `*CloseError` with code and reason (through `Ended`'s pass-through).
- After a transport-limit refusal, it holds the adapter's `CloseError{1009, Local: true}`, whose reason, over the WebSocket, is not the one sent.
- After a local `Close()`, a local 4011 or a local abort, it holds only a Go cause, with no code.
- The root endpoint's receiver is told the same thing whatever happened:

  ```go
  // engine/go/wire.go, when the peer ends:
  receiver.Closed(transports.CodeGoingAway, "peer ended") // always 1001
  // and the root endpoint's Close (the bitwire Endpoint method):
  func (w *rootWire) Close(code wire.Code, reason string) error {
  	w.peer.end(transports.ErrClosed, code, reason) // any code that is not Sendable aborts
  	return nil                                    // never an error
  }
  ```

**Other carriers:**
- The **local pair** (`core/go/pair.go`, and `core/ts/src/pair.ts` alike) delivers any code and any reason verbatim to both sides' `Closed` (for example 1004, 5000, non-UTF-8, 10 KB). Its close always succeeds, and it has no abort. It ends itself with **4011** for its own overload (`"local wire queue limit reached"`, `"local wire event consumer stalled"`) and when an application's event receiver panics. It speaks no protocol, so 4011 there is a local code handed to `Closed`.
- The **dispatcher** (`dispatch/go/dispatch.go`, and the TypeScript twin alike) runs `registry.Close(1002, "wire event rejected")` when an application's event handler returns an error. By default the dispatcher *borrows* its endpoint: it then only detaches and tells its routes `Closed(1002, …)`, and the connection stays open. Only when created with an option that transfers ownership of the endpoint does it close the peer's connection with 1002.
- So an application handler's failure is handled three ways today: the engine aborts (event receiver panic), the pair sends 4011, and the dispatcher closes 1002.

**Who implements `Conn`.** Nobody outside bitruntime. bitsystem3 and bitwire's drivers only use `ErrClosed`, `CodeNormal`, `Peer.Close()` and `Endpoint.Close(1000, "")`, and discard the results.

Inside bitruntime, test wrappers and the testee embed `Conn`. The testee has to latch the ending code itself, because the peer does not record it:

```go
// cmd/bitwire-testee/go/closed.go: "a peer's Err says why it ended but not the code:
// a frame it refused (4011) and a transport it gave up on (1006) end it alike."
type watched struct{ transports.Conn; … code transports.Code }
func (w *watched) Receive(ctx context.Context) (transports.Frame, error) {
	frame, err := w.Conn.Receive(ctx); w.saw(err); return frame, err // a remote CloseError latches first
}
func (w *watched) Close(ctx context.Context, code transports.Code, reason string) error {
	if transports.Sendable(code) { w.end(code) } // latched before the transport even succeeds
	return w.Conn.Close(ctx, code, reason)
}
func (w *watched) Abort() error { w.end(transports.CodeAbnormalClosure); return w.Conn.Abort() }
// Embedding does not forward the optional Subprotocol() method, so it is re-declared by hand:
func (w *watched) Subprotocol() string { … }
```

### Relevant code: TypeScript

Excerpts are from bitruntime v0.4.2. The server side runs over sockets from the Node library `ws` 8.21.3. Clients run over the platform's `WebSocket`, which follows the browser (WHATWG) API; Node's global `WebSocket` follows it too. Lines marked **[probed]** were checked under Node 24.

**The seam** (`transports/ts/src/index.ts`). It has no abort and no capability declaration, and `close` returns nothing to wait on:

```ts
export type Frame = { kind: 'text'; data: string } | { kind: 'binary'; data: ArrayBuffer | Uint8Array };
export type ConnectionState = 'connecting' | 'open' | 'closing' | 'closed';
export interface ConnectionHandlers {
  open?: () => void;
  frame?: (frame: Frame) => void;
  close?: (code: number, reason: string) => void; // no local/remote, no "clean"
  error?: () => void;                              // no detail
}
export interface FrameConnection {
  readonly state: ConnectionState;
  /** What a send left with the connection and the transport has not taken yet, in the
   *  transport's own unit (a WebSocket's bytes, a pipe's frames); the engine reads only
   *  whether it is zero. */
  readonly buffered: number;
  /** Throws when the connection is not open. */
  send(frame: Frame): void;
  /** Throws a RangeError, and sends nothing, for a code that is not `sendable`. */
  close(code?: number, reason?: string): void;
  /** Registers handlers; returns a function that detaches all of them. */
  listen(handlers: ConnectionHandlers): () => void;
}
export function sendable(code: number): boolean { /* exactly R1–R3's valid set */ }

/** The browser WebSocket surface, also implemented by Node's native WebSocket. */
export interface WebSocketLike {
  readonly readyState: number;
  readonly bufferedAmount: number;
  send(data: string): void;
  close(code?: number, reason?: string): void;
  addEventListener(type: string, listener: EventListener): void;
  removeEventListener(type: string, listener: EventListener): void;
}
```

**The pipe** validates the code only, not the reason. It has no abort. Its `close` fires `close(code, reason)` on both ends' listeners, including the closer's own, discarding frames in flight.

**The WebSocket adapter**, `webSocketConnection(socket: WebSocketLike)`, wraps either a `ws` socket or a browser socket. Its close silently drops a code or a reason the platform refuses:

```ts
close(code = CODE_NORMAL, reason = '') {
  refuseUnsendable(code);            // RangeError "may only be observed", even for 1004 or 5000
  try {
    socket.close(code, reason);
  } catch {
    // The browser WebSocket API accepts only 1000 and 3000–4999 and a
    // reason of at most 123 bytes. A code or reason it refuses must not
    // leave the connection open: close without them, and the far side
    // observes no status (1005).
    socket.close();
  }
},
```

It maps the socket's close event to `close(code, reason)` and drops `wasClean`; a missing code becomes 1005. `WebSocketLike` has no `terminate`, so `ws`'s real abort is unreachable through the seam. A test pins the fallback: a mocked browser socket asked to close with 1002 records the calls `[[1002, 'wire event rejected'], []]`.

**The platforms' `close(code, reason)`:**
- **The browser API** validates before any state change. It throws `InvalidAccessError` for a code other than 1000 or 3000–4999, and `SyntaxError` for a reason over 123 UTF-8 bytes. A reason with an unpaired surrogate is silently repaired to U+FFFD. It has no abort. Its close event carries `code`, `reason` and `wasClean`. **[probed]**, Node's global `WebSocket` client against a `ws` server:

  | `close(...)` | Throws | Server observed (after the adapter's fallback) |
  | --- | --- | --- |
  | 1000, 'ok' | — | 1000 'ok' |
  | 1009 | `InvalidAccessError` | **1005**, with no reason |
  | 1002 | `InvalidAccessError` | **1005** |
  | 4011, 'Duplex connection closed' | — | 4011 with that reason |
  | 4000, a 124-byte reason | `SyntaxError` | **1005** |
  | 4000, `'a\uD800b'` | — | 4000, `'a�b'` |

- **`ws`** accepts exactly the valid set, and has `terminate()`, a real abort. It also silently repairs unpaired surrogates, when it encodes the reason. But its `close` changes state *before* validating the reason:

  ```js
  // ws 8.21.3, lib/websocket.js
  close(code, data) {
    if (this.readyState === WebSocket.CLOSED) return;
    …
    if (this.readyState === WebSocket.CLOSING) { … return; } // a later close does nothing new
    this._readyState = WebSocket.CLOSING;                     // state changes first
    this._sender.close(code, data, !this._isServer, …);       // throws RangeError for > 123 bytes
    setCloseTimer(this);                                      // never reached after the throw
  }
  ```

  **[probed]** Through the adapter, `close(4000, a 124-byte reason)` on a `ws` socket throws inside `ws` after the state has moved to CLOSING. The adapter's fallback `socket.close()` then finds CLOSING and does nothing. The socket stays in CLOSING, with nothing sent and no timer, until something else tears the connection down.

**The engine's ending** (`engine/ts/src/peer.ts`). One method handles every ending:

```ts
private fail(error: PublicError, closeConnection = true, code = 4011, reason = CLOSE_REASON): void {
  const connection = this.connection;
  if (!connection) return;             // a repeated ending is a no-op
  this.connection = undefined;         // all state is torn down, and the peer's listeners
  this.state = 'disconnected';         // detached, BEFORE the connection is closed; the peer
  this.detach?.();                     // therefore never sees its own connection's close event
  … // reject every pending call with ended(error), abort incoming handlers, empty the queue
  if (closeConnection) {
    try {
      // A code that may only be observed is never sent. A connection of the
      // seam ends only by a close, so it ends with a normal one instead.
      if (sendable(code)) connection.close(code, reason);
      else connection.close();         // substitutes 1000
    } catch {
      /* Already closed. */            // any failure is swallowed
    }
  }
  … // notify onError and onClose listeners with the raw error
}

// The one closed classification (core/ts/src/internal/frame.ts):
export function ended(cause?: unknown): PublicError {
  if (cause instanceof PublicError && cause.code === 'disconnected' /* and not "unpublished" */)
    return cause;                      // passes through: then there is no separate cause
  const error = new PublicError('disconnected', 'Connection ended; outcome may be unknown.');
  if (cause !== undefined) Object.defineProperty(error, 'cause', { value: cause });
  return error;
}

// A response envelope's error becomes the same class (peer.ts, on a response):
if (isObject(frame.error)) pending.reject(new PublicError(frame.error.code, frame.error.message, frame.error.data));
```

So a remote `{"error":{"code":"disconnected"}}` is indistinguishable from this side's own ending (against R15). After a remote close, `peer.close()` or `root.close()`, a pending call rejects with a `disconnected` error that has no further cause.

The engine's endings and what each sends:

| Trigger | Error code | Close sent | Kind |
| --- | --- | --- | --- |
| binary frame, undecodable envelope, serial did not increase, duplicate request id | `invalid_message` | 4011 | protocol violation |
| incoming frame over `maxFrameBytes` | `frame_too_large` | 1009 | over limit |
| `connection.send` threw: the connection is not open, or the platform's `send` threw | `send_failed` | **4011** | write path |
| output did not drain within the write deadline | `write_timeout` | **4011** | write path |
| a response, or its fallback, could not be sent | varies | **4011** | write path |
| output queue full, event queue over capacity, a handler past its deadline, the root queue full | `busy`, `stalled_consumer` | **4011** | operational |
| the socket's `error` event, a connect timeout | `connection_failed`, `connect_timeout` | 4011 | operational |
| the remote closed | `disconnected` | none; the remote's code and reason are **dropped** | observed |
| `peer.close()` | `disconnected` | 1000 | caller |
| a caller's `root.close(code, reason)` | `disconnected` | the caller's code; an invalid one becomes 1000 | caller |

The write path calls `send` only when `buffered` is 0, then polls every 5 ms for the output to drain, up to the write deadline. So it detects a stuck write, but it can only respond with a 4011 close.

What a user of the TypeScript peer can observe:
- `onClose` and `onError` receive the raw `PublicError`, such as `frame_too_large`, `send_failed` or `disconnected`.
- Pending calls reject with `disconnected`, with or without a cause, as described above.
- No close code or reason is available anywhere, neither the one sent nor the one received.
- The root endpoint's receiver always gets `closed(1001, 'peer ended')`.
- The testee recovers codes only by listening to the `ws` socket directly, or to a `FrameConnection`'s own `close` handler when there is no socket.

**Who implements `FrameConnection`.** Only bitruntime itself: the WebSocket adapter, the pipe, and seven test doubles. Adding a required `abort()` breaks only bitruntime's own test doubles at compile time.

### Constraints

- **What is fixed:**
  - The `bitwire/1` wire behaviour. CI's byte-for-byte test against nightseam covers how a server ends after a protocol violation (4011). It does not cover operational endings, so moving those from 4011 to an abort is visible to that test only where it should be.
  - bitwire's `Endpoint` and `Receiver` types.
  - bitwire's rules, which we implement and never redefine. "Raise it in bitwire" is an acceptable answer, and for the question of whether the browser client conforms, probably the right one.
- **What is ours to change:** both transport seams, both engines, the local pair, the dispatcher, and the adapters. The compatibility budget is small but real: pre-1.0, few pinned users, no outside implementers. A breaking change should be deliberate, versioned and small.
- **Two languages, one guarantee.** Go's seam is pull-based and blocking; TypeScript's is push-based, reporting a `buffered` count and delivering through handlers. Go returns errors, and TypeScript throws. The observable guarantee must match; the API shape need not.
- **Platform limits we cannot remove:** the browser API (codes, no abort, reason repair); `ws` (repair, early state change); coder/websocket (late validation, a blocking abort during a close, its own 1009 reason).
- **Tests before scenarios.** Until bitwire's API-boundary scenarios exist, expected outcomes must come from the rule text (the R-ids), not from what the code does today. They are to be replaced by bitwire's scenarios when those arrive. Every check so far used Node's global `WebSocket` as the browser stand-in; CI has no real browser today.
- **What "done" looks like for this step:**
  - a bitruntime release in which each combination in the adapter table states its claim;
  - every adopted rule is either tested in bitruntime with its R-id, or listed as a documented deviation;
  - bitsystem3 upgrades with a known, short list of changes;
  - later, bitruntime passes bitwire's scenarios, and moves to bitwire's own seam types when they are published.

### What we have considered, with our leans

- **Declaring capabilities.** Options:
  - (a) A method on the seam, such as `capabilities() → {unsendable codes, abort}`. It breaks non-embedding implementers; ours are few.
  - (b) An optional method discovered at run time (a Go type assertion, a TypeScript feature check). Invisible in the type, and not forwarded by Go embedding.
  - (c) A value fixed when the adapter is constructed.

  The TypeScript WebSocket adapter's capabilities depend on the socket it wraps, so any choice must allow a per-connection value. **Lean:** (a) with per-connection values, in both languages. We are unsure.
- **When a required code is unsendable.** Options:
  - (a) Refuse to construct an engine over such an adapter when edition 1 is requested.
  - (b) Construct it, report the claim as `bitwire/1` only, and at the moment of need do one of: close 1000, send a private 4xxx code, or leave the connection to the platform. Each is either forbidden by R9 or unavailable.
  - (c) Tell bitwire the combination cannot conform, and ask how it wants such a peer described.

  **Lean:** (a) for edition 1, plus (c). We have no lean on what the non-claiming engine does at the moment itself.
- **A TypeScript abort.** Options:
  - (a) An optional `abort?(): void` with a capability flag.
  - (b) A separate interface the engine also accepts.
  - (c) A required member, as a breaking change in the next minor pre-1.0 version.

  **Lean:** (c), with `abort` exposed only where the socket supports it. Browser sockets would declare "no abort", so the member must be allowed to throw an unsupported-capability error there.
- **The termination record.** Options:
  - (a) Extend Go's `CloseError`, and one TypeScript error class.
  - (b) A new record type carried as the closed error's cause, reachable with `errors.As` or `instanceof`.
  - (c) Also an accessor on the peer, such as `peer.termination()`.

  **Lean:** (b), plus (c). Where a library hides a fact, mark it unknown rather than guessing. Note that coder/websocket's `Close` already reveals "handshake complete with the same code".
- **Validation.** Validate code and reason in bitruntime before calling any library: in the adapter, and again at every endpoint layer. **Lean:** in each language, distinct argument and unsupported-capability error types, both carrying the rule id. For TypeScript's UTF-16 strings, "not valid UTF-8" will mean an unpaired surrogate. The code can settle this detail; we do not ask about it.
- **The endpoint layers.** Today the local pair's 4011s and the dispatcher's 1002 end carriers for overload and for application failures. **Lean:** the local pair reports its logical ending with a local code that is never sent. An application handler's failure fails that call or event, and does not end a shared connection. We are least sure of this one.

## Questions for the expert

1. **A combination that cannot do what `bitwire/1` binds.** The contract settles several things. An adapter declares the valid codes it cannot send and whether it has an abort. A combination lacking a capability does not claim edition 1, and never remaps a code (R5, R9). The browser `WebSocket` adapter has no abort, so it cannot claim edition 1 whatever we decide about codes.

   What stays open is `bitwire/1` itself. It binds 1009 for an over-limit frame, and bitwire lists "a receive limit … ends with 1009" among the properties every transport provides. Yet this adapter can neither enforce a limit before delivery nor send 1009. How would you approach such a combination:
   - what should the engine do when the case arises;
   - what should it promise its users beforehand;
   - where should a claim be decided and reported (the adapter, the connection, or the combination of adapter, engine and revision);
   - what would you propose to bitwire about whether such a peer conforms to `bitwire/1` at all?

   Where comparable stacks met a mandated behaviour a browser could not perform (gRPC-Web beside gRPC, for example), what did they do, and what would you borrow?

2. **Shaping the seam so it survives bitwire's own types.** Both languages need:
   - a capability declaration (per connection, for the TypeScript WebSocket adapter);
   - distinct argument and unsupported-capability errors;
   - a termination record carried by every layer's closed error (R12).

   TypeScript also needs an abort. bitwire's `Endpoint` and `Receiver` are fixed for us. The transport seams are ours, and nobody outside bitruntime implements them. bitwire plans to publish its own types for them later, as language-neutral milestones that each binding expresses idiomatically. How would you shape these additions, and sequence their breaking parts, so that:
   - Go stays pull-based with `errors.Is`/`errors.As`, and TypeScript stays push-based and throwing;
   - both give the same observable guarantee;
   - adopting bitwire's types later is a small, mechanical change?

   Our options above are there to react to, not a menu.

3. **An ending as a process, over libraries that act early, block, or hide what happened.** Both engines treat an ending as one event today. Go uses a `sync.Once`. TypeScript's `fail` detaches before it closes, so it never sees its own close. Edition 1 treats an ending as a process:
   - a repeated close joins the ending under way (R10);
   - an abort may interrupt a close in progress (R11);
   - the record distinguishes a close not started, partly written or complete, and whether the handshake completed.

   How would you model that process so that:
   - the adapter's guarantees hold over coder/websocket, `ws` and the browser API, as probed above;
   - each record member states only what was observed, and marks the rest unknown or inferred;
   - it can later take D3's admission barrier, writer seal and single close deadline without a redesign?

   Which layer should own which facts? And which layers should be allowed to end a shared carrier at all? Today a dispatcher can close a whole connection with 1002 because one event handler failed, and a local pair ends itself with 4011 for its own overload.

4. **Evidence before the owner's scenarios exist.** bitwire has not yet written its API-boundary or session scenarios for these rules. Until it does, bitruntime's own tests must carry the evidence, with expectations taken from the rule text and not from our code. bitwire's published vectors "assert what a side does, never what the far side observes", and its conformance keeps separate suites for the codec, the carrier and API boundary, stream framing, and each transport binding. How would you build interim evidence that:
   - tests the contract rather than our code;
   - covers behaviour that depends on the platform (a real browser, Node's global `WebSocket`, a mock) and on timing (an abort during a stalled close, a partly written close);
   - shows that Go and TypeScript give the same guarantee;
   - can be handed to bitwire, or retired cleanly, when its scenarios arrive?

5. **What are we not seeing?** Look at the whole task:
   - edition 1 now;
   - the drafted publication, ownership and drain rules (D1–D3) as the next claim;
   - two runtimes with different concurrency models;
   - the owner's scenarios still to come.

   What risks, design traps or better framings do you see that our questions miss, and what would you deliberately leave out of this first step?
