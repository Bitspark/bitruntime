# Research: Carrying bitwire's carrier contract, edition 1, into bitruntime's Go and TypeScript APIs

**ID:** 0001
**Date:** 27 September 2026
**Status:** draft
**Owner:** bitruntime-17 (the bitruntime session)
**Issue:** [Bitspark/bitruntime#29](https://github.com/Bitspark/bitruntime/issues/29)

## Question

bitwire, the repository that owns our messaging contract, has just adopted a *carrier contract*: rules for how a connection closes, which close codes may be sent, how every layer reports a connection that has ended, and what a protocol engine does when its transport fails. bitruntime implements that contract in two languages, Go and TypeScript. Its current code breaks several of the rules, and two of them it cannot follow as written with the platforms it runs on:
- A browser's `WebSocket` API can send only close codes 1000 and 3000–4999. The protocol, however, requires code 1009 when a frame is too large.
- That same API has no way to abort a connection, but the contract says a peer whose write path has failed *aborts*.

We are asking how to shape the implementation. Specifically:
- how an adapter declares what it cannot do, and what the engine does when it needs something the adapter lacks;
- how to add an abort operation and a local "termination record" to public interfaces that others may implement;
- how to fill that record honestly on top of WebSocket libraries that hide some of its facts;
- how to test all of this in two languages before the contract's owner has written its own test scenarios.

## Context

### System overview

Four projects are involved. Each is a repository in the Bitspark organization, and each is written exactly as its repository is named.

- **bitwire** holds contracts and never implementations. It defines:
  - a small messaging model;
  - a wire protocol, **`bitwire/1`**;
  - independent conformance cases that any implementation must pass.

  Its protocol revisions are immutable: `bitwire/1` is a frozen, hash-identified bundle of documents describing exactly what crosses the network.
- **bitruntime** is the implementation of bitwire's contracts, in Go and in TypeScript. It is released as a Go module and an npm-compatible tarball; the current version is v0.4.2. It is the subject of this document.
- **bitsystem3** is an application built on bitruntime. It has a Go server, a Go command-line client, and a browser client written in TypeScript. That browser client is the reason bitruntime's TypeScript half has to work over the browser's `WebSocket` API.
- **nightseam** is the project bitruntime was ported from, at its release v0.6.0. `bitwire/1` is defined as nightseam v0.6.0's behaviour on the wire, byte for byte, and bitruntime reproduces it byte for byte (checked against nightseam in every pairing of the two languages).

**What `bitwire/1` is.** A JSON protocol over a message-oriented connection, usually a WebSocket. Every WebSocket message is one JSON *envelope*:

```json
{"version":1,"kind":"request","id":"c:1","method":"4:echo","params":{"x":1}}
```

There are four kinds of envelope: a request, a response, an event, and a cancel. A request's `method` is a path of string segments, encoded as length-prefixed text. The protocol binds two close codes that matter here:
- **4011** is sent when the other side breaks the protocol, for example with a malformed envelope.
- **1009** is sent when a frame exceeds the receiver's size limit.

Close codes are the WebSocket registry's numbers on every transport, including transports that are not WebSockets.

**bitruntime's layers.** Each exists in both Go and TypeScript.

| Layer | What it is | Go | TypeScript |
| --- | --- | --- | --- |
| **transport** | Moves whole frames (text or binary) between two ends, and closes with a code and a reason. Implementations: an in-memory pipe, and a WebSocket adapter. | `transports.Conn` | `FrameConnection` |
| **engine** | Speaks `bitwire/1` over one transport connection. It encodes and decodes envelopes, correlates responses with requests, paces outgoing frames, and refuses protocol violations. It exposes the connection as one *root endpoint*. | `engine.Peer` | `Peer` |
| **core** | bitwire's messaging model. An **endpoint** can send a message at a path, attach one *receiver*, and close. A receiver gets messages and one final `Closed(code, reason)` notification. Core also has an in-process *local pair*: two endpoints connected in memory, with no transport and no protocol engine. | `core.NewPair` | `pair()` |
| **dispatch** | Routes delivered messages to handlers by path. | `dispatch.Dispatcher` | `Dispatcher` |

The endpoint interface bitruntime implements comes from bitwire's Go and TypeScript packages, v0.3.0:

```go
// bitwire v0.3.0, wire/go/wire.go
type Code int // a close code

type Receiver struct {
	Message func(path []string, message Message)
	Closed  func(code Code, reason string)
}

type Endpoint interface {
	AddressedWire                                   // Send(path []string, message Message) error
	Receive(receiver Receiver) (detach func(), err error)
	Close(code Code, reason string) error
}
```

In TypeScript the same interface has `close(code?: number, reason?: string): void`.

A **carrier**, in bitwire's vocabulary, is anything that carries these messages: a transport connection, a protocol peer, a local pair, or a tunnel channel. The carrier contract applies to all of them.

### The contract being implemented

**How it is layered.** bitwire's decision 0013 governs the relationship between the carrier contract and the protocol:

> A separately identified and versioned carrier contract may add local API guarantees. It may also constrain the implementations that claim it to behaviors that a named protocol revision already permits. It does not change that revision's standalone conformance requirements, defaults or normative test suite. […] A conformance claim therefore names what it covers: "`bitwire/1`", or "`bitwire/1` and carrier contract, edition N". A peer that conforms to `bitwire/1` alone is not made nonconforming by a carrier contract.

So bitruntime can keep claiming `bitwire/1` whatever it does here. The work is to earn the second claim, "`bitwire/1` and carrier contract, edition 1", and to say precisely which adapters earn it.

**The adopted rules**, quoted from bitwire's `docs/wire/carriers.md`, section "Adopted: close codes and the closed classification". The text uses no MUST/SHOULD keywords, so every declarative rule binds a carrier that claims the contract. The ids are ours.

| Id | Rule (verbatim) |
| --- | --- |
| R1 | "The valid set is frozen at 1000–1003, 1007–1014 and 3000–4999. A later change to the WebSocket registry does not change it." |
| R2 | "1005, 1006 and 1015 are *observe-only*: they describe what a side observed, and are never sent." |
| R3 | "1004 and every other number are reserved or unassigned, and never sent either." |
| R5 | "**Can this adapter emit it?** An adapter declares the valid codes it cannot send. The browser WebSocket API, for one, sends only 1000 and 3000–4999 and has no abort. An adapter that lacks a capability a claim needs does not make that claim; it does not remap codes to hide the gap." |
| R7 | "Numeric validity does not override a transport's own rules. A WebSocket's role-specific restrictions still apply." |
| R8 | "**A close request** is validated before it changes any state: A code outside the valid set, or a reason that is not valid UTF-8 or is longer than 123 bytes, is an argument error. Nothing is sent, the connection stays as it was, and the reason is never truncated or repaired." |
| R9 | "A valid code that the adapter cannot emit is an unsupported-capability error, with the same effect. The adapter never substitutes another code, never omits the code, and never aborts instead." |
| R10 | "A repeated valid close joins the ending already under way. It neither changes its code and reason nor sends a second close." |
| R11 | "`Abort` is idempotent, and may interrupt a close in progress." |
| R12 | "**One closed classification.** Every endpoint, operator and helper reports a closed or ended carrier as one local error kind, whatever layer noticed it. The error carries a **termination record**" (the table below) |
| R13 | "The record is local. It is never serialized into a `ProfileError`." (A `ProfileError` is the public error object a response envelope carries.) |
| R14 | "The public projection of a closed carrier stays the code `disconnected`." |
| R15 | "A received public error named `disconnected` is never evidence that this side's carrier closed." |
| R16 | "A call that the carrier's end cuts off reports the local kind with its cause, through every call helper, not only the public projection." |
| R17 | "**`Closed(code, reason)`**, a receiver's ending notification: On a network transport it reports the first valid peer close when one arrived. Otherwise it reports what the transport observed without one: 1006, or another observe-only code such as a WebSocket's 1015. The code this side selected stays available separately, in the termination record." |
| R18 | "A local pair reports its logical ending directly." |
| R19 | "A `Closed` notification is final for its attachment: no message is delivered after it." |
| R20 | "**Under `bitwire/1`.** The revision binds 4011 for a protocol violation and 1009 for an over-limit frame, and a carrier claiming this contract keeps both." |
| R21 | "When its own write path has failed, or a partly written record cannot be completed safely, it aborts and sends nothing more." |
| R22 | "For other operational failures, such as overload, a stalled consumer or a full root queue, it prefers an abort. It stops using 4011 as a catch-all for them. The revision permits other codes here, so a peer that sends 4011 is not made nonconforming to `bitwire/1`." |
| R23 | "A dedicated operational-failure code waits for a later protocol revision, or for another explicitly versioned claim like this contract." |

**The termination record** (R12), verbatim:

| Member | Holds |
| --- | --- |
| resource | What closed, by kind and identity: a root, a pair, a channel or a connection. A root's error names the root and keeps the connection's as its cause. |
| cause | The original local cause, such as backpressure, a remote close or a context's end. |
| local close | The code and reason this side selected, if any, and how far its close was written: not started, partial or complete. |
| peer close | The first complete, valid close the other side sent, if any. |
| observed code | What this side observed the connection end under. |
| drain complete | Whether the queued work covered by a drain reached the transport. Until that later rule is adopted, a carrier that does not track it reports it as unknown. |
| handshake complete | Whether both close records, or both close frames, were exchanged. |

**What is already settled, and not asked here.** An earlier consultation, bitwire's research 0004, produced this contract. Its advice already answers:
- which code each condition gets;
- that validity, capability and permission are three separate questions;
- what the record contains;
- that the browser's gap must be declared rather than hidden.

It does **not** answer the implementation questions below, and describes the browser case as "the mandatory 1009 problem" without resolving it. Its words:

> The browser API permits script-requested close codes only at 1000 and 3000–4999. Its `close()` also does not discard previously queued messages before closing, and its interface exposes no immediate abort operation. Therefore a wrapper around that API cannot simply promise a genuine bounded, immediate transport abort or reliable script-requested 1009 at a custom receive limit. A private operational code does not fix the mandatory 1009 problem. Publish an explicit capability distinction. An adapter missing a required capability must not claim full conformance for a combination that requires it. Do not conceal the gap by remapping codes, and do not weaken the full seam to match the least capable API.

bitwire has not yet published types for any of this in its own language bindings. It intends to ("We intend to declare the transport seam, its close codes and the closed classification in bitwire's eight language bindings, so that a carrier written outside bitruntime implements a bitwire interface"). It also has not yet written the conformance scenarios that will test these rules at the API boundary. Until it does, bitruntime is to "record the behavior in bitruntime's own tests, and list any deviation you keep as a documented deviation".

### Relevant code: Go

All excerpts are from bitruntime v0.4.2. The Go WebSocket adapter wraps the library `github.com/coder/websocket` v1.8.15. Lines marked **[probed]** were checked by running a small test against v0.4.2.

**The transport interface and its close codes** (`transports/go/transport.go`):

```go
type Code = wire.Code // bitwire's close-code type

// Sendable says whether a close code may be sent. Codes that only describe
// what a side observed — no status, an abnormal closure, a failed TLS
// handshake — are never transmitted; an abort is how a side ends without one.
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

// CloseError is what Receive returns once the remote side closed: the code
// and the reason it gave. It is also a closed carrier, so errors.Is(err,
// ErrClosed) holds for it. Local marks a close this side chose, as when a
// receiver refuses a frame over its limit with 1009.
type CloseError struct {
	Code   Code
	Reason string
	Local  bool
}
func (e *CloseError) Is(target error) bool { return target == ErrClosed }

// Conn is a frames duplex connection. A transport implements it; a protocol
// uses it and nothing beneath it. […] Once Close or Abort was called, every
// Send and Receive returns ErrClosed; once the remote closed, Receive returns
// a *CloseError and Send fails.
type Conn interface {
	// Send writes one frame after every frame sent before it. It blocks while
	// the transport cannot take more, until ctx ends.
	Send(ctx context.Context, frame Frame) error
	// Receive returns the next frame. A frame larger than the connection's
	// receive limit is not delivered: Receive returns an error and the
	// connection ends with CodeTooLarge.
	Receive(ctx context.Context) (Frame, error)
	// Close ends the connection with a code and a reason the remote side will
	// see, waiting for its acknowledgement until ctx ends where the transport
	// has one. A code that is not Sendable is refused with ErrUnsendableCode.
	Close(ctx context.Context, code Code, reason string) error
	// Abort ends the connection at once, with no handshake and nothing sent,
	// and releases every blocked Send and Receive. The remote side observes
	// CodeAbnormalClosure.
	Abort() error
}
```

`Sendable` already equals the valid set of R1–R3. What is missing at this layer:
- reason validation, in both length and UTF-8;
- a distinction between an argument error and an unsupported capability;
- any way to declare a capability;
- any record of a close this side made. After a local `Close` or `Abort`, the local side sees only the bare `ErrClosed`.

**The in-memory pipe** (`transports/go/pipe.go`):

```go
func (e *pipeEnd) Close(ctx context.Context, code Code, reason string) error {
	if !Sendable(code) {
		return ErrUnsendableCode
	}
	e.end(&CloseError{Code: code, Reason: reason}) // a sync.Once: a repeated Close is a silent no-op
	return nil
}
func (e *pipeEnd) Abort() error { e.end(nil); return nil } // the remote then reads CloseError{1006}
```

**[probed]** A 501-byte reason that is not UTF-8 is accepted and reaches the remote unchanged.

**The WebSocket adapter** (`transports/websocket/go/websocket.go`):

```go
func (c *connection) Close(ctx context.Context, code transports.Code, reason string) error {
	if !transports.Sendable(code) {
		return transports.ErrUnsendableCode
	}
	if c.closed() {
		return transports.ErrClosed
	}
	c.end()                                                // mark this side done
	return c.conn.Close(websocket.StatusCode(code), reason) // the library; ctx is ignored
}

// Abort closes the socket at once with no close frame.
func (c *connection) Abort() error {
	c.end()
	return c.conn.CloseNow()
}
```

The library underneath (coder/websocket v1.8.15, `close.go`) behaves like this:

```go
// The connection can only be closed once. Additional calls to Close are no-ops.
func (c *Conn) Close(code StatusCode, reason string) (err error) {
	if c.casClosing() {            // already closing: wait for the goroutines, then
		err = c.waitGoroutines()   // report net.ErrClosed
		…
		return net.ErrClosed
	}
	err = c.closeHandshake(code, reason) // write the close frame, wait up to 5 s for the reply
	err2 := c.close()                    // then drop the socket
	…
}
// CloseNow has the same "already closing" branch: it waits instead of dropping.

const maxCloseReason = maxControlPayload - 2 // 123 bytes; no UTF-8 check
func (ce CloseError) bytesErr() ([]byte, error) {
	if len(ce.Reason) > maxCloseReason {
		return nil, fmt.Errorf("reason string max is %v …", maxCloseReason, …)
	}
	…
}
```

What the probes found:
- **[probed]** `Close(1000, a 124-byte reason)` returns an error. But the adapter has already marked itself done, and the library drops the socket without writing a close frame. So the remote observes **1006**. An invalid argument therefore becomes an abort that the caller never asked for (against R8).
- **[probed]** A second `Close` returns `ErrClosed` immediately, without waiting for the first close's handshake (against R10).
- **[probed]** An `Abort` during a `Close` whose remote is not reading blocks for **4.9 s**, until the library's close handshake gives up (against R11).
- **[probed]** A second `Abort` returns an error wrapping `net.ErrClosed` instead of succeeding (against R11).
- **[probed]** When the receive limit trips, the library itself sends 1009 with its own reason, `"read limited at 1025 bytes"`. The local side's record says `"frame exceeds the receive limit"`. So the "local close" reason in the record is not the reason that was sent.
- Cancelling the context of a pending library read or write drops the socket, and the remote reads 1006.

**The engine's ending** (`engine/go/peer.go`):

```go
// Close ends the peer, closing the connection with 1000 and failing every
// pending call. It is safe to call more than once.
func (p *Peer) Close() error { p.end(transports.ErrClosed, transports.CodeNormal, ""); return nil }

// fail ends the peer on a transport there is nothing to say over: a write that
// failed, the context ending, a consumer that stalled past its deadline. The
// connection is aborted and the far side reads an abnormal closure.
func (p *Peer) fail(err error) { p.end(err, codeAborted, "") }

// refuse ends the peer on a frame the protocol does not admit […] with 4011.
func (p *Peer) refuse(err error) { p.end(err, transports.CodeProtocol, err.Error()) }

const codeAborted transports.Code = 0 // "no close at all"

func (p *Peer) end(err error, code transports.Code, reason string) {
	p.once.Do(func() {
		err = core.WithoutUnpublishedProof(core.Ended(err)) // the closed classification, with its cause
		p.mu.Lock(); p.err = err; p.mu.Unlock()
		close(p.done)
		if code == codeAborted || !transports.Sendable(code) {
			_ = p.conn.Abort()            // an invalid code becomes an abort
		} else {
			ctx, cancel := context.WithTimeout(context.Background(), p.options.WriteTimeout)
			_ = p.conn.Close(ctx, code, closeReason(reason)) // errors discarded
			cancel()
		}
		p.cancel()
	})
}

// closeReason is what a close frame admits: a reason of at most 123 bytes of
// valid UTF-8, cut on a rune […]
func closeReason(reason string) string { /* truncates silently; a short invalid reason passes */ }
```

The reader and writer loops already follow R20–R22:
- A failed write calls `fail`, which aborts.
- A failed read, including a remote close, calls `fail`.
- The five `refuse` (4011) call sites are all protocol violations by the far side: a binary frame, an undecodable envelope, a bad identifier, a request serial that did not increase, and a duplicate request id.
- A frame over the peer's own limit ends with 1009.
- Every operational failure aborts: backpressure, a stalled consumer, the peer's context ending, or a full queue.

What a user of the peer can observe about how it ended:
- `Peer.Err()` satisfies `errors.Is(err, ErrClosed)` and wraps the cause.
- After the remote closed, it holds the remote's `CloseError`, with its code and reason.
- After a local `Close()`, a local 4011 or a local abort, it holds only a Go cause, with no code recorded.
- The root endpoint's receiver is always told the same thing, whatever happened:

  ```go
  // engine/go/wire.go, when the peer ends
  if receiver != nil && receiver.Closed != nil {
  	receiver.Closed(transports.CodeGoingAway, "peer ended") // always 1001
  }
  // and the root endpoint's Close (the bitwire Endpoint method):
  func (w *rootWire) Close(code wire.Code, reason string) error {
  	w.peer.end(transports.ErrClosed, code, reason) // an observe-only code aborts instead
  	return nil                                    // never an error
  }
  ```

**Other carriers in core and dispatch:**
- **The local pair** (`core/go/pair.go`). Its `Close(code, reason)` delivers any integer and any reason verbatim to both sides' `Closed`, including 1004, 5000, non-UTF-8 text and 10 KB. It always returns `nil`, and has no abort. It also ends itself with **4011** for operational failures: `"local wire queue limit reached"`, `"local wire event consumer stalled"` and `"wire event receiver failed"`. It speaks no protocol, so 4011 there is only a local code handed to `Closed`.
- **The dispatcher** (`dispatch/go/dispatch.go`). When an application's event handler returns an error, it runs `registry.Close(CodeProtocolError /* 1002 */, "wire event rejected")`. When a dispatcher owns a peer's root endpoint, an application bug therefore closes the WebSocket with 1002.

**Who else implements `Conn`.** Nobody outside bitruntime. bitsystem3 and bitwire's conformance drivers only call `ErrClosed`, `CodeNormal`, `Peer.Close()` and `Endpoint.Close(1000, "")`, and discard the results. Inside bitruntime, test wrappers and the conformance testee *embed* `Conn`. The testee also has to build its own latch to learn the code a connection ended under, because the peer does not record it:

```go
// cmd/bitwire-testee/go/closed.go
// watched is a peer's connection, watched for how it ended […] bitruntime has
// no observer, and a peer's Err says why it ended but not the code: a frame it
// refused (4011) and a transport it gave up on (1006) end it alike.
type watched struct { transports.Conn; once sync.Once; ended chan struct{}; code transports.Code }
func (w *watched) Close(ctx context.Context, code transports.Code, reason string) error {
	if transports.Sendable(code) { w.end(code) }   // latched before the transport even succeeds
	return w.Conn.Close(ctx, code, reason)
}
func (w *watched) Abort() error { w.end(transports.CodeAbnormalClosure); return w.Conn.Abort() }
```

### Relevant code: TypeScript

All excerpts are from bitruntime v0.4.2. On the server side the TypeScript engine runs over sockets from the Node library `ws` 8.21.3. On the client side it runs over the platform's `WebSocket`, which follows the browser (WHATWG) API; Node's global `WebSocket` follows it too. Lines marked **[measured]** were checked by running scripts under Node 24.

**The transport interface** (`transports/ts/src/index.ts`). There is no abort and no capability declaration, and `close` returns nothing to wait on:

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
  /** What a send left with the connection and the transport has not taken yet […] */
  readonly buffered: number;
  /** Throws when the connection is not open. */
  send(frame: Frame): void;
  /** Throws a RangeError, and sends nothing, for a code that is not `sendable`. */
  close(code?: number, reason?: string): void;
  /** Registers handlers; returns a function that detaches all of them. */
  listen(handlers: ConnectionHandlers): () => void;
}

export function sendable(code: number): boolean {
  if (!Number.isInteger(code)) return false;
  if (code === 1005 || code === 1006 || code === 1015 || code === 1004) return false;
  return (code >= 1000 && code <= 1014) || (code >= 3000 && code <= 4999);
}
```

**The WebSocket adapter's close.** It wraps any socket with the browser API's shape, and silently drops a code or reason the platform refuses:

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

A test pins this fallback: a mocked browser socket asked to close with 1002 records the two calls `[[1002, 'wire event rejected'], []]`.

**What the platforms do with `close(code, reason)`:**
- **The browser API** (and Node's global `WebSocket`) validates before any state change: an `InvalidAccessError` for a code other than 1000 or 3000–4999, and a `SyntaxError` for a reason over 123 UTF-8 bytes. A reason containing an unpaired surrogate is silently replaced with U+FFFD, never refused. There is no abort. **[measured]**, Node's global `WebSocket` client against a `ws` server:

  | `close(...)` | Throws | Server observed (after the adapter's fallback) |
  | --- | --- | --- |
  | 1000, 'ok' | — | 1000 'ok' |
  | 1009 | `InvalidAccessError` | **1005**, with no reason |
  | 1002 | `InvalidAccessError` | **1005** |
  | 4011, 'Duplex connection closed' | — | 4011 with that reason |
  | 4000, a 124-byte reason | `SyntaxError` | **1005** |
  | 4000, `'a\uD800b'` | — | 4000, `'a�b'` (repaired silently) |

- **`ws`** accepts exactly the valid set of R1–R3 and has `terminate()`, a real abort. But its `close` sets the socket's state to CLOSING *before* validating the reason:

  ```js
  // ws 8.21.3, lib/websocket.js
  close(code, data) {
    if (this.readyState === WebSocket.CLOSED) return;
    …
    if (this.readyState === WebSocket.CLOSING) { … return; }  // a second close: nothing
    this._readyState = WebSocket.CLOSING;                      // state changes first
    this._sender.close(code, data, !this._isServer, …);        // throws RangeError for > 123 bytes
    setCloseTimer(this);                                       // never reached after the throw
  }
  ```

  **[measured]** Through bitruntime's adapter, `close(4000, a 124-byte reason)` on a `ws` socket throws inside `ws`, after the state has already moved to CLOSING. The adapter's fallback `socket.close()` then finds CLOSING and does nothing. The socket stays in CLOSING, with nothing sent and no timer, until something else tears the connection down.

**The engine's ending** (`engine/ts/src/peer.ts`). Every ending goes through one method:

```ts
private fail(error: PublicError, closeConnection = true, code = 4011, reason = CLOSE_REASON): void {
  const connection = this.connection;
  if (!connection) return;             // a repeated ending is a no-op
  this.connection = undefined;         // all state is torn down, and the peer's listeners are
  this.state = 'disconnected';         // detached, BEFORE the connection is closed
  this.detach?.();
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
const CLOSE_REASON = 'Duplex connection closed';
```

Its call sites, and the code each sends:

| Trigger | Error code | Close sent | Kind |
| --- | --- | --- | --- |
| binary frame, undecodable envelope, serial did not increase, duplicate request id | `invalid_message` | 4011 | protocol violation |
| incoming frame over `maxFrameBytes` | `frame_too_large` | 1009 | over limit |
| `connection.send` threw | `send_failed` | **4011** | write path |
| output did not drain within the write deadline | `write_timeout` | **4011** | write path |
| output queue full, event queue over capacity, a handler past its deadline, the root queue full | `busy`, `stalled_consumer` | **4011** | operational |
| the socket's `error` event, a connect timeout | `connection_failed`, `connect_timeout` | 4011 | operational |
| the remote closed | `disconnected` | none; the remote's code and reason are **dropped** | observed |
| `peer.close()` | `disconnected` | 1000 | caller |
| a caller's `root.close(code, reason)` | `disconnected` | the caller's code; an unsendable one becomes 1000 | caller |

The write path already detects failure: it calls `send` only when `buffered` is 0, then polls every 5 ms for the output to drain, up to the write deadline. But a write-path failure ends in a 4011 close, not an abort, because TypeScript has no abort. On the browser API, that close frame even queues behind the data that did not drain.

**What a user of the TypeScript peer can observe about how it ended:**
- `onClose` and `onError` receive the raw `PublicError`, such as `frame_too_large`, `send_failed` or `disconnected`.
- Pending calls reject with the one closed classification: `PublicError('disconnected')`, with the raw error as its `cause`.
- No close code or reason is available anywhere, neither the one sent nor the one received.
- As in Go, the root endpoint's receiver always gets `closed(1001, 'peer ended')`.
- bitruntime's conformance testee recovers the code only by listening to the underlying `ws` socket directly.

**The conflict, step by step.** This is a TypeScript client peer over the browser API: bitsystem3's browser client, `new Peer({ maxFrameBytes })` then `peer.connect(url)`.
1. The server sends a frame larger than `maxFrameBytes`. The browser has no receive limit, so it takes the whole message and delivers it.
2. The engine sees the frame is over its limit. It calls `fail(frame_too_large, true, 1009, 'frame exceeds the receive limit')`, which tears down the peer's state and detaches its listeners.
3. `sendable(1009)` is true, so the engine calls the adapter's `close(1009, …)`. The platform throws `InvalidAccessError`, and the fallback `socket.close()` closes with no code.
4. The server reads **1005**. `bitwire/1` binds 1009 here, and nothing tells the local user that 1009 was not sent.

The same happens to 1002 (the dispatcher's event-handler failure) and to any caller-requested code in 1001–1003 or 1007–1014. 4011 works on this platform, since it lies in 3000–4999.

So under R5 and R9 the adapter must declare "cannot send 1001–1003 or 1007–1014" and "no abort". Once it does, the engine has no conforming way to end the connection after an over-limit frame or a write failure.

**Who implements `FrameConnection`.** Only bitruntime itself: the WebSocket adapter, the pipe, and seven test doubles. bitwire's conformance drivers and bitsystem3 use the adapter and the peer, but implement nothing. Adding a required `abort()` would therefore break only bitruntime's own test doubles at compile time.

### How an ending travels today

In both languages, one connection's ending passes through the layers like this:

1. The **transport** notices: a remote close, a dropped socket, a frame over its receive limit, or a local `Close` or `Abort`.
2. The **engine** notices through its reader or writer loop, or through its own decision (`refuse` or `fail`). It then:
   - ends once;
   - records a Go error or TypeScript `PublicError` with a cause;
   - closes or aborts the transport;
   - fails every pending call;
   - tells the root endpoint's receiver `Closed(…)`.
3. The **dispatcher** attached to that root gets `Closed`, and tells each of its routes' receivers `Closed(…)`.
4. **Pending and later calls** fail with the closed classification. In Go that is `errors.Is(err, ErrClosed)`, with the cause wrapped. In TypeScript it is a `PublicError` with code `disconnected`, keeping the cause.

At each step the question "what code did this side send, and what did it observe" is either lost or answered with a constant. The contract asks for a record that keeps both, through every layer.

### Constraints

- **The wire is frozen.** `bitwire/1` binds 4011 for protocol violations and 1009 for over-limit frames. bitruntime reproduces nightseam v0.6.0's bytes exactly, and a byte-for-byte interoperability test runs in CI. Anything we change here must change no byte a conforming peer sends, except where edition 1 narrows a choice the revision leaves open, such as aborting instead of sending 4011 for an operational failure.
- **bitwire owns the contract; bitruntime implements it.** We may not redefine a rule. If a rule looks wrong, the fix is raised in bitwire, not worked around here. bitwire plans to publish the transport seam, the close codes and the closed classification as types in its own eight language bindings. So whatever shape bitruntime chooses now may later be replaced by bitwire's, and should make that migration easy.
- **Compatibility budget.** The Go module and the TypeScript package are pre-1.0 (v0.4.x), released together, and installed from GitHub releases rather than a registry. The known downstream users are:
  - bitsystem3;
  - bitwire's conformance drivers, which pin exact releases;
  - bitruntime's own conformance testees.

  No outside implementer of the Go `Conn` or the TypeScript `FrameConnection` is known. A breaking change is therefore possible, but it should be deliberate, versioned and small.
- **The two languages differ in shape, and should.** Go's seam is pull-based and blocking: `Send` blocks under backpressure, and `Receive` is called in a loop. TypeScript's seam is push-based: `send` returns at once and reports a `buffered` count, and frames arrive through registered handlers. Each language keeps its idioms: Go returns errors and uses `errors.Is`/`errors.As`, TypeScript throws. What must match is the observable guarantee, not the API shape.
- **Platform limits we cannot remove.**
  - The browser `WebSocket` API sends only codes 1000 and 3000–4999.
  - Its `close()` throws for other codes, and for a reason over 123 bytes.
  - It has no abort.
  - The Node `ws` library, which the TypeScript server side uses, accepts every valid code and has `terminate()`.
  - The Go library coder/websocket accepts every valid code, but has the close and abort behaviour probed above.
- **Tests before scenarios.** bitwire's API-boundary scenarios for these rules do not exist yet. bitruntime's own tests must carry the evidence until they do, without defining the expectations themselves.

### What we have considered

**Declaring capabilities.** Three shapes:
- (a) A method on the transport interface, such as `Capabilities() → {unsendable codes, has abort}`.
- (b) An optional interface discovered at run time. In Go that is a type assertion, `conn.(interface{ Capabilities() Capabilities })`, a pattern bitruntime already uses for `Subprotocol()`. In TypeScript it is a feature check.
- (c) A value fixed when the adapter is constructed and read by the engine at attach time.

Shape (a) breaks every implementer; (b) is invisible in the type; (c) cannot vary per connection.

**When the protocol needs a code the adapter cannot send.** The only real case is a TypeScript client peer over a browser `WebSocket` that must end with 1009 (over-limit). The same would apply to 1002, which the dispatcher uses. Options:
- (a) The engine refuses to be constructed over such an adapter when the claim is requested.
- (b) The engine is constructed, but its claim is reported as "`bitwire/1`" only, and it does something else when the case arises: aborts, closes 1000, or sends a private 4xxx code. Each of these is either not permitted (R9 forbids remapping) or not available (the platform has no abort).
- (c) The adapter implements 1009 some other way. For example, it enforces the limit before the platform does, but that does not change which code the platform can send.

We lean to: the TypeScript client over a browser socket claims `bitwire/1` alone, with the gap documented, while the Node `ws` adapter and both Go adapters claim edition 1. We are not sure what the engine should *do* when the case arises in the non-claiming combination. We are also not sure whether a peer that cannot send a code `bitwire/1` binds is even a conforming `bitwire/1` peer; bitwire's documents do not say.

**Adding abort to TypeScript's `FrameConnection`.** Options:
- (a) An optional member `abort?(): void`, paired with a capability flag.
- (b) A new interface, e.g. `AbortableFrameConnection`, which the engine accepts in addition to the old one.
- (c) A required member, taken as a breaking change in the next minor pre-1.0 version.

In Go, `Abort()` already exists on `Conn`; the question there is making it immediate and idempotent over coder/websocket. One option is to drop the underlying `net.Conn` directly instead of calling the library's `CloseNow`.

**The termination record.** Options:
- (a) Extend the existing Go `CloseError` with the record's members, and use one TypeScript error class.
- (b) A new record type carried as the cause of the closed error, with accessors like Go's `errors.As(err, &record)`.
- (c) An accessor on the peer, such as `peer.Termination()`, in addition to the error.

Some members are hard to fill on top of these libraries:
- "how far its close was written" (not started, partial or complete);
- "handshake complete";
- the reason actually sent, which coder/websocket replaces with its own.

We lean to reporting "unknown" where the library hides the fact, rather than guessing.

**Validation before state change.** Validate code and reason in bitruntime before calling into any library, in the adapter and again at the endpoint layers, because the libraries either validate too late (coder/websocket drops the socket after an invalid reason) or throw after partially acting. The questions are:
- whether invalid-argument and unsupported-capability should be distinct error *types* in each language, or one type with a kind;
- what "not valid UTF-8" means for a TypeScript `string`, which is UTF-16 and can hold unpaired surrogates.

**The endpoint layers.** `Close` on a bitwire `Endpoint` (the peer's root, the local pair, the dispatcher) today accepts anything and returns `nil`. Under R8 it must validate and return an argument error. The local pair's 4011s and the dispatcher's 1002 are operational or application failures, not protocol violations, so R22 suggests an abort or some other local ending. But a local pair has no transport to abort, and R18 says it "reports its logical ending directly".

## Questions for the expert

1. **Capabilities and claims across adapter, engine and platform.** How would you model what an adapter cannot do (valid codes it cannot send, a missing abort), and where would you decide which claim a given combination of adapter, engine and protocol revision may make? The hard case: the protocol binds a code, 1009, that a browser-hosted adapter cannot send and cannot replace. What should the engine do at that moment, and what should it promise beforehand? What have comparable systems done when a mandated behaviour is impossible on one platform? Examples might be protocol stacks with optional transport features, or TLS and QUIC libraries with platform-dependent capabilities.

2. **Evolving the seam without churning it twice.** We need to add an abort to TypeScript's transport interface, a capability declaration to both, distinct argument and unsupported-capability errors, and a local termination record through every layer. That spans the Go `Conn`, the TypeScript `FrameConnection`, and bitwire's `Endpoint.Close`, which returns `error` in Go and `void` in TypeScript. Our users are few and pinned, and the contract's owner plans to publish its own types for this seam later. How would you stage these changes so that each language stays idiomatic, the two stay equivalent in guarantee, and a later move to bitwire's types is cheap?

3. **An honest termination record on libraries that hide facts.** Given the behaviours quoted above, how would you fill the record where the library does not tell us something (unknown, or inferred)? The behaviours are: coder/websocket replaces the reason it sends, drops the socket after an invalid reason, and makes an abort wait for a close handshake; the browser API has no abort and no close-write status. Where should the record be exposed: on the error, on the peer, in the `Closed` notification? And how should `Closed(code, reason)` be projected for a protocol peer, which today always reports 1001, and for a local pair, which has no wire at all?

4. **Imposing "validate first, abort at once, close once" over libraries that do none of these.** How would you structure an adapter so that its guarantees hold even when the library underneath validates late, blocks, or reports errors after acting? We want it to validate before any state change, abort immediately and idempotently even during a close, and have a repeated close join the ending under way. Relatedly, how should an engine classify an error from a transport `send` as a write-path failure, and so abort, when the seam does not distinguish transport failure from other throws? And should a failure of the application's own handler, such as the dispatcher's 1002 today, end a connection at all?

5. **What are we not seeing?** Looking at the whole task (edition 1 now; publication, ownership and drain rules later; two runtimes with different concurrency models; independent test scenarios still to be written by the contract's owner), what would you do first, what would you deliberately leave for later, and what risks or design traps do you see that our framing misses?
