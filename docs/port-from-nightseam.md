# Ported from Nightseam v0.6.0

The runtime path hand-written adapters use is ported from Nightseam
**v0.6.0**, commit `5cc9723a24646c40ed1861f892b2b23eb6d785d7` (tag `v0.6.0`).
None of the 41 unreleased commits after it is ported: the six that touch the
runtime rename types, fix a test's own race, or add the declared-composition API
that Bitwire decision 0012 supersedes. The verbatim import is its own commit, so
every adaptation is visible as a diff; `NOTICE` records the provenance.

`bitwire/1` is the behavior of that release (Bitwire decision 0008). The engine
sends, accepts and refuses the same frames, with the same close codes, bounds and
serial rules. The envelope vectors in `vectors/bitwire-1/` are that release's
tables, byte for byte.

## Where each piece went

| Nightseam v0.6.0 | bitruntime (Go) | TypeScript |
| --- | --- | --- |
| `duplex/go` `Conn`, `Frame`, codes, `Pipe` | `transports/go` | `transports/ts` |
| `duplex/go/ws` | `transports/websocket/go` | `transports/ts` (`webSocketConnection`) |
| `duplex/go` `At`, `Mount`; `runtime/go` `ForwardWire` | `core/go` `At`, `Mount`, `Forward` | `core/ts` `at`, `mount`, `forward` |
| `runtime/go` `NewWirePair`, `Invocation`, publication evidence, meta, trace | `core/go` `NewPair`, `Invocation`, `Unpublished`, `WithMeta`, `Propagator` | `core/ts` |
| `runtime/go` `NewDispatcher`, `CallWire`, `EmitWire`, `HandleWire`, `RegisterWire` | `dispatch/go` `NewDispatcher`, `Call`, `Emit`, `Handle`, `Register` | `dispatch/ts` |
| `runtime/go` `Peer`, `NewPeer` | `engine/go` `Peer`, `NewPeer` | `engine/ts` `Peer` |
| `runtime/go` `Accept`, `NewHandler`, `Dial` | `engine/websocket/go` | `engine/ts` `Peer.connect`, `Peer.attach` |
| envelope codec, path encoding, Unicode guard | `internal/profile/go` (not public) | module-private |
| received context (private association) | `internal/delivery/go` (not public) | module-private |

## Go names

| v0.6.0 | bitruntime |
| --- | --- |
| `duplex.At`, `duplex.Mount`, `runtime.ForwardWire` | `core.At`, `core.Mount`, `core.Forward` |
| `duplex.ErrNoRoute`, `duplex.ErrPath`, `duplex.ErrReceiverExists` | `core.ErrMissingPath`, `core.ErrInvalidPath`, `core.ErrReceiverExists` |
| `duplex.ErrClosed`, `runtime.ErrClosed` | `transports.ErrClosed`, the one closed classification |
| `runtime.ErrBackpressure` | `core.ErrBackpressure` |
| `duplex.Code*`, `duplex.CodeDuplex` | `transports.Code*`, `transports.CodeProtocol` (4011) |
| `duplex.Pipe`, `duplex.Conn`, `duplex.Frame`, `duplex.CloseError` | `transports.Pipe`, `transports.Conn`, `transports.Frame`, `transports.CloseError` |
| `ws.New` | `websocket.New` (`transports/websocket/go`) |
| `runtime.NewWirePair(runtime.Options{…})` | `core.NewPair(core.PairOptions{…})` |
| `runtime.PublicError`, `Unpublished`, `UnpublishedError`, `WithoutUnpublishedProof` | `core.*` |
| `runtime.Invocation`, `CaptureInvocation`, `BeginInvocationBody`, `RelayInvocationControl`, limits and errors | `core.*` |
| `runtime.WithMeta`, `MetaFrom`, `Trace`, `Propagator`, `DefaultPropagator`, `TraceOf` | `core.*` |
| `runtime.NewDispatcher`, `Dispatcher`, `SelectedEndpoint`, `DispatcherOptions`, `HandlerRegistry` | `dispatch.NewDispatcher`, `Dispatcher`, `SelectedEndpoint`, `DispatcherOptions`, `Registry` |
| `runtime.CallWire(…, WireCallOptions{RequestTimeout})` | `dispatch.Call(…, CallOptions{Timeout})` |
| `runtime.EmitWire`, `WireEmitOptions` | `dispatch.Emit`, `EmitOptions` |
| `runtime.HandleWire`, `RegisterWire`, `WireHandler`, `WireEventHandler`, `WireHandlers` | `dispatch.Handle`, `Register`, `Handler`, `EventHandler`, `Handlers` |
| `runtime.NewPeer`, `ClientRole`, `ServerRole`, `Options` | `engine.NewPeer`, `ClientRole`, `ServerRole`, `Options` |
| `runtime.Accept`, `NewHandler`, `Dial`, `ServerOptions`, `DialOptions` | `websocket.Accept`, `NewHandler`, `Dial`, … (`engine/websocket/go`) |
| `peer.Wire()` | `peer.Wire()`, an `Endpoint` over `bitwire.AddressedWire` |

## Behavior that changed

Each change is a recorded defect or research verdict, fixed rather than ported.
None changes a `bitwire/1` frame.

- **nightseam#722.** A closing local pair answers every refusal still queued
  with the refusal it was admitted with; v0.6.0 dropped them, leaving callers to
  their own deadlines. The peer's root likewise answers requests still queued
  when the peer ends (research R28), with their refusal or `disconnected`.
- **nightseam#658.** A pair's response frees its call's pending slot before the
  caller can hold the answer, so a caller at the limit can issue its next call
  at once. A call whose cancellation is still queued keeps its slot until that
  cancellation drains.
- **One closed classification (R26).** Every ended carrier reports an error for
  which `errors.Is(err, transports.ErrClosed)` holds, keeping its cause
  (`core.ErrBackpressure`, a remote `CloseError`, a context error). A forwarded
  request whose destination closed or overflowed is answered `disconnected`,
  not `internal`.
- **Observe-only close codes (R27).** `transports.Sendable` separates codes that
  may be sent from 1005, 1006 and 1015. Transports refuse the latter with
  `ErrUnsendableCode`; a peer asked to close with one aborts instead.
- **Receive limits.** A frame over the pipe's limit ends it with 1009 on both
  sides, as a WebSocket does; a WebSocket past its read limit is ended promptly
  instead of being left half closed.
- **Forwarding (research 0001, row 14).** A message the destination refuses
  fails only that message; v0.6.0 detached both directions.
- **Removed:** the peer's raw method-name API (`Handle`, `HandleEvent`,
  `OnEvent`, `Call`, `Emit`, `Options.Handlers`, `Options.Events`). Only the root
  Endpoint presents the protocol (research R20); a request whose method is not
  a canonical path encoding is answered `method_not_found`, as a v0.6.0 peer
  without that handler answers. Observer hooks and family labels are removed
  until the engine's observation hooks are designed with Bitwire's
  received-context revision (charter §1).

## Not ported in this milestone

Live references, tunnels, the framed byte stream `bitwire-stream/1`, the
declaration identity check, authentication, observers and the generator. They
keep their own issues (nightseam#720, #721, #723, #724 and bitruntime#2).
