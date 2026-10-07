# bitruntime

Go and TypeScript implementations of [bitwire 0.5.0](https://github.com/Bitspark/bitwire/tree/v0.5.0): addressless endpoints, addressed access, complete byte-keyed trees, local pairs and WebSocket carriers.

Wire grants sending ground ontos values. Endpoint adds one detachable receive owner, termination and closure. AddressedWire adds a separate path parameter; an AddressedEndpoint presents that layer over an existing endpoint. WireNode is a complete deixis tree whose own values are send capabilities.

## TypeScript

Install the release package and shared contract:

```sh
npm install --allow-remote=root --@bitspark:registry=https://registry.npmjs.org @bitspark/bitwire@0.5.0 https://github.com/Bitspark/bitruntime/releases/download/v0.6.0/bitspark-bitruntime-0.6.0.tgz
```

```ts
import { atom } from '@bitspark/bitwire';
import { pair, addressed, bind } from '@bitspark/bitruntime/core';

const [left, right] = pair();
const a = addressed(left), b = addressed(right);
b.receive((path, message) => console.log(path, message));
await bind(a, [atom([255]), atom([])]).send(atom([1]));
await left.close();
```

A raw endpoint can send an atom directly. The addressed facade works on the same endpoints, retains their lifetime, and attaches no receiver until requested. Raw and addressed receiving share one ownership slot.

`/core` exports `pair`, `addressed`, `bind`, `under`, `asAddressed`, `compose`, `select` and `route`. Binding captures a path and returns send authority only; `under` captures a prefix. `asAddressed` selects a WireNode node and invokes its own sender once. MissingPathError differs from refusal by that sender. `route(tree,path,message)` does the analogous explicit selection for receiver trees. Structural construction and selection invoke no own capabilities.

`/hydrated` realizes bitwire's hydrated layer (decision 0019): `Scope`, `Endpoint`, `Namespace`, `hydratedTuple` and `items`. A message may carry live Wires. A participant's dispatcher hands its `Scope` each value addressed to its path, with the received context. An `Endpoint`'s `wire` is the only exportable local Wire, and closing the endpoint ends its export.

`/websocket` exports `connectWebSocket`, `listenWebSocket` and `bindWebSocketServer`. A binding owns accepted endpoints and leaves a supplied HTTP/HTTPS server open. A convenience listener owns its HTTP server too. WebSocket negotiates `bitwire.ontos.v2` and carries whole ontos-codec-v1 values. The optional addressed layer encodes `bitwire/addressed/1` as an ordinary value above any carrier.

## Go

```sh
go get github.com/Bitspark/bitruntime@v0.7.0
```

Use `core/go.NewPair` and `websocket/go.Dial`, `Accept`, `NewServer` or `Listen`. They return bitwire Endpoints. `core/go.Addressed`, `Bind`, `Under` and `AsAddressed` implement the same layering as TypeScript; absent tree paths return ErrMissingPath. `hydrated/go` provides `NewScope`, `NewEndpoint`, `NewNamespace` and `NewTuple` over bitwire's hydrated declarations. A receiver runs on its endpoint's dispatcher. Close does not wait for or cancel dispatched application work.

## Limits and ownership

Send success means local admission. Defaults: 16 MiB encoded message, 64 MiB queued encoded bytes and 1024 queued messages per direction; codec depth 4096. Invalid or oversized outgoing values are refused before admission. Incoming overflow fails the endpoint. Local-pair overflow fails both ends and refuses the overflowing send. Repeated equal values remain distinct admissions. The addressed wrapper adds encoded overhead within these same bounds.

Connections use normal TLS verification. Dialing has a ten-second establishment bound and closure a five-second grace period before forced release. These bounds impose no service deadline. Servers accept absent Origin or matching authority by default. TypeScript's `allowedOrigins` and Go's `OriginPatterns` permit explicit other origins; `authorize`/`Authorize` supplies host authentication policy. Paths and messages do not authenticate callers.

Complete tree construction rejects duplicate byte keys, cycles and malformed children, with validation depth bounded at 4096. Child nodes retain identity; supplied implementations must keep obeying the tree laws. Opaque addressed access does not provide complete tree discovery. Invocation, replies, subscriptions, authorization and live-wire allocation belong to explicitly defined layers above the wire.

The [composition plan](docs/COMPOSITION.md) records the next target: reusable
runtime-tree routing, multiplexing, export/import of existing wires and stream
carriers. It distinguishes missing protocols and implementations from this
released foundation, and separates runtime machinery from executable host policy
and durable allocation services.

## Validation and delivery

```sh
go vet ./...
go test -race -count=1 ./...
npm ci --ignore-scripts
npm run check
npm test
node scripts/interop.mjs
node scripts/package-smoke.mjs
```

Tests use bitwire-owned observations, independent bytes, native carrier failures, TLS, resource release and cross-language peers. Packed-consumer tests install into a fresh project. [RELEASING.md](RELEASING.md) defines the public Go tag and GitHub package asset process. [The realization record](docs/WIRE-RUNTIME.md) states the contract and evidence.
