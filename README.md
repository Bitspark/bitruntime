# bitruntime

Go and TypeScript implementations of [bitwire 0.4.0](https://github.com/Bitspark/bitwire/tree/v0.4.0): a generic duplex envelope Wire, local pairs, binary WebSocket carriers and complete byte-keyed trees.

`send` means local admission. Paths and identifiers contain exact bytes; payloads are immutable ground ontos values. Receivers have one detachable owner. Endpoints enforce finite envelope and queue bounds, and `closed` observes release of owned carrier resources. Invocation and service conventions belong to consumers.

## TypeScript

Install the release package and the shared contract:

```sh
npm install --allow-remote=root --@bitspark:registry=https://registry.npmjs.org @bitspark/bitwire@0.4.0 https://github.com/Bitspark/bitruntime/releases/download/v0.5.0/bitspark-bitruntime-0.5.0.tgz
```

```ts
import { atom, tuple } from '@bitspark/bitwire';
import { pair } from '@bitspark/bitruntime/core';

const [left, right] = pair();
right.receive(envelope => console.log(envelope.payload));
await left.send({ source: [], destination: [atom([])], id: atom([1]), payload: tuple([]) });
await left.close();
```

`/core` exports `pair`, `compose`, `select` and `route`. Trees are separate from endpoint discovery: accepting paths does not imply complete child enumeration.

`/websocket` exports `connectWebSocket`, `listenWebSocket` and `bindWebSocketServer`. A binding owns its accepted endpoints but leaves a supplied HTTP/HTTPS server open. A convenience listener owns its HTTP server too. WebSocket uses `bitwire.ontos.v1` and canonical binary `bitwire/envelope/1` messages.

## Go

```sh
go get github.com/Bitspark/bitruntime@v0.5.0
```

Use `core/go.NewPair` and `websocket/go.Dial`, `Accept`, `NewServer` or `Listen`. These return endpoints implementing `github.com/Bitspark/bitwire/wire/go.Wire` directly. A Go receiver runs on its endpoint's dispatcher; application work may spawn its own goroutines. Close does not wait for or cancel already dispatched application work.

## Limits and ownership

Defaults: 16 MiB encoded envelope, 64 MiB queued encoded bytes and 1024 queued envelopes in each direction; codec depth 4096. Invalid or oversized outgoing envelopes are refused before admission. Incoming overflow fails the endpoint. A local pair's overflow fails both ends and refuses the overflowing send. Duplicate IDs remain distinct admissions.

Connections use normal TLS certificate verification. Dialing has a ten-second establishment bound. Closure has a five-second grace period before forced carrier release. These bounds do not impose service deadlines. Servers accept absent Origin or a matching authority by default. TypeScript's `allowedOrigins` and Go's `OriginPatterns` explicitly permit other origins; `authorize`/`Authorize` supplies host authentication policy. Claimed source paths never authenticate callers.

Complete tree construction rejects duplicate byte keys, cycles and malformed children. Validation has a depth bound of 4096. Child nodes retain identity; supplied implementations must keep obeying the tree laws after construction.

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

Tests use bitwire-owned conformance observations, independent envelope bytes, native carrier failures, TLS, resource release and cross-language peers. The package smoke installs a packed tarball into a fresh project. [RELEASING.md](RELEASING.md) defines the public Go tag and GitHub package asset process. [The realization record](docs/ENVELOPE-RUNTIME.md) defines this clean replacement and its evidence.
