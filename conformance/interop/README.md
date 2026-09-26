# Interoperability with Nightseam v0.6.0

Test-only evidence that bitruntime speaks `bitwire/1` as Nightseam v0.6.0 does.
Bitwire decision 0008 defines `bitwire/1` as that release's behavior, so a
bitruntime peer and a Nightseam v0.6.0 peer must serve each other in both roles,
in both languages, and send the same bytes for the same exchange.

Nightseam v0.6.0 compiles against Bitwire 0.2.0, whose `Wire` is path-taking,
and bitruntime against Bitwire 0.3.0, so the two cannot share one Go binary. Each
implementation is its own program, and they meet only over real WebSockets.
The Nightseam programs are isolated in their own test-only modules and are never
a dependency of a published package.

| Program | Implementation |
| --- | --- |
| `bitruntime/go` | this repository's Go runtime |
| `bitruntime/ts` | this repository's TypeScript runtime |
| `nightseam/go` | Nightseam v0.6.0 Go (`github.com/Bitspark/nightseam v0.6.0`) |
| `nightseam/ts` | Nightseam v0.6.0 TypeScript (`@nightseam/runtime` and `@nightseam/duplex` 0.6.0) |

## The scenario

Every program has two roles. `server <port>` listens on `127.0.0.1:<port>` and
prints `LISTEN ws://127.0.0.1:<port>/wire` once it accepts connections. `client
<url>` connects, runs the calls below and prints one JSON object of observations.
Both sides use a 4 MiB frame budget, as bitsystem3 does.

The server serves, at its connection's root:

| Path | Kind | Behavior |
| --- | --- | --- |
| `["echo"]` | request | returns its params |
| `["spaces", "a/b", "echo"]` | request | returns `{"space": "a/b", "params": params}` |
| `["spaces", "é", ""]` | request | returns `"unicode-empty"` |
| `["fail"]` | request | refuses with public error `bad_request`, `refused on purpose`, data `{"n": 1}` |
| `["wait"]` | request | waits until cancelled, then records the cancellation |
| `["cancelled"]` | request | returns whether a `wait` was cancelled, waiting up to 2 s |
| `["meta"]` | request | returns the meta its request carried, `{}` for none |
| `["reverse"]` | request | calls the client's `["whoami"]` over the same connection and returns its result |
| `["big"]` | request | returns a string of `params.n` `x` characters |
| `["ping"]` | event | emits event `["pong"]` with `{"echo": data}` |

The client serves `["whoami"]`, returning `"<name>-client"` where `<name>` is its
program's name, and listens for `["pong"]`. It observes:

| Observation | Expected |
| --- | --- |
| `echo` | the params `{"a":[1,"x",null,true],"n":1000,"u":"😀"}`, as a JSON value |
| `nested` | `{"space":"a/b","params":{"x":1}}` |
| `unicodeEmpty` | `"unicode-empty"` |
| `missing` | error code `method_not_found` for `["missing"]` |
| `fail` | `{"code":"bad_request","message":"refused on purpose","data":{"n":1}}` |
| `pong` | `{"echo":7}` after emitting `["ping"]` with `7` |
| `withdrawn` | `true`: a `wait` call withdrawn after 300 ms ended without a response |
| `serverSawCancel` | `true` |
| `meta` | `{"tenant":"t1"}` for a `meta` call carrying that meta |
| `reverse` | `"<name>-client"` of the client program |
| `bigLength` | `3145728` |
| `concurrent` | `20`: twenty concurrent `echo` calls each returned its own params |

`scripts/interop.mjs` builds every program, starts each server and runs each
client against it, and compares the observations with this table.

## Byte-level identity

`recorder/go` is a raw WebSocket client that sends fixed envelopes to a server
and records every frame the server sends back, answering the server's reverse
request itself. The runner records each server and requires the bitruntime
transcripts to equal the Nightseam transcripts of the same language byte for
byte, after masking only the random span identifiers a peer mints for its own
requests.
