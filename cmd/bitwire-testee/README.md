# bitwire-testee

bitruntime's driver-1 testees for bitwire's
[`bitwire/1` conformance contract](https://github.com/Bitspark/bitwire/blob/main/conformance/protocol/CONTRACT.md),
edition 1. A testee is one bitruntime peer under remote control. It reads
requests as JSON lines on stdin and writes answers as JSON lines on stdout.
bitwire's runner drives two testees against each other and judges the
answers; a testee never sees the expectations.

Both testees use only bitruntime's public API. They are ported from nightseam
v0.6.0's testees (`5cc9723`), with provenance in [`NOTICE`](../../NOTICE).

| | Go | TypeScript |
| --- | --- | --- |
| Source | [`go/`](go) | [`ts/src/`](ts/src) |
| Released as | the module `github.com/Bitspark/bitruntime`, package `cmd/bitwire-testee/go`, at the release tag | the release asset `bitspark-bitruntime-testee-<version>.tgz`, whose `bin` is `bitwire-testee` |
| Build | `go build -o bitwire-testee github.com/Bitspark/bitruntime/cmd/bitwire-testee/go` inside a module that requires the release (the directory is named `go`, so name the output) | `npm install <runtime asset URL> <testee asset URL>` (see below) |
| Run | `./bitwire-testee` | `npx bitwire-testee` |

The TypeScript testee depends on the runtime by the same release's asset URL.
npm 11 installs it from the testee's URL alone. npm 12 installs URL
dependencies only as the project allows, and `allow-remote=root` in `.npmrc`
admits only URLs the project names itself. So name both assets, as above, and
the runtime's URL is the project's own.

**Claims.** Each testee implements the core layers, `seam` and `peer`, with the
features `listen` (WebSocket), `pipe`, `lazy` and `propagator`. Tunnel ops
answer `unsupported` until bitruntime implements tunnels
([bitruntime#11](https://github.com/Bitspark/bitruntime/issues/11)). So do the
excluded ops of the contract's §4.5 (the identity exchange, the recorded-wire
witness, `live.*`), and the optional observer, which bitruntime does not have.

`node scripts/testee-smoke.mjs` drives both testees through `hello`, a peer
pairing in each direction with an echo call, and `bye`. That checks the
testees, not conformance, which bitwire's runner judges
([bitruntime#20](https://github.com/Bitspark/bitruntime/issues/20)).
