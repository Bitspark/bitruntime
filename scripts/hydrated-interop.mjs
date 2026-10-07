// Observation 16 of bitwire decision 0019: Go and TypeScript peers
// exchange bitwire/hydrated/1 frames over a real WebSocket, each in both roles.
// The exchange: a request carrying a reply Wire, a reply carrying a continuation
// Wire, and a third Wire sent back through the continuation.
import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
import { atom, pathEqual } from '@bitspark/bitwire';
import { addressed } from '../dist/core/ts/src/index.js';
import { connectWebSocket, listenWebSocket } from '../dist/websocket/ts/src/index.js';
import { Endpoint, Namespace, Scope, hydratedTuple, items, readReference, referenceValue } from '../dist/hydrated/ts/src/index.js';

const directory = mkdtempSync(join(tmpdir(), 'bitruntime-hydrated-'));
const binary = join(directory, process.platform === 'win32' ? 'hydrated.exe' : 'hydrated');
assert.equal(spawnSync('go', ['build', '-o', binary, './conformance/interop/hydrated'], { stdio: 'inherit' }).status, 0, 'Go peer build failed');

const text = (s) => atom(new TextEncoder().encode(s));
const str = (a) => new TextDecoder().decode(a.bytes());
const ns = new Namespace('interop');
const BOOTSTRAP = [text('bootstrap')];
const within = (promise, what) => Promise.race([promise, new Promise((_, reject) => setTimeout(() => reject(new Error(what)), 10000))]);

function participant(link, path, onBootstrap) {
  const scope = new Scope(ns, path, link);
  link.receive((p, v) => {
    if (pathEqual(p, BOOTSTRAP)) return onBootstrap(v);
    try { scope.deliver(p, v, 'link'); } catch (e) { console.error('diagnostic:', e.reason ?? e.message); }
  });
  return scope;
}

function peer(args) {
  const child = spawn(binary, args, { stdio: ['pipe', 'pipe', 'inherit'] });
  const lines = [], readers = [];
  createInterface({ input: child.stdout }).on('line', (line) => { const r = readers.shift(); if (r) r(line); else lines.push(line); });
  const timeout = setTimeout(() => child.kill(), 30000);
  const ended = new Promise((resolve, reject) => { child.once('error', reject); child.once('exit', (code) => { clearTimeout(timeout); resolve(code); }); });
  return { child, ended, line: () => (lines.length ? Promise.resolve(lines.shift()) : new Promise((r) => readers.push(r))) };
}

// Go serves, TypeScript calls.
{
  const server = peer(['serve']);
  const url = await server.line();
  assert.match(url ?? '', /^ws:/);
  const endpoint = await connectWebSocket(url);
  let bootstrapped;
  const bootstrap = new Promise((r) => { bootstrapped = r; });
  const scope = participant(addressed(endpoint), [text('c')], (v) => bootstrapped(v));
  const target = scope.connect(readReference(await within(bootstrap, 'no bootstrap reference')));
  const reply = new Endpoint(1);
  const replied = new Promise((r) => reply.receive(r));
  await target.send(hydratedTuple([text('continue'), text('hello'), reply]));
  const [arg, continuation] = items(await within(replied, 'no reply'));
  assert.equal(str(arg), 'hello');
  const third = new Endpoint(1);
  const reached = new Promise((r) => third.receive(r));
  await continuation.send(hydratedTuple([text('third Wire'), third]));
  assert.equal(str(await within(reached, 'third Wire not reached')), 'third Wire');
  await endpoint.close();
  server.child.stdin.end();
  assert.equal(await server.ended, 0, 'Go server failed');
  console.log('Go serves, TypeScript calls: ok');
}

// TypeScript serves, Go calls.
{
  const listener = await listenWebSocket({ host: '127.0.0.1' }, (endpoint) => {
    const link = addressed(endpoint);
    const scope = participant(link, [text('s')], () => {});
    const service = new Endpoint(16);
    service.receive((v) => {
      const [, arg, replyWire] = items(v);
      const cont = new Endpoint(1);
      cont.receive((next) => { const [value, wire] = items(next); void wire.send(value); void cont.close(); });
      void replyWire.send(hydratedTuple([arg, cont]));
    });
    void link.send(BOOTSTRAP, referenceValue(scope.expose(service)));
  });
  const client = peer(['client', listener.url]);
  assert.equal(await within(client.line(), 'Go client did not finish'), 'ok');
  assert.equal(await client.ended, 0, 'Go client failed');
  await listener.close();
  console.log('TypeScript serves, Go calls: ok');
}
process.exit(0);
