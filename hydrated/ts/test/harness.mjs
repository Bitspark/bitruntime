// Shared harness for the hydrated TypeScript observations and consumer suites:
// participants of one namespace around an opaque middle router that knows only
// routes and ground values, over local pairs or real WebSockets. Consumer suites
// import it rather than editing hydrated.test.mjs.
import assert from 'node:assert/strict';
import { atom } from '@bitspark/bitwire';
import { pair, addressed } from '../../../dist/core/ts/src/index.js';
import { listenWebSocket, connectWebSocket } from '../../../dist/websocket/ts/src/index.js';
import { Endpoint, Namespace, Scope, hydratedTuple } from '../../../dist/hydrated/ts/src/index.js';

export const text = (s) => atom(new TextEncoder().encode(s));
export const str = (a) => new TextDecoder().decode(a.bytes());
export const tick = () => new Promise((r) => setTimeout(r, 5));
export async function eventually(cond, what) {
  for (let i = 0; i < 1000; i++) { if (cond()) return; await tick(); }
  assert.fail(what);
}

export const PATHS = { client: [text('client42')], provider: [text('a'), text('b'), text('service2')], third: [text('c')] };
export const key = (p) => p.map((a) => Buffer.from(a.bytes()).toString('hex')).join('/') + `#${p.length}`;

export async function links(t, carrier, n) {
  if (carrier === 'pair') return Array.from({ length: n }, () => { const ends = pair(); t.after(() => ends[0].close()); return ends; });
  const accepted = [];
  let wake;
  const listener = await listenWebSocket({ host: '127.0.0.1' }, (e) => { accepted.push(e); wake?.(); });
  t.after(() => listener.close());
  const out = [];
  for (let i = 0; i < n; i++) {
    const c = await connectWebSocket(listener.url);
    t.after(() => c.close());
    while (accepted.length <= i) await new Promise((r) => { wake = r; });
    out.push([c, accepted[i]]);
  }
  return out;
}

// Participants around an opaque middle that knows routes and ground values only.
export async function tree(t, carrier, ...names) {
  const ns = new Namespace('test');
  const routes = new Map(), trace = [], diags = [], parts = {};
  const ls = await links(t, carrier, names.length);
  names.forEach((name, i) => {
    const mine = addressed(ls[i][0]), mid = addressed(ls[i][1]);
    routes.set(key(PATHS[name]), mid);
    mid.receive((p, v) => { trace.push(v); void routes.get(key(p))?.send(p, v); });
    const part = { name, ep: mine, scope: new Scope(ns, PATHS[name], mine) };
    mine.receive((p, v) => {
      try { part.scope.deliver(p, v, `${name}-link`); } catch (e) { diags.push({ who: name, reason: e.reason ?? e.message }); }
    });
    parts[name] = part;
  });
  return { ns, trace, diags, parts };
}

export function serve(t, scope, handler) {
  const e = new Endpoint(64);
  e.receive(handler);
  t.after(() => e.close());
  return [e, scope.expose(e)];
}

export async function call(target, op, ...args) {
  const reply = new Endpoint(1);
  const got = new Promise((resolve) => reply.receive((value, context) => resolve({ value, context })));
  await target.send(hydratedTuple([text(op), ...args, reply]));
  const r = await got;
  await reply.close();
  return r;
}
