// Observations of bitwire decision 0019 (proposed) for the TypeScript evidence;
// hydrated/go runs all thirteen, this suite the ones a second language must repeat.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { Atom, Tuple, atom, tuple, encodeMessage } from '@bitspark/bitwire';
import {
  Endpoint, Namespace, Scope, Refusal, hydratedTuple, items,
  UNKNOWN_EXPORT, STALE_SCOPE, UNEXPORTABLE, FOREIGN_NAMESPACE, MALFORMED_FRAME,
} from '../../../dist/hydrated/ts/src/index.js';
import { text, str, eventually, PATHS, tree, serve, call } from './harness.mjs';

test('observation 1: vectors round-trip (bitwire#83 at 20a6b6b, pinned)', () => {
  const raw = readFileSync(new URL('../../go/testdata/hydrated-vectors.json', import.meta.url));
  assert.equal(createHash('sha256').update(raw).digest('hex'), '621325467a19fa5d5ad34a1805e38feadb5609ad13fe5e71e991cf87b4d77278');
  const v = JSON.parse(raw);
  const from = (x) => ('atom' in x ? atom(Buffer.from(x.atom, 'hex')) : tuple(x.tuple.map(from)));
  const wires = (x) => (x instanceof Atom || x instanceof Tuple ? 0 : (items(x)?.reduce((n, c) => n + wires(c), 0) ?? 1));
  const s = new Scope(new Namespace('vectors'), [text('vectors')], { send: async () => {} });
  for (const c of v.encode) {
    const body = from(c.body);
    const live = s._decode(body, 0, new Map(), { nodes: 0, bytes: 0 });
    assert.equal(wires(live), (c.live.match(/wire\{/g) ?? []).length, c.name);
    const again = s._encode(live, 0, new Map(), { nodes: 0, bytes: 0 });
    assert.equal(Buffer.from(encodeMessage(again)).toString('hex'), c.hex, c.name);
  }
  for (const c of v.rejectBody) {
    assert.throws(() => s._decode(from(c.value), 0, new Map(), { nodes: 0, bytes: 0 }), (e) => e.reason === MALFORMED_FRAME, c.name);
  }
});

function echo(value) {
  const xs = items(value);
  const op = str(xs[0]), arg = xs[1], reply = xs.at(-1);
  if (op === 'echo') return void reply.send(arg);
  const cont = new Endpoint(1);
  cont.receive((next) => { const [v, w] = items(next); void w.send(v); void cont.close(); });
  void reply.send(hydratedTuple([arg, cont]));
}

for (const carrier of ['pair', 'websocket']) {
  test(`${carrier}: observation 2, recursion through an opaque router`, async (t) => {
    const n = await tree(t, carrier, 'client', 'provider');
    const [, svc] = serve(t, n.parts.provider.scope, echo);
    const r = await call(n.parts.client.scope.connect(svc), 'continue', text('hello'));
    const [arg, continuation] = items(r.value);
    assert.equal(str(arg), 'hello');
    const third = new Endpoint(1);
    const got = new Promise((resolve) => third.receive(resolve));
    await continuation.send(hydratedTuple([text('third Wire'), third]));
    assert.equal(str(await got), 'third Wire');
    assert.equal(n.trace.length, 4);
    assert.deepEqual(n.diags, []);
  });

  test(`${carrier}: observation 3, 200 long-lived calls stay bounded`, async (t) => {
    const n = await tree(t, carrier, 'client', 'provider');
    const client = n.parts.client.scope, provider = n.parts.provider.scope;
    const [, svc] = serve(t, provider, echo);
    const target = client.connect(svc);
    for (let i = 0; i < 200; i++) {
      const r = await call(target, 'echo', text(String(i)));
      assert.equal(str(r.value), String(i));
      assert.ok(client.live <= 1 && provider.live === 1, `call ${i}: ${client.live} ${provider.live}`);
    }
    assert.equal(client.live, 0);
    assert.deepEqual(n.diags, []);
  });

  test(`${carrier}: observation 4, a shared Wire ends only with its owner`, async (t) => {
    const n = await tree(t, carrier, 'client', 'provider', 'third');
    const { client: { scope: a }, provider: { scope: b }, third: { scope: c } } = n.parts;
    let atB, atC;
    const [, refC] = serve(t, c, (v) => { atC = v; });
    const toC = b.connect(refC);
    const [, refB] = serve(t, b, (v) => { atB = v; void toC.send(v); });
    const w = new Endpoint(8);
    const seen = [];
    w.receive((v, ctx) => seen.push(ctx));
    await a.connect(refB).send(w);
    await eventually(() => atC, 'C did not receive the Wire');
    assert.equal(b.live + c.live, 2, 'forwarding registered state');
    await atC.send(text('from C'));
    await eventually(() => seen.length === 1, "C's send did not reach W");
    await w.close();
    await atB.send(text('late B'));
    await atC.send(text('late C'));
    await eventually(() => n.diags.filter((d) => d.who === 'client' && d.reason === UNKNOWN_EXPORT).length === 2, 'late sends not refused at A');
    assert.equal(seen.length, 1);
  });

  test(`${carrier}: observation 7, a stale scope cannot reach the replacement`, async (t) => {
    const n = await tree(t, carrier, 'client', 'provider');
    const [, svc] = serve(t, n.parts.provider.scope, () => {});
    const old = n.parts.client.scope.connect(svc);
    n.parts.provider.scope.close();
    n.parts.provider.scope = new Scope(n.ns, PATHS.provider, n.parts.provider.ep);
    let hit = false;
    serve(t, n.parts.provider.scope, () => { hit = true; });
    await old.send(text('stale'));
    await eventually(() => n.diags.length === 1, 'not refused');
    assert.deepEqual(n.diags, [{ who: 'provider', reason: STALE_SCOPE }]);
    assert.equal(hit, false);
  });
}

test('observation 6: refusals before admission register and send nothing', async (t) => {
  const n = await tree(t, 'pair', 'client', 'provider');
  const a = n.parts.client.scope;
  const [, svc] = serve(t, n.parts.provider.scope, () => {});
  const target = a.connect(svc);
  const kept = new Endpoint(1);
  await assert.rejects(target.send(hydratedTuple([kept, { send: async () => {} }])), (e) => e.reason === UNEXPORTABLE);
  const foreign = new Scope(new Namespace('other'), [text('x')], { send: async () => {} })
    .connect({ path: [text('y')], scope: atom(new Uint8Array(16)), id: text('1') });
  await assert.rejects(target.send(hydratedTuple([kept, foreign])), (e) => e.reason === FOREIGN_NAMESPACE);
  const closed = new Endpoint(1);
  await closed.close();
  await assert.rejects(target.send(closed), (e) => e.reason === UNEXPORTABLE);
  assert.equal(a.live, 0);
  assert.equal(n.trace.length, 0);
});

test('observation 8: returning home', async (t) => {
  const n = await tree(t, 'pair', 'client', 'provider');
  const a = n.parts.client.scope;
  let proceed;
  const gate = new Promise((r) => { proceed = r; });
  const [, svc] = serve(t, n.parts.provider.scope, async (v) => {
    await gate;
    const [home, gone, reply] = items(v);
    void reply.send(hydratedTuple([text('data'), home, gone]));
  });
  const home = new Endpoint(1), gone = new Endpoint(1), reply = new Endpoint(1);
  const got = new Promise((resolve) => reply.receive(resolve));
  await a.connect(svc).send(hydratedTuple([home, gone, reply]));
  await gone.close();
  proceed();
  const [data, back, dead] = items(await got);
  assert.equal(str(data), 'data');
  assert.equal(back, home.wire, 'own reference did not return to the original face');
  await assert.rejects(dead.send(text('x')), (e) => e instanceof Refusal && e.reason === UNKNOWN_EXPORT);
});

test('observation 9: received context is not data', async (t) => {
  const n = await tree(t, 'pair', 'client', 'provider');
  const target = new Endpoint(1);
  t.after(() => target.close());
  const seen = new Promise((resolve) => target.receive((value, context) => resolve({ value, context })));
  const forged = tuple([text('context'), text('client-link')]);
  await n.parts.client.scope.connect(n.parts.provider.scope.expose(target)).send(forged);
  const r = await seen;
  assert.equal(r.context, 'provider-link');
  assert.ok(forged.equals(r.value));
});
