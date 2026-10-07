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

test('observation 1: vectors round-trip (bitwire#83 accepted at 6e33fb3, pinned)', () => {
  const raw = readFileSync(new URL('../../go/testdata/hydrated-vectors.json', import.meta.url));
  assert.equal(createHash('sha256').update(raw).digest('hex'), '9d5f684871d1b56719b33bdff3114a101d2a7a1a8702d1f4474303df0e109312');
  const v = JSON.parse(raw);
  const from = (x) => ('atom' in x ? atom(Buffer.from(x.atom, 'hex')) : tuple(x.tuple.map(from)));
  const wires = (x) => (x instanceof Atom || x instanceof Tuple ? 0 : (items(x)?.reduce((n, c) => n + wires(c), 0) ?? 1));
  const s = new Scope(new Namespace('vectors'), [text('vectors')], { send: async () => {} });
  for (const c of v.encode) {
    const body = from(c.body);
    const live = s._decodeBody(body, new Map());
    assert.equal(wires(live), (c.live.match(/wire\{/g) ?? []).length, c.name);
    const again = s._encodeBody(live, new Map());
    assert.equal(Buffer.from(encodeMessage(again)).toString('hex'), c.hex, c.name);
  }
  for (const c of v.rejectBody) {
    assert.throws(() => s._decodeBody(from(c.value), new Map()), (e) => e.reason === MALFORMED_FRAME, c.name);
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

for (const carrier of ['pair', 'websocket']) {
  test(`${carrier}: observation 8, one reference grants no sibling`, async (t) => {
    const n = await tree(t, carrier, 'client', 'provider', 'third');
    const provider = n.parts.provider.scope;
    let privateHits = 0;
    const [, pub] = serve(t, provider, () => {});
    const [, intended] = serve(t, provider, () => { privateHits++; });
    await n.parts.third.scope.connect(intended).send(text('legitimate'));
    await eventually(() => privateHits === 1, 'the intended holder was not served');
    assert.equal(pub.id.length, 16);
    assert.ok(!pub.id.equals(intended.id));
    const bump = Uint8Array.from(pub.id.bytes()); bump[15]++;
    const forged = [atom(bump), atom(new Uint8Array(16)), text('0000000000000002')];
    for (const id of forged) await n.parts.client.scope.connect({ ...pub, id }).send(text('forged')).catch(() => {});
    // A counter-like id of the wrong length is refused before it leaves.
    await assert.rejects(n.parts.client.scope.connect({ ...pub, id: text('2') }).send(text('forged')), (e) => e.reason === MALFORMED_FRAME);
    await eventually(() => n.diags.length === forged.length, 'forged references were not refused');
    for (const d of n.diags) assert.ok(d.who === 'provider' && ['unknown-export', 'malformed-frame'].includes(d.reason), JSON.stringify(d));
    assert.equal(privateHits, 1);
  });
}

test('observation 15, incoming: a frame counts whole, reference material included', () => {
  const s = new Scope(new Namespace('x'), [text('a')], { send: async () => {} }, { exports: 4, nodes: 64, depth: 8, bytes: 128 });
  const target = new Endpoint(4);
  let delivered = 0;
  target.receive(() => { delivered++; });
  const ref = s.expose(target);
  const huge = tuple([atom([2]), tuple([tuple([atom(new Uint8Array(4096))]), atom(new Uint8Array(16)), atom(new Uint8Array(16))])]);
  const frame = tuple([text('bitwire/hydrated/1'), ref.scope, ref.id, tuple([atom([1]), tuple([huge])])]);
  assert.throws(() => s.deliver(s.path, frame, undefined), (e) => e.reason === 'limit');
  assert.equal(delivered, 0);
});

test('observation 17: Endpoint laws on every path', async () => {
  const got = [];
  const local = new Endpoint(4);
  local.receive((v) => got.push(v));
  const priv = new Endpoint(1);
  await local.wire.send(priv);
  await local.wire.send(hydratedTuple([priv]));
  await eventually(() => got.length === 2, 'local sends not delivered');
  assert.equal(got[0], priv.wire, 'a bare endpoint kept its authority');
  assert.equal(items(got[1])[0], priv.wire, 'an endpoint inside a tuple kept its authority');
  assert.equal('close' in got[0], false);

  const e = new Endpoint(4);
  const stale = e.receive(() => {});
  stale();
  let seen = 0;
  e.receive(() => { seen++; });
  stale();
  await e.send(text('after'));
  await eventually(() => seen === 1, 'a stale detach removed the newer receiver');

  const s = new Scope(new Namespace('x'), [text('a')], { send: async () => {} });
  const failing = new Endpoint(4);
  failing.receive(() => { throw new Error('receiver fault'); });
  const ref = s.expose(failing);
  s.deliver(s.path, tuple([text('bitwire/hydrated/1'), ref.scope, ref.id, text('x')]), undefined);
  await eventually(() => failing.termination?.kind === 'failed' && s.live === 0, 'the failing receiver did not terminate its endpoint');
  assert.throws(() => s.deliver(s.path, tuple([text('bitwire/hydrated/1'), ref.scope, ref.id, text('y')]), undefined), (e) => e.reason === UNKNOWN_EXPORT);
  assert.equal((await failing.closed).kind, 'failed');
  const quiet = new Endpoint(1);
  assert.equal(quiet.termination, undefined);
  await quiet.close();
  assert.deepEqual(await quiet.closed, { kind: 'closed' });
});

test('observation 15, symmetry: sender and receiver count a value alike', async () => {
  const face = new Endpoint(1);
  let nested = text('x');
  for (let i = 0; i < 3; i++) nested = hydratedTuple([nested, face]);
  const values = {
    'one face three times': hydratedTuple([face, face, face]),
    'nested faces': nested,
    'a long atom': atom(new Uint8Array(96)),
    'atoms and a face': hydratedTuple([text('a'), text('bb'), face]),
  };
  const seen = { true: 0, false: 0 };
  for (let nodes = 2; nodes <= 8; nodes++) for (let depth = 1; depth <= 5; depth++) for (const bytes of [40, 64, 96, 128, 1024]) {
    const limits = { exports: 4, nodes, depth, bytes };
    for (const [name, v] of Object.entries(values)) {
      const frames = [];
      const sender = new Scope(new Namespace('x'), [text('s')], { send: async (_p, f) => { frames.push(f); } }, limits);
      const receiver = new Scope(new Namespace('x'), [text('r')], { send: async () => {} }, limits);
      const ref = receiver.expose(new Endpoint(4));
      let accepted = true;
      try { await sender.connect(ref).send(v); } catch (e) { if (e.reason !== 'limit') throw e; accepted = false; }
      seen[accepted]++;
      let frame = frames[0];
      if (!accepted) {
        const open = new Scope(new Namespace('x'), [text('s')], { send: async (_p, f) => { frame = f; } }, { exports: 4, nodes: 1 << 20, depth: 1 << 10, bytes: 1 << 20 });
        await open.connect(ref).send(v);
      }
      let received = true;
      try { receiver.deliver(receiver.path, frame, undefined); } catch (e) { if (e.reason !== 'limit') throw e; received = false; }
      assert.equal(received, accepted, `${name} at nodes ${nodes} depth ${depth} bytes ${bytes}`);
    }
  }
  assert.ok(seen.true > 0 && seen.false > 0, JSON.stringify(seen));
});

test('observation 17, construction: the exported tuple class cannot bypass projection', async () => {
  const { HydratedTuple } = await import('../../../dist/hydrated/ts/src/index.js');
  const priv = new Endpoint(1);
  assert.throws(() => new HydratedTuple([priv]), /hydratedTuple/);
  assert.throws(() => new HydratedTuple([text('ground')]), /hydratedTuple/);
  const got = [];
  const local = new Endpoint(4);
  local.receive((v) => got.push(v));
  await local.wire.send(hydratedTuple([hydratedTuple([priv]), text('x')]));
  await eventually(() => got.length === 1, 'not delivered');
  const inner = items(items(got[0])[0])[0];
  assert.equal(inner, priv.wire, 'a nested endpoint kept its authority');
  assert.ok(hydratedTuple([text('a')]) instanceof Tuple, 'a tuple without a Wire is not ground');
});

test('observation 17, local edges: bounds, failure tracking and recognition on local sends', async () => {
  for (const bad of [0, -1, 1.5, NaN, Infinity]) assert.throws(() => new Endpoint(bad), RangeError, String(bad));
  const silent = new Endpoint(1);
  silent.receive(() => { throw undefined; });
  await silent.wire.send(text('x'));
  assert.equal((await silent.closed).kind, 'failed', 'a receiver throwing undefined still failed');

  const got = [];
  const local = new Endpoint(8);
  local.receive((v) => got.push(v));
  const foreign = { kind: 'wire', send: async () => {} };
  await assert.rejects(local.wire.send(foreign), (e) => e.reason === UNEXPORTABLE);
  await assert.rejects(local.wire.send(hydratedTuple([text('a'), hydratedTuple([foreign])])), (e) => e.reason === UNEXPORTABLE);
  const closed = new Endpoint(1);
  const closedFace = closed.wire;
  await closed.close();
  await assert.rejects(local.wire.send(hydratedTuple([closedFace])), (e) => e.reason === UNEXPORTABLE);
  await local.wire.send(hydratedTuple([text('ok'), new Endpoint(1)]));
  await eventually(() => got.length === 1, 'the valid value was not delivered');
  assert.equal(got.length, 1, 'a refused local value was delivered');
});
