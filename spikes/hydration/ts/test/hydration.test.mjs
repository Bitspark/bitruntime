import assert from 'node:assert/strict';
import test from 'node:test';
import { atom, tuple, Tuple, isValue } from '@bitspark/bitwire';
import { Hydration } from '../dist/hydration.js';
import { live, wire, items, LiveWire } from '../dist/live.js';
import { provideEcho, echoFromWire } from '../dist/echo.js';
import { tree, bytes, leftPath, rightPath, until } from './harness.mjs';

for (const carrier of ['pair', 'websocket']) {
  test(`${carrier}: nested reply and continuation Wires cross an opaque router`, async t => {
    const network = await tree(t, carrier);
    const callback = wire(() => {});
    const service = network.right.expose(provideEcho()); // composition only
    const api = echoFromWire(network.left.connect(service));
    const request = atom([0, 255, 10, 47]);
    const result = await api.echo(request, callback);
    assert.ok(result.value.equals(request));
    assert.equal(result.returned, callback, 'returning home restores the original object');
    assert.ok(result.continuation instanceof LiveWire);
    assert.deepEqual(Object.keys(result.continuation), ['send']);
    assert.equal('close' in result.continuation, false);
    assert.equal('receive' in result.continuation, false);
    assert.equal(isValue(result.continuation), false);
    let delivered;
    await result.continuation.send(live(bytes('third Wire'), wire(v => { delivered = v; })));
    await until(() => delivered !== undefined);
    assert.ok(delivered.equals(bytes('third Wire')));
    assert.equal(network.trace.length, 4);
    assert.ok(network.trace.every(entry => isValue(entry.value)));
    assert.deepEqual(network.trace.map(entry => entry.path), [rightPath, leftPath, rightPath, leftPath]);
    assert.deepEqual(network.faults, []);
    assert.equal(network.right.retained.exports, 2, 'forwarded proxies are not re-exported');
  });

  test(`${carrier}: reference-shaped data stays data; aliases stay aliases`, async t => {
    const network = await tree(t, carrier);
    let observed;
    const destination = network.right.expose(wire(v => { observed = v; }));
    const target = network.left.connect(destination);
    const local = wire(() => {});
    const ref = network.left.expose(local);
    const lookalike = tuple([bytes('hydration-spike/1'), tuple(ref.path), ref.scope, ref.id]);
    const taggedLookalike = tuple([atom([2]), tuple([tuple(ref.path), ref.scope, ref.id])]);
    await target.send(live(lookalike, taggedLookalike, local, live(local)));
    await until(() => observed !== undefined);
    const [ordinary, tagged, first, nested] = items(observed);
    assert.ok(ordinary instanceof Tuple && ordinary.equals(lookalike));
    assert.ok(tagged instanceof Tuple && tagged.equals(taggedLookalike));
    assert.equal(first, items(nested)[0]);
    assert.equal(network.right.retained.imports, 1);
    assert.equal(network.left.retained.exports, 1);
    assert.deepEqual(network.faults, []);
  });

  test(`${carrier}: stale scope cannot reach replacement at the same byte path`, async t => {
    const network = await tree(t, carrier);
    let oldCalls = 0, newCalls = 0;
    const oldTarget = wire(() => { oldCalls++; });
    const oldReference = network.right.expose(oldTarget);
    const oldProxy = network.left.connect(oldReference);
    const replacement = network.replaceRight();
    const newReference = replacement.expose(wire(() => { newCalls++; }));
    assert.ok(oldReference.id.equals(newReference.id), 'IDs may repeat in a different incarnation');
    assert.ok(!oldReference.scope.equals(newReference.scope));
    await oldProxy.send(bytes('late')); // local admission, not remote execution
    await until(() => network.faults.length === 1);
    assert.match(network.faults[0].message, /stale scope/);
    assert.equal(newCalls, 0);
    await network.left.connect(newReference).send(bytes('fresh'));
    await until(() => newCalls === 1);
    await oldTarget.send(bytes('still borrowed'));
    assert.equal(oldCalls, 1, 'ending export scope does not close its target');
  });

  test(`${carrier}: owner withdrawal and carrier loss end associations, not borrowed targets`, async t => {
    const network = await tree(t, carrier);
    let calls = 0;
    const original = wire(() => { calls++; });
    const ref = network.right.expose(original), proxy = network.left.connect(ref);
    network.right.withdraw(original);
    const next = network.right.expose(original);
    assert.ok(!ref.id.equals(next.id), 'withdrawn IDs are never reused in the scope');
    await proxy.send(bytes('retired'));
    await until(() => network.faults.length === 1);
    assert.match(network.faults[0].message, /unknown export/);
    assert.equal(calls, 0);
    await network.closeCarrier();
    await assert.rejects(proxy.send(bytes('closed')), /scope ended/);
    assert.deepEqual(network.left.retained, { imports: 0, exports: 0 });
    await original.send(bytes('owned elsewhere'));
    assert.equal(calls, 1);
  });
}

const reference = () => ({ path: rightPath, scope: atom(new Uint8Array(16)), id: bytes('1') });

test('long-lived Echo stops at call 64: scope retention is not production reclamation', async t => {
  const network = await tree(t, 'pair');
  const callback = wire(() => {});
  const api = echoFromWire(network.left.connect(network.right.expose(provideEcho())));
  for (let i = 0; i < 63; i++) await api.echo(bytes('retained until scope close'), callback);
  await assert.rejects(api.echo(bytes('64th call'), callback), /export limit/);
  assert.deepEqual(network.left.retained, { exports: 64, imports: 64 });
  assert.deepEqual(network.right.retained, { exports: 64, imports: 64 });
  assert.deepEqual(network.faults, []);
});

test('failed encoding is atomic; registry limits apply to nested and distinct Wires', async () => {
  const sent = [];
  const host = new Hydration(leftPath, { send: async (p, v) => { sent.push([p, v]); } }, { exports: 1 });
  const proxy = host.connect(reference());
  const a = wire(() => {}), b = wire(() => {});
  await assert.rejects(proxy.send(live(a, live(b))), /export limit/);
  assert.deepEqual(host.retained, { exports: 0, imports: 1 });
  assert.equal(sent.length, 0);
  await proxy.send(live(a, a));
  assert.equal(host.retained.exports, 1);
  assert.equal(sent.length, 1);
  host.close();
});

test('value and pending-send bounds reject before adding unbounded work', async () => {
  const pending = Promise.withResolvers();
  const host = new Hydration(leftPath, { send: () => pending.promise }, { work: 1, nodes: 8, bytes: 40, depth: 3 });
  const proxy = host.connect(reference());
  await assert.rejects(proxy.send(atom(new Uint8Array(41))), /value limit/);
  const deep = live(live(live(live(bytes('deep')))));
  await assert.rejects(proxy.send(deep), /value limit/);
  const first = proxy.send(bytes('one'));
  await assert.rejects(proxy.send(bytes('two')), /send limit/);
  pending.resolve();
  await first;
  host.close();
});

test('malformed trailing data rolls back newly staged imports', async () => {
  let calls = 0;
  const host = new Hydration(leftPath, { send: async () => {} });
  const destination = host.expose(wire(() => { calls++; }));
  const ref = reference();
  const encodedRef = tuple([atom([2]), tuple([tuple(ref.path), ref.scope, ref.id])]);
  const invalid = tuple([atom([99]), tuple([])]);
  const message = tuple([bytes('hydration-spike/1'), destination.scope, destination.id,
    tuple([atom([1]), tuple([encodedRef, invalid])])]);
  await assert.rejects(host.deliver(leftPath, message), /unknown live tag/);
  assert.equal(host.retained.imports, 0);
  assert.equal(calls, 0);
  host.close();
});

test('import and in-flight receiver limits are bounded independently of carrier', async () => {
  const pending = Promise.withResolvers();
  const host = new Hydration(leftPath, { send: async () => {} }, { imports: 1, work: 1 });
  const destination = host.expose(wire(() => pending.promise));
  const ref = reference();
  host.connect(ref);
  assert.throws(() => host.connect({ ...ref, id: bytes('2') }), /import limit/);
  const message = tuple([bytes('hydration-spike/1'), destination.scope, destination.id,
    tuple([atom([0]), bytes('work')])]);
  const admitted = host.deliver(leftPath, message);
  await assert.rejects(host.deliver(leftPath, message), /receive limit/);
  host.close();
  pending.resolve();
  await admitted; // already admitted work is not canceled by scope close
  assert.deepEqual(host.retained, { exports: 0, imports: 0 });
});

test('transport rejection retains admitted exports until owner cleanup', async () => {
  const host = new Hydration(leftPath, { send: async () => { throw new Error('carrier ended'); } });
  const proxy = host.connect(reference());
  await assert.rejects(proxy.send(wire(() => {})), /carrier ended/);
  assert.equal(host.retained.exports, 1);
  host.close();
  assert.equal(host.retained.exports, 0);
});
