import assert from 'node:assert/strict';
import test from 'node:test';
import { Endpoint, UNKNOWN_EXPORT, hydratedTuple } from '../../../dist/hydrated/ts/src/index.js';
import { eventually, tree } from './harness.mjs';
import {
  AdapterOwner, DomainError, OutcomeUnknown, ProtocolError, invoke, text, tupleItems,
} from './fixtures/domain.mjs';
import { textAdapter } from './fixtures/text.mjs';
import { counterAdapter } from './fixtures/counter.mjs';
import { adaptCell, cellAdapter, wireCellAdapter } from './fixtures/cell.mjs';

async function setup(t, carrier) {
  const network = await tree(t, carrier, 'client', 'provider');
  const client = new AdapterOwner(), provider = new AdapterOwner();
  for (const [name, owner] of [['client', client], ['provider', provider]]) {
    const part = network.parts[name];
    // Attachment ownership is explicit test composition, outside the domain adapters.
    void part.ep.closed.then(() => owner.close());
    t.after(async () => { await owner.close(); part.scope.close(); });
  }
  return { network, client, provider };
}

function remoteView(setup, source, clientAdapter, providerAdapter) {
  const local = providerAdapter.toWire(source);
  const endpoint = setup.provider.ownedEndpoint(local);
  assert.ok(endpoint, 'test bootstrap needs the locally owned root endpoint');
  const reference = setup.network.parts.provider.scope.expose(endpoint);
  const remote = setup.network.parts.client.scope.connect(reference);
  return { view: clientAdapter.fromWire(remote), wire: remote };
}

function textValue(label, trace) {
  return { render: async prefix => { trace.push(`${label}.render(${prefix})`); return `${prefix}:${label}`; } };
}
function counterValue(label, initial, trace) {
  let count = initial;
  return {
    read: async () => { trace.push(`${label}.read`); return count; },
    add: async delta => {
      trace.push(`${label}.add(${delta})`);
      if (count + delta < 0) throw new DomainError('below-zero');
      count += delta;
      return count;
    },
  };
}
function cellValue(initial, trace, label = 'cell') {
  let selected = initial;
  return {
    source: {
      get: async () => { trace.push(`${label}.get`); return selected; },
      set: async value => { trace.push(`${label}.set`); selected = value; },
    },
    replace: value => { selected = value; },
  };
}

async function textScenario(project) {
  const trace = [];
  const cell = cellValue(textValue('initial', trace), trace);
  const view = project(cell.source);
  assert.deepEqual(trace, [], 'constructing adapters invoked the provider');
  cell.replace(textValue('late', trace));
  const results = [await (await view.get()).render('one')];
  await view.set(textValue('caller', trace));
  results.push(await (await cell.source.get()).render('two'));
  results.push(await (await view.get()).render('three'));
  assert.deepEqual(results, ['one:late', 'two:caller', 'three:caller']);
  assert.deepEqual(trace, [
    'cell.get', 'late.render(one)', 'cell.set', 'cell.get', 'caller.render(two)',
    'cell.get', 'caller.render(three)',
  ]);
  return { results, trace };
}

async function counterScenario(project) {
  const trace = [];
  const cell = cellValue(counterValue('initial', 0, trace), trace);
  const view = project(cell.source);
  assert.deepEqual(trace, []);
  cell.replace(counterValue('late', 10, trace));
  const returned = await view.get();
  const results = [await returned.add(3), await returned.read()];
  await view.set(counterValue('caller', 50, trace));
  results.push(await (await cell.source.get()).add(2));
  const alias = await view.get();
  results.push(await alias.read());
  await assert.rejects(alias.add(-100), error => error instanceof DomainError && error.code === 'below-zero');
  results.push(await alias.read());
  assert.deepEqual(results, [13, 13, 52, 52, 52]);
  assert.deepEqual(trace, [
    'cell.get', 'late.add(3)', 'late.read', 'cell.set', 'cell.get', 'caller.add(2)',
    'cell.get', 'caller.read', 'caller.add(-100)', 'caller.read',
  ]);
  return { results, trace };
}

for (const carrier of ['pair', 'websocket']) {
  for (const [name, makeInner, scenario] of [
    ['Text', textAdapter, textScenario], ['Counter', counterAdapter, counterScenario],
  ]) {
    test(`${carrier}: Cell<${name}> agrees with direct calls, lazy reads and reverse writes`, async t => {
      const s = await setup(t, carrier);
      const provider = cellAdapter(wireCellAdapter(s.provider), makeInner(s.provider));
      const client = cellAdapter(wireCellAdapter(s.client), makeInner(s.client));
      const direct = await scenario(source => source);
      const adapted = await scenario(source => remoteView(s, source, client, provider).view);
      assert.deepEqual(adapted, direct);
      assert.deepEqual(s.network.diags, []);
      assert.deepEqual([...s.client.faults, ...s.provider.faults], []);
    });
  }

  test(`${carrier}: Cell<Cell<Text>> reuses the outer adapter for both nested write directions`, async t => {
    const s = await setup(t, carrier), trace = [];
    const outerProvider = wireCellAdapter(s.provider), outerClient = wireCellAdapter(s.client);
    const leafProvider = cellAdapter(outerProvider, textAdapter(s.provider));
    const leafClient = cellAdapter(outerClient, textAdapter(s.client));
    const nestedProvider = cellAdapter(outerProvider, leafProvider);
    const nestedClient = cellAdapter(outerClient, leafClient);
    const inner = cellValue(textValue('original', trace), trace, 'inner');
    const root = cellValue(inner.source, trace, 'root');
    const { view } = remoteView(s, root.source, nestedClient, nestedProvider);
    assert.deepEqual(trace, []);
    inner.replace(textValue('late', trace));
    const first = await view.get();
    assert.equal(await (await first.get()).render('a'), 'a:late');
    await first.set(textValue('caller-leaf', trace));
    assert.equal(await (await inner.source.get()).render('b'), 'b:caller-leaf');

    const callerInner = cellValue(textValue('caller-inner', trace), trace, 'caller-cell');
    await view.set(callerInner.source);
    // Provider calls an inner cell that originated at the caller.
    const atProvider = await root.source.get();
    assert.equal(await (await atProvider.get()).render('c'), 'c:caller-inner');
    await atProvider.set(textValue('provider-leaf', trace));
    // Caller calls the provider capability that travelled through its own cell.
    assert.equal(await (await (await view.get()).get()).render('d'), 'd:provider-leaf');
    assert.deepEqual(trace, [
      'root.get', 'inner.get', 'late.render(a)', 'inner.set', 'inner.get',
      'caller-leaf.render(b)', 'root.set', 'root.get', 'caller-cell.get',
      'caller-inner.render(c)', 'caller-cell.set', 'root.get', 'caller-cell.get',
      'provider-leaf.render(d)',
    ]);
    assert.deepEqual(s.network.diags, []);
    assert.deepEqual([...s.client.faults, ...s.provider.faults], []);
  });

  test(`${carrier}: malformed domain messages and declared failures stay distinct`, async t => {
    const s = await setup(t, carrier), trace = [];
    const provider = counterAdapter(s.provider), client = counterAdapter(s.client);
    const { view, wire } = remoteView(s, counterValue('counter', 4, trace), client, provider);
    await assert.rejects(invoke(s.client, wire, 'add', [text('not an integer')]), ProtocolError);
    await assert.rejects(invoke(s.client, wire, 'missing', []), ProtocolError);
    assert.deepEqual(trace, [], 'malformed input reached the provider');
    await assert.rejects(view.add(-5), error => error instanceof DomainError && error.code === 'below-zero');
    assert.equal(await view.read(), 4);
    assert.deepEqual(trace, ['counter.add(-5)', 'counter.read']);

    const bad = s.provider.endpoint(request => {
      const reply = tupleItems(request, 3)[2];
      return reply.send(hydratedTuple([text('unknown status'), text('data')]));
    });
    const badProxy = s.network.parts.client.scope.connect(s.network.parts.provider.scope.expose(bad));
    await assert.rejects(invoke(s.client, badProxy, 'anything', []), ProtocolError);
    assert.equal(s.client.open, 0, 'failed calls kept reply endpoints');
    assert.equal(s.network.parts.client.scope.live, 0, 'failed calls kept reply exports');
    assert.deepEqual(s.network.diags, []);
    assert.deepEqual([...s.client.faults, ...s.provider.faults], []);
  });

  test(`${carrier}: Cell.set preserves asynchronous completion and provider failure`, async t => {
    const s = await setup(t, carrier), started = Promise.withResolvers(), finish = Promise.withResolvers();
    const trace = [];
    const source = {
      get: async () => { throw new Error('get must not be invoked'); },
      set: async () => {
        trace.push('started');
        started.resolve();
        await finish.promise;
        trace.push('refused');
        throw new DomainError('write-denied');
      },
    };
    const provider = cellAdapter(wireCellAdapter(s.provider), textAdapter(s.provider));
    const client = cellAdapter(wireCellAdapter(s.client), textAdapter(s.client));
    const { view } = remoteView(s, source, client, provider);
    assert.deepEqual(trace, []);
    let settled = false;
    const operation = view.set(textValue('borrowed', []));
    const checked = assert.rejects(operation, error => error instanceof DomainError && error.code === 'write-denied');
    void operation.then(() => { settled = true; }, () => { settled = true; });
    await started.promise;
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(settled, false, 'local admission was mistaken for domain completion');
    finish.resolve();
    await checked;
    assert.deepEqual(trace, ['started', 'refused']);
    assert.equal(s.client.open, 1, 'reply must close while the offered service remains owned');
    assert.equal(s.network.parts.client.scope.live, 1);
    assert.deepEqual(s.network.diags, []);
    assert.deepEqual([...s.client.faults, ...s.provider.faults], []);
  });

  test(`${carrier}: 200 Cell<Text> reads release replies and keep service exports bounded`, async t => {
    const s = await setup(t, carrier), trace = [];
    const provider = cellAdapter(wireCellAdapter(s.provider), textAdapter(s.provider));
    const client = cellAdapter(wireCellAdapter(s.client), textAdapter(s.client));
    const source = cellValue(textValue('stable', trace), trace).source;
    const { view, wire } = remoteView(s, source, client, provider);
    const a = s.network.parts.client.scope, b = s.network.parts.provider.scope;
    for (let i = 0; i < 200; i++) {
      assert.equal(await (await view.get()).render(String(i)), `${i}:stable`);
      assert.equal(a.live, 0, `read ${i}: reply export retained`);
      assert.equal(b.live, 2, `read ${i}: service rematerialized`);
      assert.equal(s.client.open, 0);
      assert.equal(s.provider.open, 2);
    }
    assert.equal(trace.length, 400, 'extra provider invocations');
    const borrowed = new Endpoint(1), arrived = Promise.withResolvers();
    borrowed.receive(arrived.resolve);
    t.after(() => borrowed.close());
    const borrowedProxy = a.connect(b.expose(borrowed));
    await s.provider.close();
    assert.equal(b.live, 1, 'owner closed an independently owned endpoint');
    await wire.send(text('late'));
    await eventually(() => s.network.diags.length === 1, 'closed service was not refused');
    assert.deepEqual(s.network.diags, [{ who: 'provider', reason: UNKNOWN_EXPORT }]);
    await borrowedProxy.send(text('still alive'));
    assert.ok((await arrived.promise).equals(text('still alive')));
    assert.equal(trace.length, 400);
    assert.deepEqual([...s.client.faults, ...s.provider.faults], []);
  });

  test(`${carrier}: attachment loss settles a pending call without claiming rollback`, async t => {
    const s = await setup(t, carrier);
    const started = Promise.withResolvers(), finish = Promise.withResolvers();
    let completed = false;
    const source = { render: async () => { started.resolve(); await finish.promise; completed = true; return 'effect completed'; } };
    const { view } = remoteView(s, source, textAdapter(s.client), textAdapter(s.provider));
    const pending = assert.rejects(view.render('lost'), OutcomeUnknown);
    await started.promise;
    await s.network.parts.client.ep.close();
    await pending;
    assert.equal(s.client.open, 0);
    assert.equal(s.network.parts.client.scope.live, 0);
    // End the provider's service lifetime; already-started work can still finish.
    await s.provider.close();
    finish.resolve();
    await eventually(() => completed, 'provider effect was incorrectly cancelled');
    assert.deepEqual([...s.client.faults, ...s.provider.faults], []);
  });
}

test('Cell lifting respects identity and composition for reads, writes and provider failures', async () => {
  const r = { to: number => String(number), from: value => Number(value) };
  const s = { to: value => ({ decimal: value }), from: value => value.decimal };
  const composed = { to: value => s.to(r.to(value)), from: value => r.from(s.from(value)) };
  const identity = { to: value => value, from: value => value };
  async function observe(project) {
    const trace = [];
    let value = 7;
    const source = {
      get: async () => { trace.push(['get', value]); return value; },
      set: async next => {
        trace.push(['set', next]);
        if (next < 0) throw new DomainError('negative');
        value = next;
      },
    };
    const view = project(source);
    assert.deepEqual(trace, []);
    const before = await view.get();
    await view.set(19);
    await assert.rejects(view.set(-1), error => error instanceof DomainError && error.code === 'negative');
    const after = await view.get();
    assert.equal(before, 7);
    assert.equal(after, 19);
    assert.deepEqual(trace, [['get', 7], ['set', 19], ['set', -1], ['get', 19]]);
    return { before, after, trace };
  }
  const direct = await observe(source => source);
  assert.deepEqual(await observe(source => adaptCell(source, identity)), direct);
  const lower = cell => ({ get: async () => Number((await cell.get()).decimal), set: value => cell.set({ decimal: String(value) }) });
  const successive = await observe(source => lower(adaptCell(adaptCell(source, r), s)));
  const together = await observe(source => lower(adaptCell(source, composed)));
  assert.deepEqual(successive, direct);
  assert.deepEqual(together, direct);
});
