import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { atom, tuple, encodeMessage, packAddressed } from '@bitspark/bitwire';
import { pair, addressed } from '../../../dist/core/ts/src/index.js';
import {
  Table, Importer, reference, parseReference, ExportLimitError,
  FOREIGN_REFERENCE, UNKNOWN_REFERENCE,
} from '../../../dist/export/ts/src/index.js';

// export/go/testdata/export-vectors.json is bitwire's corpus at record commit
// f0f72525048971d98e299154532c4734f89b9042, pinned by hash for both languages.
const raw = readFileSync(new URL('../../go/testdata/export-vectors.json', import.meta.url));
assert.equal(createHash('sha256').update(raw).digest('hex'), '2b8bd52d06493d752d3c962e9fd5ac62c00da72e54071c8cd3efeb9e092064f0');
const v = JSON.parse(raw);
const hex = (b) => Buffer.from(b).toString('hex');
const a = (h) => atom(Buffer.from(h, 'hex'));
const text = (s) => atom(new TextEncoder().encode(s));
const from = (x) => ('atom' in x ? a(x.atom) : tuple(x.tuple.map(from)));

function recorder() {
  const got = [];
  return { got, send: async (value) => { got.push(value); } };
}
const tick = () => new Promise((r) => setTimeout(r, 10));
async function eventually(cond, what) {
  for (let i = 0; i < 300; i++) { if (cond()) return; await tick(); }
  assert.fail(what);
}

test('encode and reject vectors', () => {
  for (const c of v.encode) {
    const value = c.path ? packAddressed(c.path.map(a), from(c.message)) : from(c.value);
    if (!c.path) {
      const { scope, id } = parseReference(value);
      assert.ok(reference(scope, id).equals(value), c.name);
    }
    assert.equal(hex(encodeMessage(value)), c.hex, c.name);
  }
  for (const c of v.rejectReference) assert.throws(() => parseReference(from(c.value)), c.name);
});

for (const c of v.deliveries) {
  test(`delivery: ${c.name}`, () => {
    const tb = new Table(8, a(v.table.scope));
    const targets = {};
    for (const idHex of ['31', '32', '33']) {
      const r = recorder();
      const { id } = parseReference(tb.export(r));
      assert.equal(hex(id.bytes()), idHex);
      targets[idHex] = r;
    }
    tb.withdraw(reference(tb.scope, a('33')));
    const refused = [];
    tb.onRefuse = (reason) => refused.push(reason);
    const msg = text('m');
    tb.deliver(c.path.map(a), msg);
    if (c.expect === 'nothing') {
      assert.equal(tb.live, 2); assert.deepEqual(refused, []);
    } else if (c.expect.deliver) {
      assert.equal(targets[c.expect.deliver].got.length, 1);
      assert.ok(targets[c.expect.deliver].got[0].equals(msg));
    } else if (c.expect.release) {
      assert.equal(tb.live, 1);
    } else {
      assert.deepEqual(refused, [c.expect.refuse]);
    }
    for (const [id, r] of Object.entries(targets)) {
      if (!c.expect.deliver || id !== c.expect.deliver) assert.equal(r.got.length, 0, `export ${id} received`);
    }
  });
}

const ROUTE = text('r'), ROOT = [text('x')];

function connect() {
  const [x, y] = pair();
  const mk = (e) => {
    const ep = addressed(e);
    const s = { ep, table: new Table(8), importer: new Importer(ep, ROOT), refused: [] };
    s.table.onRefuse = (reason) => s.refused.push(reason);
    return s;
  };
  return [mk(x), mk(y), x];
}
function listen(s, onRoute) {
  s.ep.receive((path, value) => {
    if (path.length && path[0].equals(ROOT[0])) s.table.deliver(path.slice(1), value);
    else if (path.length && path[0].equals(ROUTE) && onRoute) onRoute(value);
  });
}
function hop(inSide, outSide) {
  listen(inSide, (rec) => {
    const refs = rec.at(1).items().map((ref) => outSide.table.reExport(inSide.importer.import(ref)));
    void outSide.ep.send([ROUTE], tuple([rec.at(0), tuple(refs)]));
  });
}

test('a reply crosses two forwarding boundaries and release travels back', async () => {
  const [k, r1in] = connect(); const [r1out, r2in] = connect(); const [r2out, s] = connect();
  listen(k); hop(r1in, r1out); hop(r2in, r2out); listen(r1out); listen(r2out);
  listen(s, async (rec) => {
    const imp = s.importer.import(rec.at(1).at(0));
    const release = imp.hold();
    await imp.wire.send(tuple([text('ok'), rec.at(0)]));
    release();
  });
  const reply = recorder();
  await k.ep.send([ROUTE], tuple([text('hello'), tuple([k.table.export(reply)])]));
  await eventually(() => reply.got.length === 1, 'no reply');
  assert.ok(reply.got[0].equals(tuple([text('ok'), text('hello')])));
  await eventually(() => k.table.live + r1out.table.live + r2out.table.live === 0, 'release did not reach the caller');
});

test('two-connection alias: a copied reference is foreign even when ids coincide', async () => {
  const [a1, b1] = connect(); const [a2, b2] = connect();
  for (const s of [a1, b1, a2, b2]) listen(s);
  const first = recorder(), second = recorder();
  const ref1 = a1.table.export(first), ref2 = a2.table.export(second);
  assert.ok(parseReference(ref1).id.equals(parseReference(ref2).id));
  await b2.importer.import(ref1).wire.send(text('forged'));
  await eventually(() => a2.refused.length === 1, 'not refused');
  assert.deepEqual(a2.refused, [FOREIGN_REFERENCE]);
  assert.equal(second.got.length + first.got.length, 0);
});

test('fan-out: releasing one branch keeps the import; the last release sends once', async () => {
  const [k, fin] = connect(); const [fout1, b1] = connect(); const [fout2, b2] = connect();
  for (const s of [k, fin, fout1, fout2, b1, b2]) listen(s);
  const target = recorder();
  const imp = fin.importer.import(k.table.export(target));
  const branch1 = b1.importer.import(fout1.table.reExport(imp));
  const branch2 = b2.importer.import(fout2.table.reExport(imp));
  const rel1 = branch1.hold(), rel2 = branch2.hold();
  rel1();
  await eventually(() => fout1.table.live === 0, 'branch 1 not released');
  assert.equal(k.table.live, 1, 'one branch released the import upstream');
  await branch2.wire.send(text('survivor'));
  await eventually(() => target.got.length === 1, 'surviving branch did not deliver');
  rel2();
  await eventually(() => k.table.live === 0, 'last release did not reach the exporter');
  assert.deepEqual(k.refused, []);
});

test('a re-export retires with its source connection', async () => {
  const [k, fin, kLink] = connect(); const [fout, b] = connect();
  for (const s of [k, fin, fout, b]) listen(s);
  const target = recorder();
  const imp = fin.importer.import(k.table.export(target));
  const downstream = b.importer.import(fout.table.reExport(imp));
  await kLink.close();
  fin.importer.close();
  assert.equal(fout.table.live, 0);
  await downstream.wire.send(text('late'));
  await eventually(() => fout.refused.length === 1, 'not refused');
  assert.deepEqual(fout.refused, [UNKNOWN_REFERENCE]);
  assert.equal(target.got.length, 0);
  assert.throws(() => fout.table.reExport(imp));
});

test('ids are never reused; release never closes the target; the table is bounded', () => {
  const tb = new Table(1);
  const target = recorder();
  const ref1 = tb.export(target);
  assert.throws(() => tb.export(target), ExportLimitError);
  tb.withdraw(ref1); tb.withdraw(ref1);
  const ref2 = tb.export(target);
  assert.ok(!parseReference(ref1).id.equals(parseReference(ref2).id));
  assert.notEqual(new Table(1).scope.bytes().toString(), tb.scope.bytes().toString());
});
