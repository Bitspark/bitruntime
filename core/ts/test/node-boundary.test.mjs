import {test} from 'node:test';
import assert from 'node:assert/strict';
import {compose as composeBytes} from '@bitspark/deixis-core';
import {atom, tuple} from '@bitspark/bitwire';
import {compose, asAddressed, addressed, pair, route} from '../../../dist/core/ts/src/index.js';

// Each implementation is checked against these hand-written observations. The
// conversion below is only the key representation at the test boundary.
const factories = [
  {name:'deixis bytes', key: b => Uint8Array.from(b), bytes: b => b, compose: composeBytes},
  {name:'bitwire Atom / bitruntime node', key: b => atom(b), bytes: b => b.bytes(), compose},
];
const cases = [
  {path:[], own:'shared'},
  {path:[[]], own:'empty'},
  {path:[[255,0,47]], own:'binary'},
  {path:[[255,0,47],[]], own:'shared'},
  {path:[[97,47,98]], own:'slash'},
  {path:[[97],[98]], own:undefined},
  {path:[[255,0,47],[1]], own:undefined},
];
function fixture(f) {
  const trace = [], slots = new Map(['shared','empty','binary','slash'].map(name =>
    [name, {name, count:0, send(value) { this.count++; trace.push([this.name,this.count,value]); }}]));
  const source = Uint8Array.of(255,0,47), key = f.key(source);
  const alias = f.compose(slots.get('shared'), []);
  const branch = f.compose(slots.get('binary'), [[f.key([]),alias]]);
  const node = f.compose(slots.get('shared'), [
    [key,branch], [f.key([]),f.compose(slots.get('empty'),[])],
    [f.key([97,47,98]),f.compose(slots.get('slash'),[])],
  ]);
  source.fill(9); f.bytes(key).fill(8);
  for (const [childKey] of node.children()) f.bytes(childKey).fill(7);
  assert.deepEqual(trace, [], 'structural construction invoked a slot');
  return {node, slots, trace};
}
for (const f of factories) {
  test(`${f.name}: complete node, exact byte keys, aliases, every cut and capture`, () => {
    const initial = fixture(f), parts = initial.node.decompose();
    const rebuilt = f.compose(parts.own, parts.children);
    assert.equal(rebuilt.own(), initial.slots.get('shared'));
    assert.equal(rebuilt.children().length, 3);
    for (const {path,own} of cases) {
      const selected = rebuilt.at(path.map(f.key));
      assert.equal(selected?.own(), initial.slots.get(own));
      for (let cut=0;cut<=path.length;cut++) {
        // Corresponding executions start with separate but equivalent mutable
        // slots. Sequential sends against one shared fixture would be unsound.
        const direct = fixture(f), split = fixture(f), message = tuple([]);
        direct.node.at(path.map(f.key))?.own().send(message);
        split.node.at(path.slice(0,cut).map(f.key))?.at(path.slice(cut).map(f.key))?.own().send(message);
        assert.deepEqual(split.trace, direct.trace);
        assert.equal(direct.trace.length, own === undefined ? 0 : 1);
        if (own !== undefined) assert.equal(direct.trace[0][2], message);
      }
    }
    // Root and a descendant intentionally share a live capability; a different
    // node with another capability must not be merged with it.
    rebuilt.own().send('one');
    rebuilt.at([f.key([255,0,47]),f.key([])]).own().send('two');
    rebuilt.at([f.key([])]).own().send('three');
    assert.deepEqual(initial.trace.map(x=>x.slice(0,2)), [['shared',1],['shared',2],['empty',1]]);
  });
}

test('remote missing path remains distinct from successful local admission', async () => {
  const [a,b] = pair(), received = [], misses = [];
  let done;
  const completion = new Promise(resolve => {done=resolve;});
  const node = compose(value => {received.push(value);done();});
  addressed(b).receive((path,message) => { if (!route(node,path,message)) misses.push(path); });
  try {
    await addressed(a).send([atom([255])], atom([1]));
    await addressed(a).send([], atom([2]));
    await completion;
    assert.equal(misses.length,1);
    assert.equal(received.length,1);
    assert.ok(received[0].equals(atom([2])));
    // An explicitly local selection has a different, observable missing result.
    await assert.rejects(asAddressed(compose({send:async()=>{}})).send([atom([255])],atom([])), {name:'MissingPathError'});
  } finally { await a.close(); await b.closed; }
});
