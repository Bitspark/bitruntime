import assert from 'node:assert/strict';
import test from 'node:test';
import {
  asAddressed, compose, InvalidPathError, MissingPathError, select, send,
} from '../dist/index.js';

const key = (...bytes) => Uint8Array.from(bytes);
const text = value => new TextEncoder().encode(value);
const message = { frame: { version: 1, kind: 'event', data: null } };

function foreign(own, children) {
  return {
    own: () => own,
    children: () => children,
    at(path) { return select(this, path); },
    decompose: () => ({ own, children }),
  };
}

test('exact binary and empty keys select nodes, not prefixes or normalized text', () => {
  const empty = compose('empty');
  const binary = compose('binary');
  const slash = compose('slash');
  const composed = compose('composed');
  const tree = compose('root', [
    [key(), empty], [key(255, 0), binary], [text('a/b'), slash],
    [text('\u00e9'), composed],
  ]);
  assert.equal(select(tree, []), tree);
  assert.equal(tree.at([key()]), empty);
  assert.equal(tree.at([key(255, 0)]), binary);
  assert.equal(tree.at([key(255)]), undefined);
  assert.equal(tree.at([text('a/b')]), slash);
  assert.equal(tree.at([text('a'), text('b')]), undefined);
  assert.equal(tree.at([text('e\u0301')]), undefined);
  assert.equal(tree.at([text('\u00e9')]), composed);
});

test('selection composition and reconstruction preserve child and own identity', () => {
  const capability = {};
  const leaf = compose(capability);
  const branch = compose({}, [[text('leaf'), leaf]]);
  const tree = compose({}, [[text('branch'), branch], [text('alias'), leaf]]);
  assert.equal(select(select(tree, [text('branch')]), [text('leaf')]), select(tree, [text('branch'), text('leaf')]));
  const parts = tree.decompose();
  const rebuilt = compose(parts.own, parts.children);
  assert.equal(rebuilt.own(), tree.own());
  assert.equal(rebuilt.at([text('branch')]), branch);
  assert.equal(rebuilt.at([text('branch'), text('leaf')]).own(), capability);
  assert.equal(rebuilt.at([text('alias')]), leaf);
  assert.equal(parts.children.length, 2);
});

test('key buffers and child collections cannot mutate the represented structure', () => {
  const input = Buffer.from([1, 2]);
  const child = compose('child');
  const entries = [[input, child]];
  const tree = compose('root', entries);
  input.fill(0);
  entries.splice(0);
  const exposed = tree.children();
  exposed[0][0].fill(9);
  assert.throws(() => exposed.push([key(3), child]), TypeError);
  assert.throws(() => { exposed[0][1] = compose('other'); }, TypeError);
  const parts = tree.decompose();
  parts.children[0][0].fill(8);
  assert.equal(tree.at([key(1, 2)]), child);
  assert.equal(tree.at([key(0, 0)]), undefined);
  assert.deepEqual([...tree.children()[0][0]], [1, 2]);
});

test('composition rejects duplicate byte contents including duplicate empty keys', () => {
  const child = compose(0);
  assert.throws(() => compose(0, [[key(1), child], [key(1), child]]), /Duplicate byte key/);
  assert.throws(() => compose(0, [[key(), child], [key(), child]]), /Duplicate byte key/);
  assert.throws(() => compose(0, [[null, child]]), TypeError);
  assert.throws(() => compose(0, [[key(1), null]]), TypeError);
});

test('foreign complete nodes retain their identity and reject duplicate descendants', () => {
  const leaf = foreign('leaf', []);
  const branch = foreign('branch', [[key(1), leaf]]);
  const tree = compose('root', [[key(1), branch], [key(2), branch]]);
  assert.equal(tree.at([key(1)]), branch);
  assert.equal(tree.at([key(2), key(1)]), leaf);
  const invalid = foreign('invalid', [[key(1), leaf], [key(1), leaf]]);
  assert.throws(() => compose('root', [[key(1), invalid]]), /Duplicate byte key/);
});

test('foreign structural cycles are rejected while shared children remain legal', () => {
  const entries = [];
  const cyclic = foreign('cycle', entries);
  entries.push([key(1), cyclic]);
  assert.throws(() => compose('root', [[key(1), cyclic]]), /Structural cycle/);
  const aEntries = [];
  const bEntries = [];
  const a = foreign('a', aEntries);
  const b = foreign('b', bEntries);
  aEntries.push([key(1), b]);
  bEntries.push([key(1), a]);
  assert.throws(() => compose('root', [[key(1), a]]), /Structural cycle/);
  const shared = foreign('shared', []);
  assert.equal(compose('root', [[key(1), shared], [key(2), shared]]).children().length, 2);
});

test('deep finite foreign trees validate and select without recursive stack limits', () => {
  const leaf = foreign('leaf', []);
  let tree = leaf;
  const path = [];
  for (let i = 0; i < 20_000; i++) {
    tree = foreign('branch', [[key(1), tree]]);
    path.push(key(1));
  }
  const root = compose('root', [[key(1), tree]]);
  assert.equal(select(root, [key(1), ...path]), leaf);
});

test('derived sending preserves message, frame, return capability and refusal identity', () => {
  const admitted = [];
  const receiver = { send: value => admitted.push(value) };
  const refusal = new Error('admission refused');
  const refusing = { send() { throw refusal; } };
  const tree = compose(receiver, [[text('child'), compose(receiver)], [text('refusing'), compose(refusing)]]);
  const returnWire = { send() {} };
  const returned = { frame: message.frame, return: { wire: returnWire } };
  send(tree, [text('child')], returned);
  assert.equal(admitted[0], returned);
  assert.equal(admitted[0].frame, message.frame);
  assert.equal(admitted[0].return.wire, returnWire);
  assert.throws(() => send(tree, [text('refusing')], returned), error => error === refusal);
});

test('missing sends refuse without falling back to root or nearest ancestor', () => {
  const admitted = [];
  const wire = { send: value => admitted.push(value) };
  const tree = compose(wire, [[text('child'), compose(wire)]]);
  assert.throws(() => send(tree, [text('missing')], message), MissingPathError);
  assert.throws(() => send(tree, [text('child'), text('missing')], message), MissingPathError);
  assert.deepEqual(admitted, []);
  send(tree, [], message);
  assert.deepEqual(admitted, [message]);
});

test('addressed bridge converts exact UTF-8 segments and preserves literal keys', () => {
  const admitted = [];
  const wire = label => ({ send: value => admitted.push([label, value]) });
  const tree = compose(wire('root'), [
    [text(''), compose(wire('empty'))],
    [text('a/b'), compose(wire('slash'))],
    [text('..'), compose(wire('dots'))],
    [text('\u00e9'), compose(wire('composed'))],
    [text('\ud83d\ude80'), compose(wire('astral'))],
    [key(255), compose(wire('binary'))],
  ]);
  const addressed = asAddressed(tree);
  for (const path of [[], [''], ['a/b'], ['..'], ['\u00e9'], ['\ud83d\ude80']]) addressed.send(path, message);
  assert.deepEqual(admitted.map(([name]) => name), ['root', 'empty', 'slash', 'dots', 'composed', 'astral']);
  assert.ok(admitted.every(([, value]) => value === message));
  assert.throws(() => addressed.send(['e\u0301'], message), MissingPathError);
  assert.throws(() => addressed.send(['a', 'b'], message), MissingPathError);
  assert.throws(() => addressed.send(['\ufffd'], message), MissingPathError);
  send(tree, [key(255)], message);
  assert.equal(admitted.at(-1)[0], 'binary');
  assert.equal('receive' in addressed, false);
  assert.equal('close' in addressed, false);
  assert.equal('children' in addressed, false);
});

test('addressed bridge refuses malformed UTF-16 before selecting or sending', () => {
  let admissions = 0;
  const wire = { send() { admissions++; } };
  const tree = compose(wire, [[text('\ufffd'), compose(wire)]]);
  const addressed = asAddressed(tree);
  for (const invalid of ['\ud800', '\udc00', '\ud800a', 'a\udfff', '\ud800\ud800', '\udc00\ud800']) {
    assert.throws(() => addressed.send([invalid], message), InvalidPathError);
    assert.throws(() => addressed.send(['missing', invalid], message), InvalidPathError);
  }
  assert.throws(() => addressed.send([null], message), InvalidPathError);
  assert.equal(admissions, 0);
});
