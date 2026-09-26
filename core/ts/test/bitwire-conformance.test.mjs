import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { compose, select, send } from '../../../dist/core/ts/src/index.js';

// Independent oracle: Bitspark/bitwire 0.3.0, conformance/trees/expected.json.
// This driver uses the production runtime, not Bitwire's test interpreter.
const expected = JSON.parse(readFileSync(new URL('./bitwire-expected.json', import.meta.url), 'utf8'));

test('production core matches Bitwire full-tree independent observations', () => {
  const k = (...values) => Uint8Array.from(values);
  const admissions = [];
  const labels = new Map();
  const node = (name, children = []) => {
    const wire = { send(message) { admissions.push(`${name}:${message.frame.kind}`); } };
    labels.set(wire, name);
    return compose(wire, children);
  };
  const leaf = node('leaf');
  const refusal = new Error('refused');
  const refusing = compose({ send() { throw refusal; } });
  const binary = node('binary');
  const tree = node('root', [
    [k(), node('empty')], [k(255), binary], [k(97, 47, 98), refusing],
    [k(97), node('branch', [[k(98), leaf]])],
  ]);
  const parts = tree.decompose();
  const rebuilt = compose(parts.own, parts.children);
  tree.children().find(([key]) => key[0] === 255)[0][0] = 0;
  const message = { frame: { version: 1, kind: 'event', data: null } };
  send(tree, [k(255)], message);
  let refusingSend = false;
  try { send(tree, [k(97, 47, 98)], message); } catch (error) { refusingSend = error === refusal; }
  const label = path => labels.get(select(tree, path).own());
  const observations = {
    self: select(tree, []) === tree,
    binary: label([k(255)]), emptyKey: label([k()]), missing: select(tree, [k(0)]) === undefined,
    nested: label([k(97), k(98)]),
    nestedLaw: select(select(tree, [k(97)]), [k(98)]) === select(tree, [k(97), k(98)]),
    children: tree.children().map(([key]) => Buffer.from(key).toString('hex')).sort(),
    slashIsLiteral: select(tree, [k(97, 47, 98)]) !== select(tree, [k(97), k(98)]),
    partsIdentity: parts.own === tree.own() && parts.children.find(([key]) => key[0] === 255)[1] === binary,
    rebuildIdentity: select(rebuilt, [k(97), k(98)]).own() === leaf.own(),
    keyCopy: label([k(255)]) === 'binary',
    refusingExists: select(tree, [k(97, 47, 98)]) !== undefined,
    refusingSend, admissions,
  };
  assert.deepEqual(observations, expected);
});
