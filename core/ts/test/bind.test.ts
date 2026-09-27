// bind and compose's own-value rule (bitruntime#15).
import assert from 'node:assert/strict';
import test from 'node:test';
import type { AddressedWire, Message, Path, Wire } from '@bitspark/bitwire';
import { bind, compose } from '../src/index.ts';

test('compose refuses a missing own value and accepts other values', () => {
  assert.throws(() => compose<Wire>(undefined as unknown as Wire), TypeError);
  assert.throws(() => compose<Wire>(null as unknown as Wire), TypeError);
  for (const own of [0, '', false]) assert.equal(compose(own).own(), own);
});

test('bind sends at its copied path with the message and refusal unchanged, and grants only send', () => {
  const paths: Path[] = [];
  const messages: Message[] = [];
  let refusal: Error | undefined;
  const access: AddressedWire = {
    send(path, message) {
      paths.push([...path]);
      messages.push(message);
      // A callee may reuse the array it was given.
      (path as string[]).fill('mutated');
      if (refusal) throw refusal;
    },
  };
  const path = ['spaces', 'a/b', '', 'é', '﻿'];
  const bound = bind(access, path);
  path[0] = 'changed';
  const back = { wire: { send() {} } };
  const message: Message = { frame: { version: 1, kind: 'event', event: 'x', data: 1 }, return: back } as Message;
  bound.send(message);
  bound.send(message);
  assert.deepEqual(paths, [
    ['spaces', 'a/b', '', 'é', '﻿'],
    ['spaces', 'a/b', '', 'é', '﻿'],
  ]);
  assert.equal(messages[0], message);
  assert.equal(messages[0]!.return, back);
  refusal = new Error('refused');
  assert.throws(() => bound.send(message), (error) => error === refusal);
  assert.deepEqual(Object.keys(bound), ['send']);
  assert.ok(Object.isFrozen(bound));
});
