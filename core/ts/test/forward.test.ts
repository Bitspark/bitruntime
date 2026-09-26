// Regressions of what forward changes from Nightseam v0.6.0's forwardWire:
// research 0001 row 14 (a refused message fails only itself) and R26 (a
// closed destination answers disconnected, never internal).
import assert from 'node:assert/strict';
import test from 'node:test';
import type { Endpoint } from '@bitspark/bitwire';
import { forward, mount, pair, PublicError } from '../src/index.ts';
import { call, createDispatcher, handle } from '../../../dispatch/ts/src/index.ts';

test('R26: a request forwarded to a closed mount is answered disconnected, not internal', async (t) => {
  const [caller, forwarding] = pair();
  const [leaf, leafServer] = pair();
  const [other] = pair();
  const inner = mount(new Map([['leaf', leaf]]));
  const outer = mount(new Map<string, Endpoint>([
    ['inner', inner],
    ['other', other],
  ]));
  const stop = forward(forwarding, outer);
  t.after(() => {
    stop();
    caller.close();
    leaf.close();
    other.close();
  });
  const dispatcher = createDispatcher(leafServer);
  handle(dispatcher, ['read'], () => 'open');
  assert.equal(await call(caller, ['inner', 'leaf', 'read']), 'open');
  inner.close();
  await assert.rejects(call(caller, ['inner', 'leaf', 'read']), (error: unknown) => {
    assert.ok(error instanceof PublicError);
    assert.equal(error.code, 'disconnected', 'a closed carrier behind forward surfaced as something else');
    return true;
  });
});

test('row 14: a message the destination refuses fails only itself, and forwarding goes on', async (t) => {
  const [caller, forwarding] = pair();
  const [service, serviceServer] = pair();
  const stop = forward(forwarding, mount(new Map([['service', service]])));
  t.after(() => {
    stop();
    caller.close();
    service.close();
  });
  const dispatcher = createDispatcher(serviceServer);
  handle(dispatcher, ['echo'], (value) => value);
  // The mount has no child here: the refusal answers this request alone.
  await assert.rejects(call(caller, ['absent', 'echo'], 1), { code: 'internal' });
  // An event the destination refuses is dropped, as an event has no answer.
  caller.send(['absent'], { frame: { version: 1, kind: 'event', data: null } });
  // v0.6.0 detached both directions on the first refusal.
  assert.equal(await call(caller, ['service', 'echo'], 2), 2);
  assert.equal(await call(caller, ['service', 'echo'], 3), 3);
});
