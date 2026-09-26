// Ported from Nightseam v0.6.0 runtime/ts/src/peer-pacing.test.ts. The peer
// paces only what it sends of its own accord — a response — for one write
// deadline; what the root hands it is admitted or refused at once. Not ported,
// because they held the pacing of the removed raw emit and call: "public emit
// waits for room while a shorter caller wrapper times out before the write
// deadline", "a paced public emit enters in order when the transport drains
// within its deadline" and "a paced raw call retains its own timeout and
// cancellation without publishing afterward".
import assert from 'node:assert/strict';
import test from 'node:test';
import { setImmediate as nextTurn } from 'node:timers/promises';
import { forward, pair, PublicError } from '../../../core/ts/src/index.ts';
import { pipe, type Frame, type FrameConnection } from '../../../transports/ts/src/index.ts';
import { call, createDispatcher, emit, handle } from '../../../dispatch/ts/src/index.ts';
import { Peer } from '../src/index.ts';

function heldConnection() {
  const [a, b] = pipe();
  let buffered = 1;
  const sent: Frame[] = [];
  const connection: FrameConnection = {
    get state() {
      return a.state;
    },
    get buffered() {
      return buffered;
    },
    send(frame) {
      sent.push(frame);
      a.send(frame);
    },
    close: (code, reason) => a.close(code, reason),
    listen: (receiver) => a.listen(receiver),
  };
  return {
    connection,
    sent,
    receive: (frame: Frame) => b.send(frame),
    drain: () => {
      buffered = 0;
    },
    close: () => {
      a.close();
      b.close();
    },
  };
}

test('a public raw response waits for queue room and then follows the accepted event', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout', 'Date'] });
  const held = heldConnection();
  const peer = new Peer({ queueCapacity: 1, writeTimeoutMs: 1000 });
  t.after(() => {
    peer.close();
    held.close();
  });
  handle(createDispatcher(peer.wire()), ['echo'], (value) => value);
  await peer.attach(held.connection);
  emit(peer.wire(), ['accepted']);
  await Promise.resolve();
  held.receive({
    kind: 'text',
    data: JSON.stringify({ version: 1, kind: 'request', id: 's:1', method: '4:echo', params: 7 }),
  });
  await nextTurn();
  assert.equal(peer.status, 'connected');
  held.drain();
  t.mock.timers.tick(5);
  await nextTurn();
  assert.deepEqual(
    held.sent.map((frame) => (frame.kind === 'text' ? JSON.parse(frame.data).kind : null)),
    ['event', 'response'],
  );
  assert.equal(JSON.parse((held.sent[1] as { data: string }).data).result, 7);
});

test('a composed wire handoff still refuses a full physical queue immediately and ends only its destination', async (t) => {
  const held = heldConnection();
  const destination = new Peer({ queueCapacity: 1, writeTimeoutMs: 5000 });
  const [caller, forwarding] = pair();
  const detach = forward(forwarding, destination.wire());
  t.after(() => {
    detach();
    caller.close();
    forwarding.close();
    destination.close();
    held.close();
  });
  await destination.attach(held.connection);
  emit(destination.wire(), ['accepted']);
  await Promise.resolve();
  const refused = await Promise.race([
    call(caller, ['refused']).catch((error: unknown) => error),
    nextTurn().then(() => 'waited'),
  ]);
  // R26: the overflow ended the destination, and a forwarded request whose
  // destination ended is answered disconnected (v0.6.0: busy).
  assert.ok(refused instanceof PublicError);
  assert.equal(refused.code, 'disconnected');
  assert.equal(destination.status, 'disconnected');
  detach();
  const dispatcher = createDispatcher(forwarding);
  t.after(() => dispatcher.close());
  handle(dispatcher, ['healthy'], () => 'still open');
  assert.equal(await call(caller, ['healthy']), 'still open');
  assert.equal(held.sent.length, 0);
});
