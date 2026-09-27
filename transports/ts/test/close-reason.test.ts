// A close reason that cannot be sent as it is — over 123 bytes of UTF-8, or not
// UTF-8 at all — is refused before anything changes (carrier contract, edition
// 1; bitruntime#31, where ws was left stuck in CLOSING with nothing sent).
import assert from 'node:assert/strict';
import type { AddressInfo } from 'node:net';
import test from 'node:test';
import { WebSocket, WebSocketServer } from 'ws';
import { pipe, validCloseReason, webSocketConnection, type WebSocketLike } from '../src/index.ts';

const long = 'x'.repeat(124);

test('a close reason is valid UTF-8 of at most 123 bytes, never repaired', () => {
  assert.equal(validCloseReason(''), true);
  assert.equal(validCloseReason('x'.repeat(123)), true);
  // 61 two-byte scalars and one byte: 123 bytes in 62 UTF-16 units.
  assert.equal(validCloseReason('é'.repeat(61) + 'x'), true);
  assert.equal(validCloseReason('é'.repeat(62)), false);
  assert.equal(validCloseReason(long), false);
  assert.equal(validCloseReason('😀'), true);
  assert.equal(validCloseReason('a\uD800b'), false);
  assert.equal(validCloseReason('a\uDC00'), false);
  assert.equal(validCloseReason('\uD83D'), false);
});

test('a pipe refuses a reason it cannot send, and stays open', async () => {
  const [a, b] = pipe();
  const frames: unknown[] = [];
  b.listen({ frame: (frame) => frames.push(frame.data) });
  for (const reason of [long, 'a\uD800b']) assert.throws(() => a.close(4000, reason), RangeError);
  assert.equal(a.state, 'open');
  a.send({ kind: 'text', data: 'still open' });
  await new Promise((resolve) => setTimeout(resolve, 10));
  assert.deepEqual(frames, ['still open']);
  a.close();
});

test('over a ws socket, a reason it cannot send is refused and the socket closes cleanly afterwards', async (t) => {
  const server = new WebSocketServer({ host: '127.0.0.1', port: 0 });
  await new Promise<void>((resolve) => server.once('listening', () => resolve()));
  t.after(() => new Promise<void>((resolve) => server.close(() => resolve())));
  const observed = new Promise<[number, string]>((resolve) =>
    server.once('connection', (socket) => socket.once('close', (code, reason) => resolve([code, String(reason)]))),
  );
  const socket = new WebSocket(`ws://127.0.0.1:${(server.address() as AddressInfo).port}/`);
  await new Promise((resolve) => socket.once('open', resolve));
  const connection = webSocketConnection(socket as unknown as WebSocketLike);
  assert.throws(() => connection.close(4000, long), RangeError);
  assert.throws(() => connection.close(4000, 'a\uD800b'), RangeError);
  // Before the fix, ws was left in CLOSING here, and the close below sent nothing.
  assert.equal(connection.state, 'open');
  connection.close(4000, 'done');
  assert.deepEqual(await observed, [4000, 'done']);
});
