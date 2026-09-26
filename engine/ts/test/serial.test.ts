// Ported from Nightseam v0.6.0 runtime/ts/src/serial.test.ts. The peer serves
// requests only through its root, so a request names a path: the rows of
// vectors/bitwire-1/serials.json spell the method "echo", which encodes no
// path, and are answered method_not_found — an answer, which is all the table
// asks of an admitted serial.
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { setImmediate as nextTurn } from 'node:timers/promises';
import { Peer, type PeerOptions } from '../src/index.ts';
import type { WebSocketLike } from '../../../transports/ts/src/index.ts';
import { call, createDispatcher, handle } from '../../../dispatch/ts/src/index.ts';

/** A raw socket, so a test spells the frames it sends byte for byte. */
class RawSocket extends EventTarget implements WebSocketLike {
  readyState = 1;
  bufferedAmount = 0;
  protocol = '';
  sent: Record<string, unknown>[] = [];
  send(text: string): void {
    if (this.readyState !== 1) throw new Error('Closed');
    this.sent.push(JSON.parse(text) as Record<string, unknown>);
  }
  receive(frame: string): void {
    this.dispatchEvent(new MessageEvent('message', { data: frame }));
  }
  close(): void {
    if (this.readyState === 3) return;
    this.readyState = 3;
    this.dispatchEvent(new Event('close'));
  }
}

async function raw(options: PeerOptions = {}) {
  const socket = new RawSocket();
  const peer = new Peer({ ...options, role: 'server' });
  await peer.attach(socket);
  return { socket, peer };
}

/**
 * Every row of tables/serials.json, held the way the peer holds what arrives —
 * the first frame published, then the request that follows it — so that the two
 * runtimes and the suite read one description of the order, this one.
 */
interface SerialRow {
  name: string;
  before: string;
  frame: string;
  valid: boolean;
}
const serials = JSON.parse(
  readFileSync(new URL('../../../vectors/bitwire-1/serials.json', import.meta.url), 'utf8'),
) as { rows: SerialRow[] };

assert.ok(serials.rows.length > 0, 'the serials table has no rows');
for (const row of serials.rows) {
  test(`serials table: ${row.name}`, async (t) => {
    const { socket, peer } = await raw();
    t.after(() => peer.close());
    handle(createDispatcher(peer.wire()), ['echo'], (data) => data);
    socket.receive(row.before);
    socket.receive(row.frame);
    await nextTurn();
    await nextTurn();
    if (!row.valid) {
      assert.equal(socket.readyState, 3, 'a serial that did not increase was admitted');
      return;
    }
    assert.equal(socket.readyState, 1, 'an admissible serial ended the connection');
    assert.ok(
      socket.sent.some((frame) => frame.kind === 'response'),
      'an admissible serial was never answered',
    );
  });
}

test('only request admission advances the mark', async (t) => {
  const { socket, peer } = await raw();
  t.after(() => peer.close());
  handle(
    createDispatcher(peer.wire()),
    ['wait'],
    (_data, context) =>
      new Promise((_resolve, reject) => {
        context.signal.addEventListener('abort', () => reject(new Error('cancelled')), { once: true });
      }),
  );
  socket.receive(JSON.stringify({ version: 1, kind: 'request', id: 'c:4', method: '4:wait', params: null }));
  socket.receive(JSON.stringify({ version: 1, kind: 'cancel', id: 'c:4' }));
  socket.receive(JSON.stringify({ version: 1, kind: 'response', id: 's:1', result: null }));
  socket.receive(JSON.stringify({ version: 1, kind: 'request', id: 'c:5', method: '4:wait', params: null }));
  await nextTurn();
  await nextTurn();
  assert.equal(socket.readyState, 1, 'a control or a response advanced the mark');
});

test('serials are published in the order they were reserved', async (t) => {
  // v0.6.0 paced 24 raw calls through a queue of 4. The root admits requests
  // immediately and in its own order, so no pacing can reorder them; the 24
  // calls go through the root with the default queue instead.
  const { socket, peer } = await raw();
  t.after(() => peer.close());
  const calls: Promise<unknown>[] = [];
  for (let index = 0; index < 24; index++) calls.push(call(peer.wire(), ['probe']).catch(() => undefined));
  await nextTurn();
  await nextTurn();
  let previous = 0;
  for (const frame of socket.sent) {
    if (frame.kind !== 'request') continue;
    const serial = Number((frame.id as string).slice('s:'.length));
    assert.ok(serial > previous, `published ${serial} after ${previous}`);
    previous = serial;
  }
  assert.ok(previous > 0, 'nothing was published');
  peer.close();
  await Promise.all(calls);
});

test('a carrier bridge mints its own serials', async (t) => {
  const { socket, peer } = await raw();
  t.after(() => peer.close());
  void call(peer.wire(), ['probe']).catch(() => undefined);
  await nextTurn();
  const published = socket.sent.find((frame) => frame.kind === 'request');
  assert.ok(published, 'the bridge published nothing');
  const id = published.id as string;
  assert.ok(id.startsWith('s:'), `the bridge published ${id} rather than a serial of its own`);
  assert.ok(Number(id.slice(2)) > 0, `the bridge published ${id}`);
});
