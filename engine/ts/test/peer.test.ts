// Ported from Nightseam v0.6.0 runtime/ts/src/peer.test.ts. The peer presents
// the protocol only through its root, wire(): what v0.6.0 served with the raw
// handle and onEvent is served here by a dispatcher on the root, what it sent
// with the raw call and emit goes through the root with call and emit, and a
// raw frame names a path by its canonical encoding ('4:wait' for ['wait']).
//
// Not ported: tests of the observer alone — "no payload reaches an observer",
// "for one call, one event and one close an observer sees the events in
// order", "a handler that throws yields handler.panic", "an outcome is what
// ended the call", "an observer that throws interrupts no routing" and "a
// frame is observed sent immediately before its bytes reach the transport" —
// and the observer's assertions in the tests kept below, each named where it
// stood. Every other change is noted where it stands.
import assert from 'node:assert/strict';
import test from 'node:test';
import { setImmediate as nextTurn } from 'node:timers/promises';
import type { Message } from '@bitspark/bitwire';
import {
  forward,
  InvalidPathError,
  pair,
  PublicError,
  UnpublishedError,
} from '../../../core/ts/src/index.ts';
import { decodeEnvelope } from '../../../core/ts/src/internal/envelope.ts';
import { positiveInteger } from '../../../core/ts/src/internal/limits.ts';
import { encodePath } from '../../../core/ts/src/internal/path.ts';
import {
  pipe,
  webSocketConnection,
  type ConnectionHandlers,
  type ConnectionState,
  type Frame,
  type FrameConnection,
  type WebSocketLike,
} from '../../../transports/ts/src/index.ts';
import {
  call,
  createDispatcher,
  emit,
  handle,
  onEvent,
  type RequestContext,
} from '../../../dispatch/ts/src/index.ts';
import { Peer, type PeerOptions } from '../src/index.ts';

/** The name a frame carries for a path of one segment. */
const name = (segment: string): string => encodePath([segment]);

test('component limits share validation while the peer keeps its safe-integer bound', () => {
  for (const safe of [false, true]) {
    assert.equal(positiveInteger(32, 'window', safe), 32);
    for (const value of [undefined, null, '32', 0, -1, 1.5, NaN, Infinity]) {
      assert.throws(() => positiveInteger(value, 'window', safe), {
        // The public error class is PublicError (v0.6.0: DuplexError).
        name: 'PublicError',
        code: 'invalid_options',
        message: `window must be a positive ${safe ? 'safe ' : ''}integer.`,
      });
    }
  }
  assert.equal(positiveInteger(Number.MAX_SAFE_INTEGER + 1, 'window'), Number.MAX_SAFE_INTEGER + 1);
  assert.throws(() => positiveInteger(Number.MAX_SAFE_INTEGER + 1, 'timeoutMs', true), {
    code: 'invalid_options',
    message: 'timeoutMs must be a positive safe integer.',
  });
});

class Socket extends EventTarget implements WebSocketLike {
  readyState = 1;
  bufferedAmount = 0;
  /** What the handshake selected, as a real WebSocket spells it. */
  protocol = '';
  sent: Record<string, unknown>[] = [];
  partner?: Socket;
  closeCount = 0;
  send(text: string): void {
    if (this.readyState !== 1) throw new Error('Closed');
    this.sent.push(JSON.parse(text));
    const partner = this.partner;
    if (partner)
      queueMicrotask(() => {
        if (partner.readyState === 1) partner.receive(text);
      });
  }
  receive(frame: unknown): void {
    this.dispatchEvent(
      new MessageEvent('message', { data: typeof frame === 'string' ? frame : JSON.stringify(frame) }),
    );
  }
  close(): void {
    this.closeCount++;
    if (this.readyState === 3) return;
    this.readyState = 3;
    this.dispatchEvent(new Event('close'));
    this.partner?.close();
  }
}

/** An in-memory frames duplex connection; no WebSocket is involved anywhere. */
class Pipe implements FrameConnection {
  state: ConnectionState = 'open';
  readonly buffered = 0;
  partner!: Pipe;
  readonly frames: Frame[] = [];
  private readonly listeners = new Set<ConnectionHandlers>();
  static pair(): [Pipe, Pipe] {
    const left = new Pipe();
    const right = new Pipe();
    left.partner = right;
    right.partner = left;
    return [left, right];
  }
  send(frame: Frame): void {
    if (this.state !== 'open') throw new Error('Not open');
    this.frames.push(frame);
    const partner = this.partner;
    queueMicrotask(() => {
      if (partner.state === 'open') for (const handlers of [...partner.listeners]) handlers.frame?.(frame);
    });
  }
  close(code = 1000, reason = ''): void {
    if (this.state === 'closed') return;
    this.state = 'closed';
    for (const handlers of [...this.listeners]) handlers.close?.(code, reason);
    this.partner.close(code, reason);
  }
  listen(handlers: ConnectionHandlers): () => void {
    this.listeners.add(handlers);
    return () => {
      this.listeners.delete(handlers);
    };
  }
}

async function paired(clientOptions: PeerOptions = {}, serverOptions: PeerOptions = {}) {
  const left = new Socket();
  const right = new Socket();
  left.partner = right;
  right.partner = left;
  const client = new Peer(clientOptions);
  const server = new Peer({ ...serverOptions, role: 'server' });
  await Promise.all([client.attach(left), server.attach(right)]);
  const clients = createDispatcher(client.wire());
  const servers = createDispatcher(server.wire());
  return { client, server, left, right, clients, servers };
}

test('peer refuses malformed outgoing Unicode without losing valid strings', async (t) => {
  const { client, servers, left } = await paired();
  t.after(() => client.close());
  handle(servers, ['echo'], (value) => value);
  handle(servers, ['bad'], () => '\uD800');
  for (const value of ['\uD800', { x: ['\uDC00'] }, { ['\uD800']: 1 }, { toJSON: () => '\uD800' }]) {
    assert.throws(() => emit(client.wire(), ['probe'], value), { code: 'invalid_message' });
    await assert.rejects(call(client.wire(), ['echo'], value), { code: 'invalid_message' });
  }
  // An event's name is a path now, and a malformed segment is refused as one
  // before anything is admitted (v0.6.0's raw emit: invalid_message).
  assert.throws(
    () => emit(client.wire(), ['\uD800'], null),
    (error: unknown) => error instanceof UnpublishedError && error.cause instanceof InvalidPathError,
  );
  assert.throws(() => emit(client.wire(), ['probe'], null, { meta: { x: '\uD800' } }), { code: 'invalid_message' });
  await settled();
  assert.equal(left.sent.length, 0);
  await assert.rejects(call(client.wire(), ['bad']), { code: 'internal' });
  assert.equal(await call(client.wire(), ['echo'], '😀�'), '😀�');
});

function deferred<T = void>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  const promise = new Promise<T>((accept) => {
    resolve = accept;
  });
  return { promise, resolve };
}

/** Lets whatever a frame set going run: the handlers, the queues, the writer. */
function settled(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 10));
}

/** Waits until a condition holds, for what a frame sets going asynchronously. */
async function eventually(ready: () => boolean): Promise<void> {
  for (let waited = 0; waited < 200 && !ready(); waited++) await new Promise((resolve) => setTimeout(resolve, 5));
  assert.ok(ready(), 'the condition never held');
}

test('duplex routing permits reverse calls during an outstanding request', async (t) => {
  const { client, clients, servers, left, right } = await paired();
  t.after(() => client.close());
  handle(clients, ['multiply'], (params) => (params as { value: number }).value * 3);
  handle(servers, ['roundtrip'], async (_params, context) => ({
    value: await call(context.wire, ['multiply'], { value: 7 }),
  }));
  assert.deepEqual(await call(client.wire(), ['roundtrip']), { value: 21 });
  assert.equal(left.sent[0]!.id, 'c:1');
  assert.equal(right.sent[0]!.id, 's:1');
  assert.equal(left.sent[1]!.kind, 'response');
});

test('responses correlate out of order while events run independently', async (t) => {
  const { client, server, clients, servers } = await paired();
  t.after(() => client.close());
  const first = deferred<number>();
  const blockEvent = deferred();
  const eventStarted = deferred();
  handle(servers, ['first'], () => first.promise);
  handle(servers, ['second'], () => 2);
  onEvent(clients, ['notice'], async () => {
    eventStarted.resolve();
    await blockEvent.promise;
  });
  const one = call(client.wire(), ['first']);
  emit(server.wire(), ['notice'], { value: 1 });
  await eventStarted.promise;
  assert.equal(await call(client.wire(), ['second']), 2);
  first.resolve(1);
  assert.equal(await one, 1);
  blockEvent.resolve();
});

test('public handler errors survive and unexpected errors remain private', async (t) => {
  const { client, servers } = await paired();
  t.after(() => client.close());
  handle(servers, ['public'], () => {
    throw new PublicError('denied', 'Access denied', { field: 'project' });
  });
  handle(servers, ['private'], () => {
    throw new Error('Database password: secret');
  });
  await assert.rejects(call(client.wire(), ['public']), {
    code: 'denied',
    message: 'Access denied',
    data: { field: 'project' },
  });
  await assert.rejects(call(client.wire(), ['private']), { code: 'internal', message: 'Request handler failed.' });
  await assert.rejects(call(client.wire(), ['missing']), { code: 'method_not_found' });
});

test('AbortSignal sends cancellation and aborts the remote handler', async (t) => {
  const { client, servers, left } = await paired();
  t.after(() => client.close());
  const started = deferred();
  const aborted = deferred();
  handle(
    servers,
    ['wait'],
    (_params, context) =>
      new Promise((resolve) => {
        context.signal.addEventListener(
          'abort',
          () => {
            aborted.resolve();
            resolve('ignored late result');
          },
          { once: true },
        );
        started.resolve();
      }),
  );
  const controller = new AbortController();
  const pending = call(client.wire(), ['wait'], {}, { signal: controller.signal });
  const failure = assert.rejects(pending, { code: 'cancelled' });
  await started.promise;
  controller.abort();
  await failure;
  await aborted.promise;
  assert.equal(left.sent.at(-1)?.kind, 'cancel');
  assert.equal(client.status, 'connected');
});

test('a cancel answers nothing itself: the response is the handler returning, and it is cancelled', async (t) => {
  // Not ported: the observer's request.ended assertions.
  const { client, server, servers, right } = await paired();
  t.after(() => {
    client.close();
    server.close();
  });
  const started = deferred();
  const release = deferred<string>();
  // A handler that ignores its signal. The cancel withdraws the request; what
  // answers it is this returning, whenever it does, as the profile says and
  // the Go peer does.
  handle(servers, ['deaf'], () => {
    started.resolve();
    return release.promise;
  });
  const controller = new AbortController();
  const pending = assert.rejects(call(client.wire(), ['deaf'], {}, { signal: controller.signal }), {
    code: 'cancelled',
  });
  await started.promise;
  controller.abort();
  await pending;
  await settled();
  // The caller has given up and nothing has answered the request, because
  // nothing has finished it.
  assert.deepEqual(
    right.sent.filter((frame) => frame.kind === 'response'),
    [],
  );
  release.resolve('a result nobody is waiting for');
  await settled();
  const responses = right.sent.filter((frame) => frame.kind === 'response');
  assert.equal(responses.length, 1);
  assert.equal(responses[0]!.id, 'c:1');
  assert.deepEqual(responses[0]!.error, { code: 'cancelled', message: 'Request was cancelled.' });
});

test('pre-aborted calls and outstanding capacity do not send extra requests', async (t) => {
  const { client, servers, left } = await paired({ maxPendingRequests: 1 });
  t.after(() => client.close());
  const blocked = deferred();
  handle(servers, ['wait'], () => blocked.promise);
  const waiting = call(client.wire(), ['wait']);
  const failed = assert.rejects(waiting, { code: 'disconnected' });
  await assert.rejects(call(client.wire(), ['overflow']), { code: 'busy' });
  const controller = new AbortController();
  controller.abort();
  await assert.rejects(call(client.wire(), ['cancelled'], {}, { signal: controller.signal }), { code: 'cancelled' });
  await settled();
  assert.equal(left.sent.length, 1);
  client.close();
  blocked.resolve();
  await failed;
});

test('local request deadline cancels remotely without retrying', async (t) => {
  const { client, servers, left } = await paired();
  t.after(() => client.close());
  handle(
    servers,
    ['wait'],
    (_params, context) =>
      new Promise((resolve) => {
        context.signal.addEventListener('abort', () => resolve(null), { once: true });
      }),
  );
  await assert.rejects(call(client.wire(), ['wait'], {}, { timeoutMs: 10 }), { code: 'request_timeout' });
  await settled();
  assert.deepEqual(
    left.sent.map((frame) => frame.kind),
    ['request', 'cancel'],
  );
});

test('incoming deadlines abort handlers and retain occupied slots until completion', async (t) => {
  const { client, servers } = await paired({}, { maxConcurrentHandlers: 1, requestTimeoutMs: 10 });
  t.after(() => client.close());
  const blocked = deferred();
  let signal: AbortSignal | undefined;
  handle(servers, ['wait'], (_params, context) => {
    signal = context.signal;
    return blocked.promise;
  });
  // The receiver's own deadline abandons the request, and `cancelled` is what
  // it answers: `request_timeout` is a caller's own error and never a frame.
  await assert.rejects(call(client.wire(), ['wait']), { code: 'cancelled' });
  assert.equal(signal?.aborted, true);
  await assert.rejects(call(client.wire(), ['wait']), { code: 'busy' });
  blocked.resolve();
});

test('disconnect cancels handlers and rejects pending calls without reconnecting', async () => {
  const { client, server, servers, left } = await paired();
  const started = deferred();
  const aborted = deferred();
  handle(
    servers,
    ['wait'],
    (_params, context) =>
      new Promise((resolve) => {
        context.signal.addEventListener(
          'abort',
          () => {
            aborted.resolve();
            resolve(1);
          },
          { once: true },
        );
        started.resolve();
      }),
  );
  const waiting = call(client.wire(), ['wait']);
  const failed = assert.rejects(waiting, { code: 'disconnected' });
  await started.promise;
  client.close();
  await Promise.all([failed, aborted.promise]);
  assert.equal(client.status, 'disconnected');
  assert.equal(server.status, 'disconnected');
  assert.equal(left.sent.filter((frame) => frame.kind === 'request').length, 1);
  // The root refuses a call on an ended peer with the one closed
  // classification (v0.6.0's raw call: not_connected).
  await assert.rejects(call(client.wire(), ['another']), { code: 'disconnected' });
});

test('incoming saturation responds busy without blocking responses', async (t) => {
  const { client, server, clients, servers } = await paired({}, { maxConcurrentHandlers: 1 });
  t.after(() => client.close());
  const occupied = deferred();
  const started = deferred();
  handle(servers, ['wait'], () => {
    started.resolve();
    return occupied.promise;
  });
  handle(clients, ['ping'], () => 'pong');
  const first = call(client.wire(), ['wait']);
  await started.promise;
  await assert.rejects(call(client.wire(), ['wait']), { code: 'busy' });
  assert.equal(await call(server.wire(), ['ping']), 'pong');
  occupied.resolve();
  assert.equal(await first, null);
});

test('wire output overflow ends the carrier without waiting for the socket to drain', async (t) => {
  const socket = new Socket();
  socket.bufferedAmount = 1;
  const peer = new Peer({ queueCapacity: 1, writeTimeoutMs: 5_000 });
  t.after(() => peer.close());
  await peer.attach(socket);
  // The first is accepted for sending, which is queued and no more.
  emit(peer.wire(), ['first']);
  await Promise.resolve();
  // The consumer remains blocked. Admission must settle before another turn,
  // independently of the much longer transport write deadline.
  const ended = deferred<PublicError>();
  peer.onClose(ended.resolve);
  emit(peer.wire(), ['second']);
  const outcome = await Promise.race([
    ended.promise.then((error) => {
      assert.equal(error.code, 'busy');
      return 'refused';
    }),
    nextTurn().then(() => 'waited'),
  ]);
  assert.equal(outcome, 'refused');
  assert.equal(peer.status, 'disconnected');
  assert.equal(socket.closeCount, 1);
  assert.equal(socket.sent.length, 0);
});

test('the accepted output prefix drains in order within its bound', async (t) => {
  for (const capacity of [2, 8]) {
    await t.test(`capacity ${capacity}`, async (t) => {
      const socket = new Socket();
      socket.bufferedAmount = 1;
      const drained = deferred();
      const send = socket.send.bind(socket);
      socket.send = (text) => {
        send(text);
        if (socket.sent.length === capacity) drained.resolve();
      };
      const peer = new Peer({ queueCapacity: capacity, writeTimeoutMs: 5_000 });
      t.after(() => peer.close());
      await peer.attach(socket);
      for (let sequence = 0; sequence < capacity; sequence++) emit(peer.wire(), ['item'], sequence);
      await nextTurn();
      assert.equal(socket.sent.length, 0);
      // Every emit already completed while the destination remained held.
      socket.bufferedAmount = 0;
      await drained.promise;
      assert.equal(peer.status, 'connected');
      assert.deepEqual(
        socket.sent.map((frame) => frame.data),
        Array.from({ length: capacity }, (_, i) => i),
      );
      emit(peer.wire(), ['marker'], capacity);
      await nextTurn();
      assert.equal(socket.sent.at(-1)?.data, capacity);
    });
  }
});

test('a pre-aborted call does not attempt admission to a full output queue', async (t) => {
  const socket = new Socket();
  socket.bufferedAmount = 1;
  const drained = deferred();
  const send = socket.send.bind(socket);
  socket.send = (text) => {
    send(text);
    drained.resolve();
  };
  const peer = new Peer({ queueCapacity: 1, writeTimeoutMs: 5_000 });
  t.after(() => peer.close());
  await peer.attach(socket);
  emit(peer.wire(), ['accepted']);
  await Promise.resolve();
  const controller = new AbortController();
  controller.abort();
  await assert.rejects(call(peer.wire(), ['unadmitted'], {}, { signal: controller.signal }), { code: 'cancelled' });
  assert.equal(peer.status, 'connected');
  assert.equal(socket.closeCount, 0);
  assert.equal(socket.sent.length, 0);
  socket.bufferedAmount = 0;
  await drained.promise;
  assert.deepEqual(
    socket.sent.map((frame) => frame.event),
    [name('accepted')],
  );
});

test('a sent call cancels promptly when its best-effort cancellation cannot be queued', async (t) => {
  const { client, server, servers, left } = await paired({ queueCapacity: 1, writeTimeoutMs: 5_000 });
  t.after(() => client.close());
  const started = deferred();
  handle(servers, ['wait'], (_params, context) => {
    started.resolve();
    return new Promise((resolve) => context.signal.addEventListener('abort', () => resolve(null), { once: true }));
  });
  const controller = new AbortController();
  const cancelled = assert.rejects(call(client.wire(), ['wait'], {}, { signal: controller.signal }), {
    code: 'cancelled',
  });
  await started.promise;
  left.bufferedAmount = 1;
  emit(client.wire(), ['accepted']);
  await Promise.resolve();
  controller.abort();
  const outcome = await Promise.race([cancelled.then(() => 'cancelled'), nextTurn().then(() => 'waited')]);
  assert.equal(outcome, 'cancelled');
  assert.equal(client.status, 'connected');
  assert.equal(server.status, 'connected');
  assert.deepEqual(
    left.sent.map((frame) => frame.kind),
    ['request'],
  );
});

test('an emit over a transport that never drains resolves anyway, and the write deadline still ends the connection', async () => {
  // What an emit promises is that the frame was accepted for sending, as the
  // profile says and the Go peer returns: queued for this connection, with
  // the drain and its deadline continuing behind the caller.
  const socket = new Socket();
  socket.bufferedAmount = 1;
  const peer = new Peer({ writeTimeoutMs: 10 });
  const closed = deferred<PublicError>();
  peer.onClose(closed.resolve);
  await peer.attach(socket);
  emit(peer.wire(), ['blocked']);
  await Promise.resolve();
  assert.equal(peer.status, 'connected');
  assert.equal(socket.sent.length, 0);
  assert.equal((await closed.promise).code, 'write_timeout');
  assert.equal(peer.status, 'disconnected');
});

test('event queues are bounded and a slow listener is paced, then disconnected', async () => {
  const socket = new Socket();
  const peer = new Peer({ queueCapacity: 1, writeTimeoutMs: 40 });
  const closed = deferred<PublicError>();
  peer.onClose(closed.resolve);
  await peer.attach(socket);
  const blocked = deferred();
  peer.wire().receive({ message: () => blocked.promise });
  socket.receive({ version: 1, kind: 'event', event: name('one'), data: {} });
  socket.receive({ version: 1, kind: 'event', event: name('two'), data: {} });
  // A full queue is a burst until its deadline passes. This listener never
  // returns, so its own deadline is the first to pass and names what stalled;
  // the queue's deadline behind it is the backstop for a consumer that does
  // return, only never fast enough.
  assert.equal(peer.status, 'connected');
  assert.equal((await closed.promise).code, 'stalled_consumer');
  assert.equal(peer.status, 'disconnected');
  blocked.resolve();
});

test('a producer that outruns its consumer for a whole deadline is a stalled consumer', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const socket = new Socket();
  const peer = new Peer({ queueCapacity: 1, writeTimeoutMs: 40 });
  const closed = deferred<PublicError>();
  peer.onClose(closed.resolve);
  await peer.attach(socket);
  // The first listener finishes before its deadline and the second is still
  // within its deadline when the backlog gives out. Advance a controlled
  // clock so that scheduler load cannot make the listener lose that race.
  const first = deferred();
  const second = deferred();
  let calls = 0;
  peer.wire().receive({ message: () => (++calls === 1 ? first.promise : second.promise) });
  t.after(() => {
    first.resolve();
    second.resolve();
    peer.close();
  });
  for (let i = 0; i < 20; i++) socket.receive({ version: 1, kind: 'event', event: name(`burst-${i}`), data: {} });
  t.mock.timers.tick(20);
  first.resolve();
  await nextTurn();
  assert.equal(calls, 2);
  // The backlog has now lasted 40 ms; neither listener has reached its own deadline.
  t.mock.timers.tick(20);
  assert.equal((await closed.promise).code, 'busy');
});

test('an event burst that drains within the deadline is paced, not disconnected', async (t) => {
  const socket = new Socket();
  const peer = new Peer({ queueCapacity: 1, writeTimeoutMs: 1_000 });
  await peer.attach(socket);
  const held = deferred();
  const drained = deferred();
  t.after(() => {
    held.resolve();
    peer.close();
  });
  const delivered: string[] = [];
  peer.wire().receive({
    message: async (path) => {
      await held.promise;
      delivered.push(path[0]!);
      if (delivered.length === 2) drained.resolve();
    },
  });
  socket.receive({ version: 1, kind: 'event', event: name('one'), data: {} });
  socket.receive({ version: 1, kind: 'event', event: name('two'), data: {} });
  held.resolve();
  // The backlog clears inside the deadline, so the burst was a burst: both
  // events arrive in order and the connection is whole.
  await drained.promise;
  assert.deepEqual(delivered, ['one', 'two']);
  assert.equal(peer.status, 'connected');
});

test('stalled asynchronous event listener closes the peer', async () => {
  const socket = new Socket();
  const peer = new Peer({ writeTimeoutMs: 10 });
  const closed = deferred<PublicError>();
  const blocked = deferred();
  peer.onClose(closed.resolve);
  peer.wire().receive({ message: () => blocked.promise });
  await peer.attach(socket);
  socket.receive({ version: 1, kind: 'event', event: name('one'), data: {} });
  assert.equal((await closed.promise).code, 'stalled_consumer');
  blocked.resolve();
});

test('large replay bursts do not queue already completed synchronous listeners', async (t) => {
  const socket = new Socket();
  const peer = new Peer();
  await peer.attach(socket);
  t.after(() => peer.close());
  const received: number[] = [];
  onEvent(createDispatcher(peer.wire()), ['replay'], (value) => {
    received.push(value as number);
  });
  for (let sequence = 1; sequence <= 300; sequence++) {
    socket.receive({ version: 1, kind: 'event', event: name('replay'), data: sequence });
  }
  assert.equal(peer.status, 'connected');
  assert.deepEqual(
    received,
    Array.from({ length: 300 }, (_, index) => index + 1),
  );
});

test('event subscriptions preserve order, isolate failures, and unsubscribe', async (t) => {
  // v0.6.0 held two raw listeners of one name, the first of which threw. A
  // path has one receiver now, so the one receiver throws on the first event:
  // the failure is reported, the next event is still delivered, in order, and
  // nothing is delivered once it is detached.
  const errors: string[] = [];
  const { client, server, clients } = await paired({ onError: (error) => errors.push(error.code) });
  t.after(() => client.close());
  const observed: unknown[] = [];
  const done = deferred();
  const off = clients.register(['notice'], {
    message: (_path, message) => {
      if (message.frame.kind !== 'event') return;
      observed.push(message.frame.data);
      if (observed.length === 1) throw new Error('listener failure');
      if (observed.length === 2) done.resolve();
    },
  });
  emit(server.wire(), ['notice'], 1);
  emit(server.wire(), ['notice'], 2);
  await done.promise;
  off();
  emit(server.wire(), ['notice'], 3);
  await settled();
  assert.deepEqual(observed, [1, 2]);
  assert.equal(errors[0], 'event_handler_failed');
});

test('malformed envelopes, binary messages, opposite IDs, and oversize frames close the peer', async () => {
  const invalid: unknown[] = [
    '{}',
    '{bad',
    '{"version":1,"version":1,"kind":"cancel","id":"c:1"}',
    { version: 2, kind: 'event', event: 'notice', data: null },
    { version: 1, kind: 'request', id: 'c:1', method: 'x', params: {} },
    { version: 1, kind: 'response', id: 'c:1', result: 1, error: { code: 'bad', message: 'bad' } },
    { version: 1, kind: 'event', event: 'x', data: 1, extra: true },
    // An error is a code and a message and both are non-empty, as the Go peer
    // refuses them: a response nobody can read is no answer to a call.
    { version: 1, kind: 'response', id: 'c:1', error: { code: 'denied', message: '' } },
    { version: 1, kind: 'response', id: 'c:1', error: { code: '', message: 'Denied' } },
  ];
  for (const frame of invalid) {
    const socket = new Socket();
    const peer = new Peer();
    await peer.attach(socket);
    socket.receive(frame);
    assert.equal(peer.status, 'disconnected', JSON.stringify(frame));
  }
  const socket = new Socket();
  const peer = new Peer({ maxFrameBytes: 70 });
  await peer.attach(socket);
  socket.receive({ version: 1, kind: 'event', event: 'x', data: 'é'.repeat(30) });
  assert.equal(peer.status, 'disconnected');
  const binary = new Socket();
  const binaryPeer = new Peer();
  await binaryPeer.attach(binary);
  binary.dispatchEvent(new MessageEvent('message', { data: new Uint8Array([1]) }));
  assert.equal(binaryPeer.status, 'disconnected');
});

test('outgoing oversize or unserializable values reject without sending', async (t) => {
  const socket = new Socket();
  const peer = new Peer({ maxFrameBytes: 100 });
  await peer.attach(socket);
  t.after(() => peer.close());
  assert.throws(() => emit(peer.wire(), ['large'], 'é'.repeat(100)), { code: 'frame_too_large' });
  await assert.rejects(call(peer.wire(), ['bigint'], 1n), { code: 'invalid_message' });
  await assert.rejects(
    call(peer.wire(), ['function'], () => 1),
    { code: 'invalid_message' },
  );
  assert.throws(() => emit(peer.wire(), ['nan'], Number.NaN), { code: 'invalid_message' });
  await settled();
  assert.equal(socket.sent.length, 0);
  assert.equal(peer.status, 'connected');
});

test('connection requires an explicit ws/wss endpoint and never sets browser headers', async (t) => {
  const socket = new Socket();
  socket.readyState = 0;
  let observedURL = '';
  const peer = new Peer({
    webSocketFactory: (url) => {
      observedURL = url;
      return socket;
    },
  });
  t.after(() => peer.close());
  await assert.rejects(peer.connect('/api'), { code: 'invalid_url' });
  await assert.rejects(peer.connect('https://localhost/api'), { code: 'invalid_url' });
  await assert.rejects(peer.connect('ws://user:password@localhost/api'), { code: 'invalid_url' });
  const connecting = peer.connect('ws://localhost/api');
  assert.equal(peer.status, 'connecting');
  socket.readyState = 1;
  socket.dispatchEvent(new Event('open'));
  await connecting;
  assert.equal(observedURL, 'ws://localhost/api');
  await assert.rejects(peer.connect('ws://localhost/api'), { code: 'already_connected' });
});

test('connection timeout closes the socket; manual reconnection remains explicit', async (t) => {
  const socket = new Socket();
  socket.readyState = 0;
  let created = 0;
  const peer = new Peer({
    connectTimeoutMs: 10,
    webSocketFactory: () => {
      created++;
      return socket;
    },
  });
  await assert.rejects(peer.connect('ws://localhost/api'), { code: 'connect_timeout' });
  assert.equal(socket.readyState, 3);
  assert.equal(created, 1);
  await peer.attach(new Socket());
  t.after(() => peer.close());
  assert.equal(peer.status, 'connected');
});

test('late work from an old connection cannot answer a new connection', async (t) => {
  const peer = new Peer();
  const old = new Socket();
  const gate = deferred<string>();
  const started = deferred();
  handle(createDispatcher(peer.wire()), ['wait'], () => {
    started.resolve();
    return gate.promise;
  });
  await peer.attach(old);
  old.receive({ version: 1, kind: 'request', id: 's:1', method: name('wait'), params: {} });
  await started.promise;
  peer.close();
  const current = new Socket();
  await peer.attach(current);
  t.after(() => peer.close());
  gate.resolve('old');
  await gate.promise;
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.deepEqual(current.sent, []);
});

test('handler registration is explicit, removable, and rejects duplicates', async (t) => {
  const { client, servers } = await paired();
  t.after(() => client.close());
  const remove = handle(servers, ['x'], () => 1);
  // A path has one registration: a second is refused as a second receiver
  // (v0.6.0's raw handle refused it with duplicate_handler).
  assert.throws(() => handle(servers, ['x'], () => 2), { code: 'receiver_exists' });
  assert.equal(await call(client.wire(), ['x']), 1);
  remove();
  await assert.rejects(call(client.wire(), ['x']), { code: 'method_not_found' });
});

test('two peers complete a call, an event and a cancel over an in-memory frame pipe', async (t) => {
  const [left, right] = Pipe.pair();
  const client = new Peer();
  const server = new Peer({ role: 'server' });
  await Promise.all([client.attach(left), server.attach(right)]);
  t.after(() => client.close());
  const notice = deferred<unknown>();
  const started = deferred();
  const aborted = deferred();
  const servers = createDispatcher(server.wire());
  handle(servers, ['add'], (params) => {
    const { a, b } = params as { a: number; b: number };
    return a + b;
  });
  handle(
    servers,
    ['wait'],
    (_params, context) =>
      new Promise((resolve) => {
        context.signal.addEventListener(
          'abort',
          () => {
            aborted.resolve();
            resolve(null);
          },
          { once: true },
        );
        started.resolve();
      }),
  );
  onEvent(createDispatcher(client.wire()), ['notice'], (data) => {
    notice.resolve(data);
  });
  assert.equal(await call(client.wire(), ['add'], { a: 2, b: 3 }), 5);
  emit(server.wire(), ['notice'], { value: 1 });
  assert.deepEqual(await notice.promise, { value: 1 });
  const controller = new AbortController();
  const cancelled = assert.rejects(call(client.wire(), ['wait'], {}, { signal: controller.signal }), {
    code: 'cancelled',
  });
  await started.promise;
  controller.abort();
  await Promise.all([cancelled, aborted.promise]);
  await settled();
  assert.equal(client.status, 'connected');
  assert.equal(server.status, 'connected');
  assert.deepEqual(
    left.frames.map((frame) => frame.kind),
    ['text', 'text', 'text'],
  );
  assert.deepEqual(
    left.frames.map((frame) => JSON.parse(frame.data as string).kind),
    ['request', 'request', 'cancel'],
  );
  assert.deepEqual(
    right.frames.map((frame) => JSON.parse(frame.data as string).kind),
    ['response', 'event', 'response'],
  );
});

/** One W3C traceparent, the example of the specification, and a vendor's state beside it. */
const TRACEPARENT = '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01';
const TRACESTATE = 'vendor=t61rcWkgMzE';
/** Every kind as a server peer receives it: a request and a cancel from the client, a response to its own. */
const everyKind: Record<string, unknown>[] = [
  { version: 1, kind: 'request', id: 'c:1', method: 'echo', params: {} },
  { version: 1, kind: 'response', id: 's:1', result: 1 },
  { version: 1, kind: 'cancel', id: 'c:1' },
  { version: 1, kind: 'event', event: 'notice', data: null },
];

test('trace context is kept on the decoded envelope of every kind, and tracestate stands alone', () => {
  for (const kind of everyKind) {
    const traced = { ...kind, traceparent: TRACEPARENT, tracestate: TRACESTATE };
    assert.deepEqual(decodeEnvelope(JSON.stringify(traced), 's:', 'c:'), traced, kind.kind as string);
    // An intermediary may strip one member and not the other.
    const alone = { ...kind, tracestate: TRACESTATE };
    const decoded = decodeEnvelope(JSON.stringify(alone), 's:', 'c:');
    assert.deepEqual(decoded, alone);
    assert.equal(Object.hasOwn(decoded, 'traceparent'), false);
    // A frame without either decodes as before.
    assert.deepEqual(decodeEnvelope(JSON.stringify(kind), 's:', 'c:'), kind);
  }
});

test("a traced frame of every kind routes as before, and a response carries its request's trace", async (t) => {
  const socket = new Socket();
  const peer = new Peer();
  await peer.attach(socket);
  t.after(() => peer.close());
  const trace = { traceparent: TRACEPARENT, tracestate: TRACESTATE };
  const notice = deferred<unknown>();
  const started = deferred();
  const aborted = deferred();
  const handlers = createDispatcher(peer.wire());
  onEvent(handlers, ['notice'], (data) => {
    notice.resolve(data);
  });
  handle(handlers, ['echo'], (params) => params);
  handle(
    handlers,
    ['wait'],
    (_params, context) =>
      new Promise((resolve) => {
        context.signal.addEventListener(
          'abort',
          () => {
            aborted.resolve();
            resolve(null);
          },
          { once: true },
        );
        started.resolve();
      }),
  );
  const pending = call(peer.wire(), ['ping']);
  // The root hands the call to the peer, which publishes it as c:1.
  await eventually(() => socket.sent.length === 1);
  socket.receive({ version: 1, kind: 'response', id: 'c:1', result: 'pong', ...trace });
  assert.equal(await pending, 'pong');
  socket.receive({ version: 1, kind: 'event', event: name('notice'), data: { value: 1 }, ...trace });
  assert.deepEqual(await notice.promise, { value: 1 });
  socket.receive({ version: 1, kind: 'request', id: 's:1', method: name('echo'), params: { value: 2 }, ...trace });
  socket.receive({ version: 1, kind: 'request', id: 's:2', method: name('wait'), params: {}, ...trace });
  await started.promise;
  socket.receive({ version: 1, kind: 'cancel', id: 's:2', ...trace });
  await aborted.promise;
  await settled();
  assert.equal(peer.status, 'connected');
  assert.deepEqual(
    socket.sent.map((frame) => frame.id),
    ['c:1', 's:1', 's:2'],
  );
  assert.deepEqual(socket.sent.find((frame) => frame.id === 's:1')?.result, { value: 2 });
  // Each response repeats the members of the request it answers; trace.test.ts holds the rest.
  for (const id of ['s:1', 's:2']) {
    const response = socket.sent.find((frame) => frame.id === id);
    assert.equal(response?.traceparent, TRACEPARENT, id);
    assert.equal(response?.tracestate, TRACESTATE, id);
  }
});

test('a malformed traceparent is refused as any invalid frame is', async () => {
  const malformed = [
    '',
    '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7',
    '00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01',
    '00-4bf92f3577b34da6a3ce929d0e0e473-00f067aa0ba902b7-01',
    ' 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01',
  ];
  for (const kind of everyKind) {
    for (const traceparent of malformed) {
      const socket = new Socket();
      const peer = new Peer({ role: 'server' });
      await peer.attach(socket);
      socket.receive({ ...kind, traceparent });
      assert.equal(peer.status, 'disconnected', `${kind.kind as string} ${traceparent}`);
    }
    // A member that is present but not a string is refused the same way.
    const socket = new Socket();
    const peer = new Peer({ role: 'server' });
    await peer.attach(socket);
    socket.receive({ ...kind, traceparent: TRACEPARENT, tracestate: 7 });
    assert.equal(peer.status, 'disconnected', kind.kind as string);
  }
});

test('the WebSocket adapter maps state, buffered bytes, frames, and the close code and reason', async () => {
  const socket = new Socket();
  socket.readyState = 0;
  const connection = webSocketConnection(socket);
  assert.equal(connection.state, 'connecting');
  assert.throws(() => connection.send({ kind: 'text', data: 'early' }));
  socket.readyState = 1;
  assert.equal(connection.state, 'open');
  socket.bufferedAmount = 7;
  assert.equal(connection.buffered, 7);
  connection.send({ kind: 'text', data: '{"a":1}' });
  assert.deepEqual(socket.sent, [{ a: 1 }]);
  const frames: Frame[] = [];
  let closed: [number, string] | undefined;
  const off = connection.listen({
    frame: (frame) => {
      frames.push(frame);
    },
    close: (code, reason) => {
      closed = [code, reason];
    },
  });
  socket.receive('"text"');
  socket.dispatchEvent(new MessageEvent('message', { data: new Uint8Array([1, 2]) }));
  socket.dispatchEvent(new MessageEvent('message', { data: new ArrayBuffer(3) }));
  assert.deepEqual(frames, [
    { kind: 'text', data: '"text"' },
    { kind: 'binary', data: new Uint8Array([1, 2]) },
    { kind: 'binary', data: new ArrayBuffer(3) },
  ]);
  // A Blob is read asynchronously; a text frame behind it keeps its place.
  socket.dispatchEvent(new MessageEvent('message', { data: new Blob([new Uint8Array([9])]) }));
  socket.receive('"after"');
  assert.equal(frames.length, 3);
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.deepEqual(frames.slice(3), [
    { kind: 'binary', data: new Uint8Array([9]).buffer },
    { kind: 'text', data: '"after"' },
  ]);
  socket.readyState = 2;
  assert.equal(connection.state, 'closing');
  assert.throws(() => connection.send({ kind: 'text', data: 'late' }));
  socket.readyState = 3;
  socket.dispatchEvent(new CloseEvent('close', { code: 4001, reason: 'gone' }));
  assert.equal(connection.state, 'closed');
  assert.deepEqual(closed, [4001, 'gone']);
  off();
  // The peer refuses a binary frame delivered through the adapter.
  const binary = new Socket();
  const peer = new Peer();
  const failure = deferred<PublicError>();
  peer.onClose(failure.resolve);
  await peer.attach(binary);
  binary.dispatchEvent(new MessageEvent('message', { data: new ArrayBuffer(1) }));
  const error = await failure.promise;
  assert.equal(error.code, 'invalid_message');
  assert.equal(error.message, 'Only JSON text frames are supported.');
  assert.equal(peer.status, 'disconnected');
  assert.equal(binary.readyState, 3);
});

test('a close from the far side surfaces its code and reason to the close handler', async () => {
  const [left, right] = Pipe.pair();
  const client = new Peer();
  const server = new Peer({ role: 'server' });
  await Promise.all([client.attach(left), server.attach(right)]);
  let observed: [number, string] | undefined;
  left.listen({
    close: (code, reason) => {
      observed = [code, reason];
    },
  });
  const closed = deferred<PublicError>();
  client.onClose(closed.resolve);
  server.close();
  assert.deepEqual(observed, [1000, 'Duplex connection closed']);
  assert.equal((await closed.promise).code, 'disconnected');
  assert.equal(client.status, 'disconnected');
  // The same through the adapter: the socket's close event carries the far side's code.
  const socket = new Socket();
  const connection = webSocketConnection(socket);
  const peer = new Peer();
  await peer.attach(connection);
  let seen: [number, string] | undefined;
  connection.listen({
    close: (code, reason) => {
      seen = [code, reason];
    },
  });
  const failure = deferred<PublicError>();
  peer.onClose(failure.resolve);
  socket.readyState = 3;
  socket.dispatchEvent(new CloseEvent('close', { code: 1008, reason: 'policy violation' }));
  assert.deepEqual(seen, [1008, 'policy violation']);
  assert.equal((await failure.promise).code, 'disconnected');
  assert.equal(socket.closeCount, 0);
});

for (const code of ['cancelled', 'request_timeout']) {
  test(`a public ${code} refusal crosses as that code`, async (t) => {
    // v0.6.0: "... is observed as an error in both directions"; the observer's
    // request.ended assertions are not ported.
    const { client, servers } = await paired();
    t.after(() => client.close());
    handle(servers, ['deny'], () => {
      throw new PublicError(code, 'Refused.');
    });
    await assert.rejects(call(client.wire(), ['deny']), { code });
  });
}

test('a handler public refusal stays an error after the caller withdraws', async (t) => {
  // The observer's request.ended (outcome error, code cancelled) is not
  // ported; the response frame that says the same is held instead.
  const started = deferred();
  const { client, servers, right } = await paired();
  t.after(() => client.close());
  handle(
    servers,
    ['deny'],
    (_params, context) =>
      new Promise((_resolve, reject) => {
        started.resolve();
        context.signal.addEventListener('abort', () => reject(new PublicError('cancelled', 'Refused.')), {
          once: true,
        });
      }),
  );
  const controller = new AbortController();
  const pending = assert.rejects(call(client.wire(), ['deny'], {}, { signal: controller.signal }), {
    code: 'cancelled',
  });
  await started.promise;
  controller.abort();
  await pending;
  await eventually(() => right.sent.some((frame) => frame.kind === 'response'));
  assert.deepEqual(right.sent.find((frame) => frame.kind === 'response')?.error, {
    code: 'cancelled',
    message: 'Refused.',
  });
});

test('a cancellation before handler dispatch is answered once as a local cancellation', async (t) => {
  // v0.6.0: "... is observed once as a local cancellation"; the observer's
  // request.ended and handler.panic assertions are not ported.
  const socket = new Socket();
  const peer = new Peer();
  t.after(() => peer.close());
  await peer.attach(socket);
  let dispatched = false;
  handle(createDispatcher(peer.wire()), ['wait'], () => {
    dispatched = true;
    return null;
  });
  socket.receive({ version: 1, kind: 'request', id: 's:1', method: name('wait'), params: {} });
  socket.receive({ version: 1, kind: 'cancel', id: 's:1' });
  await eventually(() => socket.sent.some((frame) => frame.kind === 'response'));
  await settled();
  assert.equal(dispatched, false);
  assert.equal(socket.sent.filter((frame) => frame.kind === 'response').length, 1);
  assert.deepEqual(socket.sent.find((frame) => frame.kind === 'response')?.error, {
    code: 'cancelled',
    message: 'Request was cancelled.',
  });
});

for (const outcome of ['cancelled', 'timeout'] as const) {
  test(`a local ${outcome} sends its cancel, and the receiver answers a withdrawal`, async (t) => {
    // v0.6.0: "... is observed before its cancel, and the receiver observes a
    // withdrawal"; the observer's ordering is not ported, the frames are held.
    const { client, servers, left, right } = await paired();
    t.after(() => client.close());
    const started = deferred();
    handle(servers, ['wait'], (_params, context) => {
      started.resolve();
      return new Promise((resolve) => {
        context.signal.addEventListener('abort', () => resolve(null), { once: true });
      });
    });
    const controller = new AbortController();
    const code = outcome === 'timeout' ? 'request_timeout' : 'cancelled';
    const pending = assert.rejects(
      call(client.wire(), ['wait'], {}, { signal: controller.signal, timeoutMs: outcome === 'timeout' ? 20 : 5000 }),
      { code },
    );
    await started.promise;
    if (outcome === 'cancelled') controller.abort();
    await pending;
    await eventually(() => left.sent.some((frame) => frame.kind === 'cancel'));
    assert.deepEqual(
      left.sent.map((frame) => [frame.kind, frame.id]),
      [
        ['request', 'c:1'],
        ['cancel', 'c:1'],
      ],
    );
    await eventually(() => right.sent.some((frame) => frame.kind === 'response'));
    assert.deepEqual(right.sent.find((frame) => frame.kind === 'response')?.error, {
      code: 'cancelled',
      message: 'Request was cancelled.',
    });
  });
}

test('a handler deadline is answered to the caller as a refusal', async (t) => {
  // v0.6.0: "... is observed locally as a timeout and remotely as a refusal";
  // the observer's request.ended assertions are not ported.
  const { client, servers, right } = await paired({}, { requestTimeoutMs: 20 });
  t.after(() => client.close());
  handle(
    servers,
    ['wait'],
    (_params, context) =>
      new Promise((resolve) => {
        context.signal.addEventListener('abort', () => resolve(null), { once: true });
      }),
  );
  await assert.rejects(call(client.wire(), ['wait']), { code: 'cancelled' });
  assert.deepEqual(right.sent.find((frame) => frame.kind === 'response')?.error, {
    code: 'cancelled',
    message: 'Request deadline exceeded.',
  });
});

test('wire output overflow ends the carrier and accepted writes retain their transport deadline', async () => {
  // v0.6.0: "... is observed once and ..."; the observer's backpressure
  // events are not ported, and the delivery it observed is held by the
  // listener instead.
  const socket = new Socket();
  socket.bufferedAmount = 1;
  const peer = new Peer({ queueCapacity: 1, writeTimeoutMs: 40 });
  await peer.attach(socket);
  emit(peer.wire(), ['first']);
  await Promise.resolve();
  // The queue limit ends admission immediately; a transport timeout is a
  // separate case below.
  const ended = deferred<PublicError>();
  peer.onClose(ended.resolve);
  emit(peer.wire(), ['second']);
  assert.equal((await ended.promise).code, 'busy');
  assert.equal(peer.status, 'disconnected');

  const slow = new Socket();
  slow.bufferedAmount = 1;
  const blocked = new Peer({ writeTimeoutMs: 10 });
  const gaveOut = deferred<PublicError>();
  blocked.onClose(gaveOut.resolve);
  await blocked.attach(slow);
  // The emit is accepted for sending and the caller is told so; what the
  // deadline holds is the queue it went into, and a frame that never drains
  // is what passes it.
  emit(blocked.wire(), ['blocked']);
  assert.equal((await gaveOut.promise).code, 'write_timeout');

  // The event queue is the other side of the same limit.
  const listening = new Socket();
  const receiver = new Peer({ queueCapacity: 1 });
  await receiver.attach(listening);
  const held = deferred();
  const delivered: string[] = [];
  receiver.wire().receive({
    message: (path) => {
      delivered.push(path[0]!);
      return held.promise;
    },
  });
  listening.receive({ version: 1, kind: 'event', event: name('one'), data: {} });
  listening.receive({ version: 1, kind: 'event', event: name('two'), data: {} });
  // Paced, not disconnected: the consumer has a deadline to drain in and has
  // not passed it. This peer cannot pause what a socket hands it, so the
  // event is held where the Go peer stops reading; the deadline is the same.
  assert.equal(receiver.status, 'connected');
  held.resolve();
  await Promise.resolve();
  await settled();
  // Both are delivered, in order: the second was held while the first was in
  // hand and went the moment it was free, which is what pacing is for.
  assert.deepEqual(delivered, ['one', 'two']);
  assert.equal(receiver.status, 'connected');
  receiver.close();
});

test('a subprotocol is offered at the handshake and the selection is what the peer reports', async (t) => {
  const socket = new Socket();
  socket.readyState = 0;
  let offered: string[] | undefined;
  const peer = new Peer({
    subprotocols: ['a', 'b'],
    webSocketFactory: (_url, protocols) => {
      offered = protocols;
      return socket;
    },
  });
  t.after(() => peer.close());
  const connecting = peer.connect('ws://localhost/api');
  assert.deepEqual(offered, ['a', 'b'], 'the factory is handed what to offer, so a custom one honours it');
  assert.equal(peer.subprotocol, '', 'nothing is selected until the handshake is done');
  socket.protocol = 'b';
  socket.readyState = 1;
  socket.dispatchEvent(new Event('open'));
  await connecting;
  assert.equal(peer.subprotocol, 'b');
  peer.close();
  assert.equal(peer.subprotocol, '', 'a peer with no connection negotiated nothing');
});

test('an offer the server selected none of leaves the peer with none, and the profile is spoken anyway', async (t) => {
  const socket = new Socket();
  const peer = new Peer({ subprotocols: ['c'], webSocketFactory: () => socket });
  t.after(() => peer.close());
  await peer.connect('ws://localhost/api');
  assert.equal(peer.subprotocol, '');
  emit(peer.wire(), ['progress'], 1);
  await eventually(() => socket.sent.length === 1);
  const { version, kind, event, data } = socket.sent[0] as Record<string, unknown>;
  assert.deepEqual({ version, kind, event, data }, { version: 1, kind: 'event', event: name('progress'), data: 1 });
});

test('a peer that offers no subprotocol offers nothing at all', async (t) => {
  const socket = new Socket();
  let offered: string[] | undefined = ['unasked'];
  const peer = new Peer({
    webSocketFactory: (_url, protocols) => {
      offered = protocols;
      return socket;
    },
  });
  t.after(() => peer.close());
  await peer.connect('ws://localhost/api');
  assert.equal(offered, undefined);
  assert.equal(peer.subprotocol, '');
});

test('a peer over a connection that is no WebSocket negotiated nothing', async (t) => {
  const [near, far] = Pipe.pair();
  const peer = new Peer();
  const other = new Peer({ role: 'server' });
  t.after(() => {
    peer.close();
    other.close();
  });
  await peer.attach(near);
  await other.attach(far);
  assert.equal(peer.subprotocol, '');
  assert.equal(other.subprotocol, '');
});

// What the port adds: the protocol presented only through the root.

test('a request whose method encodes no path is answered method_not_found, as by a v0.6.0 peer without that handler', async (t) => {
  const socket = new Socket();
  const peer = new Peer({ role: 'server' });
  await peer.attach(socket);
  t.after(() => peer.close());
  const handlers = createDispatcher(peer.wire());
  handle(handlers, ['raw.method'], () => 'served');
  const events: unknown[] = [];
  onEvent(handlers, ['raw.event'], (data) => void events.push(data));
  socket.receive({ version: 1, kind: 'request', id: 'c:1', method: 'raw.method', params: null });
  socket.receive({ version: 1, kind: 'request', id: 'c:2', method: '1:a1', params: null });
  socket.receive({ version: 1, kind: 'request', id: 'c:3', method: name('raw.method'), params: null });
  // An event whose name encodes no path reaches nothing, as an event with no
  // listener does; the connection stays whole.
  socket.receive({ version: 1, kind: 'event', event: 'raw.event', data: 'dropped' });
  socket.receive({ version: 1, kind: 'event', event: name('raw.event'), data: 'delivered' });
  await eventually(() => socket.sent.length === 3);
  await settled();
  assert.equal(peer.status, 'connected');
  const byId = Object.fromEntries(socket.sent.map((frame) => [frame.id, frame]));
  assert.deepEqual(byId['c:1']!.error, { code: 'method_not_found', message: 'Unknown method raw.method.' });
  assert.deepEqual(byId['c:2']!.error, { code: 'method_not_found', message: 'Unknown method 1:a1.' });
  assert.equal(byId['c:3']!.result, 'served');
  assert.deepEqual(events, ['delivered']);
  // With nothing attached to the root, every request is answered the same way.
  const bare = new Socket();
  const idle = new Peer({ role: 'server' });
  await idle.attach(bare);
  t.after(() => idle.close());
  bare.receive({ version: 1, kind: 'request', id: 'c:1', method: name('raw.method'), params: null });
  await eventually(() => bare.sent.length === 1);
  assert.deepEqual(bare.sent[0]!.error, { code: 'method_not_found', message: `Unknown method ${name('raw.method')}.` });
});

test('R19/R23: a handler context does not reach the peer that delivered its request', async (t) => {
  const { client, servers } = await paired();
  t.after(() => client.close());
  const seen = deferred<RequestContext>();
  handle(servers, ['inspect'], (_params, context) => {
    seen.resolve(context);
    return null;
  });
  await call(client.wire(), ['inspect']);
  const context = await seen.promise;
  assert.equal('peer' in context, false);
  for (let prototype = Object.getPrototypeOf(context); prototype; prototype = Object.getPrototypeOf(prototype))
    for (const key of Reflect.ownKeys(prototype)) assert.ok(!(prototype[key] instanceof Peer), String(key));
});

test('a request the peer handed on and its end cuts off is answered disconnected, not cancelled', async (t) => {
  for (const ending of ['closed here', 'closed there', 'refused frame'] as const) {
    await t.test(ending, async () => {
      const { client, server, clients, servers, left } = await paired();
      const started = deferred();
      handle(servers, ['hold'], () => {
        started.resolve();
        return new Promise(() => {});
      });
      // Straight through the root, and forwarded to it through a local pair.
      const [caller, forwarding] = pair();
      const stop = forward(forwarding, clients.select([]));
      const direct = call(client.wire(), ['hold']).catch((error: unknown) => error);
      const forwarded = call(caller, ['hold']).catch((error: unknown) => error);
      await started.promise;
      await eventually(() => left.sent.filter((frame) => frame.kind === 'request').length === 2);
      if (ending === 'closed here') client.close();
      else if (ending === 'closed there') server.close();
      else left.partner!.send('{"version":2}');
      for (const outcome of await Promise.all([direct, forwarded])) {
        assert.ok(outcome instanceof PublicError, String(outcome));
        assert.equal(outcome.code, 'disconnected');
        assert.equal(outcome instanceof UnpublishedError, false);
      }
      stop();
      caller.close();
      server.close();
    });
  }
});

test('a peer ended from outside transmits the close code it chose', async (t) => {
  const endings: [string, (peer: Peer) => void, [number, string]][] = [
    ['close()', (peer) => peer.close(), [1000, 'Duplex connection closed']],
    ['wire().close(code, reason)', (peer) => peer.wire().close(4001, 'bye'), [4001, 'bye']],
    ['a refused frame', () => {}, [4011, 'Duplex connection closed']],
  ];
  for (const [label, end, expected] of endings) {
    await t.test(label, async () => {
      const [near, far] = pipe();
      const peer = new Peer();
      await peer.attach(near);
      let observed: [number, string] | undefined;
      far.listen({ close: (code, reason) => void (observed = [code, reason]) });
      const pending = call(peer.wire(), ['held']).catch((error: unknown) => error);
      await nextTurn();
      if (label === 'a refused frame') far.send({ kind: 'text', data: '{"version":2}' });
      else end(peer);
      await eventually(() => observed !== undefined);
      assert.deepEqual(observed, expected);
      assert.equal(((await pending) as PublicError).code, 'disconnected');
    });
  }
});

test('R27: a peer asked to close with an observe-only code never transmits it', async () => {
  for (const code of [1005, 1006, 1015]) {
    const [near, far] = pipe();
    const peer = new Peer();
    await peer.attach(near);
    let observed: [number, string] | undefined;
    far.listen({ close: (closed, reason) => void (observed = [closed, reason]) });
    const ended = deferred<PublicError>();
    peer.onClose(ended.resolve);
    const pending = call(peer.wire(), ['held']).catch((error: unknown) => error);
    await nextTurn();
    peer.wire().close(code, 'observed');
    // A connection of the seam ends only by a close, so the peer closes with a
    // normal one instead of the code (Go aborts, and the far side reads 1006).
    await eventually(() => observed !== undefined);
    assert.deepEqual(observed, [1000, ''], `${code}`);
    assert.equal(peer.status, 'disconnected');
    assert.equal((await ended.promise).code, 'disconnected');
    assert.equal(((await pending) as PublicError).code, 'disconnected');
  }
});

test('R28: a closing peer answers every request its root still holds', async (t) => {
  const { client, servers } = await paired({ maxPendingRequests: 1 });
  handle(servers, ['hold'], () => new Promise(() => {}));
  const answers = new Map<string, string>();
  const returning = (label: string) => ({
    wire: {
      send: (_path: readonly string[], message: Message) => {
        const frame = message.frame;
        answers.set(label, frame.kind === 'response' && frame.error ? frame.error.code : 'result');
      },
    },
  });
  const request = (id: string, label: string) =>
    client.wire().send(['hold'], { frame: { version: 1, kind: 'request', id, params: null }, return: returning(label) });
  request('c:1', 'admitted');
  request('c:2', 'over the bound');
  // The peer ends before its root has handed either on.
  client.close();
  await settled();
  t.after(() => client.close());
  assert.deepEqual(Object.fromEntries(answers), { admitted: 'disconnected', 'over the bound': 'busy' });
});
