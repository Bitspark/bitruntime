// Ported from Nightseam v0.6.0 runtime/ts/src/wire-pair.test.ts (wirePair is
// now pair, with PairOptions of its own), followed by the regressions of the
// defects this port fixes: nightseam#722, nightseam#658, R26 and R27.
import assert from 'node:assert/strict';
import test from 'node:test';
import type { AddressedWire, Message, ReturnAddress } from '@bitspark/bitwire';
import { pair, PublicError, UnpublishedError } from '../src/index.ts';
import { setDispatchContext } from '../src/internal/context.ts';
import { call, createDispatcher, emit, handle, onEvent } from '../../../dispatch/ts/src/index.ts';

function deferred<T = void>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((yes) => {
    resolve = yes;
  });
  return { promise, resolve };
}

test('local pair supports reverse calls and independent construction', async (t) => {
  const [a, b] = pair();
  const [x, y] = pair();
  const leftDispatcher = createDispatcher(a);
  const rightDispatcher = createDispatcher(b);
  const independentDispatcher = createDispatcher(y);
  t.after(() => {
    leftDispatcher.close();
    rightDispatcher.close();
    independentDispatcher.close();
    a.close();
    x.close();
  });
  handle(leftDispatcher, ['reverse'], (value) => value);
  handle(rightDispatcher, ['call'], (value) => call(b, ['reverse'], value));
  handle(independentDispatcher, ['call'], () => 'independent');
  assert.equal(await call(a, ['call'], 7), 7);
  a.close();
  assert.equal(await call(x, ['call']), 'independent');
});

test('local pending budget lives until the response and can then be reused', async (t) => {
  const [a, b] = pair({ maxPendingRequests: 1 });
  const dispatcher = createDispatcher(b);
  t.after(() => {
    dispatcher.close();
    a.close();
  });
  const started = deferred(),
    release = deferred();
  handle(dispatcher, ['hold'], async () => {
    started.resolve();
    await release.promise;
    return 1;
  });
  const first = call(a, ['hold']);
  await started.promise;
  await assert.rejects(call(a, ['hold']), { code: 'busy' });
  release.resolve();
  assert.equal(await first, 1);
  assert.equal(await call(a, ['hold']), 1);
});

test('local events await serially and cancellation owns capacity outside the full data queue', async (t) => {
  const [a, b] = pair({ queueCapacity: 1, maxPendingRequests: 1 });
  const dispatcher = createDispatcher(b);
  t.after(() => {
    dispatcher.close();
    a.close();
  });
  const started = deferred(),
    cancelled = deferred(),
    entered = deferred(),
    release = deferred(),
    drained = deferred();
  const seen: number[] = [];
  handle(dispatcher, ['hold'], async (_params, context) => {
    started.resolve();
    await new Promise<void>((resolve) =>
      context.signal.addEventListener(
        'abort',
        () => {
          cancelled.resolve();
          resolve();
        },
        { once: true },
      ),
    );
  });
  onEvent(dispatcher, ['event'], async (value) => {
    seen.push(value as number);
    if (value === 1) {
      entered.resolve();
      await release.promise;
    } else drained.resolve();
  });
  const controller = new AbortController();
  const first = call(a, ['hold'], null, { signal: controller.signal }).catch((error: unknown) => error);
  await started.promise;
  emit(a, ['event'], 1);
  await entered.promise;
  emit(a, ['event'], 2);
  controller.abort();
  assert.equal(((await first) as { code: string }).code, 'cancelled');
  release.resolve();
  await Promise.all([cancelled.promise, drained.promise]);
  assert.deepEqual(seen, [1, 2]);
});

test('a full local carrier closes even while its event consumer is held', async (t) => {
  const [a, b] = pair({ queueCapacity: 1 });
  t.after(() => a.close());
  const entered = deferred(),
    release = deferred(),
    ended = deferred();
  b.receive({
    message: async () => {
      entered.resolve();
      await release.promise;
    },
    closed: () => ended.resolve(),
  });
  emit(a, ['event']);
  await entered.promise;
  emit(a, ['event']);
  // R26: the refused send reports the carrier ended, keeping why as its cause.
  assert.throws(
    () => emit(a, ['event']),
    (error: unknown) =>
      error instanceof UnpublishedError &&
      error.code === 'disconnected' &&
      ((error.cause as PublicError).cause as PublicError).code === 'busy',
  );
  await ended.promise;
  release.resolve();
});

test('request and cancellation use one mapped return capability and responses retire on throwing returns', async (t) => {
  const [a, b] = pair({ maxPendingRequests: 1 });
  const dispatcher = createDispatcher(b);
  t.after(() => {
    dispatcher.close();
    a.close();
  });
  const received = deferred<Message>(),
    cancelled = deferred<Message>();
  dispatcher.register(['raw'], {
    message: (_path, message) => {
      (message.frame.kind === 'request' ? received : cancelled).resolve(message);
    },
  });
  const returning: ReturnAddress = {
    wire: {
      send: () => {
        throw new Error('return failed');
      },
    },
  };
  a.send(['raw'], { frame: { version: 1, kind: 'request', id: 'c:1', params: null }, return: returning });
  const request = await received.promise;
  assert.notEqual(request.return, returning);
  a.send(['raw'], { frame: { version: 1, kind: 'cancel', id: 'c:1' }, return: returning });
  assert.equal((await cancelled.promise).return, request.return);
  assert.throws(
    () => request.return!.wire.send([], { frame: { version: 1, kind: 'response', id: 'c:1', result: null } }),
    /return failed/,
  );
  handle(dispatcher, ['next'], () => 'reused');
  assert.equal(await call(a, ['next']), 'reused');
});

test('local roots preserve private dispatch context and keep metadata explicit', async (t) => {
  const [a, b] = pair();
  const dispatcher = createDispatcher(b);
  t.after(() => {
    dispatcher.close();
    a.close();
  });
  const verified = Object.freeze({ identity: 'verified locally' });
  const source = { signal: new AbortController().signal, requestId: 'physical:17' };
  Object.defineProperty(source, 'verified', { value: verified });
  const traced: AddressedWire = {
    send: (path, message) => {
      if (message.frame.kind === 'request') setDispatchContext(message.return!, { context: source, maxFrameBytes: 1024 });
      a.send(path, message);
    },
  };
  handle(dispatcher, ['inspect'], (_value, context) => {
    assert.equal((context as typeof context & { verified: unknown }).verified, verified);
    assert.equal(context.requestId, 'physical:17');
    assert.deepEqual(context.meta, { explicit: 'yes' });
    return 'observed';
  });
  assert.equal(await call(traced, ['inspect'], null, { meta: { explicit: 'yes' } }), 'observed');
});

test('local exact routes win over the longest namespace and detach reveals fallback', async (t) => {
  const [a, b] = pair();
  const dispatcher = createDispatcher(b);
  t.after(() => {
    dispatcher.close();
    a.close();
  });
  const seen: string[] = [];
  const receiver = (label: string) => ({
    message: (path: readonly string[], message: Message) => {
      seen.push(`${label}:${path.join('/')}`);
      message.return!.wire.send([], {
        frame: { version: 1, kind: 'response', id: (message.frame as { id: string }).id, result: label },
      });
    },
  });
  dispatcher.registerPrefix([], receiver('root'));
  dispatcher.registerPrefix(['a'], receiver('a'));
  const detach = handle(dispatcher, ['a', 'b'], () => 'exact');
  assert.equal(await call(a, ['a', 'b']), 'exact');
  detach();
  assert.equal(await call(a, ['a', 'b']), 'a');
  assert.equal(await call(a, ['other']), 'root');
  assert.deepEqual(seen, ['a:a/b', 'root:other']);
});

test('local deadline cancels handlers but retains noncooperative handler capacity', async (t) => {
  const [a, b] = pair({ requestTimeoutMs: 15, maxConcurrentHandlers: 1 });
  const dispatcher = createDispatcher(b);
  t.after(() => {
    dispatcher.close();
    a.close();
  });
  const release = deferred(),
    started = deferred(),
    cancelled = deferred();
  let invocations = 0;
  handle(dispatcher, ['hold'], async (_value, context) => {
    invocations++;
    started.resolve();
    context.signal.addEventListener('abort', () => cancelled.resolve(), { once: true });
    await release.promise;
  });
  const first = call(a, ['hold']).catch((error: unknown) => error);
  await started.promise;
  assert.equal(((await first) as { code: string }).code, 'cancelled');
  await cancelled.promise;
  await assert.rejects(call(a, ['hold']), { code: 'busy' });
  assert.equal(invocations, 1);
  release.resolve();
});

test('local queued payload snapshots cannot be changed by the sender', async (t) => {
  const [a, b] = pair();
  const dispatcher = createDispatcher(b);
  t.after(() => {
    dispatcher.close();
    a.close();
  });
  const seen = deferred<unknown>();
  onEvent(dispatcher, ['event'], (value) => seen.resolve(value));
  const data = { nested: ['original'] };
  a.send(['event'], { frame: { version: 1, kind: 'event', data } });
  data.nested[0] = 'mutated';
  assert.deepEqual(await seen.promise, { nested: ['original'] });
});

test('a stalled local event reaches the configured deadline', async (t) => {
  // The observer's backpressure event is not ported (observers are removed);
  // the pair's own diagnostic says what stalled instead.
  const stalled = deferred<string>();
  const [a, b] = pair({ writeTimeoutMs: 15, onError: (error) => stalled.resolve(error.code) });
  t.after(() => a.close());
  const release = deferred(),
    closed = deferred();
  const started = Date.now();
  b.receive({ message: () => release.promise, closed: () => closed.resolve() });
  emit(a, ['event']);
  assert.equal(await stalled.promise, 'stalled_consumer');
  assert.ok(Date.now() - started >= 10, 'the pair ended before its configured deadline');
  await closed.promise;
  assert.throws(() => emit(a, ['event']), { code: 'disconnected' });
  release.resolve();
});

test('an oversized local response settles through the bounded internal fallback', async (t) => {
  const [a, b] = pair({ maxFrameBytes: 512 });
  const dispatcher = createDispatcher(b);
  t.after(() => {
    dispatcher.close();
    a.close();
  });
  handle(dispatcher, ['large'], () => 'x'.repeat(2048));
  await assert.rejects(call(a, ['large'], null, { timeoutMs: 1000 }), { code: 'internal' });
});

const settle = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 0));
const outcome = (message: Message): string => {
  const frame = message.frame;
  if (frame.kind !== 'response') return `unexpected ${frame.kind}`;
  return frame.error ? frame.error.code : String(frame.result);
};

test('nightseam#722: a closing pair answers every queued refusal with the refusal it was admitted with', async () => {
  const [a, b] = pair({ maxPendingRequests: 1 });
  b.receive({ message: () => {} });
  const answers = new Map<string, string>();
  const returning = (label: string): ReturnAddress => ({
    wire: { send: (_path, message) => void answers.set(label, outcome(message)) },
  });
  const request = (id: string, address: ReturnAddress): void =>
    a.send(['op'], { frame: { version: 1, kind: 'request', id, params: null }, return: address });
  const admitted = returning('admitted');
  request('c:1', admitted);
  // Refused at admission and queued behind it: a duplicate identifier on the
  // same return capability, and a call past the pending bound.
  request('c:1', admitted);
  request('c:2', returning('over the bound'));
  // The pair closes before its queue drains; v0.6.0 dropped both refusals.
  a.close();
  await settle();
  assert.deepEqual(Object.fromEntries(answers), {
    admitted: 'disconnected',
    'over the bound': 'busy',
  });
  // The duplicate shares the admitted request's return capability: what it
  // was answered is recorded under that label, in the order they were given.
  const both: string[] = [];
  const [c, d] = pair({ maxPendingRequests: 1 });
  d.receive({ message: () => {} });
  const shared: ReturnAddress = { wire: { send: (_path, message) => void both.push(outcome(message)) } };
  c.send(['op'], { frame: { version: 1, kind: 'request', id: 'c:1', params: null }, return: shared });
  c.send(['op'], { frame: { version: 1, kind: 'request', id: 'c:1', params: null }, return: shared });
  c.close();
  await settle();
  assert.deepEqual(both, ['invalid_message', 'disconnected']);
});

test('nightseam#722: a caller of a closing pair is answered, not left to its own deadline', async () => {
  const [a, b] = pair({ maxPendingRequests: 1 });
  const dispatcher = createDispatcher(b);
  handle(dispatcher, ['hold'], () => new Promise(() => {}));
  const first = call(a, ['hold'], null, { timeoutMs: 5_000 }).catch((error: unknown) => error);
  const second = call(a, ['hold'], null, { timeoutMs: 5_000 }).catch((error: unknown) => error);
  a.close();
  const settled = await Promise.race([
    Promise.all([first, second]),
    new Promise<'waited'>((resolve) => setTimeout(() => resolve('waited'), 1_000)),
  ]);
  assert.notEqual(settled, 'waited', 'a queued refusal was left to the caller deadline');
  assert.equal(((await first) as PublicError).code, 'disconnected');
  assert.equal(((await second) as PublicError).code, 'busy');
});

test('nightseam#658: a pair response frees its call slot before the caller holds the answer', async () => {
  const [a, b] = pair({ maxPendingRequests: 1 });
  const dispatcher = createDispatcher(b);
  handle(dispatcher, ['echo'], (value) => value);
  // Each caller issues its next call from inside the delivery of its answer,
  // with a fresh return capability, as a synchronous consumer does. The slot
  // must already be free: v0.6.0 retired the call only after forwarding.
  const answers: string[] = [];
  const done = deferred();
  const next = (index: number): void => {
    const returning: AddressedWire = {
      send: (_path, message) => {
        answers.push(outcome(message));
        if (index + 1 < 200) next(index + 1);
        else done.resolve();
      },
    };
    a.send(['echo'], { frame: { version: 1, kind: 'request', id: 'c:1', params: index }, return: { wire: returning } });
  };
  next(0);
  await done.promise;
  assert.deepEqual(
    answers,
    Array.from({ length: 200 }, (_, index) => String(index)),
  );
  // The same through the call helper, 200 times over.
  for (let index = 0; index < 200; index++) assert.equal(await call(a, ['echo'], index), index);
  dispatcher.close();
  a.close();
});

test('nightseam#658: a call whose cancellation is still queued keeps its slot until it drains', async () => {
  const [a, b] = pair({ maxPendingRequests: 1 });
  let held: Message | undefined;
  b.receive({
    message: (_path, message) => {
      if (message.frame.kind === 'request') held = message;
    },
  });
  const answers: string[] = [];
  const returning = (label: string): ReturnAddress => ({
    wire: { send: (_path, message) => void answers.push(`${label}:${outcome(message)}`) },
  });
  const request = (address: ReturnAddress): void =>
    a.send(['op'], { frame: { version: 1, kind: 'request', id: 'c:1', params: null }, return: address });
  const first = returning('first');
  request(first);
  await settle();
  a.send(['op'], { frame: { version: 1, kind: 'cancel', id: 'c:1' }, return: first });
  // The handler answers before the queued cancellation has drained.
  held!.return!.wire.send([], { frame: { version: 1, kind: 'response', id: 'c:1', result: 'one' } });
  request(returning('second'));
  await settle();
  request(returning('third'));
  await settle();
  held!.return!.wire.send([], { frame: { version: 1, kind: 'response', id: 'c:1', result: 'three' } });
  await settle();
  assert.deepEqual(answers, ['first:one', 'second:busy', 'third:three']);
  a.close();
});

test('R26: a closed pair reports the one closed classification', async () => {
  const [a, b] = pair();
  a.close();
  await settle();
  const event: Message = { frame: { version: 1, kind: 'event', data: null } };
  assert.throws(() => a.send(['x'], event), { code: 'disconnected' });
  assert.throws(() => b.receive({}), { code: 'disconnected' });
  await assert.rejects(call(a, ['x']), (error: unknown) => error instanceof UnpublishedError && error.code === 'disconnected');
});

test('R27: a pair closed with an observe-only code tells its receivers that code', async () => {
  const [a, b] = pair();
  const endings: [number, string][] = [];
  a.receive({ closed: (code, reason) => void endings.push([code, reason]) });
  b.receive({ closed: (code, reason) => void endings.push([code, reason]) });
  // Nothing is transmitted in-process, so the pair closes as an abort would.
  a.close(1006, 'observed');
  await settle();
  assert.deepEqual(endings, [
    [1006, 'observed'],
    [1006, 'observed'],
  ]);
});
