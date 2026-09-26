// Ported from Nightseam v0.6.0 runtime/ts/src/publication.test.ts, with the
// raw peer calls and handlers expressed through the root and the dispatch
// helpers. Where the root changes what can be observed, the test says so.
import assert from 'node:assert/strict';
import test from 'node:test';
import { PublicError, UnpublishedError } from '../../../core/ts/src/index.ts';
import { pipe, type FrameConnection } from '../../../transports/ts/src/index.ts';
import { call, createDispatcher, emit, handle } from '../../../dispatch/ts/src/index.ts';
import { Peer } from '../src/index.ts';

function deferred<V>(): { promise: Promise<V>; resolve: (value: V) => void } {
  let resolve!: (value: V) => void;
  const promise = new Promise<V>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

test('unpublished proof belongs to the local send attempt', async () => {
  const [a, b] = pipe();
  const client = new Peer({ maxPendingRequests: 1, maxFrameBytes: 512 });
  const server = new Peer({ role: 'server' });
  const entered = deferred<void>();
  const finish = deferred<void>();
  const handlers = createDispatcher(server.wire());
  handle(handlers, ['wait'], async () => {
    entered.resolve();
    await finish.promise;
    return null;
  });
  handle(handlers, ['busy'], async () => {
    throw new PublicError('busy', 'retained before refusing');
  });
  handle(handlers, ['nested'], async (_params, context) => {
    const controller = new AbortController();
    controller.abort();
    return call(context.wire, ['never.sent'], undefined, { signal: controller.signal });
  });
  await Promise.all([client.attach(a), server.attach(b)]);
  try {
    const controller = new AbortController();
    controller.abort();
    await assert.rejects(call(client.wire(), ['wait'], undefined, { signal: controller.signal }), (error: unknown) => {
      assert.ok(error instanceof UnpublishedError);
      assert.ok(error instanceof PublicError);
      assert.equal(error.code, 'cancelled');
      assert.ok(error.cause instanceof PublicError);
      assert.equal(error.cause.code, 'cancelled');
      return true;
    });
    for (const request of [() => 1, 'x'.repeat(1024)]) {
      await assert.rejects(call(client.wire(), ['wait'], request), UnpublishedError);
      assert.throws(() => emit(client.wire(), ['event'], request), UnpublishedError);
    }
    const held = call(client.wire(), ['wait']);
    await entered.promise;
    // v0.6.0's raw call refused the call past the pending bound before it
    // queued, with proof. The root refuses it at admission and answers it
    // through its return capability, as every wire refusal is answered, so
    // the caller receives the refusal as a response and holds no proof.
    await assert.rejects(call(client.wire(), ['busy']), (error: unknown) => {
      assert.ok(error instanceof PublicError);
      assert.equal(error.code, 'busy');
      return true;
    });
    finish.resolve();
    await held;
    for (const [method, code] of [
      ['busy', 'busy'],
      ['nested', 'cancelled'],
    ]) {
      await assert.rejects(call(client.wire(), [method!]), (error: unknown) => {
        assert.ok(error instanceof PublicError);
        assert.equal(error.code, code);
        assert.ok(!(error instanceof UnpublishedError), 'remote error carried local publication proof');
        return true;
      });
    }
  } finally {
    finish.resolve();
    client.close();
    server.close();
  }
});

test('a queued write failure has no unpublished proof', async () => {
  const [a, b] = pipe();
  const cause = new PublicError('send_failed', 'transport failed after queue acceptance');
  const attempted = deferred<void>();
  const connection: FrameConnection = {
    get state() {
      return a.state;
    },
    get buffered() {
      return a.buffered;
    },
    send() {
      attempted.resolve();
      throw cause;
    },
    close: (code, reason) => a.close(code, reason),
    listen: (handlers) => a.listen(handlers),
  };
  const peer = new Peer();
  await peer.attach(connection);
  try {
    await assert.rejects(call(peer.wire(), ['supply']), (error: unknown) => {
      assert.ok(error instanceof PublicError);
      assert.ok(!(error instanceof UnpublishedError));
      // R26: the write failed after the queue accepted it, which ended the
      // carrier; the call reports the carrier ended (v0.6.0: its cause's code).
      assert.equal(error.code, 'disconnected');
      assert.equal(cause.code, 'send_failed');
      return true;
    });
    await attempted.promise;
  } finally {
    peer.close();
    b.close();
  }
});

test('a refused reverse reply cannot lend its proof to an already delivered call', async () => {
  const [a, b] = pipe();
  // The reply and its bounded fallback, which repeats the request's trace,
  // are both over the client's frame limit. v0.6.0's raw handler refused them
  // at the client peer, which ended with frame_too_large at once and settled
  // the delivered call with that. Behind the root the local return capability
  // refuses them first, so nothing answers the reverse call before the
  // deadlines: the delivered call ends at its own (request_timeout), and the
  // client peer ends as v0.6.0's did when its deadline's answer is refused in
  // turn. The deadline is shortened here. The delivered call holds no proof
  // either way, which is what this holds.
  const client = new Peer({ maxFrameBytes: 160, requestTimeoutMs: 100 });
  const server = new Peer({ role: 'server' });
  let delivered = false;
  const ended = deferred<string>();
  client.onClose((error) => ended.resolve(error.code));
  handle(createDispatcher(client.wire()), ['b'], async () => 'x'.repeat(2000));
  handle(createDispatcher(server.wire()), ['a'], async (_params, context) => {
    delivered = true;
    return call(context.wire, ['b']);
  });
  await Promise.all([client.attach(a), server.attach(b)]);
  try {
    await assert.rejects(call(client.wire(), ['a']), (error: unknown) => {
      assert.equal(delivered, true);
      assert.ok(error instanceof PublicError);
      assert.equal(error.code, 'request_timeout');
      assert.ok(!(error instanceof UnpublishedError), 'another reply lent proof to this delivered request');
      return true;
    });
    assert.equal(await ended.promise, 'frame_too_large');
  } finally {
    client.close();
    server.close();
  }
});

test('an adapter getter failure after writing cannot prove its queued request unpublished', async () => {
  const [a, b] = pipe();
  const client = new Peer();
  const server = new Peer({ role: 'server' });
  const delivered = deferred<void>();
  let writes = 0;
  let failAfterWrite = false;
  let nested!: UnpublishedError;
  const connection: FrameConnection = {
    get state() {
      return a.state;
    },
    get buffered() {
      if (failAfterWrite) {
        failAfterWrite = false;
        throw nested;
      }
      return a.buffered;
    },
    send(frame) {
      a.send(frame);
      writes++;
      if (writes === 1) failAfterWrite = true;
    },
    close: (code, reason) => a.close(code, reason),
    listen: (handlers) => a.listen(handlers),
  };
  const handlers = createDispatcher(server.wire());
  handle(handlers, ['supply'], async () => {
    delivered.resolve();
    return null;
  });
  handle(handlers, ['ordinary'], async () => 42);
  await Promise.all([client.attach(connection), server.attach(b)]);
  try {
    const controller = new AbortController();
    controller.abort();
    await assert.rejects(call(client.wire(), ['nested'], undefined, { signal: controller.signal }), (error: unknown) => {
      assert.ok(error instanceof UnpublishedError);
      nested = error;
      return true;
    });
    await assert.rejects(call(client.wire(), ['supply']), (error: unknown) => {
      assert.equal(writes, 1);
      assert.ok(error instanceof PublicError);
      assert.ok(!(error instanceof UnpublishedError), 'a post-write adapter error carried nested proof');
      assert.equal(error.code, nested.code);
      // Not ported: the peer's own error object as this error's cause. The
      // root answers through the caller's return capability with the public
      // fields alone, which is also what strips the proof.
      return true;
    });
    await delivered.promise;
    assert.equal(await call(client.wire(), ['ordinary']), 42);
    assert.equal(writes, 2);
  } finally {
    client.close();
    server.close();
  }
});
