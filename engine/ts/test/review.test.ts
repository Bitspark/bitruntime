// Regressions for what the independent review of the TypeScript port found.
// Each held for nightseam v0.6.0 as well; each is fixed here.
import assert from 'node:assert/strict';
import test from 'node:test';
import type { Endpoint } from '@bitspark/bitwire';
import { forward, pair, PublicError } from '../../../core/ts/src/index.ts';
import { call, createDispatcher, emit, handle } from '../../../dispatch/ts/src/index.ts';
import { pipe, webSocketConnection, type FrameConnection, type WebSocketLike } from '../../../transports/ts/src/index.ts';
import { Peer } from '../src/index.ts';

test('a close code the WebSocket API refuses still closes the socket', () => {
  const closes: unknown[][] = [];
  const socket: WebSocketLike = {
    readyState: 1,
    bufferedAmount: 0,
    send() {},
    // The browser API accepts only 1000 and 3000–4999.
    close(...args: unknown[]) {
      const [code] = args as [number | undefined];
      closes.push(args);
      if (code !== undefined && code !== 1000 && (code < 3000 || code > 4999)) {
        throw new DOMException('refused', 'InvalidAccessError');
      }
    },
    addEventListener() {},
    removeEventListener() {},
  };
  webSocketConnection(socket).close(1002, 'wire event rejected');
  assert.deepEqual(closes, [[1002, 'wire event rejected'], []]);
});

test('a connected peer whose connection is closing reports disconnected, and a racing emit keeps the remote close', async () => {
  const [a, b] = pipe();
  let state: FrameConnection['state'] = 'open';
  const closing: FrameConnection = {
    get state() {
      return state === 'open' ? a.state : state;
    },
    get buffered() {
      return a.buffered;
    },
    send: (frame) => a.send(frame),
    close: (code, reason) => a.close(code, reason),
    listen: (handlers) => a.listen(handlers),
  };
  const client = new Peer();
  const server = new Peer({ role: 'server' });
  await Promise.all([client.attach(closing), server.attach(b)]);
  const ended = new Promise<PublicError>((resolve) => client.onClose(resolve));
  // The socket has received the far side's close frame but not yet fired
  // its close event.
  state = 'closing';
  await assert.rejects(call(client.wire(), ['anything']), (error: unknown) => {
    assert.ok(error instanceof PublicError);
    assert.equal(error.code, 'disconnected');
    return true;
  });
  emit(client.wire(), ['event'], 1);
  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(client.status, 'connected', 'a racing emit failed the peer before its close arrived');
  state = 'open';
  server.close();
  const error = await ended;
  assert.equal(error.code, 'disconnected');
});

test('a reply refused further back reaches the caller as the bounded internal error', async () => {
  // caller → pair A (small frames) → forward → pair B → handler. B admits the
  // reply; A's return capability refuses it for its size.
  const [callerSide, forwardA] = pair({ maxFrameBytes: 600, requestTimeoutMs: 10_000 });
  const [forwardB, handlerSide] = pair();
  const stop = forward(forwardA, forwardB);
  handle(createDispatcher(handlerSide), ['big'], () => 'x'.repeat(2000));
  const started = Date.now();
  try {
    await assert.rejects(call(callerSide, ['big'], null), (error: unknown) => {
      assert.ok(error instanceof PublicError);
      assert.equal(error.code, 'internal');
      return true;
    });
    assert.ok(Date.now() - started < 5_000, 'the caller waited for its deadline');
  } finally {
    stop();
    callerSide.close();
    forwardB.close();
  }
});

test("a handler's context does not reach the carrier behind its dispatcher", async () => {
  const [a, b] = pair();
  let reached: unknown = 'unset';
  handle(createDispatcher(b), ['probe'], (_params, context) => {
    const registry = context.wire as unknown as Record<string, unknown>;
    reached = registry.root ?? Object.values(registry).find((value) => value === b);
    return null;
  });
  await call(a, ['probe'], null);
  assert.equal(reached, undefined);
  a.close();
});

// Keep the Endpoint import meaningful to the type checker.
export type { Endpoint };
