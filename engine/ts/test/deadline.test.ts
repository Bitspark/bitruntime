// A peer's own deadline for a call is its caller's (bitwire/1, bitruntime#23):
// an application's own call fails with its local request_timeout, and a
// forwarded call is answered cancelled across the wire, since a
// request_timeout is never a frame.
import assert from 'node:assert/strict';
import test from 'node:test';
import { forward, PublicError } from '../../../core/ts/src/index.ts';
import { pipe } from '../../../transports/ts/src/index.ts';
import { call, createDispatcher, handle } from '../../../dispatch/ts/src/index.ts';
import { Peer, type PeerOptions } from '../src/index.ts';

async function connected(clientOptions: PeerOptions = {}) {
  const [a, b] = pipe();
  const client = new Peer(clientOptions);
  const server = new Peer({ role: 'server' });
  await Promise.all([client.attach(a), server.attach(b)]);
  return { client, server };
}

/** Serves ['wait'] with a handler that answers only when cancelled, and reports the cancellation. */
function waiting(peer: Peer): Promise<void> {
  return new Promise((cancelled) => {
    handle(createDispatcher(peer.wire()), ['wait'], (_params, context) =>
      new Promise((resolve) =>
        context.signal.addEventListener('abort', () => {
          cancelled();
          resolve(null);
        }),
      ),
    );
  });
}

const code = async (promise: Promise<unknown>): Promise<string> => {
  try {
    await promise;
    return 'answered';
  } catch (error) {
    return error instanceof PublicError ? error.code : String(error);
  }
};

test("a peer's deadline fails its own caller with request_timeout and cancels the remote handler", async () => {
  const { client, server } = await connected({ requestTimeoutMs: 200 });
  const cancelled = waiting(server);
  try {
    assert.equal(await code(call(client.wire(), ['wait'], null, { timeoutMs: 10_000 })), 'request_timeout');
    await cancelled;
  } finally {
    client.close();
    server.close();
  }
});

test("a forwarded call past the onward peer's deadline is answered cancelled across the wire", async () => {
  const first = await connected();
  const second = await connected({ requestTimeoutMs: 200 });
  const cancelled = waiting(second.server);
  const stop = forward(first.server.wire(), second.client.wire());
  try {
    assert.equal(await code(call(first.client.wire(), ['wait'], null, { timeoutMs: 10_000 })), 'cancelled');
    await cancelled;
  } finally {
    stop();
    for (const peer of [first.client, first.server, second.client, second.server]) peer.close();
  }
});
