// Two peers over a real socket: a client Peer that connects with Node's global
// WebSocket, and a server Peer attached to a connection a `ws` WebSocketServer
// accepted. A call, an event and a cancellation cross in each direction, and
// a close carries the code the closing peer chose.
import assert from 'node:assert/strict';
import type { AddressInfo } from 'node:net';
import test from 'node:test';
import { WebSocketServer, type WebSocket as ServerSocket } from 'ws';
import type { Endpoint } from '@bitspark/bitwire';
import { PublicError } from '../../../core/ts/src/index.ts';
import type { WebSocketLike } from '../../../transports/ts/src/index.ts';
import { call, createDispatcher, emit, handle, onEvent, type Dispatcher } from '../../../dispatch/ts/src/index.ts';
import { Peer, PROTOCOL } from '../src/index.ts';

function deferred<T = void>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  const promise = new Promise<T>((accept) => {
    resolve = accept;
  });
  return { promise, resolve };
}

/** Serves one side: an echo, a request held until it is cancelled, and a notice listener. */
function serve(root: Endpoint, side: string) {
  const dispatcher: Dispatcher = createDispatcher(root);
  const held = deferred();
  const cancelled = deferred();
  const notices: unknown[] = [];
  const noticed = deferred();
  handle(dispatcher, ['echo'], (params) => ({ side, params }));
  handle(dispatcher, ['hold'], (_params, context) => {
    held.resolve();
    return new Promise((resolve) => {
      context.signal.addEventListener(
        'abort',
        () => {
          cancelled.resolve();
          resolve('too late');
        },
        { once: true },
      );
    });
  });
  onEvent(dispatcher, ['notice'], (data) => {
    notices.push(data);
    noticed.resolve();
  });
  return { dispatcher, held, cancelled, notices, noticed };
}

async function connected(t: test.TestContext) {
  const server = new WebSocketServer({ host: '127.0.0.1', port: 0 });
  await new Promise<void>((resolve) => server.once('listening', () => resolve()));
  const accepted = deferred<{ peer: Peer; socket: ServerSocket; served: ReturnType<typeof serve> }>();
  server.on('connection', (socket) => {
    let served!: ReturnType<typeof serve>;
    const peer = new Peer({ role: 'server', prepare: (peer) => void (served = serve(peer.wire(), 'server')) });
    void peer.attach(socket as unknown as WebSocketLike).then(() => accepted.resolve({ peer, socket, served }));
  });
  let clientSocket: WebSocket | undefined;
  let served!: ReturnType<typeof serve>;
  const client = new Peer({
    prepare: (peer) => void (served = serve(peer.wire(), 'client')),
    webSocketFactory: (url, protocols) => (clientSocket = new WebSocket(url, protocols)),
  });
  const { port } = server.address() as AddressInfo;
  await client.connect(`ws://127.0.0.1:${port}/`);
  const far = await accepted.promise;
  t.after(async () => {
    client.close();
    far.peer.close();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  });
  return { client, clientServed: served, clientSocket: clientSocket!, server: far };
}

test('two peers call, emit and cancel both ways over a real WebSocket', async (t) => {
  const { client, clientServed, server } = await connected(t);
  assert.equal(PROTOCOL, 'bitwire/1');
  assert.equal(client.status, 'connected');
  assert.equal(server.peer.status, 'connected');

  // Client to server, and server to client.
  assert.deepEqual(await call(client.wire(), ['echo'], 1), { side: 'server', params: 1 });
  assert.deepEqual(await call(server.peer.wire(), ['echo'], 2), { side: 'client', params: 2 });

  emit(client.wire(), ['notice'], 'to the server');
  emit(server.peer.wire(), ['notice'], 'to the client');
  await Promise.all([server.served.noticed.promise, clientServed.noticed.promise]);
  assert.deepEqual(server.served.notices, ['to the server']);
  assert.deepEqual(clientServed.notices, ['to the client']);

  for (const [caller, callee] of [
    [client, server.served],
    [server.peer, clientServed],
  ] as const) {
    const controller = new AbortController();
    const pending = call(caller.wire(), ['hold'], null, { signal: controller.signal }).catch((error: unknown) => error);
    await callee.held.promise;
    controller.abort();
    const outcome = await pending;
    assert.ok(outcome instanceof PublicError);
    assert.equal(outcome.code, 'cancelled');
    await callee.cancelled.promise;
  }
  // Both carriers are whole after the cancellations, and still answer.
  assert.deepEqual(await call(client.wire(), ['echo'], 3), { side: 'server', params: 3 });
  assert.deepEqual(await call(server.peer.wire(), ['echo'], 4), { side: 'client', params: 4 });
});

test('a peer that closes over a real WebSocket transmits the code it chose', async (t) => {
  const { client, clientSocket, server } = await connected(t);
  const seen = deferred<[number, string]>();
  clientSocket.addEventListener('close', (event) => seen.resolve([event.code, event.reason]));
  const ended = deferred<PublicError>();
  client.onClose(ended.resolve);
  const pending = call(client.wire(), ['hold']).catch((error: unknown) => error);
  await server.served.held.promise;
  server.peer.wire().close(4001, 'closed by the server');
  assert.deepEqual(await seen.promise, [4001, 'closed by the server']);
  assert.equal((await ended.promise).code, 'disconnected');
  assert.equal(((await pending) as PublicError).code, 'disconnected');
  assert.equal(client.status, 'disconnected');
});
