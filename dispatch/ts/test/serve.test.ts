// Route sets and serving a WireTree (bitruntime#15).
import assert from 'node:assert/strict';
import type { AddressInfo } from 'node:net';
import test from 'node:test';
import { WebSocketServer } from 'ws';
import type { Child, Message, Receiver, Wire, WireTree } from '@bitspark/bitwire';
import { compose, InvalidPathError, pair, PublicError, ReceiverExistsError, respond } from '../../../core/ts/src/index.ts';
import type { WebSocketLike } from '../../../transports/ts/src/index.ts';
import { Peer } from '../../../engine/ts/src/index.ts';
import { call, createDispatcher, emit, serve, type Dispatcher } from '../src/index.ts';

/** An own Wire that records what reaches it and answers requests with its name. */
class Node implements Wire {
  readonly seen: string[] = [];
  hold = false;
  refusal: unknown;
  #waiting: Array<{ entry: string; resolve: () => void }> = [];
  readonly name: string;
  constructor(name: string) {
    this.name = name;
  }
  send(message: Message): void {
    const entry = `${this.name}:${message.frame.kind}`;
    this.seen.push(entry);
    this.#waiting = this.#waiting.filter((waiter) => (waiter.entry === entry ? (waiter.resolve(), false) : true));
    if (this.refusal !== undefined) throw this.refusal;
    if (message.frame.kind === 'request' && !this.hold) respond(message, this.name);
  }
  arrival(entry: string): Promise<void> {
    if (this.seen.includes(entry)) return Promise.resolve();
    return new Promise((resolve) => this.#waiting.push({ entry, resolve }));
  }
}

const key = (text: string): Uint8Array => new TextEncoder().encode(text);
const tree = (own: Wire, children: Child<Wire>[] = []): WireTree => compose<Wire>(own, children);
const answering = (name: string): Receiver => ({
  message: (_path, message) => {
    if (message.frame.kind === 'request') respond(message, name);
  },
});
const code = async (promise: Promise<unknown>): Promise<string> => {
  try {
    await promise;
    return 'answered';
  } catch (error) {
    return error instanceof PublicError ? error.code : String(error);
  }
};

function local(t: { after(fn: () => void): void }): { near: ReturnType<typeof pair>[0]; dispatcher: Dispatcher } {
  const [near, far] = pair();
  t.after(() => near.close());
  return { near, dispatcher: createDispatcher(far) };
}

test('a route set refuses without changing and closes only its own routes', async (t) => {
  const { near, dispatcher } = local(t);
  dispatcher.register(['y'], answering('outside'));
  const routes = dispatcher.routeSet();
  routes.set([{ path: ['x'], receiver: answering('x1') }]);
  const refused: Array<[() => void, unknown]> = [
    [() => routes.set([{ path: ['x'], receiver: answering('x2') }, { path: ['y'], receiver: answering('mine') }]), ReceiverExistsError],
    [() => routes.set([{ path: ['z'], receiver: answering('z') }, { path: ['z'], receiver: answering('z') }]), ReceiverExistsError],
    [() => routes.set([{ path: ['x'], receiver: answering('x2') }, { path: ['\ud800'], receiver: answering('bad') }]), InvalidPathError],
  ];
  for (const [attempt, error] of refused) {
    assert.throws(attempt, error as never);
    assert.equal(await call(near, ['x']), 'x1');
    assert.equal(await call(near, ['y']), 'outside');
  }
  routes.set([
    { path: ['p'], receiver: answering('exact') },
    { path: ['p'], prefix: true, receiver: answering('prefix') },
  ]);
  assert.equal(await call(near, ['p', 'deeper']), 'prefix');
  assert.equal(await code(call(near, ['x'])), 'method_not_found');
  routes.close();
  assert.equal(await code(call(near, ['p'])), 'method_not_found');
  assert.throws(() => routes.set([]));
  assert.equal(await call(near, ['y']), 'outside');
  const other = dispatcher.routeSet();
  dispatcher.close();
  assert.throws(() => other.set([]));
});

test('a request admitted before a swap is cancelled at the route that admitted it', async (t) => {
  const { near, dispatcher } = local(t);
  const old = new Node('old');
  old.hold = true;
  const replacement = new Node('new');
  const own = (node: Node): Receiver => ({ message: (_path, message) => node.send(message) });
  const routes = dispatcher.routeSet();
  routes.set([{ path: ['w'], receiver: own(old) }]);
  const controller = new AbortController();
  const pending = code(call(near, ['w'], null, { signal: controller.signal }));
  await old.arrival('old:request');
  routes.set([{ path: ['w'], receiver: own(replacement) }]);
  controller.abort();
  await old.arrival('old:cancel');
  await pending;
  assert.deepEqual(replacement.seen, []);
});

test('serve routes each UTF-8 position, skips binary-keyed subtrees and keeps a leading BOM', async (t) => {
  const { near, dispatcher } = local(t);
  const nodes = Object.fromEntries(['root', 'a', 'empty', 'unicode', 'bom', 'binary', 'under-binary', 'shared'].map((name) => [name, new Node(name)]));
  const shared = tree(nodes.shared!);
  const served = serve(dispatcher, ['t'], tree(nodes.root!, [
    [key('a'), tree(nodes.a!, [[key(''), tree(nodes.empty!)], [key('s'), shared]])],
    [key('é/x'), tree(nodes.unicode!)],
    [Uint8Array.of(0xef, 0xbb, 0xbf, 0x61), tree(nodes.bom!)],
    [Uint8Array.of(0xff), tree(nodes.binary!, [[key('c'), tree(nodes['under-binary']!)]])],
    [key('s'), shared],
  ]));
  const cases: Array<[string[], string]> = [
    [['t'], 'root'],
    [['t', 'a'], 'a'],
    [['t', 'a', ''], 'empty'],
    [['t', 'é/x'], 'unicode'],
    [['t', '﻿a'], 'bom'],
    [['t', 's'], 'shared'],
    [['t', 'a', 's'], 'shared'],
  ];
  for (const [path, name] of cases) assert.equal(await call(near, path), name, path.join('/'));
  for (const path of [['t', 'c'], ['t', 'é/x'], ['t', 'a', 'missing']])
    assert.equal(await code(call(near, path)), 'method_not_found', path.join('/'));
  assert.deepEqual([...nodes.binary!.seen, ...nodes['under-binary']!.seen], []);
  assert.deepEqual(served.unreachable(), [[Uint8Array.of(0xff)]]);
});

test('over a real WebSocket a refused request is answered and a refused event is dropped, and neither ends the carrier', async (t) => {
  const plain = new Node('plain');
  plain.refusal = new Error('not public');
  const publicRefusal = new Node('public');
  publicRefusal.refusal = new PublicError('bad_request', 'refused on purpose');
  const fine = new Node('fine');
  const server = new WebSocketServer({ host: '127.0.0.1', port: 0 });
  await new Promise<void>((resolve) => server.once('listening', () => resolve()));
  let serverPeer: Peer | undefined;
  server.on('connection', (socket) => {
    serverPeer = new Peer({
      role: 'server',
      prepare: (peer) => {
        serve(createDispatcher(peer.wire()), ['t'], tree(fine, [[key('plain'), tree(plain)], [key('public'), tree(publicRefusal)]]));
      },
    });
    void serverPeer.attach(socket as unknown as WebSocketLike);
  });
  const client = new Peer({ webSocketFactory: (url, protocols) => new WebSocket(url, protocols) });
  t.after(async () => {
    client.close();
    serverPeer?.close();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  });
  await client.connect(`ws://127.0.0.1:${(server.address() as AddressInfo).port}/`);
  assert.equal(await code(call(client.wire(), ['t', 'plain'])), 'internal');
  assert.equal(await code(call(client.wire(), ['t', 'public'])), 'bad_request');
  emit(client.wire(), ['t', 'plain'], 1);
  await plain.arrival('plain:event');
  assert.equal(await call(client.wire(), ['t']), 'fine');
  assert.equal(client.status, 'connected');
  assert.equal(serverPeer?.status, 'connected');
});

test('serve refuses what it cannot serve and leaves no routes behind', async (t) => {
  const { near, dispatcher } = local(t);
  assert.throws(() => serve(dispatcher, [], tree(new Node('root'))), InvalidPathError);
  const foreign: WireTree = {
    own: () => undefined as unknown as Wire,
    children: () => [],
    at: (path) => (path.length === 0 ? foreign : undefined),
    decompose: () => ({ own: undefined as unknown as Wire, children: [] }),
  };
  assert.throws(() => serve(dispatcher, ['t'], tree(new Node('root'), [[key('k'), foreign]])), TypeError);
  dispatcher.register(['t', 'taken'], answering('outside'));
  assert.throws(() => serve(dispatcher, ['t'], tree(new Node('root'), [[key('taken'), tree(new Node('mine'))]])), ReceiverExistsError);
  assert.equal(await code(call(near, ['t'])), 'method_not_found');
  assert.equal(await call(near, ['t', 'taken']), 'outside');
});

test('update keeps admitted requests with their node, a refused update changes nothing, and close keeps the dispatcher', async (t) => {
  const { near, dispatcher } = local(t);
  const old = new Node('old');
  old.hold = true;
  const served = serve(dispatcher, ['t'], tree(old));
  const controller = new AbortController();
  const pending = code(call(near, ['t'], null, { signal: controller.signal }));
  await old.arrival('old:request');
  served.update(tree(new Node('new')));
  controller.abort();
  await old.arrival('old:cancel');
  await pending;
  assert.equal(await call(near, ['t']), 'new');
  assert.throws(() => served.update(tree(new Node('x'), [[key('k'), { own: () => null as unknown as Wire, children: () => [], at: () => undefined, decompose: () => ({ own: null as unknown as Wire, children: [] }) }]])), TypeError);
  assert.equal(await call(near, ['t']), 'new');
  served.close();
  assert.equal(await code(call(near, ['t'])), 'method_not_found');
  dispatcher.register(['t'], answering('after'));
  assert.equal(await call(near, ['t']), 'after');
});
