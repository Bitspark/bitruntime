// bitruntime's TypeScript program of the interoperability scenario in
// conformance/interop/README.md. It imports the package it is part of by name.
import {WebSocketServer} from 'ws';
import {at, PublicError} from '@bitspark/bitruntime/core';
import {call, createDispatcher, emit, handle, onEvent} from '@bitspark/bitruntime/dispatch';
import {Peer} from '@bitspark/bitruntime/engine';

const name = 'bitruntime-ts';
const maxFrameBytes = 4 * 1024 * 1024;
const [role, where] = process.argv.slice(2);

function serve(port) {
  const server = new WebSocketServer({host: '127.0.0.1', port: Number(port), path: '/wire', maxPayload: maxFrameBytes});
  let sawCancel = false;
  const waiting = new Set();
  server.on('connection', (socket) => {
    const peer = new Peer({
      role: 'server',
      maxFrameBytes,
      prepare(peer) {
        const d = createDispatcher(peer.wire());
        handle(d, ['echo'], (params) => params);
        handle(d, ['spaces', 'a/b', 'echo'], (params) => ({space: 'a/b', params}));
        handle(d, ['spaces', 'é', ''], () => 'unicode-empty');
        handle(d, ['fail'], () => {
          throw new PublicError('bad_request', 'refused on purpose', {n: 1});
        });
        handle(d, ['wait'], (_params, context) => new Promise((_resolve, reject) => {
          emit(peer.wire(), ['waiting'], null, {context});
          context.signal.addEventListener('abort', () => {
            sawCancel = true;
            for (const wake of waiting) wake(true);
            reject(new PublicError('cancelled', 'Request cancelled'));
          }, {once: true});
        }));
        handle(d, ['cancelled'], () => sawCancel || new Promise((resolve) => {
          waiting.add(resolve);
          setTimeout(() => resolve(false), 2000);
        }));
        handle(d, ['meta'], (_params, context) => context.meta ?? {});
        handle(d, ['reverse'], (_params, context) => call(peer.wire(), ['whoami'], null, {context}));
        handle(d, ['big'], (params) => 'x'.repeat(params.n));
        onEvent(d, ['ping'], (data) => emit(peer.wire(), ['pong'], {echo: data}));
      },
    });
    void peer.attach(socket);
  });
  server.on('listening', () => console.log(`LISTEN ws://127.0.0.1:${server.address().port}/wire`));
}

async function client(url) {
  let pong;
  const pongs = new Promise((resolve) => { pong = resolve; });
  const peer = new Peer({
    maxFrameBytes,
    prepare(peer) {
      const d = createDispatcher(peer.wire());
      handle(d, ['whoami'], () => `${name}-client`);
      onEvent(d, ['pong'], (data) => pong(data));
    },
  });
  await peer.connect(url);
  const root = peer.wire();
  const observed = {};
  const outcome = async (promise) => {
    try {
      return await promise;
    } catch (error) {
      return {code: error.code, message: error.message, ...(error.data === undefined ? {} : {data: error.data})};
    }
  };
  observed.echo = await outcome(call(root, ['echo'], {a: [1, 'x', null, true], n: 1e3, u: '😀'}));
  observed.nested = await outcome(call(at(root, ['spaces', 'a/b']), ['echo'], {x: 1}));
  observed.unicodeEmpty = await outcome(call(root, ['spaces', 'é', ''], null));
  observed.missing = (await outcome(call(root, ['missing'], null))).code;
  observed.fail = await outcome(call(root, ['fail'], null));
  emit(root, ['ping'], 7);
  observed.pong = await Promise.race([pongs, new Promise((resolve) => setTimeout(() => resolve('no pong'), 5000))]);
  const withdrawn = await outcome(call(root, ['wait'], null, {timeoutMs: 300}));
  observed.withdrawn = typeof withdrawn === 'object' && withdrawn !== null && typeof withdrawn.code === 'string';
  observed.serverSawCancel = await outcome(call(root, ['cancelled'], null));
  observed.meta = await outcome(call(root, ['meta'], null, {meta: {tenant: 't1'}}));
  observed.reverse = await outcome(call(root, ['reverse'], null));
  const big = await outcome(call(root, ['big'], {n: 3 * 1024 * 1024}));
  observed.bigLength = typeof big === 'string' ? big.length : big;
  const results = await Promise.all(Array.from({length: 20}, (_, i) => outcome(call(root, ['echo'], {i}))));
  observed.concurrent = results.filter((result, i) => result?.i === i).length;
  console.log(JSON.stringify(observed));
  peer.close();
}

if (role === 'server') serve(where);
else if (role === 'client') await client(where);
else {
  console.error('usage: server <port> | client <url>');
  process.exit(2);
}
