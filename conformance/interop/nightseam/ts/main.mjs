// nightseam v0.6.0's TypeScript program of the interoperability scenario in
// conformance/interop/README.md. Test-only, pinned to the released packages.
import {WebSocketServer} from 'ws';
import {at} from '@nightseam/duplex';
import {DuplexError, DuplexPeer, callWire, createDispatcher, emitWire, handleWire, onWireEvent} from '@nightseam/runtime';

const name = 'nightseam-ts';
const maxFrameBytes = 4 * 1024 * 1024;
const [role, where] = process.argv.slice(2);

function serve(port) {
  const server = new WebSocketServer({host: '127.0.0.1', port: Number(port), path: '/wire', maxPayload: maxFrameBytes});
  let sawCancel = false;
  const waiting = new Set();
  server.on('connection', (socket) => {
    const peer = new DuplexPeer({
      role: 'server',
      maxFrameBytes,
      prepare(peer) {
        const d = createDispatcher(peer.wire());
        handleWire(d, ['echo'], (params) => params);
        handleWire(d, ['spaces', 'a/b', 'echo'], (params) => ({space: 'a/b', params}));
        handleWire(d, ['spaces', 'é', ''], () => 'unicode-empty');
        handleWire(d, ['fail'], () => {
          throw new DuplexError('bad_request', 'refused on purpose', {n: 1});
        });
        handleWire(d, ['wait'], (_params, context) => new Promise((_resolve, reject) => {
          emitWire(peer.wire(), ['waiting'], null, {context});
          context.signal.addEventListener('abort', () => {
            sawCancel = true;
            for (const wake of waiting) wake(true);
            reject(new DuplexError('cancelled', 'Request cancelled'));
          }, {once: true});
        }));
        handleWire(d, ['cancelled'], () => sawCancel || new Promise((resolve) => {
          waiting.add(resolve);
          setTimeout(() => resolve(false), 2000);
        }));
        handleWire(d, ['meta'], (_params, context) => context.meta ?? {});
        handleWire(d, ['reverse'], (_params, context) => callWire(peer.wire(), ['whoami'], null, {context}));
        handleWire(d, ['big'], (params) => 'x'.repeat(params.n));
        onWireEvent(d, ['ping'], (data) => emitWire(peer.wire(), ['pong'], {echo: data}));
      },
    });
    void peer.attach(socket);
  });
  server.on('listening', () => console.log(`LISTEN ws://127.0.0.1:${server.address().port}/wire`));
}

async function client(url) {
  let pong;
  const pongs = new Promise((resolve) => { pong = resolve; });
  const peer = new DuplexPeer({
    maxFrameBytes,
    prepare(peer) {
      const d = createDispatcher(peer.wire());
      handleWire(d, ['whoami'], () => `${name}-client`);
      onWireEvent(d, ['pong'], (data) => pong(data));
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
  observed.echo = await outcome(callWire(root, ['echo'], {a: [1, 'x', null, true], n: 1e3, u: '😀'}));
  observed.nested = await outcome(callWire(at(root, ['spaces', 'a/b']), ['echo'], {x: 1}));
  observed.unicodeEmpty = await outcome(callWire(root, ['spaces', 'é', ''], null));
  observed.missing = (await outcome(callWire(root, ['missing'], null))).code;
  observed.fail = await outcome(callWire(root, ['fail'], null));
  emitWire(root, ['ping'], 7);
  observed.pong = await Promise.race([pongs, new Promise((resolve) => setTimeout(() => resolve('no pong'), 5000))]);
  const withdrawn = await outcome(callWire(root, ['wait'], null, {timeoutMs: 300}));
  observed.withdrawn = typeof withdrawn === 'object' && withdrawn !== null && typeof withdrawn.code === 'string';
  observed.serverSawCancel = await outcome(callWire(root, ['cancelled'], null));
  observed.meta = await outcome(callWire(root, ['meta'], null, {meta: {tenant: 't1'}}));
  observed.reverse = await outcome(callWire(root, ['reverse'], null));
  const big = await outcome(callWire(root, ['big'], {n: 3 * 1024 * 1024}));
  observed.bigLength = typeof big === 'string' ? big.length : big;
  const results = await Promise.all(Array.from({length: 20}, (_, i) => outcome(callWire(root, ['echo'], {i}))));
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
