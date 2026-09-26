// Ported from nightseam v0.6.0 runtime/ts/src/wire-forward.test.ts, with
// forward as forward and the wire helpers as the dispatch helpers.
// Changed, each where it stands: the raw-handler lines of the first test,
// forward keeping on after a refusal (research 0001 row 14), and the model
// context of the register test, which v0.6.0's generated adapters supplied.
import assert from 'node:assert/strict';
import test from 'node:test';
import type { AddressedWire, Endpoint, Message, Path, Receiver } from '@bitspark/bitwire';
import {
  at,
  defaultPropagator,
  forward,
  mount,
  PublicError,
  UnpublishedError,
  type Meta,
} from '../../../core/ts/src/index.ts';
import { pipe } from '../../../transports/ts/src/index.ts';
import { call, createDispatcher, emit, handle, register, type RequestContext } from '../../../dispatch/ts/src/index.ts';
import { Peer } from '../src/index.ts';

function deferred<T = void>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((yes) => {
    resolve = yes;
  });
  return { promise, resolve };
}
async function pair() {
  const [a, b] = pipe();
  const left = new Peer(),
    right = new Peer({ role: 'server' });
  await Promise.all([left.attach(a), right.attach(b)]);
  return {
    left,
    right,
    close: () => {
      left.close();
      right.close();
    },
  };
}
function reply(message: Message, value: unknown) {
  if (message.frame.kind === 'request') {
    message.return!.wire.send([], { frame: { version: 1, kind: 'response', id: message.frame.id, result: value } });
  }
}

test('wire namespace dispatch selects exact then longest segment prefix and preserves raw handlers', async (t) => {
  const peers = await pair();
  t.after(peers.close);
  const events: string[] = [];
  const dispatcher = createDispatcher(peers.right.wire());
  t.after(() => dispatcher.close());
  const receive = (path: Path, label: string, prefix = true) =>
    (prefix ? dispatcher.registerPrefix.bind(dispatcher) : dispatcher.register.bind(dispatcher))(path, {
      message: (received, message) => {
        if (message.frame.kind === 'event') events.push(label);
        else reply(message, { label, path: received });
      },
    });
  const removeRoot = receive([], 'root');
  receive(['a'], 'a');
  receive(['a', 'b'], 'ab');
  const removeExact = receive(['a', 'b'], 'exact', false);
  assert.throws(() => receive(['a'], 'duplicate'), { code: 'receiver_exists' });
  for (const [path, label] of [
    [['a', 'b'], 'exact'],
    [['a', 'b', 'c'], 'ab'],
    [['a', 'bc'], 'a'],
    [['ab'], 'root'],
  ] as const) {
    assert.deepEqual(await call(peers.left.wire(), path), { label, path });
    emit(peers.left.wire(), path);
  }
  // A subsequent request is a delivery barrier for the prior serial events.
  await call(peers.left.wire(), ['barrier']);
  assert.deepEqual(events, ['exact', 'ab', 'a', 'root']);
  removeExact();
  assert.equal((await call<{ label: string }>(peers.left.wire(), ['a', 'b'])).label, 'ab');
  // Not ported: a raw handler beside the root ('raw.method'), and a raw call
  // of the non-canonical name '1:a1'. The peer serves only its root now; the
  // answer to a name that encodes no path is held in peer.test.ts.
  removeRoot();
  await assert.rejects(call(peers.left.wire(), ['unmatched']), { code: 'method_not_found' });
});

test('forward carries nested paths and reverse traffic through a physical root and mounted local model', async (t) => {
  const peers = await pair();
  t.after(peers.close);
  let outgoing: Receiver | undefined,
    closes = 0;
  const arrived = deferred(),
    cancelled = deferred(),
    event = deferred<unknown>();
  const received: { path: Path; message: Message }[] = [];
  const model: Endpoint = {
    send: (path, message) => {
      queueMicrotask(() => {
        received.push({ path, message });
        if (message.frame.kind === 'request') {
          if (path.at(-1) === 'held') arrived.resolve();
          else reply(message, { path, params: message.frame.params });
        } else if (message.frame.kind === 'cancel') cancelled.resolve();
        else if (message.frame.kind === 'event') event.resolve(message.frame.data);
      });
    },
    receive: (receiver) => {
      assert.equal(outgoing, undefined);
      outgoing = receiver;
      return () => {
        outgoing = undefined;
      };
    },
    close: () => {
      closes++;
    },
  };
  const rootBefore = peers.right.wire();
  const detach = forward(rootBefore, mount(new Map([['service', model]])));
  t.after(detach);
  assert.equal(peers.right.wire(), rootBefore);
  const selected = at(peers.left.wire(), ['service', 'deep']);
  assert.deepEqual(await call(selected, ['echo'], 7), { path: ['deep', 'echo'], params: 7 });
  emit(selected, ['notice'], 'event');
  assert.equal(await event.promise, 'event');
  const leftDispatcher = createDispatcher(peers.left.wire());
  t.after(() => leftDispatcher.close());
  handle(leftDispatcher, ['service', 'deep', 'reverse'], (value) => value);
  const reverse: AddressedWire = {
    send: (path, message) => {
      queueMicrotask(() => {
        void outgoing!.message!(path, message);
      });
    },
  };
  assert.equal(await call(reverse, ['deep', 'reverse'], 'back'), 'back');
  const controller = new AbortController();
  const pending = call(selected, ['held'], null, { signal: controller.signal });
  const result = pending.catch((error: unknown) => error);
  await arrived.promise;
  controller.abort();
  await assert.rejects(pending, { code: 'cancelled' });
  await cancelled.promise;
  await result;
  const request = received.find(
    ({ message }) => message.frame.kind === 'request' && 'params' in message.frame && message.frame.params === null,
  )!;
  const cancel = received.find(({ message }) => message.frame.kind === 'cancel')!;
  assert.equal(cancel.message.return, request.message.return);
  assert.deepEqual(cancel.path, request.path);
  detach();
  detach();
  assert.equal(outgoing, undefined);
  assert.equal(closes, 0);
  assert.equal(peers.right.status, 'connected');
  await assert.rejects(call(selected, ['echo']), { code: 'method_not_found' });
});

test('forward is a non-owning pair of attachments and preserves message identity', () => {
  const receivers: Receiver[] = [],
    detached: number[] = [],
    sent: { path: Path; message: Message }[] = [];
  const endpoint = (index: number): Endpoint => ({
    send: (path, message) => {
      sent.push({ path, message });
    },
    receive: (receiver) => {
      receivers[index] = receiver;
      return () => {
        detached.push(index);
      };
    },
    close: () => {
      assert.fail('forwarding owns no endpoint');
    },
  });
  const a = endpoint(0),
    b = endpoint(1);
  const stop = forward(a, b);
  const path = ['opaque', ''],
    message: Message = { frame: { version: 1, kind: 'event', data: null } };
  receivers[0]!.message!(path, message);
  assert.equal(sent[0]!.path, path);
  assert.equal(sent[0]!.message, message);
  receivers[1]!.closed!(1000, 'closed');
  stop();
  assert.deepEqual(detached, [0, 1]);
});

test('structured wire request and event traces cross the physical bridge verbatim', async (t) => {
  const peers = await pair();
  t.after(peers.close);
  const trace = { traceparent: '00-11111111111111111111111111111111-2222222222222222-01', tracestate: 'test=value' };
  const request = deferred<Message>(),
    event = deferred<Message>();
  const dispatcher = createDispatcher(peers.right.wire());
  t.after(() => dispatcher.close());
  dispatcher.registerPrefix(['trace'], {
    message: (_path, message) => {
      if (message.frame.kind === 'request') {
        request.resolve(message);
        reply(message, null);
      } else event.resolve(message);
    },
  });
  const returning: AddressedWire = { send: () => {} };
  peers.left.wire().send(['trace', 'call'], {
    frame: { version: 1, kind: 'request', id: 'c:1', params: null, ...trace },
    return: { wire: returning },
  });
  peers.left.wire().send(['trace', 'event'], { frame: { version: 1, kind: 'event', data: null, ...trace } });
  for (const delivered of await Promise.all([request.promise, event.promise])) {
    assert.equal(delivered.frame.traceparent, trace.traceparent);
    assert.equal(delivered.frame.tracestate, trace.tracestate);
  }
});

test('forward cleans partial setup and replies to send failure without lending publication proof', async () => {
  let receiver: Receiver | undefined,
    removed = 0;
  const a: Endpoint = {
    send: () => {},
    receive: (next) => {
      receiver = next;
      return () => {
        removed++;
      };
    },
    close: () => assert.fail('borrowed endpoint closed'),
  };
  const b: Endpoint = {
    send: () => {
      throw new UnpublishedError(new PublicError('busy', 'Refused downstream.'));
    },
    receive: () => {
      throw new Error('Registration refused');
    },
    close: () => assert.fail('borrowed endpoint closed'),
  };
  assert.throws(() => forward(a, b), /Registration refused/);
  assert.equal(removed, 1);
  b.receive = () => () => {
    removed++;
  };
  const stop = forward(a, b);
  const origin: AddressedWire = {
    send: (path, message) => {
      void receiver!.message!(path, message);
    },
  };
  await assert.rejects(call(origin, ['rejected']), (error: unknown) => {
    assert.ok(error instanceof PublicError);
    assert.equal(error.code, 'busy');
    assert.equal(error instanceof UnpublishedError, false);
    return true;
  });
  // Research 0001 row 14: the refusal fails that request alone and forwarding
  // goes on; v0.6.0 detached both directions here (removed was 3).
  assert.equal(removed, 1);
  stop();
  assert.equal(removed, 3);
});

test('detaching a forward leaves captured request cancellation routed to its original destination', async (t) => {
  const peers = await pair();
  t.after(peers.close);
  const started = deferred(),
    cancelled = deferred();
  const destination: Endpoint = {
    send: (_path, message) => {
      if (message.frame.kind === 'request') started.resolve();
      if (message.frame.kind === 'cancel') cancelled.resolve();
    },
    receive: () => () => {},
    close: () => {},
  };
  const mounted = mount(new Map([['route', peers.right.wire()]]));
  const dispatcher = createDispatcher(mounted);
  t.after(() => {
    dispatcher.close();
    mounted.close();
  });
  const selected = dispatcher.select(['route']);
  const stop = forward(selected, destination);
  t.after(stop);
  const controller = new AbortController();
  const result = call(peers.left.wire(), ['held'], {}, { signal: controller.signal }).catch(
    (error: unknown) => error,
  );
  await started.promise;
  stop();
  controller.abort();
  await result;
  await cancelled.promise;
});

test('register groups request event and cancellation while preserving verified model context', async (t) => {
  const [a, b] = pipe();
  const left = new Peer(),
    right = new Peer({
      role: 'server',
      propagator: {
        inject: (context) => defaultPropagator.inject(context),
        extract: (context, trace) => {
          defaultPropagator.extract(context, trace);
          Object.defineProperty(context, 'verified', { value: 'trusted', enumerable: false });
        },
      },
    });
  await Promise.all([left.attach(a), right.attach(b)]);
  t.after(() => {
    left.close();
    right.close();
  });
  const leftDispatcher = createDispatcher(left.wire());
  const rightDispatcher = createDispatcher(right.wire());
  t.after(() => {
    leftDispatcher.close();
    rightDispatcher.close();
  });
  handle(leftDispatcher, ['metadata'], (_params, context) => context.meta ?? null);
  // v0.6.0 took the outgoing meta and deadline from a generated adapter's
  // model context; the call states them itself here.
  const model = (context?: RequestContext, outgoingMeta?: Meta) =>
    call(right.wire(), ['metadata'], null, {
      context,
      signal: context?.signal,
      meta: outgoingMeta,
    });
  const arrived = deferred(),
    cancelled = deferred(),
    event = deferred<unknown>();
  const detach = register(rightDispatcher, ['both'], {
    request: async (params, context) => {
      assert.equal(Reflect.get(context, 'verified'), 'trusted');
      if (params === 'hold') {
        context.signal.addEventListener('abort', () => cancelled.resolve(), { once: true });
        arrived.resolve();
        await cancelled.promise;
        return null;
      }
      return {
        incoming: context.meta,
        implicit: await model(context),
        explicit: await model(context, { selected: 'yes' }),
      };
    },
    event: async (data) => {
      event.resolve(data);
    },
  });
  assert.deepEqual(await call(left.wire(), ['both'], 'read', { meta: { incoming: 'only' } }), {
    incoming: { incoming: 'only' },
    implicit: null,
    explicit: { selected: 'yes' },
  });
  emit(left.wire(), ['both'], 'notice');
  assert.equal(await event.promise, 'notice');
  const controller = new AbortController();
  const pending = call(left.wire(), ['both'], 'hold', { signal: controller.signal }).catch(
    (error: unknown) => error,
  );
  await arrived.promise;
  controller.abort();
  await cancelled.promise;
  assert.equal(((await pending) as PublicError).code, 'cancelled');
  detach();
  await assert.rejects(call(left.wire(), ['both']), { code: 'method_not_found' });
});

test('register sanitizes synchronous and asynchronous event failures into its registry close', async () => {
  for (const failure of ['sync', 'async'] as const) {
    let receiver!: Receiver;
    const closed: unknown[] = [];
    const wire: Endpoint = {
      send: () => {},
      receive: (next) => {
        receiver = next;
        return () => {};
      },
      close: () => assert.fail('registry owns no borrowed endpoint'),
    };
    const dispatcher = createDispatcher(wire);
    dispatcher.registerPrefix([], { closed: (code, reason) => closed.push([code, reason]) });
    register(dispatcher, ['event'], {
      event: () => {
        if (failure === 'sync') throw new Error('private panic');
        return Promise.reject(new PublicError('private', 'private refusal'));
      },
    });
    await receiver.message!(['event'], { frame: { version: 1, kind: 'event', data: null } });
    assert.deepEqual(closed, [[1002, 'wire event rejected']]);
  }
});
