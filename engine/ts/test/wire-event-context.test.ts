// Ported from Nightseam v0.6.0 runtime/ts/src/wire-event-context.test.ts.
// The one changed assertion: a listener's context no longer reaches the peer
// that delivered its event (R19/R23 remove that escape hatch), where v0.6.0
// held that it did.
import assert from 'node:assert/strict';
import test from 'node:test';
import type { AddressedWire, Endpoint, Message } from '@bitspark/bitwire';
import { defaultPropagator, forward, mount, pair } from '../../../core/ts/src/index.ts';
import { pipe } from '../../../transports/ts/src/index.ts';
import {
  call,
  createDispatcher,
  emit,
  handle,
  register,
  type EventContext,
} from '../../../dispatch/ts/src/index.ts';
import { Peer, type PeerOptions } from '../src/index.ts';

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((yes) => {
    resolve = yes;
  });
  return { promise, resolve };
}
async function physical(serverOptions: PeerOptions = {}) {
  const [a, b] = pipe();
  const client = new Peer(),
    server = new Peer({ ...serverOptions, role: 'server' });
  await Promise.all([client.attach(a), server.attach(b)]);
  return {
    client,
    server,
    close: () => {
      client.close();
      server.close();
    },
  };
}

test('physical events keep verified context through forwarding local pair and mount without ambient outgoing metadata', async (t) => {
  const verified = Object.freeze({ source: 'trusted context' });
  const marker = Symbol('verified');
  const peers = await physical({
    propagator: {
      extract: (context, trace) => {
        defaultPropagator.extract(context, trace);
        Object.defineProperty(context, marker, { value: verified });
      },
      inject: defaultPropagator.inject,
    },
  });
  t.after(peers.close);
  const [access, binding] = pair({ maxPendingRequests: 1 });
  t.after(() => access.close());
  const accessMount = mount(new Map([['local', access]]));
  const accessDispatcher = createDispatcher(accessMount);
  const modelMount = mount(new Map([['model', binding]]));
  const model = createDispatcher(modelMount);
  const clientDispatcher = createDispatcher(peers.client.wire());
  t.after(() => {
    accessDispatcher.close();
    model.close();
    clientDispatcher.close();
    accessMount.close();
    modelMount.close();
  });
  t.after(forward(peers.server.wire(), accessDispatcher.select(['local'])));
  const observed = deferred<EventContext>();
  let effects = 0;
  register(model, ['model', 'events', 'change'], {
    event: (_data, context) => {
      observed.resolve(context);
      if ((context as unknown as Record<symbol, unknown>)[marker] !== verified) throw new Error('Unverified event');
      effects++;
    },
  });
  const meta = { tenant: 'explicit', verified: 'cannot manufacture context' };
  emit(peers.client.wire(), ['events', 'change'], null, { meta });
  const context = await observed.promise;
  assert.equal((context as unknown as Record<symbol, unknown>)[marker], verified);
  assert.deepEqual(context.meta, meta);
  // R19/R23: the peer is not reachable from a listener's context.
  assert.equal('peer' in context, false);
  handle(model, ['model', 'barrier'], () => effects);
  assert.equal(await call(access, ['barrier']), 1);
  const received: EventContext[] = [];
  const arrived = deferred<void>();
  register(clientDispatcher, ['outgoing'], {
    event: (_value, context) => {
      received.push(context);
      if (received.length === 2) arrived.resolve();
    },
  });
  emit(peers.server.wire(), ['outgoing'], null, { context });
  emit(peers.server.wire(), ['outgoing'], null, { context, meta: context.meta });
  await arrived.promise;
  assert.equal((received[0] as unknown as Record<symbol, unknown>)[marker], undefined);
  assert.equal(received[0]!.meta, undefined);
  assert.deepEqual(received[1]!.meta, meta);
});

test('event metadata cannot create the private context required by an effect guard', async (t) => {
  const marker = Symbol('verified');
  const peers = await physical();
  t.after(peers.close);
  const [access, binding] = pair();
  t.after(() => access.close());
  t.after(forward(peers.server.wire(), access));
  const denied = deferred<boolean>();
  const dispatcher = createDispatcher(binding);
  t.after(() => dispatcher.close());
  register(dispatcher, ['guard'], {
    event: (_value, context) => {
      if (!(context as unknown as Record<symbol, unknown>)[marker]) {
        denied.resolve(true);
        throw new Error('Denied');
      }
      denied.resolve(false);
    },
  });
  emit(peers.client.wire(), ['guard'], null, { meta: { verified: 'yes' } });
  assert.equal(await denied.promise, true);
});

test('a forwarded event context ends at the next physical transport boundary', async (t) => {
  const marker = Symbol('verified');
  const source = await physical({
    propagator: {
      extract: (context, trace) => {
        defaultPropagator.extract(context, trace);
        Object.defineProperty(context, marker, { value: true });
      },
      inject: defaultPropagator.inject,
    },
  });
  const destination = await physical();
  t.after(source.close);
  t.after(destination.close);
  t.after(forward(source.server.wire(), destination.client.wire()));
  const observed = deferred<EventContext>();
  const dispatcher = createDispatcher(destination.server.wire());
  t.after(() => dispatcher.close());
  register(dispatcher, ['event'], { event: (_value, context) => observed.resolve(context) });
  emit(source.client.wire(), ['event'], null, { meta: { explicit: 'yes' } });
  const context = await observed.promise;
  assert.equal((context as unknown as Record<symbol, unknown>)[marker], undefined);
  assert.deepEqual(context.meta, { explicit: 'yes' });
});

test('pure local events use the configured propagator', async (t) => {
  const marker = Symbol('local');
  const [a, b] = pair({
    propagator: {
      extract: (context, trace) => {
        defaultPropagator.extract(context, trace);
        Object.defineProperty(context, marker, { value: true });
      },
      inject: defaultPropagator.inject,
    },
  });
  t.after(() => a.close());
  const observed = deferred<EventContext>();
  const dispatcher = createDispatcher(b);
  t.after(() => dispatcher.close());
  register(dispatcher, ['event'], { event: (_value, context) => observed.resolve(context) });
  emit(a, ['event']);
  assert.equal(((await observed.promise) as unknown as Record<symbol, unknown>)[marker], true);
});

test('a custom event propagator cannot rewrite the received frame when it is forwarded', async (t) => {
  const changed = { traceparent: '00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01' };
  const peers = await physical({
    propagator: {
      extract: (context) => {
        context.trace = changed;
      },
      inject: defaultPropagator.inject,
    },
  });
  t.after(peers.close);
  const [access, binding] = pair();
  t.after(() => access.close());
  const sent = deferred<Message>(),
    received = deferred<Message>();
  const source: AddressedWire = {
    send: (path, message) => {
      sent.resolve(message);
      peers.client.wire().send(path, message);
    },
  };
  const destination: Endpoint = {
    ...access,
    send: (path, message) => {
      received.resolve(message);
      access.send(path, message);
    },
  };
  t.after(forward(peers.server.wire(), destination));
  const delivered = deferred<EventContext>();
  const dispatcher = createDispatcher(binding);
  t.after(() => dispatcher.close());
  register(dispatcher, ['event'], { event: (_value, context) => delivered.resolve(context) });
  emit(source, ['event']);
  assert.equal((await received.promise).frame.traceparent, (await sent.promise).frame.traceparent);
  assert.equal((await delivered.promise).trace, changed);
});
