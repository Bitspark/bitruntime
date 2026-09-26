// Ported from nightseam v0.6.0 runtime/ts/src/trace.test.ts. Handlers and
// calls go through the peer's root: a request's method and an event's name
// are the canonical encodings of their paths ('5:outer' for ['outer']), and a
// call made from a handler names its context, and its propagator, explicitly.
import assert from 'node:assert/strict';
import test from 'node:test';
import type { Propagator, Trace, TraceContext } from '../../../core/ts/src/index.ts';
import type { WebSocketLike } from '../../../transports/ts/src/index.ts';
import { call, createDispatcher, emit, handle, type RequestContext } from '../../../dispatch/ts/src/index.ts';
import { Peer } from '../src/index.ts';

/** The socket of peer.test.ts, with only what a trace assertion needs of it. */
class Socket extends EventTarget implements WebSocketLike {
  readyState = 1;
  bufferedAmount = 0;
  sent: Record<string, unknown>[] = [];
  send(text: string): void {
    this.sent.push(JSON.parse(text));
  }
  receive(frame: unknown): void {
    this.dispatchEvent(new MessageEvent('message', { data: JSON.stringify(frame) }));
  }
  close(): void {
    this.readyState = 3;
    this.dispatchEvent(new Event('close'));
  }
}

/** One W3C traceparent, the example of the specification, and a vendor's state beside it. */
const TRACEPARENT = '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01';
const TRACESTATE = 'vendor=t61rcWkgMzE';
const TRACE_ID = '4bf92f3577b34da6a3ce929d0e0e4736';
const SPAN_ID = '00f067aa0ba902b7';
const FORM = /^[0-9a-f]{2}-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$/;

/** A peer with no propagator configured: every case below holds through the default. */
async function peered() {
  const socket = new Socket();
  const peer = new Peer();
  await peer.attach(socket);
  return { socket, peer };
}
function parts(frame: Record<string, unknown> | undefined): [string, string, string, string] {
  const traceparent = frame?.traceparent;
  assert.ok(typeof traceparent === 'string', 'frame carries a traceparent');
  assert.match(traceparent, FORM);
  return traceparent.split('-') as [string, string, string, string];
}
function tick(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

test('a request and an event made inside a handler are children of the handler, and the response repeats it', async (t) => {
  const { socket, peer } = await peered();
  t.after(() => peer.close());
  let observed: RequestContext | undefined;
  handle(createDispatcher(peer.wire()), ['outer'], (_params, context) => {
    observed = context;
    void call(context.wire, ['inner'], {}, { context }).catch(() => {});
    emit(context.wire, ['progress'], 1, { context });
    return 'done';
  });
  socket.receive({
    version: 1,
    kind: 'request',
    id: 's:1',
    method: '5:outer',
    params: {},
    traceparent: TRACEPARENT,
    tracestate: TRACESTATE,
  });
  await tick();
  // The incoming trace is on the context the handler ran with, member for member.
  assert.deepEqual(observed?.trace, { traceparent: TRACEPARENT, tracestate: TRACESTATE });
  // A child keeps the version, trace id and flags of its parent and mints a span of its own.
  for (const frame of [
    socket.sent.find((f) => f.method === '5:inner'),
    socket.sent.find((f) => f.event === '8:progress'),
  ]) {
    const [version, traceID, spanID, flags] = parts(frame);
    assert.deepEqual([version, traceID, flags], ['00', TRACE_ID, '01']);
    assert.notEqual(spanID, SPAN_ID);
    assert.equal(frame?.tracestate, TRACESTATE);
  }
  assert.notEqual(
    socket.sent.find((f) => f.method === '5:inner')?.traceparent,
    socket.sent.find((f) => f.event === '8:progress')?.traceparent,
  );
  // The response carries the request's members verbatim: it is no span of its own.
  const response = socket.sent.find((frame) => frame.id === 's:1');
  assert.deepEqual(response, {
    version: 1,
    kind: 'response',
    id: 's:1',
    result: 'done',
    traceparent: TRACEPARENT,
    tracestate: TRACESTATE,
  });
});

test('a cancel carries the trace of the request it cancels', async (t) => {
  const { socket, peer } = await peered();
  t.after(() => peer.close());
  const controller = new AbortController();
  const pending = assert.rejects(call(peer.wire(), ['wait'], {}, { signal: controller.signal }), { code: 'cancelled' });
  controller.abort();
  await pending;
  await tick();
  const request = socket.sent.find((frame) => frame.kind === 'request');
  const cancel = socket.sent.find((frame) => frame.kind === 'cancel');
  assert.equal(cancel?.id, request?.id);
  assert.equal(cancel?.traceparent, request?.traceparent);
  assert.equal(Object.hasOwn(cancel ?? {}, 'tracestate'), false);
});

test('a call from a bare context carries a new trace, sampled, one per call', async (t) => {
  const { socket, peer } = await peered();
  t.after(() => peer.close());
  void call(peer.wire(), ['first']).catch(() => {});
  void call(peer.wire(), ['second']).catch(() => {});
  emit(peer.wire(), ['notice']);
  await tick();
  const [first, second, event] = socket.sent.map(parts);
  for (const [version, , , flags] of [first, second, event]) assert.deepEqual([version, flags], ['00', '01']);
  assert.notEqual(first[1], second[1]);
  assert.notEqual(second[1], event[1]);
  assert.equal(
    socket.sent.some((frame) => Object.hasOwn(frame, 'tracestate')),
    false,
  );
});

test('a custom propagator sees extract and inject, and what it mints travels verbatim', async (t) => {
  const extracted: (Trace | undefined)[] = [];
  const injected: (TraceContext | undefined)[] = [];
  const minted: Trace = { traceparent: `00-${'a'.repeat(32)}-${'b'.repeat(16)}-00`, tracestate: 'custom=1' };
  const propagator: Propagator = {
    extract(context, trace) {
      extracted.push(trace);
      context.trace = trace;
    },
    inject(context) {
      injected.push(context);
      return minted;
    },
  };
  const socket = new Socket();
  const peer = new Peer({ propagator });
  await peer.attach(socket);
  t.after(() => peer.close());
  const handled: RequestContext[] = [];
  handle(createDispatcher(peer.wire()), ['outer'], (_params, context) => {
    handled.push(context);
    void call(context.wire, ['inner'], {}, { context, propagator }).catch(() => {});
    return null;
  });
  socket.receive({
    version: 1,
    kind: 'request',
    id: 's:1',
    method: '5:outer',
    params: {},
    traceparent: TRACEPARENT,
    tracestate: TRACESTATE,
  });
  await tick();
  // An intermediary may strip one member and not the other: both reach the
  // propagator as they arrived, and a tracestate alone continues no trace.
  socket.receive({ version: 1, kind: 'request', id: 's:2', method: '5:outer', params: {}, tracestate: TRACESTATE });
  await tick();
  assert.deepEqual(extracted, [
    { traceparent: TRACEPARENT, tracestate: TRACESTATE },
    { traceparent: '', tracestate: TRACESTATE },
  ]);
  assert.equal(injected[0], handled[0]);
  assert.deepEqual(
    socket.sent.find((frame) => frame.method === '5:inner'),
    {
      version: 1,
      kind: 'request',
      id: 'c:1',
      method: '5:inner',
      params: {},
      ...minted,
    },
  );
  // A response carries the request's members, whatever the propagator would mint.
  assert.deepEqual(
    socket.sent.find((frame) => frame.id === 's:1'),
    {
      version: 1,
      kind: 'response',
      id: 's:1',
      result: null,
      traceparent: TRACEPARENT,
      tracestate: TRACESTATE,
    },
  );
  assert.deepEqual(
    socket.sent.find((frame) => frame.id === 's:2'),
    { version: 1, kind: 'response', id: 's:2', result: null, tracestate: TRACESTATE },
  );
});
