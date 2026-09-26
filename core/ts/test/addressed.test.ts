// Ported from Nightseam v0.6.0 duplex/ts/src/wire.test.ts. A closed mount now
// reports the one closed classification, `disconnected` (was the duplex
// wire error 'closed', R26), and a path that selects no child is refused with
// MissingPathError ('missing_path', was 'no_route').
import assert from 'node:assert/strict';
import test from 'node:test';
import type { AddressedWire, Endpoint, Message, Receiver, Path, ProfileFrame } from '@bitspark/bitwire';
import { at, mount, PublicError, ReceiverExistsError } from '../src/index.ts';
import { encodePath, decodePath } from '../src/internal/path.ts';

test('path encoding is canonical, injective and composes by concatenation', () => {
  const paths: Path[] = [[], [''], ['a', 'b'], ['a.b'], ['a', 'b:c'], ['é', 'e\u0301', '😀', '\ufeff', '\0']];
  const seen = new Set<string>();
  for (const path of paths) {
    const encoded = encodePath(path);
    assert.equal(seen.has(encoded), false);
    seen.add(encoded);
    assert.deepEqual(decodePath(encoded), path);
    for (const suffix of paths) assert.equal(encodePath([...path, ...suffix]), encoded + encodePath(suffix));
  }
  assert.equal(encodePath(['a', '😀', '']), '1:a4:😀0:');
  for (const malformed of [
    '01:a',
    '00:',
    '1',
    ':',
    '-1:a',
    '2:a',
    '1:é',
    '99999999999999999999999999999:x',
    '1:\ud800',
  ])
    assert.throws(() => decodePath(malformed));
  assert.throws(() => encodePath(['\ud800']));
  assert.throws(() => encodePath(['\udc00']));
});

// A controlled single-attachment fixture: no scheduler or physical-carrier claim.
class QueuedEndpoint implements Endpoint {
  readonly queue: { path: Path; message: Message }[] = [];
  receiver: Receiver | undefined;
  attachments = 0;
  detaches = 0;
  closes = 0;
  send(path: Path, message: Message): void {
    if (this.closes) throw new PublicError('disconnected', 'Closed.');
    encodePath(path);
    this.queue.push({ path: [...path], message });
  }
  receive(receiver: Receiver): () => void {
    if (this.closes) throw new PublicError('disconnected', 'Closed.');
    if (this.receiver) throw new ReceiverExistsError();
    this.attachments++;
    this.receiver = receiver;
    let active = true;
    return () => {
      if (!active) return;
      active = false;
      this.detaches++;
      if (this.receiver === receiver) this.receiver = undefined;
    };
  }
  close(code = 1000, reason = ''): void {
    if (this.closes) return;
    this.closes++;
    const receiver = this.receiver;
    this.receiver = undefined;
    receiver?.closed?.(code, reason);
  }
  async drain(): Promise<void> {
    for (let entry = this.queue.shift(); entry; entry = this.queue.shift()) {
      await this.receiver?.message?.(entry.path, entry.message);
    }
  }
}

const event: Message = { frame: { version: 1, kind: 'event', data: null } };

test('selection is send-only, copies prefixes and delegates nested and empty paths without dispatching', () => {
  const root = new QueuedEndpoint();
  let delivered = 0;
  root.receive({
    message: () => {
      delivered++;
    },
  });
  const prefix = ['a.b'];
  const selected = at(root, prefix);
  prefix[0] = 'changed';
  const nested = at(selected, ['😀']);
  const suffix = ['call'];
  nested.send(suffix, event);
  suffix[0] = 'changed';
  at(root, []).send([], event);
  at(at(root, ['a.b']), ['😀']).send(['call'], event);
  assert.equal(delivered, 0);
  assert.deepEqual(
    root.queue.map((entry) => entry.path),
    [['a.b', '😀', 'call'], [], ['a.b', '😀', 'call']],
  );
  for (const entry of root.queue) assert.equal(entry.message, event);
  for (const view of [selected, nested, at(root, [])]) {
    assert.deepEqual(Object.keys(view), ['send']);
    assert.equal('receive' in view, false);
    assert.equal('close' in view, false);
  }
  // A send-only implementation, not just an Endpoint narrowed statically.
  const access: AddressedWire = { send: (path, message) => root.send(path, message) };
  at(access, ['']).send([], event);
  assert.deepEqual(root.queue.at(-1)?.path, ['']);
});

test('one mount receiver spans children and preserves whole messages, return identity and queued dispatch', async () => {
  const left = new QueuedEndpoint(),
    right = new QueuedEndpoint(),
    replacement = new QueuedEndpoint();
  const children = new Map<string, Endpoint>([
    ['left', left],
    ['', right],
  ]);
  const mounted = mount(children);
  children.set('left', replacement);
  const received: { path: Path; message: Message }[] = [];
  const detach = mounted.receive({
    message: (path, message) => {
      received.push({ path, message });
    },
  });
  assert.equal(left.attachments, 1);
  assert.equal(right.attachments, 1);
  const reply: AddressedWire = { send() {} };
  const address = { wire: reply };
  const frames: ProfileFrame[] = [
    { version: 1, kind: 'request', id: 'c:1', params: { n: 42 }, meta: { tag: 'value' } },
    { version: 1, kind: 'response', id: 'c:1', error: { code: 'refused', message: 'No', data: { why: 'test' } } },
    { version: 1, kind: 'event', data: null },
    { version: 1, kind: 'cancel', id: 'c:1' },
  ];
  const messages = frames.map((frame) => ({ frame, return: address }));
  const contexts = new WeakMap<Message, object>();
  for (const message of messages) {
    contexts.set(message, {});
    at(at(mounted, ['left']), ['😀']).send(['call'], message);
  }
  mounted.send([''], event);
  assert.equal(received.length, 0, 'destination ran inside send');
  assert.equal(left.queue.length, 4);
  assert.equal(right.queue.length, 1);
  assert.equal(replacement.queue.length, 0);
  for (const entry of left.queue) assert.deepEqual(entry.path, ['😀', 'call']);
  assert.deepEqual(right.queue[0]?.path, []);
  await left.drain();
  await right.drain();
  assert.equal(received.length, 5);
  for (let i = 0; i < messages.length; i++) {
    const delivered = received[i]!;
    assert.deepEqual(delivered.path, ['left', '😀', 'call']);
    assert.equal(delivered.message, messages[i]);
    assert.equal(delivered.message.return, address);
    assert.equal(contexts.get(delivered.message), contexts.get(messages[i]!));
  }
  assert.deepEqual(received[4], { path: [''], message: event });
  assert.throws(() => mounted.receive({}), { code: 'receiver_exists' });
  assert.equal(left.attachments, 1);
  assert.equal(right.attachments, 1);
  detach();
  assert.equal(left.detaches, 1);
  assert.equal(right.detaches, 1);
});

test('detach allows rebind and a stale disposer cannot detach its successor or notify it', async () => {
  const root = new QueuedEndpoint();
  const mounted = mount(new Map([['service', root]]));
  const calls: string[] = [];
  let closed = 0;
  const first = mounted.receive({
    message: () => {
      calls.push('first');
    },
    closed: () => {
      closed++;
    },
  });
  const captured = root.receiver!;
  mounted.send(['service', 'first'], event);
  await root.drain();
  first();
  const second = mounted.receive({
    message: () => {
      calls.push('second');
    },
    closed: () => {
      closed++;
    },
  });
  first();
  captured.closed?.(1000, 'stale ending');
  mounted.send(['service', 'second'], event);
  await root.drain();
  assert.deepEqual(calls, ['first', 'second']);
  assert.equal(closed, 0);
  assert.equal(root.detaches, 1);
  second();
  second();
  mounted.close();
  assert.equal(root.detaches, 2);
  assert.equal(root.closes, 0);
  assert.equal(closed, 0);
});

test('partial acquisition rolls back earlier children without releasing another owner', () => {
  const left = new QueuedEndpoint(),
    right = new QueuedEndpoint();
  const other: Receiver = {};
  const releaseOther = right.receive(other);
  const mounted = mount(
    new Map([
      ['left', left],
      ['right', right],
    ]),
  );
  let closed = 0;
  assert.throws(
    () =>
      mounted.receive({
        closed: () => {
          closed++;
        },
      }),
    { code: 'receiver_exists' },
  );
  assert.equal(left.receiver, undefined);
  assert.equal(left.detaches, 1);
  assert.equal(right.receiver, other);
  assert.equal(right.detaches, 0);
  assert.equal(closed, 0);
  releaseOther();
  const detach = mounted.receive({});
  detach();
  assert.equal(left.detaches, 2);
  assert.equal(right.detaches, 2);
  assert.equal(left.closes + right.closes, 0);
});

test('two mount keys cannot acquire the same child twice and roll back the first attachment', () => {
  const child = new QueuedEndpoint();
  const mounted = mount(
    new Map([
      ['one', child],
      ['two', child],
    ]),
  );
  assert.throws(() => mounted.receive({}), { code: 'receiver_exists' });
  assert.equal(child.receiver, undefined);
  assert.equal(child.detaches, 1);
  assert.equal(child.closes, 0);
  child.receive({})();
});

test('mount close releases its attachment once and leaves borrowed children usable', async () => {
  const left = new QueuedEndpoint(),
    right = new QueuedEndpoint();
  const mounted = mount(
    new Map([
      ['left', left],
      ['right', right],
    ]),
  );
  let closed = 0;
  const detach = mounted.receive({
    closed: (code, reason) => {
      closed++;
      assert.equal(code, 1000);
      assert.equal(reason, 'mount ended');
      assert.equal(left.receiver, undefined);
      assert.equal(right.receiver, undefined);
      mounted.close();
    },
  });
  const old = left.receiver!;
  mounted.close(1000, 'mount ended');
  mounted.close(1000, 'again');
  detach();
  old.closed?.(1000, 'late child');
  assert.equal(closed, 1);
  assert.equal(left.closes + right.closes, 0);
  assert.equal(left.detaches + right.detaches, 2);
  assert.throws(() => mounted.receive({}), { code: 'disconnected' });
  assert.throws(() => at(mounted, ['left']).send(['call'], event), { code: 'disconnected' });
  let received = 0;
  for (const child of [left, right]) {
    const release = child.receive({
      message: () => {
        received++;
      },
    });
    child.send(['call'], event);
    await child.drain();
    release();
  }
  assert.equal(received, 2);
});

test('mount close during child receive releases both early and late returned disposers', () => {
  const left = new QueuedEndpoint(),
    right = new QueuedEndpoint();
  let mounted: Endpoint;
  const child: Endpoint = {
    send: (path, message) => right.send(path, message),
    receive: (receiver) => {
      const detach = right.receive(receiver);
      mounted.close(1000, 'done');
      return detach;
    },
    close: (code, reason) => right.close(code, reason),
  };
  mounted = mount(
    new Map([
      ['left', left],
      ['right', child],
    ]),
  );
  let closed = 0;
  assert.throws(
    () =>
      mounted.receive({
        closed: () => {
          closed++;
        },
      }),
    { code: 'disconnected' },
  );
  assert.equal(left.receiver, undefined);
  assert.equal(right.receiver, undefined);
  assert.equal(left.detaches, 1);
  assert.equal(right.detaches, 1);
  assert.equal(left.closes + right.closes, 0);
  assert.equal(closed, 1);
});

test('a child ending during acquisition refuses and rolls back the incomplete attachment', () => {
  const left = new QueuedEndpoint(),
    right = new QueuedEndpoint(),
    later = new QueuedEndpoint();
  const child: Endpoint = {
    send: (path, message) => right.send(path, message),
    receive: (receiver) => {
      const detach = right.receive(receiver);
      right.close(1000, 'ended during attachment');
      return detach;
    },
    close: (code, reason) => right.close(code, reason),
  };
  const mounted = mount(
    new Map([
      ['left', left],
      ['right', child],
      ['later', later],
    ]),
  );
  let closed = 0;
  assert.throws(
    () =>
      mounted.receive({
        closed: () => {
          closed++;
        },
      }),
    { code: 'disconnected' },
  );
  assert.equal(left.receiver, undefined);
  assert.equal(right.receiver, undefined);
  assert.equal(left.detaches, 1);
  assert.equal(right.detaches, 1);
  assert.equal(later.attachments, 0);
  assert.equal(left.closes, 0);
  assert.equal(closed, 0);
  left.receive({})();
});

test('a child ending leaves siblings active and the final child ends only the current attachment', async () => {
  const left = new QueuedEndpoint(),
    right = new QueuedEndpoint();
  const mounted = mount(
    new Map([
      ['left', left],
      ['right', right],
    ]),
  );
  const paths: Path[] = [];
  const endings: { code: number; reason: string }[] = [];
  const detach = mounted.receive({
    message: (path) => {
      paths.push(path);
    },
    closed: (code, reason) => {
      endings.push({ code, reason });
    },
  });
  const firstReceiver = left.receiver!;
  left.close(1000, 'left ended');
  firstReceiver.closed?.(1000, 'duplicate');
  assert.equal(endings.length, 0);
  mounted.send(['right', 'still', 'usable'], event);
  await right.drain();
  assert.deepEqual(paths, [['right', 'still', 'usable']]);
  right.close(1001, 'last ended');
  assert.deepEqual(endings, [{ code: 1001, reason: 'last ended' }]);
  assert.throws(() => mounted.send([], event), { code: 'missing_path' });
  detach();
  mounted.close();
  assert.equal(endings.length, 1);
  assert.equal(left.closes, 1);
  assert.equal(right.closes, 1);
  assert.equal(left.detaches, 1);
  assert.equal(right.detaches, 1);
});

test('captured request and cancellation retain the original receiver and async result after detach and rebind', async () => {
  const root = new QueuedEndpoint(),
    reply = new QueuedEndpoint();
  const mounted = mount(new Map([['service', root]]));
  const address = { wire: reply };
  const request: Message = { frame: { version: 1, kind: 'request', id: 'c:1', params: {} }, return: address };
  const cancel: Message = { frame: { version: 1, kind: 'cancel', id: 'c:1' }, return: address };
  const seen: { path: Path; message: Message }[] = [];
  let complete!: () => void;
  const pending = new Promise<void>((resolve) => {
    complete = resolve;
  });
  const detach = mounted.receive({
    message: (path, message) => {
      seen.push({ path, message });
      return pending;
    },
  });
  const captured = root.receiver!;
  assert.equal(captured.message?.(['wait'], request), pending);
  detach();
  let successor = 0;
  const releaseNext = mounted.receive({
    message: () => {
      successor++;
    },
  });
  assert.equal(captured.message?.(['wait'], cancel), pending);
  complete();
  await pending;
  assert.deepEqual(
    seen.map((entry) => entry.path),
    [
      ['service', 'wait'],
      ['service', 'wait'],
    ],
  );
  assert.equal(seen[0]?.message, request);
  assert.equal(seen[1]?.message, cancel);
  assert.equal(seen[1]?.message.return, address);
  assert.equal(successor, 0);
  const response: Message = { frame: { version: 1, kind: 'response', id: 'c:1', result: 42 } };
  seen[0]!.message.return!.wire.send([], response);
  assert.equal(reply.queue[0]?.message, response);
  releaseNext();
});

test('empty paths and keys remain distinct, and an empty mount can own an attachment', async () => {
  const child = new QueuedEndpoint();
  const mounted = mount(new Map([['', child]]));
  const paths: Path[] = [];
  const detach = mounted.receive({
    message: (path) => {
      paths.push(path);
    },
  });
  assert.throws(() => mounted.send([], event), { code: 'missing_path' });
  assert.throws(() => mounted.send(['missing'], event), { code: 'missing_path' });
  assert.throws(() => mounted.send(['', '\ud800'], event), { code: 'invalid_path' });
  mounted.send([''], event);
  mounted.send(['', ''], event);
  await child.drain();
  assert.deepEqual(paths, [[''], ['', '']]);
  detach();
  const empty = mount(new Map());
  let closed = 0;
  const release = empty.receive({
    closed: () => {
      closed++;
    },
  });
  assert.throws(() => empty.receive({}), { code: 'receiver_exists' });
  assert.throws(() => empty.send([], event), { code: 'missing_path' });
  release();
  empty.receive({
    closed: () => {
      closed++;
    },
  });
  empty.close();
  empty.close();
  assert.equal(closed, 1);
});
