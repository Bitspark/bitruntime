// Ported from nightseam v0.6.0 runtime/ts/src/dispatcher-ownership.test.ts.
// A path a mount has no child for is refused with MissingPathError
// ('missing_path', was the duplex wire error 'no_route').
import assert from 'node:assert/strict';
import test from 'node:test';
import type { Endpoint, Message, Path, Receiver } from '@bitspark/bitwire';
import { mount, pair } from '../../../core/ts/src/index.ts';
import { createDispatcher, emit } from '../src/index.ts';

const settled = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 0));
const anEvent = (): Message => ({ frame: { version: 1, kind: 'event', data: null } });

/** An endpoint that hands a test its one attachment, and nothing else. */
class Holder implements Endpoint {
  private attached: Receiver | undefined;
  send(): void {}
  receive(receiver: Receiver): () => void {
    if (this.attached) throw new Error('receiver_exists');
    this.attached = receiver;
    return () => {
      if (this.attached === receiver) this.attached = undefined;
    };
  }
  close(): void {
    this.attached = undefined;
  }
  deliver(path: Path, message: Message): void {
    void this.attached?.message?.(path, message);
  }
}

test('an endpoint has one owning attachment', async () => {
  const [left, right] = pair();
  try {
    const first: Path[] = [];
    const second: Path[] = [];
    const detach = right.receive({
      message(path) {
        first.push(path);
      },
    });
    assert.throws(() => right.receive({}), { code: 'receiver_exists' });
    emit(left, ['one']);
    await settled();
    assert.deepEqual(first, [['one']]);
    detach();
    detach();
    right.receive({
      message(path) {
        second.push(path);
      },
    });
    emit(left, ['two']);
    await settled();
    assert.deepEqual(second, [['two']]);
    assert.deepEqual(first, [['one']], 'the detached attachment still received');
  } finally {
    left.close();
  }
});

test('a dispatcher refuses a duplicate path and frees it on detach', () => {
  const dispatch = createDispatcher(new Holder());
  const detach = dispatch.register(['a', 'b'], { message() {} });
  assert.throws(() => dispatch.register(['a', 'b'], {}), { code: 'receiver_exists' });
  // Exact and prefix are separate spaces: one of each may hold a path.
  dispatch.registerPrefix(['a', 'b'], {});
  assert.throws(() => dispatch.registerPrefix(['a', 'b'], {}), { code: 'receiver_exists' });
  detach();
  dispatch.register(['a', 'b'], {});
});

test('overlapping routes select the longest, then the exact', () => {
  const endpoint = new Holder();
  const dispatch = createDispatcher(endpoint);
  const reached: string[] = [];
  const name = (label: string): Receiver => ({
    message() {
      reached.push(label);
    },
  });
  dispatch.registerPrefix([], name('root'));
  dispatch.registerPrefix(['a'], name('a'));
  dispatch.registerPrefix(['a', 'b'], name('a/b'));
  dispatch.register(['a', 'b', 'c'], name('exact a/b/c'));
  for (const [path, label] of [
    [['z'], 'root'],
    [['a', 'z'], 'a'],
    [['a', 'b', 'z'], 'a/b'],
    [['a', 'b', 'c'], 'exact a/b/c'],
  ] as [Path, string][]) {
    reached.length = 0;
    endpoint.deliver(path, anEvent());
    assert.deepEqual(reached, [label], `${JSON.stringify(path)}`);
  }
});

test('a nested selection prepends outgoing and strips incoming', async () => {
  const [left, right] = pair();
  try {
    const dispatch = createDispatcher(right);
    const inner = dispatch.select(['a']).select(['b']);
    const delivered: Path[] = [];
    inner.receive({
      message(path) {
        delivered.push(path);
      },
    });
    emit(left, ['a', 'b', 'read']);
    await settled();
    assert.deepEqual(delivered, [['read']]);
    const back: Path[] = [];
    left.receive({
      message(path) {
        back.push(path);
      },
    });
    emit(inner, ['reply']);
    await settled();
    assert.deepEqual(back, [['a', 'b', 'reply']]);
  } finally {
    left.close();
  }
});

test('a mount routes by one segment and borrows its children', async () => {
  const [left, right] = pair();
  try {
    const root = mount(new Map([['child', right]]));
    const delivered: Path[] = [];
    root.receive({
      message(path) {
        delivered.push(path);
      },
    });
    emit(left, ['read']);
    await settled();
    assert.deepEqual(delivered, [['child', 'read']]);
    assert.throws(() => root.send([], anEvent()), { code: 'missing_path' });
    assert.throws(() => root.send(['absent'], anEvent()), { code: 'missing_path' });
    root.close();
    const after: Path[] = [];
    right.receive({
      message(path) {
        after.push(path);
      },
    });
    emit(left, ['again']);
    await settled();
    assert.deepEqual(after, [['again']], 'closing the mount closed its borrowed child');
  } finally {
    left.close();
  }
});
