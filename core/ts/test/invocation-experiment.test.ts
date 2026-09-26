// Ported from Nightseam v0.6.0 runtime/ts/src/invocation-experiment.test.ts.
// The second integration uses the public vocabulary and nothing else.
import assert from 'node:assert/strict';
import test from 'node:test';
import type { Endpoint, Message, Path, Receiver, ReturnAddress, AddressedWire } from '@bitspark/bitwire';
import {
  forward,
  invocationBegin,
  invocationCapture,
  invocationControl,
  invocationDone,
  invocationReady,
  invocationRelease,
} from '../src/index.ts';
import { createDispatcher, handle } from '../../../dispatch/ts/src/index.ts';

/**
 * The second independent integration. It shares no ledger with the first and
 * composes none of bitruntime's lifecycle: it answers the invocation vocabulary
 * itself, out of its own state, using the operation paths and nothing else. If
 * the dispatcher works against this, the boundary is public in fact and not
 * only in name.
 */
class HandwrittenCall {
  readonly address: ReturnAddress;
  readonly outcomes: Message[] = [];
  readonly #owner: HandwrittenEndpoint;
  #sinks = new Map<string, AddressedWire>();
  #delivered = new Set<string>();
  #told = new Set<string>();
  #bodies = new Set<string>();
  #takenCaptures = 0;
  #takenBodies = 0;
  #pending = 1;
  #control: Message | undefined;
  #settled = false;
  retired = false;

  constructor(owner: HandwrittenEndpoint) {
    this.#owner = owner;
    this.address = { wire: { send: (path, message) => this.#send(path, message) } };
  }

  #send(path: Path, message: Message): void {
    if (path.length === 0) {
      if (message.frame.kind !== 'response') throw new Error('invalid outcome');
      this.outcomes.push(message);
      this.#settled = true;
      this.#retire();
      return;
    }
    if (path[0] === invocationControl) {
      if (path.length !== 1 || message.frame.kind !== 'cancel') throw new Error('unknown invocation operation');
      this.#latch(message);
      return;
    }
    if (path.length !== 2) throw new Error('unknown invocation operation');
    const id = path[1]!;
    switch (path[0]) {
      case invocationCapture: {
        const sink = message.return?.wire;
        if (!sink) throw new Error('unknown invocation operation');
        if (this.retired) throw new Error('retired');
        if (this.#takenCaptures >= this.#owner.captureBound) {
          this.#owner.refusals++;
          throw new Error('capture bound reached');
        }
        this.#takenCaptures++;
        this.#pending++;
        this.#sinks.set(id, sink);
        return;
      }
      case invocationReady: {
        const sink = this.#sinks.get(id);
        if (!sink || this.#delivered.has(id)) return;
        this.#delivered.add(id);
        this.#pending--;
        if (this.#control && !this.#told.has(id)) {
          this.#told.add(id);
          sink.send([], this.#control);
        }
        this.#retire();
        return;
      }
      case invocationRelease: {
        if (!this.#sinks.has(id)) return;
        if (!this.#delivered.has(id)) this.#pending--;
        this.#sinks.delete(id);
        this.#delivered.delete(id);
        this.#retire();
        return;
      }
      case invocationBegin: {
        if (this.retired) throw new Error('retired');
        if (this.#takenBodies >= this.#owner.bodyBound) {
          this.#owner.refusals++;
          throw new Error('body bound reached');
        }
        this.#takenBodies++;
        this.#bodies.add(id);
        return;
      }
      case invocationDone: {
        if (!this.#bodies.delete(id)) return;
        this.#retire();
        return;
      }
      default:
        throw new Error('unknown invocation operation');
    }
  }

  #latch(message: Message): void {
    if (this.retired || this.#control) return;
    this.#control = message;
    for (const [id, sink] of this.#sinks) {
      if (!this.#delivered.has(id) || this.#told.has(id)) continue;
      this.#told.add(id);
      sink.send([], message);
    }
  }

  dispatchDone(): void {
    this.#pending--;
    this.#retire();
  }

  cancel(id: string): void {
    this.address.wire.send([invocationControl], { frame: { version: 1, kind: 'cancel', id }, return: this.address });
  }

  #retire(): void {
    if (this.retired || !this.#settled || this.#pending !== 0 || this.#bodies.size !== 0) return;
    this.retired = true;
    this.#sinks = new Map();
    this.#delivered = new Set();
    this.#told = new Set();
    this.#control = undefined;
    this.#owner.retirements++;
  }
}

class HandwrittenEndpoint implements Endpoint {
  #receiver: Receiver | undefined;
  #next = 0;
  retirements = 0;
  refusals = 0;
  readonly captureBound: number;
  readonly bodyBound: number;
  constructor(captureBound = 8, bodyBound = 8) {
    this.captureBound = captureBound;
    this.bodyBound = bodyBound;
  }
  send(): void {}
  receive(receiver: Receiver): () => void {
    if (this.#receiver) throw new Error('receiver_exists');
    this.#receiver = receiver;
    return () => {
      if (this.#receiver === receiver) this.#receiver = undefined;
    };
  }
  close(code = 1000, reason = ''): void {
    const receiver = this.#receiver;
    this.#receiver = undefined;
    receiver?.closed?.(code, reason);
  }
  admit(path: Path, params: unknown = null): { id: string; call: HandwrittenCall } {
    const id = `h:${++this.#next}`;
    const call = new HandwrittenCall(this);
    void this.#receiver?.message?.(path, {
      frame: { version: 1, kind: 'request', id, params },
      return: call.address,
    });
    call.dispatchDone();
    return { id, call };
  }
}

/** Passes the complete message and return capability through, and nothing else. */
class Opaque implements Endpoint {
  private readonly inner: Endpoint;
  constructor(inner: Endpoint) {
    this.inner = inner;
  }
  send(path: Path, message: Message): void {
    this.inner.send(path, message);
  }
  receive(receiver: Receiver): () => void {
    return this.inner.receive(receiver);
  }
  close(code?: number, reason?: string): void {
    this.inner.close(code, reason);
  }
}

/**
 * Half of a pure route: what is sent on one half is delivered to the other
 * half's attachment, verbatim, with the return capability untouched. It
 * correlates nothing and admits nothing, so a composition over it is pure
 * forwarding rather than a carrier hop.
 */
class Conduit implements Endpoint {
  other: Conduit | undefined;
  private attached: Receiver | undefined;
  send(path: Path, message: Message): void {
    void this.other?.attached?.message?.(path, message);
  }
  receive(receiver: Receiver): () => void {
    if (this.attached) throw new Error('receiver_exists');
    this.attached = receiver;
    return () => {
      if (this.attached === receiver) this.attached = undefined;
    };
  }
  close(): void {}
}
const conduit = (): [Conduit, Conduit] => {
  const near = new Conduit();
  const far = new Conduit();
  near.other = far;
  far.other = near;
  return [near, far];
};

const settled = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 0));

test('a second integration participates with no shared ledger', async () => {
  const endpoint = new HandwrittenEndpoint();
  const dispatch = createDispatcher(new Opaque(endpoint));
  const view = createDispatcher(dispatch.select(['space']));
  let release: () => void = () => {};
  const held = new Promise<void>((resolve) => {
    release = resolve;
  });
  let cancelled: boolean | undefined;
  handle(view, ['read'], async (_params, context) => {
    await held;
    cancelled = context.signal.aborted;
    return 'answer';
  });
  const { id, call } = endpoint.admit(['space', 'read']);
  await settled();
  assert.equal(call.retired, false, 'retired while the body was running');
  call.cancel(id);
  release();
  await settled();
  assert.equal(cancelled, true, 'cancellation did not reach the captured traversal of the second integration');
  assert.equal(call.outcomes.length, 1);
  assert.equal(call.retired, true);
  assert.equal(endpoint.retirements, 1);
});

test('forwarding preserves lifecycle participation and the original return capability', async () => {
  const origin = new HandwrittenEndpoint();
  const [near, far] = conduit();
  forward(new Opaque(origin), new Opaque(near));
  const dispatch = createDispatcher(far);
  const controls: Message[] = [];
  const requests: Message[] = [];
  const detach = dispatch.register(['read'], {
    message(_path, message) {
      if (message.frame.kind === 'cancel') controls.push(message);
      else requests.push(message);
    },
  });
  const { id, call } = origin.admit(['read']);
  await settled();
  assert.equal(requests.length, 1, 'the forwarded request never arrived');
  assert.equal(requests[0]?.return, call.address, 'forwarding did not preserve the original return capability');
  detach();
  const rebound: Message[] = [];
  dispatch.register(['read'], {
    message(_path, message) {
      rebound.push(message);
    },
  });
  call.cancel(id);
  await settled();
  assert.equal(controls.length, 1, 'the control did not follow the captured traversal across the forwarder');
  assert.equal(rebound.length, 0, 'the control reached the rebound registration');
});

test('a queued control cannot reach a reused identity', async () => {
  const endpoint = new HandwrittenEndpoint();
  const dispatch = createDispatcher(endpoint);
  const seen: string[] = [];
  dispatch.register(['read'], {
    message(_path, message) {
      seen.push(`${message.frame.kind}:${'id' in message.frame ? message.frame.id : ''}`);
    },
  });
  const first = endpoint.admit(['read']);
  await settled();
  assert.deepEqual(seen, ['request:h:1']);
  // The first invocation settles and retires before its old control is
  // released. Its control ticket is the invocation itself, not a key.
  first.call.address.wire.send([], { frame: { version: 1, kind: 'response', id: 'h:1', result: null } });
  const second = endpoint.admit(['read']);
  await settled();
  assert.deepEqual(seen, ['request:h:1', 'request:h:2']);
  // The stale control names the first invocation's identifier and travels on
  // the first invocation's own return capability. It reaches nothing.
  first.call.cancel('h:1');
  await settled();
  assert.deepEqual(seen, ['request:h:1', 'request:h:2'], 'a stale control was delivered');
  second.call.cancel('h:2');
  await settled();
  assert.deepEqual(seen, ['request:h:1', 'request:h:2', 'cancel:h:2']);
});

test('a bound past which a traversal cannot capture refuses instead of routing', async () => {
  const endpoint = new HandwrittenEndpoint(1, 8);
  const outer = createDispatcher(endpoint);
  const inner = createDispatcher(outer.select(['a']));
  let routed = false;
  inner.register(['read'], {
    message() {
      routed = true;
    },
  });
  const { call } = endpoint.admit(['a', 'read']);
  await settled();
  assert.equal(routed, false, 'the inner receiver was reached past the bound');
  assert.equal(call.outcomes.length, 1);
  const outcome = call.outcomes[0]!.frame;
  assert.equal(outcome.kind, 'response');
  // The refusal is this integration's own: a dispatcher reports `busy` for a
  // bound it can recognize as one, and `invalid_message` for a refusal whose
  // reason a facility did not spell in the agreed vocabulary.
  if (outcome.kind === 'response') assert.equal(outcome.error?.code, 'invalid_message');
  assert.ok(endpoint.refusals > 0, 'the bound was never exercised');
});
