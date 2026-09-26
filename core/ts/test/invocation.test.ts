// Ported from nightseam v0.6.0 runtime/ts/src/invocation.test.ts.
import assert from 'node:assert/strict';
import test from 'node:test';
import type { Endpoint, Message, Path, Receiver, ReturnAddress, AddressedWire } from '@bitspark/bitwire';
import {
  Invocation,
  InvocationError,
  beginInvocationBody,
  captureInvocation,
  defaultInvocationLimits,
  invocationControl,
  type InvocationLimits,
} from '../src/index.ts';
import { createDispatcher, handle } from '../../../dispatch/ts/src/index.ts';

/**
 * An endpoint written against the public contract alone. It admits requests,
 * answers the invocation vocabulary out of its own ledger, and recognizes no
 * concrete type. It is the first of the two independent integrations.
 */
class LedgerEndpoint implements Endpoint {
  #receiver: Receiver | undefined;
  #next = 0;
  readonly #limits: InvocationLimits;
  readonly admitted = new Map<string, Invocation>();
  readonly returns = new Map<string, ReturnAddress>();
  readonly outcomes = new Map<string, Message[]>();
  retirements = 0;

  constructor(limits: InvocationLimits = defaultInvocationLimits()) {
    this.#limits = limits;
  }
  /** Loops back into this endpoint's own attachment, never on this stack. */
  send(path: Path, message: Message): void {
    queueMicrotask(() => this.deliver(path, message));
  }
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
  deliver(path: Path, message: Message): void | Promise<void> {
    return this.#receiver?.message?.(path, message);
  }
  admit(path: Path, params: unknown = null): { id: string; invocation: Invocation; delivery: void | Promise<void> } {
    const id = `x:${++this.#next}`;
    const invocation = new Invocation(this.#limits, () => {
      this.retirements++;
    });
    const outcomes: Message[] = [];
    this.outcomes.set(id, outcomes);
    const address: ReturnAddress = {
      wire: {
        send: (suffix, reply) => {
          if (suffix.length) {
            invocation.deliver(suffix, reply);
            return;
          }
          outcomes.push(reply);
          invocation.settle();
        },
      },
    };
    this.admitted.set(id, invocation);
    this.returns.set(id, address);
    const delivery = this.deliver(path, {
      frame: { version: 1, kind: 'request', id, params },
      return: address,
    });
    const finish = () => invocation.dispatchDone();
    if (delivery) return { id, invocation, delivery: delivery.then(finish) };
    finish();
    return { id, invocation, delivery: undefined };
  }
  cancel(id: string): void {
    const address = this.returns.get(id);
    address?.wire.send([invocationControl], {
      frame: { version: 1, kind: 'cancel', id },
      return: address,
    });
  }
}

/** Passes the complete message and return capability through, and nothing else. */
class OpaqueEndpoint implements Endpoint {
  readonly #inner: Endpoint;
  constructor(inner: Endpoint) {
    this.#inner = inner;
  }
  send(path: Path, message: Message): void {
    this.#inner.send(path, message);
  }
  receive(receiver: Receiver): () => void {
    return this.#inner.receive(receiver);
  }
  close(code?: number, reason?: string): void {
    this.#inner.close(code, reason);
  }
}

const settled = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 0));

test('an independent endpoint participates through the public vocabulary alone', async () => {
  const endpoint = new LedgerEndpoint();
  const dispatch = createDispatcher(new OpaqueEndpoint(endpoint));
  let release: () => void = () => {};
  const held = new Promise<void>((resolve) => {
    release = resolve;
  });
  let cancelled: boolean | undefined;
  handle(dispatch, ['read'], async (_params, context) => {
    await held;
    cancelled = context.signal.aborted;
    return 'answer';
  });
  const { id, invocation } = endpoint.admit(['read']);
  await settled();
  assert.equal(invocation.retired, false, 'retired while the body was running');
  endpoint.cancel(id);
  release();
  await settled();
  assert.equal(cancelled, true, 'cancellation did not reach the captured traversal');
  assert.equal(endpoint.outcomes.get(id)?.length, 1);
  assert.equal(invocation.retired, true);
  assert.equal(endpoint.retirements, 1);
});

test('a captured traversal keeps its receiver across detach and rebind', async () => {
  const endpoint = new LedgerEndpoint();
  const dispatch = createDispatcher(endpoint);
  const first: Message[] = [];
  const second: Message[] = [];
  const detach = dispatch.register(['read'], {
    message(_path, message) {
      first.push(message);
    },
  });
  const { id } = endpoint.admit(['read']);
  await settled();
  assert.equal(first.length, 1);
  detach();
  dispatch.register(['read'], {
    message(_path, message) {
      second.push(message);
    },
  });
  endpoint.cancel(id);
  await settled();
  assert.equal(first.length, 2);
  assert.equal(first[1]?.frame.kind, 'cancel');
  assert.equal(second.length, 0, 'a control reached the rebound receiver');
});

test('each traversal of one dispatcher captures separately', async () => {
  const endpoint = new LedgerEndpoint();
  const dispatch = createDispatcher(endpoint);
  const seen: string[] = [];
  dispatch.register(['outer'], {
    message(_path, message) {
      seen.push(`outer:${message.frame.kind}`);
      if (message.frame.kind === 'request') dispatch.send(['inner'], message);
    },
  });
  dispatch.register(['inner'], {
    message(_path, message) {
      seen.push(`inner:${message.frame.kind}`);
    },
  });
  const { id } = endpoint.admit(['outer']);
  await settled();
  assert.deepEqual(seen, ['outer:request', 'inner:request']);
  endpoint.cancel(id);
  await settled();
  assert.deepEqual(seen.slice(2).sort(), ['inner:cancel', 'outer:cancel']);
});

test('a capture installed while cancellation is latched still receives it', async () => {
  const endpoint = new LedgerEndpoint();
  const dispatch = createDispatcher(endpoint);
  const inner = createDispatcher(dispatch.select(['a']));
  const controls: Message[] = [];
  inner.register(['read'], {
    message(_path, message) {
      if (message.frame.kind === 'request') {
        endpoint.cancel(message.frame.id);
        return;
      }
      controls.push(message);
    },
  });
  endpoint.admit(['a', 'read']);
  await settled();
  assert.equal(controls.length, 1, 'a capture installed while cancellation was latched never received it');
  assert.equal(controls[0]?.frame.kind, 'cancel');
});

test('an invocation bounds its total captures and bodies', () => {
  const endpoint = new LedgerEndpoint({ captures: 2, bodies: 1 });
  const { id } = endpoint.admit(['read']);
  const message: Message = {
    frame: { version: 1, kind: 'request', id, params: null },
    return: endpoint.returns.get(id)!,
  };
  captureInvocation(message, () => {});
  captureInvocation(message, () => {});
  assert.throws(
    () => captureInvocation(message, () => {}),
    (error: unknown) => error instanceof InvocationError && error.code === 'limit',
  );
  const body = beginInvocationBody(message);
  assert.throws(
    () => beginInvocationBody(message),
    (error: unknown) => error instanceof InvocationError && error.code === 'limit',
  );
  // The bound is a total, so a finished body gives back no slot: neither depth
  // nor shallow fan-out can grow what one invocation retains.
  body.done();
  assert.throws(
    () => beginInvocationBody(message),
    (error: unknown) => error instanceof InvocationError && error.code === 'limit',
  );
});

test('retirement waits for the body and the control drain', () => {
  const endpoint = new LedgerEndpoint();
  const { id, invocation } = endpoint.admit(['read']);
  const message: Message = {
    frame: { version: 1, kind: 'request', id, params: null },
    return: endpoint.returns.get(id)!,
  };
  const capture = captureInvocation(message, () => {});
  const body = beginInvocationBody(message);
  invocation.settle();
  assert.equal(invocation.retired, false, 'retired with an undelivered capture and a running body');
  capture.ready();
  assert.equal(invocation.retired, false, 'retired while the body was still running');
  body.done();
  assert.equal(invocation.retired, true);
  assert.throws(
    () => captureInvocation(message, () => {}),
    (error: unknown) => error instanceof InvocationError && error.code === 'ended',
  );
  assert.throws(
    () => beginInvocationBody(message),
    (error: unknown) => error instanceof InvocationError && error.code === 'ended',
  );
});

test('sequential completions beyond capacity retain nothing', async () => {
  const endpoint = new LedgerEndpoint({ captures: 2, bodies: 2 });
  const dispatch = createDispatcher(endpoint);
  handle(dispatch, ['read'], () => 'answer');
  for (let index = 0; index < 32; index++) {
    const { invocation, id } = endpoint.admit(['read']);
    await settled();
    assert.equal(endpoint.outcomes.get(id)?.length, 1, `outcome ${index}`);
    assert.equal(invocation.retired, true, `retired ${index}`);
  }
  assert.equal(endpoint.retirements, 32);
});

test('a traversal past the capture bound is refused as busy', async () => {
  const endpoint = new LedgerEndpoint({ captures: 1, bodies: 8 });
  const outer = createDispatcher(endpoint);
  const inner = createDispatcher(outer.select(['a']));
  let routed = false;
  inner.register(['read'], {
    message() {
      routed = true;
    },
  });
  const { id } = endpoint.admit(['a', 'read']);
  await settled();
  assert.equal(routed, false, 'the inner receiver was reached past the bound');
  const outcomes = endpoint.outcomes.get(id) ?? [];
  assert.equal(outcomes.length, 1);
  const outcome = outcomes[0]!.frame;
  assert.equal(outcome.kind, 'response');
  if (outcome.kind === 'response') assert.equal(outcome.error?.code, 'busy');
});

test('a dispatcher refuses an invocation whose return capability carries no lifecycle', async () => {
  const endpoint = new LedgerEndpoint();
  const dispatch = createDispatcher(endpoint);
  let routed = false;
  dispatch.register(['read'], {
    message() {
      routed = true;
    },
  });
  const answers: Message[] = [];
  let uses = 0;
  const bare: AddressedWire = {
    send(path, message) {
      if (path.length) throw new Error('this return capability carries no lifecycle');
      uses++;
      answers.push(message);
    },
  };
  endpoint.deliver(['read'], {
    frame: { version: 1, kind: 'request', id: 'b:1', params: null },
    return: { wire: bare },
  });
  await settled();
  assert.equal(routed, false, 'an unmanaged invocation was routed with weaker guarantees');
  assert.equal(answers.length, 1);
  const answer = answers[0]!.frame;
  assert.equal(answer.kind, 'response');
  if (answer.kind === 'response') assert.equal(answer.error?.code, 'invalid_message');
  assert.equal(uses, 1, 'the refusal did not use the original return capability once');
});
