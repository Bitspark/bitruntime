/**
 * The TypeScript testee: @nightseam/runtime and @nightseam/tunnel under the
 * control of the conformance runner, over the
 * protocol of conformance/DRIVER.md. One request per line on stdin, one
 * answer per line on stdout, a table of handles, an inbox per handle for
 * what arrived unasked, and nothing on stdout but answers.
 */
import { createInterface } from 'node:readline';
import { seamOps } from './seam.ts';
import { peerOps } from './peer.ts';
import { tunnelOps } from './tunnel.ts';
import { liveOps } from './live.ts';
import { recordedWireWitness } from './recorded-wire.ts';

const DRIVER = 1;

/** An error answer: the protocol's codes, or the remote's, with whatever members the op says. */
class Failure extends Error {
  readonly code: string;
  readonly members: Record<string, unknown>;
  constructor(code: string, message: string, members: Record<string, unknown> = {}) {
    super(message);
    this.code = code;
    this.members = members;
  }
  toJSON(): Record<string, unknown> {
    return { code: this.code, message: this.message, ...this.members };
  }
}

export const fail = (code: string, message: string, members?: Record<string, unknown>) =>
  new Failure(code, message, members);
export const unsupported = (what: string) => fail('unsupported', what);
export const invalid = (what: string) => fail('invalid', what);

/** What a handle's object does when the testee resets. */
interface Closer {
  shutdown(): void;
}

export type Args = Record<string, unknown>;
export type Op = (args: Args) => Promise<unknown> | unknown;

/** The testee's state: every object the runner made, by handle. */
export class Testee {
  private next = 0;
  private readonly handles = new Map<string, unknown>();
  bye = false;

  mint(prefix: string, object: unknown): string {
    const handle = `${prefix}${++this.next}`;
    this.handles.set(handle, object);
    return handle;
  }

  lookup<T>(handle: unknown, kind: (object: unknown) => object is T, what: string): T {
    if (typeof handle !== 'string') throw invalid('on is a handle');
    const object = this.handles.get(handle);
    if (object === undefined) throw fail('unknown_handle', handle);
    if (!kind(object)) throw invalid(`${handle} is not ${what}`);
    return object;
  }

  reset(): void {
    const objects = [...this.handles.values()];
    this.handles.clear();
    for (const object of objects) {
      if (object && typeof (object as Closer).shutdown === 'function') {
        try {
          (object as Closer).shutdown();
        } catch {
          /* A reset forgets. */
        }
      }
    }
  }
}

/** What arrived unasked, in order, for await and drain. */
export class Inbox<T> {
  private readonly items: T[] = [];
  private readonly waiters = new Set<() => void>();
  private done = false;

  put(item: T): void {
    this.items.push(item);
    this.wake();
  }

  /** Nothing more arrives; an await then answers at once. */
  close(): void {
    this.done = true;
    this.wake();
  }

  private wake(): void {
    for (const waiter of [...this.waiters]) waiter();
  }

  /** The first item accept takes, removed, within the time; undefined when none came in time or none will. */
  async await(withinMs: number, accept: (item: T) => boolean): Promise<{ item?: T; ended: boolean }> {
    const deadline = Date.now() + withinMs;
    for (;;) {
      const index = this.items.findIndex(accept);
      if (index >= 0) return { item: this.items.splice(index, 1)[0], ended: false };
      if (this.done) return { ended: true };
      const remaining = deadline - Date.now();
      if (remaining <= 0) return { ended: false };
      await new Promise<void>((resolve) => {
        const timer = setTimeout(() => {
          this.waiters.delete(waiter);
          resolve();
        }, remaining);
        const waiter = () => {
          clearTimeout(timer);
          this.waiters.delete(waiter);
          resolve();
        };
        this.waiters.add(waiter);
      });
    }
  }

  drain(empty: boolean): T[] {
    const out = [...this.items];
    if (empty) this.items.length = 0;
    return out;
  }
}

export const withinOf = (args: Args): number => {
  const value = args.within_ms;
  if (value === undefined) return 5000;
  if (typeof value !== 'number' || !Number.isInteger(value) || value < 0) throw invalid('within_ms is an integer');
  return value;
};

export const stringOf = (args: Args, name: string, required = false): string => {
  const value = args[name];
  if (value === undefined) {
    if (required) throw invalid(`${name} is required`);
    return '';
  }
  if (typeof value !== 'string') throw invalid(`${name} is a string`);
  if (required && value === '') throw invalid(`${name} is required`);
  return value;
};

export const intOf = (args: Args, name: string, fallback: number): number => {
  const value = args[name];
  if (value === undefined) return fallback;
  if (typeof value !== 'number' || !Number.isInteger(value)) throw invalid(`${name} is an integer`);
  return value;
};

export const boolOf = (args: Args, name: string, fallback = false): boolean => {
  const value = args[name];
  if (value === undefined) return fallback;
  if (typeof value !== 'boolean') throw invalid(`${name} is a boolean`);
  return value;
};

/** Races a promise against a deadline; a loss is the driver's timeout. */
export const within = async <T>(withinMs: number, work: Promise<T>, what: string): Promise<T> => {
  let timer: NodeJS.Timeout | undefined;
  const late = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(fail('timeout', `${what} did not settle within ${withinMs}ms`)), withinMs);
  });
  try {
    return await Promise.race([work, late]);
  } finally {
    clearTimeout(timer);
  }
};

const testee = new Testee();
const ops: Record<string, Op> = {
  hello: () => ({
    driver: DRIVER,
    language: 'typescript',
    layers: ['seam', 'peer', 'tunnel', 'live'],
    features: ['listen', 'pipe', 'observer', 'propagator', 'lazy'],
  }),
  reset: () => {
    testee.reset();
    return {};
  },
  bye: () => {
    testee.bye = true;
    testee.reset();
    return {};
  },
  ...seamOps(testee),
  ...peerOps(testee),
  ...tunnelOps(testee),
  ...liveOps(testee),
  'peer.recorded_wire_witness': (args) =>
    within(withinOf(args), recordedWireWitness(withinOf(args)), 'recorded wire witness'),
};

const serve = async (line: string): Promise<string> => {
  let request: Record<string, unknown>;
  try {
    request = JSON.parse(line) as Record<string, unknown>;
  } catch (error) {
    return JSON.stringify({ id: 0, error: invalid(`not a request: ${String(error)}`) });
  }
  const id = request.id;
  const op = request.op;
  if (typeof id !== 'number') return JSON.stringify({ id: 0, error: invalid('a request carries an integer id') });
  if (typeof op !== 'string' || op === '') return JSON.stringify({ id, error: invalid('a request names its op') });
  const { id: _id, op: _op, ...args } = request;
  const handler = ops[op];
  if (!handler) return JSON.stringify({ id, error: unsupported(`no such op: ${op}`) });
  try {
    const ok = (await handler(args)) ?? {};
    return JSON.stringify({ id, ok });
  } catch (error) {
    if (error instanceof Failure) return JSON.stringify({ id, error });
    return JSON.stringify({
      id,
      error: fail('internal', error instanceof Error ? `${error.name}: ${error.message}` : String(error)),
    });
  }
};

const lines = createInterface({ input: process.stdin, crlfDelay: Infinity });
for await (const line of lines) {
  if (line.trim() === '') continue;
  const answer = await serve(line);
  await new Promise<void>((resolve) => process.stdout.write(answer + '\n', () => resolve()));
  if (testee.bye) break;
}
testee.reset();
process.exit(0);
