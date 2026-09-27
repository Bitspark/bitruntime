// Ported from nightseam v0.6.0 conformance/ts/src/testee.ts (5cc9723).
/**
 * The driver-1 machinery the ops share: error answers, the table of handles,
 * the inbox a handle holds what arrived unasked in, and the argument readers.
 * The exchange itself, one request per line and one answer per line, is in
 * testee.ts; what each op does is in seam.ts and peer.ts.
 */

/** An error answer: a driver code, or an op's own refusal, with whatever members the op says. */
export class Failure extends Error {
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

/**
 * The testee's state: every object the runner made, by handle. A handle is
 * minted once and never reused within the process, so one process can serve
 * both sides of a case.
 */
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

  /** Closes and forgets everything the runner made. */
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

/** What arrived unasked, in order, for an await to take. */
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

  /**
   * The first item accept takes, removed, within the time. No item and
   * `ended` when none will come; no item and not `ended` when none came in
   * time.
   */
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
}

/** The op's `within_ms`, 5000 when absent. */
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

/** Races work against a deadline; a loss is the driver's timeout. */
export const within = async <T>(withinMs: number, work: Promise<T>, what: string): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const late = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(fail('timeout', `${what} did not settle within ${withinMs}ms`)), withinMs);
  });
  try {
    return await Promise.race([work, late]);
  } finally {
    clearTimeout(timer);
  }
};
