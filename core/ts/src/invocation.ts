import type { Message, Path, Wire } from '@nightseam/duplex';

/**
 * An admitted request's return capability is the invocation, presented as a
 * Wire. The empty path carries its outcome, as it always has; these operations
 * carry its lifecycle. They are ordinary events of the profile — a layer's own
 * vocabulary, as `channel.` is the tunnel's — and a participant needs nothing
 * of Nightseam's to speak them but the Wire it was already handed.
 *
 * The capture or body a verb is about is one opaque segment after the
 * operation, because a path is what addresses a thing. The verbs never reach a
 * peer root and never cross a physical hop, so they take no built-in family and
 * reserve no namespace there; a return capability's path space is the
 * invocation's alone.
 */
export const invocationCapture = 'invocation.capture';
/** The captured request has been delivered; a latched control reaches it now. */
export const invocationReady = 'invocation.ready';
/** Drop a capture whose traversal wants no more controls. */
export const invocationRelease = 'invocation.release';
/** Take one execution lease. Admission is the lease. */
export const invocationBegin = 'invocation.begin';
/** Report that an executing body actually finished. */
export const invocationDone = 'invocation.done';
/** Relay a cancellation into the invocation, which latches and pushes it. */
export const invocationControl = 'invocation.control';

/** Bounds the captures of one admitted invocation, over depth and fan-out. */
export const defaultInvocationCaptures = 64;
/** Bounds the execution leases of one admitted invocation. */
export const defaultInvocationBodies = 64;

/** Why participation was refused. */
export type InvocationRefusal = 'unsupported' | 'ended' | 'limit' | 'duplicate' | 'invalid';

export class InvocationError extends Error {
  readonly code: InvocationRefusal;
  constructor(code: InvocationRefusal) {
    super(`Invocation ${code}`);
    this.code = code;
  }
}

export interface InvocationLimits {
  captures: number;
  bodies: number;
}

/** The bounds an admitting runtime uses when it states none of its own. */
export function defaultInvocationLimits(): InvocationLimits {
  return { captures: defaultInvocationCaptures, bodies: defaultInvocationBodies };
}

let identifiers = 0;
const nextIdentifier = (): string => String(++identifiers);
const event = (): Message => ({ frame: { version: 1, kind: 'event', data: null } });

interface Capture {
  sink: Wire;
  ready: boolean;
  notified: boolean;
}

/**
 * The lifecycle an admitting runtime keeps for one admitted request, and the
 * answer its return capability gives to the vocabulary above. A runtime that is
 * not Nightseam's composes it — or answers the same paths itself — and the same
 * participants work against either.
 */
export class Invocation {
  readonly #limits: InvocationLimits;
  #captures = new Map<string, Capture>();
  #bodies = new Set<string>();
  #taken = 0;
  #begun = 0;
  #unready = 1;
  #controls = 0;
  #control: Message | undefined;
  #settled = false;
  #dispatchDone = false;
  #retired = false;
  #onRetired: (() => void) | undefined;

  constructor(limits: InvocationLimits = defaultInvocationLimits(), onRetired?: () => void) {
    const captures =
      Number.isSafeInteger(limits.captures) && limits.captures > 0 ? limits.captures : defaultInvocationCaptures;
    const bodies = Number.isSafeInteger(limits.bodies) && limits.bodies > 0 ? limits.bodies : defaultInvocationBodies;
    this.#limits = { captures, bodies };
    this.#onRetired = onRetired;
  }

  /** Whether the invocation has released its captures. */
  get retired(): boolean {
    return this.#retired;
  }

  /**
   * Answers one operation of the invocation vocabulary. A return capability
   * routes every nonempty path here; the empty path stays its own.
   */
  deliver(path: Path, message: Message): void {
    if (path.length === 0) throw new InvocationError('unsupported');
    if (path[0] === invocationControl) {
      if (path.length !== 1 || message.frame.kind !== 'cancel') throw new InvocationError('unsupported');
      this.#latch(message);
      return;
    }
    if (path.length !== 2 || message.frame.kind !== 'event') throw new InvocationError('unsupported');
    const identifier = path[1]!;
    switch (path[0]) {
      case invocationCapture: {
        const sink = message.return?.wire;
        if (!sink) throw new InvocationError('unsupported');
        this.#capture(identifier, sink);
        return;
      }
      case invocationReady:
        this.#ready(identifier);
        return;
      case invocationRelease:
        this.#release(identifier);
        return;
      case invocationBegin:
        this.#begin(identifier);
        return;
      case invocationDone:
        this.#done(identifier);
        return;
      default:
        throw new InvocationError('unsupported');
    }
  }

  /** Fixes the outcome. It neither finishes a body nor drains a control. */
  settle(): void {
    this.#settled = true;
    this.#retire();
  }

  /** The admitted request's own delivery has returned. */
  dispatchDone(): void {
    if (!this.#dispatchDone) {
      this.#dispatchDone = true;
      this.#unready--;
    }
    this.#retire();
  }

  #capture(identifier: string, sink: Wire): void {
    if (this.#retired) throw new InvocationError('ended');
    if (this.#captures.has(identifier)) throw new InvocationError('duplicate');
    if (this.#taken >= this.#limits.captures) throw new InvocationError('limit');
    this.#taken++;
    this.#unready++;
    this.#captures.set(identifier, { sink, ready: false, notified: false });
  }

  #ready(identifier: string): void {
    const capture = this.#captures.get(identifier);
    if (!capture || capture.ready) return;
    capture.ready = true;
    this.#unready--;
    if (this.#control && !capture.notified) {
      capture.notified = true;
      this.#controls++;
      this.#push([capture.sink], this.#control);
      return;
    }
    this.#retire();
  }

  #release(identifier: string): void {
    const capture = this.#captures.get(identifier);
    if (!capture) return;
    if (!capture.ready) {
      capture.ready = true;
      this.#unready--;
    }
    this.#captures.delete(identifier);
    this.#retire();
  }

  #begin(identifier: string): void {
    if (this.#retired) throw new InvocationError('ended');
    if (this.#bodies.has(identifier)) throw new InvocationError('duplicate');
    if (this.#begun >= this.#limits.bodies) throw new InvocationError('limit');
    this.#begun++;
    this.#bodies.add(identifier);
  }

  #done(identifier: string): void {
    if (!this.#bodies.delete(identifier)) return;
    this.#retire();
  }

  /**
   * Latches the first cancellation and pushes it to every capture already
   * ready. A capture installed while it is latched receives it when its own
   * request delivery becomes ready. Further controls coalesce.
   */
  #latch(message: Message): void {
    if (this.#retired || this.#control) return;
    this.#control = message;
    const sinks: Wire[] = [];
    for (const capture of this.#captures.values()) {
      if (!capture.ready || capture.notified) continue;
      capture.notified = true;
      sinks.push(capture.sink);
    }
    this.#controls++;
    this.#push(sinks, message);
  }

  /**
   * Runs participant code while one control reservation is held, so that
   * retirement cannot reclaim a capture a control is still reaching.
   */
  #push(sinks: readonly Wire[], message: Message): void {
    try {
      for (const sink of sinks) {
        try {
          sink.send([], message);
        } catch {
          /* A failed participant cannot stop its siblings being told. */
        }
      }
    } finally {
      this.#controls--;
      this.#retire();
    }
  }

  #retire(): void {
    if (this.#retired || !this.#settled || this.#unready !== 0 || this.#bodies.size !== 0 || this.#controls !== 0)
      return;
    this.#retired = true;
    this.#captures = new Map();
    this.#bodies = new Set();
    this.#control = undefined;
    const notify = this.#onRetired;
    this.#onRetired = undefined;
    notify?.();
  }
}

/** The Wire a capture is pushed its control through, and nothing else. */
class InvocationSink implements Wire {
  readonly #control: (message: Message) => void;
  constructor(control: (message: Message) => void) {
    this.#control = control;
  }
  send(path: Path, message: Message): void {
    if (path.length !== 0 || message.frame.kind !== 'cancel') throw new InvocationError('invalid');
    this.#control(message);
  }
}

const invocationWire = (message: Message): Wire => {
  const wire = message.return?.wire;
  if (!wire) throw new InvocationError('unsupported');
  return wire;
};

/**
 * One immutable routing decision a participant took for one traversal of one
 * admitted invocation. Repeated traversal of the same dispatcher takes a fresh
 * handle, so no two traversals share a slot.
 */
export class InvocationCaptureHandle {
  readonly #wire: Wire;
  readonly #identifier: string;
  #ready = false;
  #released = false;
  constructor(wire: Wire, identifier: string) {
    this.#wire = wire;
    this.#identifier = identifier;
  }
  /** The captured request has been delivered; a latched control reaches it. */
  ready(): void {
    if (this.#ready) return;
    this.#ready = true;
    try {
      this.#wire.send([invocationReady, this.#identifier], event());
    } catch {
      /* A refusing lifecycle simply keeps no capture to tell. */
    }
  }
  /** Drops the capture. Both operations are idempotent. */
  release(): void {
    if (this.#released) return;
    this.#released = true;
    try {
      this.#wire.send([invocationRelease, this.#identifier], event());
    } catch {
      /* As above. */
    }
  }
}

/**
 * Claims a routing decision for this traversal and supplies the sink the
 * invocation pushes its cancellation to. The sink receives the control at most
 * once, after ready() and never before.
 *
 * A return capability that does not speak the vocabulary refuses, and the
 * refusal is the caller's to answer: routing an invocation-aware request with
 * weaker cancellation guarantees is exactly what this reports instead.
 */
export function captureInvocation(message: Message, control: (message: Message) => void): InvocationCaptureHandle {
  const wire = invocationWire(message);
  const identifier = nextIdentifier();
  wire.send([invocationCapture, identifier], {
    frame: { version: 1, kind: 'event', data: null },
    return: { wire: new InvocationSink(control) },
  });
  return new InvocationCaptureHandle(wire, identifier);
}

/** One execution lease of one admitted invocation. */
export class InvocationBodyHandle {
  readonly #wire: Wire;
  readonly #identifier: string;
  #done = false;
  constructor(wire: Wire, identifier: string) {
    this.#wire = wire;
    this.#identifier = identifier;
  }
  /**
   * Reports that the body actually finished. It is idempotent and releases only
   * the lease it took: a participant cannot finish another owner's work.
   */
  done(): void {
    if (this.#done) return;
    this.#done = true;
    try {
      this.#wire.send([invocationDone, this.#identifier], event());
    } catch {
      /* A refusing lifecycle holds no lease to release. */
    }
  }
}

/**
 * Takes an execution lease for work this participant owns. The invocation does
 * not retire while the lease is held, so an early answer to the caller — a
 * deadline, a withdrawal — never retires an invocation whose body still runs.
 */
export function beginInvocationBody(message: Message): InvocationBodyHandle {
  const wire = invocationWire(message);
  const identifier = nextIdentifier();
  wire.send([invocationBegin, identifier], event());
  return new InvocationBodyHandle(wire, identifier);
}

/**
 * Hands a cancellation to the invocation it names, which latches it and pushes
 * it to the traversals that captured it. A router that receives a control frame
 * relays it here rather than resolving a route of its own: the capture, not the
 * current registration, decides where it goes.
 */
export function relayInvocationControl(message: Message): void {
  invocationWire(message).send([invocationControl], message);
}
