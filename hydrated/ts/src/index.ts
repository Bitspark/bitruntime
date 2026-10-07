// The realization of bitwire decision 0019, the hydrated wire protocol's first
// edition, over bitwire's public hydrated declarations and its shared pure codec.
// The codec owns the grammar, validation and bounds; this module owns export ids
// and staging, reference recognition, liveness and endpoints.
import {
  Atom, Tuple, atom, tuple, pathEqual,
  HydratedCodecError, HydratedDataAtom, HydratedDataTuple, HydratedReference,
  packHydratedBody, unpackHydratedBody, packHydratedFrame, unpackHydratedFrame,
  packHydratedReference, unpackHydratedReference,
} from '@bitspark/bitwire';
import type {
  Path, AddressedWire, Termination, HydratedValue, HydratedWire, HydratedEndpoint, ReceivedContext,
  HydratedData, HydratedCodecLimits, HydratedTuple as HydratedTupleView,
} from '@bitspark/bitwire';

/** The context of a send that never left the process. */
export const LOCAL: unique symbol = Symbol('local');

export const MALFORMED_FRAME = 'malformed-frame';
export const STALE_SCOPE = 'stale-scope';
export const UNKNOWN_EXPORT = 'unknown-export';
export const LIMIT = 'limit';
export const TARGET_REFUSED = 'target-refused';
export const UNEXPORTABLE = 'unexportable';
export const FOREIGN_NAMESPACE = 'foreign-namespace';
export const SCOPE_ENDED = 'scope-ended';

/** A refusal: a host diagnostic at the owner, or a refusal before admission at the sender (D8). */
export class Refusal extends Error {
  readonly reason: string;
  constructor(reason: string) {
    super(reason);
    this.reason = reason;
  }
}

/** A tuple with at least one HydratedWire beneath it; see hydratedTuple (D1). */
/** Only this module constructs hydrated tuples, so every one is canonical (D1, D7). */
const CONSTRUCT = Symbol('hydrated tuple');

/**
 * A tuple with at least one HydratedWire beneath it, shaped like bitwire's HydratedTuple
 * (a ground Tuple has the same shape). Construct it with hydratedTuple().
 */
export class HydratedTuple implements HydratedTupleView {
  readonly kind = 'tuple' as const;
  readonly #items: readonly HydratedValue[];
  constructor(items: readonly HydratedValue[], token?: symbol) {
    if (token !== CONSTRUCT) throw new TypeError('hydrated: construct tuples with hydratedTuple()');
    this.#items = Object.freeze([...items]);
    Object.freeze(this);
  }
  get length(): number { return this.#items.length; }
  items(): readonly HydratedValue[] { return this.#items; }
  at(index: number): HydratedValue | undefined { return this.#items.at(index); }
}

// Any HydratedWire may sit in a tuple; encoding refuses one the runtime does not own (D7).
const isWire = (v: unknown): v is HydratedWire => typeof (v as HydratedWire | undefined)?.send === 'function';

/** An Endpoint travels only as its sending face, on every path (D7). */
const sendOnly = (v: HydratedValue): HydratedValue => (v instanceof Endpoint ? v.wire : v);

/**
 * Validates a value for local delivery as dehydration would for a remote one (D1,
 * D7): only runtime-recognized leaves arrive, and an endpoint arrives as its face.
 */
function local(v: HydratedValue): HydratedValue {
  // A tuple recognized its leaves when it was built and is immutable, so only
  // the top level needs checking; no traversal is needed.
  const projected = sendOnly(v);
  if (projected instanceof Atom || projected instanceof Tuple || projected instanceof HydratedTuple) return projected;
  if (projected instanceof Face || projected instanceof Proxy) return projected;
  if (projected && typeof (projected as HydratedWire).send === 'function') throw new Refusal(UNEXPORTABLE);
  throw new TypeError('hydrated: not a value');
}

/** Captures items; returns the ground tuple when no HydratedWire is beneath them. */
export function hydratedTuple(items: readonly HydratedValue[]): HydratedValue {
  const captured = items.map(sendOnly);
  let live = false;
  for (const item of captured) {
    if (item instanceof HydratedTuple || item instanceof Face || item instanceof Proxy) live = true;
    else if (isWire(item)) throw new Refusal(UNEXPORTABLE); // only the runtime's own Wires are leaves (D1)
    else if (!(item instanceof Atom) && !(item instanceof Tuple)) throw new TypeError('hydrated: not a value');
  }
  return live ? new HydratedTuple(captured, CONSTRUCT) : tuple(captured as (Atom | Tuple)[]);
}

/** Any tuple's items, ground or hydrated. */
export function items(v: HydratedValue): readonly HydratedValue[] | undefined {
  if (v instanceof HydratedTuple) return v.items();
  if (v instanceof Tuple) return v.items();
  return undefined;
}

/** One set of participants addressed by absolute paths (D6). */
export class Namespace {
  readonly name: string;
  constructor(name: string) { this.name = name; }
}

/** The ground form of a live HydratedWire (D2, D3). */
export interface Reference { readonly path: Path; readonly scope: Atom; readonly id: Atom }

const hex = (a: Atom) => Array.from(a.bytes(), (b) => b.toString(16).padStart(2, '0')).join('');
const refKey = (r: Reference) => `${r.path.map(hex).join('/')}|${r.path.length}|${hex(r.scope)}|${hex(r.id)}`;
/** A codec failure, named as the protocol's refusal (D8). */
function refusal(error: unknown): never {
  if (error instanceof HydratedCodecError) throw new Refusal(error.kind === 'limit' ? LIMIT : MALFORMED_FRAME);
  throw error;
}
const codec = <T>(f: () => T): T => { try { return f(); } catch (error) { return refusal(error); } };
const codecLimits = (l: Limits): HydratedCodecLimits => ({ nodes: l.nodes, depth: l.depth, bytes: l.bytes });
/** A Wire leaf's value bytes under D8: its path bytes and 32. */
const refBytes = (path: Path): number => path.reduce((n, a) => n + a.length, 32);

/**
 * A local HydratedEndpoint: one receiver, ordered dispatch never inline with the
 * admitting send, a bounded queue and an owner lifetime. Its face is what a value
 * carries; closing it ends its export in every scope (D7).
 */
export class Endpoint implements HydratedEndpoint {
  readonly kind = 'wire' as const;
  readonly wire: HydratedWire;
  /** Resolves with how the endpoint ended, as bitwire's Endpoint contract does. */
  readonly closed: Promise<Termination>;
  #queue: { value: HydratedValue; context: ReceivedContext }[] = [];
  #limit: number;
  #handler: ((value: HydratedValue, context: ReceivedContext) => void) | undefined;
  #generation = 0;
  #failed = false;
  #failure: unknown;
  #isClosed = false;
  #draining = false;
  #resolveClosed!: (termination: Termination) => void;
  #termination: Termination | undefined;
  readonly #exports = new Map<Scope, Atom>();

  constructor(limit = 1) {
    if (!Number.isSafeInteger(limit) || limit < 1) throw new RangeError('hydrated: an endpoint bound must be a positive integer');
    this.#limit = limit;
    this.wire = new Face(this);
    this.closed = new Promise((resolve) => { this.#resolveClosed = resolve; });
  }

  send(value: HydratedValue): Promise<void> { return this.wire.send(value); }

  /** Attaches the one receiver; its detach removes only that receiver. */
  receive(handler: (value: HydratedValue, context: ReceivedContext) => void): () => void {
    if (this.#handler) throw new Error('hydrated: receiver already attached');
    const generation = ++this.#generation;
    this.#handler = handler;
    this.#drain();
    return () => { if (this.#generation === generation) this.#handler = undefined; };
  }

  /** How the endpoint ended: failed with its receiver's failure, or closed; undefined while open. */
  get termination(): Termination | undefined { return this.#termination; }

  async close(): Promise<void> {
    if (this.#isClosed) return;
    this.#isClosed = true;
    this.#queue = [];
    const exports = [...this.#exports];
    this.#exports.clear();
    for (const [scope, id] of exports) scope._withdraw(this, id);
    this.#termination = !this.#failed
      ? Object.freeze({ kind: 'closed' })
      : Object.freeze({ kind: 'failed', message: String((this.#failure as Error)?.message ?? this.#failure) });
    this.#resolveClosed(this.#termination);
  }

  get isClosed(): boolean { return this.#isClosed; }

  /** @internal */
  _admit(value: HydratedValue, context: ReceivedContext): void {
    if (this.#isClosed || this.#queue.length >= this.#limit) throw new Refusal(TARGET_REFUSED);
    this.#queue.push({ value, context });
    this.#drain();
  }

  /** @internal */
  _register(scope: Scope, id: Atom): boolean {
    if (this.#isClosed) return false;
    this.#exports.set(scope, id);
    return true;
  }

  /** @internal */
  _unregister(scope: Scope): void { this.#exports.delete(scope); }

  #drain(): void {
    if (this.#draining) return;
    this.#draining = true;
    setImmediate(() => {
      this.#draining = false;
      while (!this.#isClosed && this.#handler && this.#queue.length) {
        const d = this.#queue.shift()!;
        try {
          this.#handler(d.value, d.context);
        } catch (error) {
          // A receiver failure terminates the endpoint, which ends its export.
          this.#failed = true;
          this.#failure = error;
          void this.close();
        }
      }
    });
  }
}

/** Each face's owner, private to this module: a face reveals no endpoint. */
const owners = new WeakMap<Face, Endpoint>();

/** An Endpoint's sending face. It grants sending only. */
class Face implements HydratedWire {
  readonly kind = 'wire' as const;
  constructor(endpoint: Endpoint) { owners.set(this, endpoint); Object.freeze(this); }
  async send(value: HydratedValue): Promise<void> { owners.get(this)!._admit(local(value), LOCAL); }
}

/**
 * An imported HydratedWire. It carries its reference and namespace, so any participant
 * in the namespace forwards it unchanged (D6). A dead reason marks this
 * participant's own stale or withdrawn reference (D7, returning home).
 */
class Proxy implements HydratedWire {
  readonly kind = 'wire' as const;
  readonly ref: Reference;
  readonly ns: Namespace;
  readonly #via: Scope;
  readonly #dead: string | undefined;
  constructor(ref: Reference, ns: Namespace, via: Scope, dead?: string) {
    this.ref = ref; this.ns = ns; this.#via = via; this.#dead = dead;
    Object.freeze(this);
  }
  async send(value: HydratedValue): Promise<void> {
    if (this.#dead) throw new Refusal(this.#dead);
    return this.#via._send(this.ref, value);
  }
}

/** Bounds of a scope (D8). */
export interface Limits { exports: number; nodes: number; depth: number; bytes: number }
export const DEFAULT_LIMITS: Limits = Object.freeze({ exports: 64, nodes: 4096, depth: 64, bytes: 1 << 20 });

/** One participant's hydration state for one incarnation (Terms). */
export class Scope {
  readonly path: Path;
  readonly token: Atom;
  readonly ns: Namespace;
  readonly #sender: AddressedWire;
  readonly #limits: Limits;
  readonly #byId = new Map<string, Endpoint>();
  readonly #byEndpoint = new Map<Endpoint, Atom>();
  #closed = false;

  constructor(ns: Namespace, path: Path, sender: AddressedWire, limits: Limits = DEFAULT_LIMITS) {
    for (const n of Object.values(limits)) if (!Number.isSafeInteger(n) || n < 1) throw new RangeError('hydrated: limits must be positive');
    this.ns = ns;
    this.path = Object.freeze([...path]);
    this.#sender = sender;
    this.#limits = { ...limits };
    this.token = atom(crypto.getRandomValues(new Uint8Array(16)));
  }

  get live(): number { return this.#byEndpoint.size; }

  /** Composition bootstrap: export an endpoint's face. Domain adapters never call it. */
  expose(endpoint: Endpoint): Reference {
    const staged = new Map<Endpoint, Atom>();
    const id = this.#exportId(endpoint, staged);
    this.#commit(staged);
    return { path: this.path, scope: this.token, id };
  }

  /** Composition bootstrap: a proxy for a reference. */
  connect(ref: Reference): HydratedWire { return this.#import(ref, new Map()); }

  /** Ends the scope: its exports end and its references go stale. Closes no endpoint. */
  close(): void {
    this.#closed = true;
    for (const endpoint of this.#byEndpoint.keys()) endpoint._unregister(this);
    this.#byEndpoint.clear();
    this.#byId.clear();
  }

  /** @internal */
  _withdraw(endpoint: Endpoint, id: Atom): void {
    if (this.#byId.get(hex(id)) === endpoint) {
      this.#byId.delete(hex(id));
      this.#byEndpoint.delete(endpoint);
    }
  }

  #exportId(endpoint: Endpoint, staged: Map<Endpoint, Atom>): Atom {
    if (endpoint.isClosed) throw new Refusal(UNEXPORTABLE);
    if (this.#closed) throw new Refusal(SCOPE_ENDED);
    const known = staged.get(endpoint) ?? this.#byEndpoint.get(endpoint);
    if (known) return known;
    if (this.#byEndpoint.size + staged.size >= this.#limits.exports) throw new Refusal(LIMIT);
    // 16 octets from a secure source, redrawn on collision: no reference reveals another's (D3).
    for (;;) {
      const id = atom(crypto.getRandomValues(new Uint8Array(16)));
      if (!this.#byId.has(hex(id)) && ![...staged.values()].some((other) => other.equals(id))) {
        staged.set(endpoint, id);
        return id;
      }
    }
  }

  #commit(staged: Map<Endpoint, Atom>): void {
    if (this.#closed) throw new Refusal(SCOPE_ENDED);
    for (const [endpoint, id] of staged) {
      if (this.#byEndpoint.has(endpoint)) continue;
      if (endpoint._register(this, id)) {
        this.#byEndpoint.set(endpoint, id);
        this.#byId.set(hex(id), endpoint);
      }
    }
  }

  /** Stages the value and registers its new exports in one step, then admits one frame (D5). */
  /** @internal */
  async _send(dest: Reference, value: HydratedValue): Promise<void> {
    if (this.#closed) throw new Refusal(SCOPE_ENDED);
    const staged = new Map<Endpoint, Atom>();
    const body = this.#toData(value, staged);
    const frame = codec(() => packHydratedFrame({ scope: dest.scope, id: dest.id, body }, codecLimits(this.#limits)));
    this.#commit(staged);
    await this.#sender.send(dest.path, frame);
  }

  /**
   * Maps a live value onto the codec's structural data, staging an export for each
   * new face. The codec checks every bound (D8); the depth guard only keeps a
   * pathological value from exhausting the stack first.
   */
  #toData(root: HydratedValue, staged: Map<Endpoint, Atom>): HydratedData {
    // Count each occurrence as D8 does and refuse before allocating, so a compact
    // value with shared subtrees cannot expand past the bounds; iterate, so a deep
    // permitted value cannot exhaust the stack. The codec stays authoritative.
    const limits = this.#limits;
    let nodes = 0, bytes = 0;
    const spend = (depth: number, n: number) => {
      nodes++; bytes += n;
      if (nodes > limits.nodes || depth > limits.depth || bytes > limits.bytes) throw new Refusal(LIMIT);
    };
    const frames: { items: readonly HydratedValue[]; depth: number; out: HydratedData[] }[] = [];
    const visit = (value: HydratedValue, depth: number): HydratedData | undefined => {
      const v = value instanceof Endpoint ? value.wire : value;
      if (v instanceof Atom) { spend(depth, v.length); return new HydratedDataAtom(v); }
      if (v instanceof Tuple || v instanceof HydratedTuple) {
        spend(depth, 0);
        frames.push({ items: items(v)!, depth, out: [] });
        return undefined;
      }
      if (v instanceof Face) {
        spend(depth, refBytes(this.path));
        return codec(() => new HydratedReference(this.path, this.token, this.#exportId(owners.get(v)!, staged)));
      }
      if (v instanceof Proxy) {
        if (v.ns !== this.ns) throw new Refusal(FOREIGN_NAMESPACE);
        spend(depth, refBytes(v.ref.path));
        return codec(() => new HydratedReference(v.ref.path, v.ref.scope, v.ref.id));
      }
      if (v && typeof (v as HydratedWire).send === 'function') throw new Refusal(UNEXPORTABLE);
      throw new TypeError('hydrated: not a value');
    };
    const first = visit(root, 0);
    if (first) return first;
    for (;;) {
      const top = frames[frames.length - 1]!;
      if (top.out.length < top.items.length) {
        const child = visit(top.items[top.out.length]!, top.depth + 1);
        if (child) top.out.push(child);
        continue;
      }
      frames.pop();
      const done = codec(() => new HydratedDataTuple(top.out));
      if (!frames.length) return done;
      frames[frames.length - 1]!.out.push(done);
    }
  }

  /** Occurrences of one reference within the value share one proxy; nothing is retained (D7). Iterative. */
  #fromData(root: HydratedData, interned: Map<string, HydratedWire>): HydratedValue {
    const leaf = (d: HydratedDataAtom | HydratedReference): HydratedValue =>
      d instanceof HydratedDataAtom ? d.value : this.#import({ path: d.path, scope: d.scope, id: d.id }, interned);
    if (!(root instanceof HydratedDataTuple)) return leaf(root);
    const frames: { items: readonly HydratedData[]; out: HydratedValue[] }[] = [{ items: root.items(), out: [] }];
    for (;;) {
      const top = frames[frames.length - 1]!;
      if (top.out.length < top.items.length) {
        const child = top.items[top.out.length]!;
        if (child instanceof HydratedDataTuple) frames.push({ items: child.items(), out: [] });
        else top.out.push(leaf(child));
        continue;
      }
      frames.pop();
      const done = hydratedTuple(top.out);
      if (!frames.length) return done;
      frames[frames.length - 1]!.out.push(done);
    }
  }

  /** @internal The body halves of a frame. */
  _encodeBody(v: HydratedValue, staged: Map<Endpoint, Atom>): Atom | Tuple {
    const data = this.#toData(v, staged);
    return codec(() => packHydratedBody(data, codecLimits(this.#limits))) as Atom | Tuple;
  }

  /** @internal */
  _decodeBody(body: Atom | Tuple, interned: Map<string, HydratedWire>): HydratedValue {
    return this.#fromData(codec(() => unpackHydratedBody(body, codecLimits(this.#limits))), interned);
  }

  /** Decides one value addressed to this participant's own path (D4); throws a Refusal as a host diagnostic. */
  deliver(path: Path, message: Atom | Tuple, context: ReceivedContext): void {
    if (!pathEqual(path, this.path)) throw new Refusal(MALFORMED_FRAME);
    if (this.#closed) throw new Refusal(SCOPE_ENDED);
    const frame = codec(() => unpackHydratedFrame(message, codecLimits(this.#limits)));
    if (!frame.scope.equals(this.token)) throw new Refusal(STALE_SCOPE);
    const target = this.#byId.get(hex(frame.id));
    if (!target) throw new Refusal(UNKNOWN_EXPORT);
    target._admit(this.#fromData(frame.body, new Map()), context);
  }

  /** A reference home in the current scope returns the original face; stale or withdrawn, a HydratedWire that refuses (D7). */
  #import(ref: Reference, interned: Map<string, HydratedWire>): HydratedWire {
    if (pathEqual(ref.path, this.path)) {
      if (!ref.scope.equals(this.token)) return new Proxy(ref, this.ns, this, STALE_SCOPE);
      const endpoint = this.#byId.get(hex(ref.id));
      return endpoint ? endpoint.wire : new Proxy(ref, this.ns, this, UNKNOWN_EXPORT);
    }
    const k = refKey(ref);
    let w = interned.get(k);
    if (!w) { w = new Proxy(ref, this.ns, this); interned.set(k, w); }
    return w;
  }
}

/** Reads a reference payload ((path...), scope, id). */
export function readReference(v: Atom | Tuple): Reference {
  const r = codec(() => unpackHydratedReference(v, codecLimits(DEFAULT_LIMITS)));
  return Object.freeze({ path: Object.freeze([...r.path]), scope: r.scope, id: r.id });
}

/** The reference payload of a reference, for composition bootstrap over ground data. */
export function referenceValue(ref: Reference): Atom | Tuple {
  return codec(() => packHydratedReference(new HydratedReference(ref.path, ref.scope, ref.id), codecLimits(DEFAULT_LIMITS))) as Atom | Tuple;
}
