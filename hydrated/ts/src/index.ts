// Evidence for bitwire decision 0019 (proposed): the first edition of the
// hydrated wire protocol, bitwire/hydrated/1, realized in TypeScript. It mirrors
// hydrated/go and is not a released API.
import { Atom, Tuple, atom, tuple, encodeMessage, pathEqual } from '@bitspark/bitwire';
import type { Path, AddressedWire } from '@bitspark/bitwire';

/** A hydrated value: a ground ontos value, a HydratedTuple or a Wire. */
export type Value = Atom | Tuple | HydratedTuple | Wire;

/** Sends one hydrated value. Only an Endpoint's face or a proxy can travel (D7). */
export interface Wire { send(value: Value): Promise<void> }

/** What the composition establishes about an arrival; never part of a value (D4). */
export type Context = unknown;

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

const HEADER = atom(new TextEncoder().encode('bitwire/hydrated/1'));
const TUPLE_TAG = atom([1]);
const WIRE_TAG = atom([2]);

/** A tuple with at least one Wire beneath it; see hydratedTuple (D1). */
export class HydratedTuple {
  readonly items: readonly Value[];
  constructor(items: readonly Value[]) {
    this.items = Object.freeze([...items]);
    Object.freeze(this);
  }
}

// Any Wire may sit in a tuple; encoding refuses one the runtime does not own (D7).
const isWire = (v: unknown): v is Wire => typeof (v as Wire | undefined)?.send === 'function';

/** An Endpoint travels only as its sending face, on every path (D7). */
const sendOnly = (v: Value): Value => (v instanceof Endpoint ? v.wire : v);

/** Captures items; returns the ground tuple when no Wire is beneath them. */
export function hydratedTuple(items: readonly Value[]): Value {
  const captured = items.map(sendOnly);
  let live = false;
  for (const item of captured) {
    if (item instanceof HydratedTuple || isWire(item)) live = true;
    else if (!(item instanceof Atom) && !(item instanceof Tuple)) throw new TypeError('hydrated: not a value');
  }
  return live ? new HydratedTuple(captured) : tuple(captured as (Atom | Tuple)[]);
}

/** Any tuple's items, ground or hydrated. */
export function items(v: Value): readonly Value[] | undefined {
  if (v instanceof HydratedTuple) return v.items;
  if (v instanceof Tuple) return v.items();
  return undefined;
}

/** One set of participants addressed by absolute paths (D6). */
export class Namespace {
  readonly name: string;
  constructor(name: string) { this.name = name; }
}

/** The ground form of a live Wire (D2, D3). */
export interface Reference { readonly path: Path; readonly scope: Atom; readonly id: Atom }

const hex = (a: Atom) => Array.from(a.bytes(), (b) => b.toString(16).padStart(2, '0')).join('');
const refKey = (r: Reference) => `${r.path.map(hex).join('/')}|${r.path.length}|${hex(r.scope)}|${hex(r.id)}`;
const refValue = (r: Reference): Tuple => tuple([WIRE_TAG, tuple([tuple(r.path), r.scope, r.id])]);

/**
 * A local HydratedEndpoint: one receiver, ordered dispatch never inline with the
 * admitting send, a bounded queue and an owner lifetime. Its face is what a value
 * carries; closing it ends its export in every scope (D7).
 */
export class Endpoint implements Wire {
  readonly wire: Wire;
  readonly closed: Promise<void>;
  #queue: { value: Value; context: Context }[] = [];
  #limit: number;
  #handler: ((value: Value, context: Context) => void) | undefined;
  #generation = 0;
  #failure: unknown;
  #isClosed = false;
  #draining = false;
  #resolveClosed!: () => void;
  readonly #exports = new Map<Scope, Atom>();

  constructor(limit = 1) {
    this.#limit = Math.max(1, limit);
    this.wire = new Face(this);
    this.closed = new Promise((resolve) => { this.#resolveClosed = resolve; });
  }

  send(value: Value): Promise<void> { return this.wire.send(value); }

  /** Attaches the one receiver; its detach removes only that receiver. */
  receive(handler: (value: Value, context: Context) => void): () => void {
    if (this.#handler) throw new Error('hydrated: receiver already attached');
    const generation = ++this.#generation;
    this.#handler = handler;
    this.#drain();
    return () => { if (this.#generation === generation) this.#handler = undefined; };
  }

  /** The receiver failure that terminated the endpoint, if any. */
  get failure(): unknown { return this.#failure; }

  async close(): Promise<void> {
    if (this.#isClosed) return;
    this.#isClosed = true;
    this.#queue = [];
    const exports = [...this.#exports];
    this.#exports.clear();
    for (const [scope, id] of exports) scope._withdraw(this, id);
    this.#resolveClosed();
  }

  get isClosed(): boolean { return this.#isClosed; }

  /** @internal */
  _admit(value: Value, context: Context): void {
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
class Face implements Wire {
  constructor(endpoint: Endpoint) { owners.set(this, endpoint); Object.freeze(this); }
  async send(value: Value): Promise<void> { owners.get(this)!._admit(sendOnly(value), LOCAL); }
}

/**
 * An imported Wire. It carries its reference and namespace, so any participant
 * in the namespace forwards it unchanged (D6). A dead reason marks this
 * participant's own stale or withdrawn reference (D7, returning home).
 */
class Proxy implements Wire {
  readonly ref: Reference;
  readonly ns: Namespace;
  readonly #via: Scope;
  readonly #dead: string | undefined;
  constructor(ref: Reference, ns: Namespace, via: Scope, dead?: string) {
    this.ref = ref; this.ns = ns; this.#via = via; this.#dead = dead;
    Object.freeze(this);
  }
  async send(value: Value): Promise<void> {
    if (this.#dead) throw new Refusal(this.#dead);
    return this.#via._send(this.ref, value);
  }
}

/** Bounds of a scope (D8). */
export interface Limits { exports: number; nodes: number; depth: number; bytes: number }
export const DEFAULT_LIMITS: Limits = Object.freeze({ exports: 64, nodes: 4096, depth: 64, bytes: 1 << 20 });

interface Budget { nodes: number; bytes: number }

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
  connect(ref: Reference): Wire { return this.#import(ref, new Map()); }

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

  #spend(b: Budget, depth: number, bytes: number): void {
    b.nodes++; b.bytes += bytes;
    if (b.nodes > this.#limits.nodes || depth > this.#limits.depth || b.bytes > this.#limits.bytes) throw new Refusal(LIMIT);
  }

  /** Encodes atomically, registers new exports, then admits one frame (D5). */
  /** @internal */
  async _send(dest: Reference, value: Value): Promise<void> {
    if (this.#closed) throw new Refusal(SCOPE_ENDED);
    const staged = new Map<Endpoint, Atom>();
    const body = this._encode(value, 0, staged, { nodes: 0, bytes: 0 });
    const frame = tuple([HEADER, dest.scope, dest.id, body]);
    try { encodeMessage(frame, this.#limits.bytes); } catch { throw new Refusal(LIMIT); }
    this.#commit(staged);
    await this.#sender.send(dest.path, frame);
  }

  /** @internal */
  _encode(v: Value, depth: number, staged: Map<Endpoint, Atom>, b: Budget): Atom | Tuple {
    if (v instanceof Atom) { this.#spend(b, depth, v.length); return v; }
    if (v instanceof Tuple || v instanceof HydratedTuple) {
      this.#spend(b, depth, 0);
      return tuple([TUPLE_TAG, tuple(items(v)!.map((c) => this._encode(c, depth + 1, staged, b)))]);
    }
    if (v instanceof Endpoint) return this._encode(v.wire, depth, staged, b);
    if (v instanceof Face) {
      this.#spend(b, depth, 0);
      return refValue({ path: this.path, scope: this.token, id: this.#exportId(owners.get(v)!, staged) });
    }
    if (v instanceof Proxy) {
      if (v.ns !== this.ns) throw new Refusal(FOREIGN_NAMESPACE);
      this.#spend(b, depth, 0);
      return refValue(v.ref);
    }
    if (v && typeof (v as Wire).send === 'function') throw new Refusal(UNEXPORTABLE);
    throw new TypeError('hydrated: not a value');
  }

  /** Decides one value addressed to this participant's own path (D4); throws a Refusal as a host diagnostic. */
  deliver(path: Path, message: Atom | Tuple, context: Context): void {
    if (!pathEqual(path, this.path)) throw new Refusal(MALFORMED_FRAME);
    if (this.#closed) throw new Refusal(SCOPE_ENDED);
    // The whole frame counts against the byte bound, reference material included (D8).
    try { encodeMessage(message, this.#limits.bytes); } catch { throw new Refusal(LIMIT); }
    if (!(message instanceof Tuple) || message.length !== 4 || !HEADER.equals(message.at(0)!)) throw new Refusal(MALFORMED_FRAME);
    const [, token, id, body] = message.items();
    if (!(token instanceof Atom) || !(id instanceof Atom) || token.length !== 16 || id.length !== 16) throw new Refusal(MALFORMED_FRAME);
    if (!token.equals(this.token)) throw new Refusal(STALE_SCOPE);
    const target = this.#byId.get(hex(id));
    if (!target) throw new Refusal(UNKNOWN_EXPORT);
    const value = this._decode(body!, 0, new Map(), { nodes: 0, bytes: 0 });
    target._admit(value, context);
  }

  /** Occurrences of one reference within the value share one proxy; nothing is retained (D7). */
  /** @internal */
  _decode(v: Atom | Tuple, depth: number, interned: Map<string, Wire>, b: Budget): Value {
    if (v instanceof Atom) { this.#spend(b, depth, v.length); return v; }
    if (v.length !== 2) throw new Refusal(MALFORMED_FRAME);
    const [tag, payload] = v.items();
    if (!(tag instanceof Atom) || tag.length !== 1) throw new Refusal(MALFORMED_FRAME);
    this.#spend(b, depth, 0);
    if (tag.equals(TUPLE_TAG)) {
      if (!(payload instanceof Tuple)) throw new Refusal(MALFORMED_FRAME);
      return hydratedTuple(payload.items().map((c) => this._decode(c, depth + 1, interned, b)));
    }
    if (tag.equals(WIRE_TAG)) {
      const ref = readReference(payload!, this.#limits.depth);
      for (const a of [...ref.path, ref.scope, ref.id]) this.#spend(b, depth + 1, a.length);
      return this.#import(ref, interned);
    }
    throw new Refusal(MALFORMED_FRAME);
  }

  /** A reference home in the current scope returns the original face; stale or withdrawn, a Wire that refuses (D7). */
  #import(ref: Reference, interned: Map<string, Wire>): Wire {
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
export function readReference(v: Atom | Tuple, maxDepth: number = DEFAULT_LIMITS.depth): Reference {
  if (!(v instanceof Tuple) || v.length !== 3) throw new Refusal(MALFORMED_FRAME);
  const [p, scope, id] = v.items();
  if (!(p instanceof Tuple) || p.length > maxDepth) throw new Refusal(MALFORMED_FRAME);
  const path = p.items().map((s) => { if (!(s instanceof Atom)) throw new Refusal(MALFORMED_FRAME); return s; });
  if (!(scope instanceof Atom) || !(id instanceof Atom) || scope.length !== 16 || id.length !== 16) throw new Refusal(MALFORMED_FRAME);
  return Object.freeze({ path: Object.freeze(path), scope, id });
}

/** The reference payload of a reference, for composition bootstrap over ground data. */
export function referenceValue(ref: Reference): Tuple { return tuple([tuple(ref.path), ref.scope, ref.id]); }
