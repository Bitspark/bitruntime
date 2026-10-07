import { atom, tuple, Atom, Tuple, type Value, type Path, type AddressedWire } from '@bitspark/bitwire';
import { LiveWire, LiveTuple, type LiveValue } from './live.js';

// Disposable experiment grammar. These are not bitwire protocol/version names.
const HEADER = atom(new TextEncoder().encode('hydration-spike/1'));
const DATA = atom([0]), SEQUENCE = atom([1]), REFERENCE = atom([2]);
const hex = (a: Atom) => Array.from(a.bytes(), b => b.toString(16).padStart(2, '0')).join('');
export const pathKey = (path: Path): string => JSON.stringify(path.map(hex));

export interface Reference { readonly path: Path; readonly scope: Atom; readonly id: Atom }
interface Entry { readonly reference: Reference; readonly target: LiveWire }
interface Limits { exports: number; imports: number; work: number; nodes: number; bytes: number; depth: number }
const defaults: Limits = { exports: 64, imports: 64, work: 16, nodes: 4096, bytes: 1024 * 1024, depth: 64 };
const key = (r: Reference) => `${pathKey(r.path)}:${hex(r.scope)}:${hex(r.id)}`;
const fail = (message: string): never => { throw new Error(message); };
const asTuple = (v: Value | undefined, length?: number): Tuple =>
  v instanceof Tuple && (length === undefined || v.length === length) ? v : fail('malformed tuple');
const asAtom = (v: Value | undefined): Atom => v instanceof Atom ? v : fail('malformed atom');

/** One owner at an admitted absolute tree location; no carrier or domain knowledge. */
export class Hydration {
  readonly path: Path;
  readonly scope = atom(crypto.getRandomValues(new Uint8Array(16)));
  readonly #sender: AddressedWire;
  readonly #limits: Limits;
  readonly #exports = new Map<string, Entry>();
  readonly #byWire = new Map<LiveWire, Reference>();
  readonly #imports = new Map<string, LiveWire>();
  readonly #proxyRefs = new WeakMap<LiveWire, Reference>();
  #next = 0n;
  #sending = 0;
  #receiving = 0;
  #closed = false;

  constructor(path: Path, sender: AddressedWire, limits: Partial<Limits> = {}) {
    this.path = Object.freeze([...path]);
    this.#sender = sender;
    this.#limits = { ...defaults, ...limits };
    for (const n of Object.values(this.#limits)) {
      if (!Number.isSafeInteger(n) || n < 1) throw new RangeError('positive finite limits required');
    }
    if (path.length > this.#limits.depth || path.some(a => !(a instanceof Atom))) fail('invalid owner path');
  }

  get retained() { return { exports: this.#exports.size, imports: this.#imports.size }; }
  #open(): void { if (this.#closed) fail('scope ended'); }

  #newReference(): Reference {
    return Object.freeze({ path: this.path, scope: this.scope, id: atom(new TextEncoder().encode(String(++this.#next))) });
  }

  /** Composition bootstrap only. Domain adapters never call this. */
  expose(target: LiveWire): Reference {
    this.#open();
    const known = this.#byWire.get(target);
    if (known) return known;
    if (this.#exports.size >= this.#limits.exports) fail('export limit');
    const ref = this.#newReference();
    this.#retain(ref, target);
    return ref;
  }

  #retain(reference: Reference, target: LiveWire): void {
    this.#exports.set(hex(reference.id), { reference, target });
    this.#byWire.set(target, reference);
  }

  /** Explicit owner withdrawal; never closes the borrowed target. */
  withdraw(target: LiveWire): void {
    const ref = this.#byWire.get(target);
    if (ref) this.#exports.delete(hex(ref.id));
    this.#byWire.delete(target);
  }

  /** Composition bootstrap only; normal imports happen during decoding. */
  connect(ref: Reference): LiveWire {
    this.#open();
    return this.#import(this.#readReference(this.#writeReference(ref)), new Map(), true);
  }

  #import(ref: Reference, staged: Map<string, LiveWire>, commit = false): LiveWire {
    if (pathKey(ref.path) === pathKey(this.path)) {
      if (!ref.scope.equals(this.scope)) return fail('stale scope');
      return this.#exports.get(hex(ref.id))?.target ?? fail('unknown export');
    }
    const name = key(ref);
    const known = this.#imports.get(name) ?? staged.get(name);
    if (known) return known;
    if (this.#imports.size + staged.size >= this.#limits.imports) fail('import limit');
    const proxy = new LiveWire(value => this.#send(ref, value));
    this.#proxyRefs.set(proxy, ref);
    if (commit) this.#imports.set(name, proxy); else staged.set(name, proxy);
    return proxy;
  }

  #writeReference(ref: Reference): Value { return tuple([tuple(ref.path), ref.scope, ref.id]); }
  #readReference(value: Value): Reference {
    const fields = asTuple(value, 3);
    const path = asTuple(fields.at(0)).items().map(asAtom);
    const scope = asAtom(fields.at(1)), id = asAtom(fields.at(2));
    if (path.length > this.#limits.depth || scope.length !== 16 || id.length === 0 || id.length > 64) fail('invalid reference');
    return Object.freeze({ path: Object.freeze(path), scope, id });
  }

  #budget(): (depth: number, bytes?: number) => void {
    let nodes = 0, total = 0;
    return (depth, bytes = 0) => {
      if (++nodes > this.#limits.nodes || depth > this.#limits.depth || (total += bytes) > this.#limits.bytes) fail('value limit');
    };
  }

  async #send(destination: Reference, value: LiveValue): Promise<void> {
    this.#open();
    if (this.#sending >= this.#limits.work) fail('send limit');
    this.#sending++;
    try {
      const staged = new Map<LiveWire, Reference>();
      const budget = this.#budget();
      const encode = (v: LiveValue, depth: number): Value => {
        budget(depth, v instanceof Atom ? v.length : 0);
        if (v instanceof Atom) return tuple([DATA, v]);
        if (v instanceof LiveWire) {
          let ref = this.#proxyRefs.get(v) ?? this.#byWire.get(v) ?? staged.get(v);
          if (!ref) {
            if (this.#exports.size + staged.size >= this.#limits.exports) fail('export limit');
            ref = this.#newReference();
            staged.set(v, ref);
          }
          for (const a of [...ref.path, ref.scope, ref.id]) budget(depth + 1, a.length);
          return tuple([REFERENCE, this.#writeReference(ref)]);
        }
        const children = v instanceof Tuple ? v.items() : v instanceof LiveTuple ? v.values : fail('not a live value');
        return tuple([SEQUENCE, tuple(children.map(child => encode(child, depth + 1)))]);
      };
      const encoded = encode(value, 0);
      this.#open();
      // Register before admitting bytes, so immediate replies find the targets.
      for (const [target, ref] of staged) this.#retain(ref, target);
      await this.#sender.send(destination.path, tuple([HEADER, destination.scope, destination.id, encoded]));
    } finally { this.#sending--; }
  }

  /** Dispatcher owns reception and reports rejection as a host diagnostic. */
  async deliver(path: Path, message: Value): Promise<void> {
    this.#open();
    if (pathKey(path) !== pathKey(this.path)) fail('wrong owner route');
    if (this.#receiving >= this.#limits.work) fail('receive limit');
    this.#receiving++;
    try {
      const frame = asTuple(message, 4);
      if (!HEADER.equals(asAtom(frame.at(0)))) fail('not a hydration frame');
      if (!this.scope.equals(asAtom(frame.at(1)))) fail('stale scope');
      const entry = this.#exports.get(hex(asAtom(frame.at(2)))) ?? fail('unknown export');
      const staged = new Map<string, LiveWire>();
      const budget = this.#budget();
      const decode = (encoded: Value, depth: number): LiveValue => {
        const fields = asTuple(encoded, 2), tag = asAtom(fields.at(0)), value = fields.at(1)!;
        if (DATA.equals(tag)) { const a = asAtom(value); budget(depth, a.length); return a; }
        budget(depth);
        if (REFERENCE.equals(tag)) {
          const ref = this.#readReference(value);
          for (const a of [...ref.path, ref.scope, ref.id]) budget(depth + 1, a.length);
          return this.#import(ref, staged);
        }
        if (!SEQUENCE.equals(tag)) return fail('unknown live tag');
        const children = asTuple(value).items().map(child => decode(child, depth + 1));
        return children.every(v => v instanceof Atom || v instanceof Tuple) ? tuple(children) : new LiveTuple(children);
      };
      const value = decode(frame.at(3)!, 0);
      this.#open();
      for (const [name, proxy] of staged) this.#imports.set(name, proxy);
      await entry.target.send(value);
    } finally { this.#receiving--; }
  }

  /** Ends the whole bounded association, not the carrier or any borrowed Wire. */
  close(): void {
    this.#closed = true;
    this.#exports.clear();
    this.#byWire.clear();
    this.#imports.clear();
  }
}
