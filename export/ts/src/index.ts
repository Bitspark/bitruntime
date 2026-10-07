// Live Wire export (bitwire decision 0018): connection-scoped exports of
// send-only Wires over addressed reference routes, import through the addressed
// facade, release, and re-export with source retirement and dependent-lifetime
// accounting. One Table and one Importer serve one side of one connection; the
// connection's dispatcher stays its single receive owner.
import { atom, tuple, Atom, Tuple, type Path, type Value, type Wire, type AddressedWire } from '@bitspark/bitwire';
import { bind, under } from '../../../core/ts/src/addressed.ts';

const REF = atom(new TextEncoder().encode('bitwire/ref/1'));
const SEND = atom(new TextEncoder().encode('send'));
const RELEASE = atom(new TextEncoder().encode('release'));

export const MALFORMED_ROUTE = 'malformed-route';
export const FOREIGN_REFERENCE = 'foreign-reference';
export const UNKNOWN_REFERENCE = 'unknown-reference';

export class ExportLimitError extends Error {
  constructor() { super('export-limit'); this.name = 'ExportLimitError'; }
}
export class ExportScopeEndedError extends Error {
  constructor() { super('export scope ended'); this.name = 'ExportScopeEndedError'; }
}

/** The reference value (bitwire/ref/1, scope, id). */
export function reference(scope: Atom, id: Atom): Value { return tuple([REF, scope, id]); }

/** Reads a reference value, or throws. */
export function parseReference(value: Value): { scope: Atom; id: Atom } {
  if (!(value instanceof Tuple) || value.length !== 3 || !REF.equals(value.at(0)!)) throw new TypeError('not a reference');
  const scope = value.at(1), id = value.at(2);
  if (!(scope instanceof Atom) || !(id instanceof Atom)) throw new TypeError('reference scope and id must be atoms');
  return { scope, id };
}

const key = (id: Atom) => Buffer.from(id.bytes()).toString('hex');

interface Entry { target: Wire; onRelease?: () => void }

export class Table {
  readonly scope: Atom;
  onRefuse?: (reason: string, path: Path) => void;
  #next = 0n;
  #entries = new Map<string, Entry>();
  #closed = false;
  readonly #limit: number;

  constructor(limit: number, scope?: Atom) {
    if (!Number.isInteger(limit) || limit <= 0) throw new RangeError('export limit must be positive');
    this.#limit = limit;
    this.scope = scope ?? atom(crypto.getRandomValues(new Uint8Array(16)));
  }

  /** Registers target and returns its reference. Ids are never reused. */
  export(target: Wire): Value { return this.#export(target); }

  #export(target: Wire, onRelease?: () => void): Value {
    if (this.#closed) throw new ExportScopeEndedError();
    if (this.#entries.size >= this.#limit) throw new ExportLimitError();
    this.#next += 1n;
    const id = atom(new TextEncoder().encode(this.#next.toString()));
    this.#entries.set(key(id), { target, onRelease });
    return reference(this.scope, id);
  }

  /** Ends an export from the exporting side; ending it twice changes nothing. */
  withdraw(ref: Value): void {
    try {
      const { scope, id } = parseReference(ref);
      if (scope.equals(this.scope)) this.#end(key(id));
    } catch { /* not this table's reference */ }
  }

  #end(id: string): void {
    const e = this.#entries.get(id);
    this.#entries.delete(id);
    e?.onRelease?.();
  }

  get live(): number { return this.#entries.size; }

  /** Decides one addressed value received under the export root; path is relative to it. */
  deliver(path: Path, message: Value): void {
    if (path.length !== 3 || !(path[0]!.equals(SEND) || path[0]!.equals(RELEASE))) return this.#refuse(MALFORMED_ROUTE, path);
    if (!path[1]!.equals(this.scope)) return this.#refuse(FOREIGN_REFERENCE, path);
    const id = key(path[2]!);
    if (path[0]!.equals(RELEASE)) return this.#end(id);
    const e = this.#entries.get(id);
    if (!e) return this.#refuse(UNKNOWN_REFERENCE, path);
    void e.target.send(message).catch(() => {}); // the target's own admission; release never closes it
  }

  /** Ends the scope with its connection: every export ends. */
  close(): void {
    this.#closed = true;
    const entries = [...this.#entries.values()];
    this.#entries.clear();
    for (const e of entries) e.onRelease?.();
  }

  /** Exports an import's proxy as one of its dependents; it retires with the import's connection. */
  reExport(imported: Imported): Value {
    const release = imported.hold();
    let ref: Value;
    try { ref = this.#export(imported.wire, release); } catch (error) { release(); throw error; }
    const { id } = parseReference(ref);
    if (!imported.addRetiree(() => this.#end(key(id)))) {
      this.#end(key(id));
      throw new ExportScopeEndedError();
    }
    return ref;
  }

  /** @internal The registered target for a returned reference, if live. */
  target(id: Atom): Wire | undefined { return this.#entries.get(key(id))?.target; }

  /** @internal */
  refuseLocal(reason: string, path: Path): void { this.#refuse(reason, path); }

  #refuse(reason: string, path: Path): void { this.onRefuse?.(reason, path); }
}

export class Imported {
  readonly wire: Wire;
  readonly importer: Importer;
  readonly scope: Atom;
  readonly id: Atom;
  readonly returned: boolean;
  #dependents = 0;
  #ended = false;
  #retirees: Array<() => void> = [];

  constructor(importer: Importer, scope: Atom, id: Atom, root: AddressedWire, local?: Table) {
    this.importer = importer;
    this.scope = scope;
    this.id = id;
    this.returned = !!local && scope.equals(local.scope);
    if (local && this.returned) {
      // The peer returned this side's own reference: swap back to the live Wire.
      const path = [SEND, scope, id];
      this.wire = local.target(id) ?? { send: async () => { local.refuseLocal(UNKNOWN_REFERENCE, path); throw new Error(UNKNOWN_REFERENCE); } };
    } else {
      this.wire = bind(root, [SEND, scope, id]);
    }
  }

  /** Adds a dependent; the returned release drops it. The last release sends one upstream release. */
  hold(): () => void {
    this.#dependents += 1;
    let done = false;
    return () => {
      if (done) return;
      done = true;
      this.#dependents -= 1;
      if (this.#dependents === 0 && !this.#ended) {
        this.#ended = true;
        this.importer.forget(this);
        // A swapped-back import never ends this side's own export.
        if (!this.returned) void this.importer.root.send([RELEASE, this.scope, this.id], tuple([])).catch(() => {});
      }
    };
  }

  /** @internal Registers a re-export to retire with this import; false if already ended. */
  addRetiree(retire: () => void): boolean {
    if (this.#ended) return false;
    this.#retirees.push(retire);
    return true;
  }

  /** @internal The import's connection ended: re-exports retire and nothing is sent. */
  retire(): void {
    this.#ended = true;
    const retirees = this.#retirees;
    this.#retirees = [];
    for (const r of retirees) r();
  }
}

export class Importer {
  readonly root: AddressedWire;
  #imports = new Set<Imported>();
  #closed = false;

  readonly #local?: Table;

  /** sender is this side's addressed sender on the connection, peerRoot the peer's
   * export root, and local this side's own table on the same connection, if any. */
  constructor(sender: AddressedWire, peerRoot: Path, local?: Table) {
    this.root = under(sender, peerRoot);
    this.#local = local;
  }

  /** Imports a reference; grants send only and validates nothing. */
  import(ref: Value): Imported {
    if (this.#closed) throw new ExportScopeEndedError();
    const { scope, id } = parseReference(ref);
    const imported = new Imported(this, scope, id, this.root, this.#local);
    this.#imports.add(imported);
    return imported;
  }

  /** @internal */
  forget(imported: Imported): void { this.#imports.delete(imported); }

  /** Retires every import from this connection, as the connection ended. */
  close(): void {
    this.#closed = true;
    const imports = [...this.#imports];
    this.#imports.clear();
    for (const i of imports) i.retire();
  }
}
