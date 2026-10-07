// Consumer fixture: an invariant generic API, with T in both input and output.
// No export table, reference grammar, routing or carrier appears here.
import { atom, Atom } from '@bitspark/bitwire';
import { live, wire, items, LiveWire, type LiveValue } from './live.js';

export interface Slot<T> { read(): Promise<T>; write(value: T): Promise<void> }
export interface Adapters<A, B> { to(value: A): B; from(value: B): A }

/** Lift both directions through Slot. A covariant map alone cannot do this. */
export function adaptSlot<A, B>(source: Slot<A>, adapters: Adapters<A, B>): Slot<B> {
  return {
    read: async () => adapters.to(await source.read()),
    write: value => source.write(adapters.from(value)),
  };
}

const READ = atom([0]), WRITE = atom([1]), OK = atom([2]);
function requireWire(value: LiveValue | undefined): LiveWire {
  if (!(value instanceof LiveWire)) throw new Error('expected live Wire');
  return value;
}

/** adapter3_: Slot<LiveWire> -> LiveWire; knows only Slot's operation meaning. */
export function slotToWire(source: Slot<LiveWire>): LiveWire {
  return wire(async message => {
    const [op, response, value] = items(message), reply = requireWire(response);
    if (!(op instanceof Atom)) throw new Error('invalid Slot operation');
    if (op.equals(READ)) await reply.send(await source.read());
    else if (op.equals(WRITE)) { await source.write(requireWire(value)); await reply.send(OK); }
    else throw new Error('unknown Slot operation');
  });
}

/** Domain-local exchange policy for the fixture, not a shared Wire RPC rule. */
async function exchange(target: LiveWire, op: Atom, value?: LiveWire): Promise<LiveValue> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('Slot outcome unknown')), 3000);
    const reply = wire(result => { clearTimeout(timer); resolve(result); });
    void target.send(value ? live(op, reply, value) : live(op, reply)).catch(error => {
      clearTimeout(timer); reject(error);
    });
  });
}

export function slotFromWire(target: LiveWire): Slot<LiveWire> {
  return {
    read: async () => requireWire(await exchange(target, READ)),
    write: async value => {
      const result = await exchange(target, WRITE, value);
      if (!(result instanceof Atom) || !result.equals(OK)) throw new Error('invalid Slot response');
    },
  };
}

/** adapter3 = adapter3_ composed with the lifting of the entity adapters. */
export function slotOfToWire<T>(source: Slot<T>, adapters: Adapters<T, LiveWire>): LiveWire {
  return slotToWire(adaptSlot(source, adapters));
}

export function slotOfFromWire<T>(target: LiveWire, adapters: Adapters<T, LiveWire>): Slot<T> {
  return adaptSlot(slotFromWire(target), { to: adapters.from, from: adapters.to });
}
