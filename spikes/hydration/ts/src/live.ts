import { Atom, Tuple } from '@bitspark/bitwire';

/** Experimental recursive extension above Ontos. Ground Value is unchanged. */
export type LiveValue = Atom | Tuple | LiveTuple | LiveWire;

export class LiveTuple {
  readonly values: readonly LiveValue[];
  constructor(values: readonly LiveValue[]) {
    this.values = Object.freeze([...values]);
    Object.freeze(this);
  }
}

/** A send-only live capability. It grants no receive/close or registry access. */
export class LiveWire {
  readonly send: (value: LiveValue) => Promise<void>;
  constructor(send: (value: LiveValue) => void | Promise<void>) {
    this.send = async value => send(value);
    Object.freeze(this);
  }
}

export const live = (...values: LiveValue[]): LiveTuple => new LiveTuple(values);
export const wire = (send: (value: LiveValue) => void | Promise<void>): LiveWire => new LiveWire(send);

export function items(value: LiveValue): readonly LiveValue[] {
  if (value instanceof Tuple) return value.items();
  if (value instanceof LiveTuple) return value.values;
  throw new TypeError('Expected tuple');
}
