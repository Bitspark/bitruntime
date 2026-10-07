// Consumer-only request and ownership convention. No Scope, route or reference API.
import { Atom, atom } from '@bitspark/bitwire';
import { Endpoint, hydratedTuple, items } from '../../../../dist/hydrated/ts/src/index.js';

export const text = value => atom(new TextEncoder().encode(value));
export function string(value) {
  if (!(value instanceof Atom)) throw new ProtocolError('expected text atom');
  try { return new TextDecoder('utf-8', { fatal: true }).decode(value.bytes()); }
  catch { throw new ProtocolError('invalid UTF-8'); }
}
export function tupleItems(value, length) {
  const values = items(value);
  if (!values || (length !== undefined && values.length !== length)) {
    throw new ProtocolError('invalid domain tuple');
  }
  return values;
}
export function sendingWire(value) {
  if (!value || typeof value.send !== 'function') throw new ProtocolError('expected Wire');
  return value;
}
export class ProtocolError extends Error { name = 'ProtocolError'; }
export class DomainError extends Error {
  name = 'DomainError';
  constructor(code) { super(code); this.code = code; }
}
export class OutcomeUnknown extends Error {
  name = 'OutcomeUnknown';
  constructor() { super('domain operation outcome unknown'); }
}

/** The domain composition owns service endpoints; each call owns its reply. */
export class AdapterOwner {
  #endpoints = new Map();
  #controller = new AbortController();
  faults = [];
  get signal() { return this.#controller.signal; }
  get open() { return this.#endpoints.size; }
  endpoint(handler) {
    if (this.signal.aborted) throw new OutcomeUnknown();
    const endpoint = new Endpoint(32);
    this.#endpoints.set(endpoint.wire, endpoint);
    endpoint.receive((value, context) => {
      void Promise.resolve().then(() => handler(value, context)).catch(error => this.faults.push(error));
    });
    return endpoint;
  }
  // Used only by test assembly to bootstrap an owned root, never by an adapter.
  ownedEndpoint(wire) { return this.#endpoints.get(wire); }
  async release(endpoint) {
    this.#endpoints.delete(endpoint.wire);
    await endpoint.close();
  }
  async close() {
    this.#controller.abort();
    await Promise.all([...this.#endpoints.values()].map(endpoint => this.release(endpoint)));
  }
}

/** Local materialization cache, not a transport reference/export table. */
export function memoAdapter(toWire, fromWire) {
  const provided = new WeakMap(), projected = new WeakMap();
  return {
    toWire(value) {
      let target = provided.get(value);
      if (!target) { target = toWire(value); provided.set(value, target); }
      return target;
    },
    fromWire(target) {
      sendingWire(target);
      let value = projected.get(target);
      if (!value) {
        value = fromWire(target);
        projected.set(target, value);
        provided.set(value, target);
      }
      return value;
    },
  };
}

/** The domain chooses one result per invocation and distinguishes its failures. */
export function provide(owner, dispatch) {
  return owner.endpoint(async message => {
    const fields = tupleItems(message);
    const reply = sendingWire(fields[2]);
    let response;
    try {
      tupleItems(message, 3);
      response = hydratedTuple([text('ok'), await dispatch(string(fields[0]), tupleItems(fields[1]))]);
    } catch (error) {
      if (error instanceof DomainError) response = hydratedTuple([text('domain'), text(error.code)]);
      else if (error instanceof ProtocolError) response = hydratedTuple([text('protocol'), text(error.message)]);
      else throw error;
    }
    // Closing this service lifetime stops replies, but does not cancel started work.
    if (!owner.signal.aborted) await reply.send(response);
  }).wire;
}

export async function invoke(owner, target, operation, args, decode = value => value) {
  const result = Promise.withResolvers();
  const reply = owner.endpoint(message => {
    try {
      const [status, value] = tupleItems(message, 2);
      switch (string(status)) {
        case 'ok': result.resolve(decode(value)); break;
        case 'domain': result.reject(new DomainError(string(value))); break;
        case 'protocol': result.reject(new ProtocolError(string(value))); break;
        default: throw new ProtocolError('unknown response status');
      }
    } catch (error) { result.reject(error); }
  });
  const end = () => result.reject(new OutcomeUnknown());
  const timer = setTimeout(end, 3000);
  owner.signal.addEventListener('abort', end, { once: true });
  void target.send(hydratedTuple([text(operation), hydratedTuple(args), reply.wire])).catch(result.reject);
  try { return await result.promise; }
  finally {
    clearTimeout(timer);
    owner.signal.removeEventListener('abort', end);
    await owner.release(reply);
  }
}
