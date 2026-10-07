import assert from 'node:assert/strict';
import { pair, addressed } from '@bitspark/bitruntime/core';
import { listenWebSocket, connectWebSocket } from '@bitspark/bitruntime/websocket';
import { atom, isValue } from '@bitspark/bitwire';
import { Hydration, pathKey } from '../dist/hydration.js';

export const bytes = text => atom(new TextEncoder().encode(text));
export const leftPath = [atom([]), atom([0, 255, 47])];
export const rightPath = [bytes('branch/inside-one-atom'), atom([255, 0])];

async function link(t, carrier) {
  if (carrier === 'pair') {
    const endpoints = pair();
    t.after(() => endpoints[0].close());
    return endpoints;
  }
  const accepted = Promise.withResolvers();
  const listener = await listenWebSocket({ host: '127.0.0.1' }, accepted.resolve);
  t.after(() => listener.close());
  const client = await connectWebSocket(listener.url);
  t.after(() => client.close());
  const server = await accepted.promise;
  t.after(() => server.close());
  return [client, server];
}

// This middle node knows only routes and ground values. It does not decode the
// payload, instantiate hydration, or hold per-Wire/per-invocation state.
function forward(path, value, routes, trace, faults) {
  assert.ok(isValue(value));
  trace.push({ path, value });
  const destination = routes.get(pathKey(path));
  if (!destination) { faults.push(new Error('missing route')); return; }
  void destination.send(path, value).catch(error => faults.push(error));
}

export async function tree(t, carrier) {
  const [a, middleA] = (await link(t, carrier)).map(addressed);
  const [c, middleC] = (await link(t, carrier)).map(addressed);
  const trace = [], faults = [];
  const routes = new Map([[pathKey(leftPath), middleA], [pathKey(rightPath), middleC]]);
  middleA.receive((p, v) => forward(p, v, routes, trace, faults));
  middleC.receive((p, v) => forward(p, v, routes, trace, faults));
  const left = new Hydration(leftPath, a);
  let right = new Hydration(rightPath, c);
  a.receive((p, v) => { void left.deliver(p, v).catch(error => faults.push(error)); });
  c.receive((p, v) => { void right.deliver(p, v).catch(error => faults.push(error)); });
  // Scope ownership belongs to composition, not domain adapters or borrowed Wires.
  void a.closed.then(() => left.close());
  void c.closed.then(() => right.close());
  t.after(() => { left.close(); right.close(); });
  return {
    left, get right() { return right; }, trace, faults,
    replaceRight() { right.close(); right = new Hydration(rightPath, c); return right; },
    closeCarrier: () => a.close(),
  };
}

export async function until(predicate) {
  const deadline = Date.now() + 3000;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error('observation did not arrive');
    await new Promise(resolve => setTimeout(resolve, 5));
  }
}
