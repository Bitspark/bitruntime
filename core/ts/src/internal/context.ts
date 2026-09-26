/**
 * The received context bitruntime's own carriers establish and its own helpers
 * recognize. It is associated with a local return capability through a
 * WeakMap that lives in this module alone, and no package subpath exports this
 * module, so no other code can construct, claim or read it: a foreign return
 * capability carries no recognized context, and a message's visible fields —
 * its meta included — never do.
 */
import type { AddressedWire, Message, ReturnAddress } from '@bitspark/bitwire';
import { PublicError } from '../error.ts';
import type { Meta } from '../meta.ts';
import type { Trace, TraceContext } from '../trace.ts';

/** What a carrier established for one admitted request's handler. */
export interface ReceivedRequestContext extends TraceContext {
  signal: AbortSignal;
  requestId: string;
  meta?: Meta;
}

/** What a carrier established for one delivered event's listener. */
export interface ReceivedEventContext extends TraceContext {
  meta?: Meta;
}

/** Received values retained beside a local return capability. */
export interface DispatchContext {
  context: ReceivedRequestContext;
  maxFrameBytes: number;
  /** A local handler's own withdrawal, which a forwarded reply keeps as its outcome. */
  completion?: { cancelled: boolean };
}

// A local capability association, never a field a caller can serialize or
// supply as ambient outgoing metadata.
const dispatchContexts = new WeakMap<ReturnAddress, DispatchContext>();

/** Read the verified context associated with a local return capability. */
export function dispatchContext(address: ReturnAddress | undefined): DispatchContext | undefined {
  return address ? dispatchContexts.get(address) : undefined;
}

/** Associate received context without adding it to serialized frame data. */
export function setDispatchContext(address: ReturnAddress, context: DispatchContext): () => void {
  dispatchContexts.set(address, context);
  return () => {
    if (dispatchContexts.get(address) === context) dispatchContexts.delete(address);
  };
}

/** Release a return capability's association. */
export function clearDispatchContext(address: ReturnAddress): void {
  dispatchContexts.delete(address);
}

// An event's local context capability has no waiter, id or callable return.
// Weak ownership lets queued deliveries outlive the source receiver's return.
const eventContexts = new WeakMap<ReturnAddress, ReceivedEventContext>();
const receivedEventTraces = new WeakMap<ReceivedEventContext, Trace | undefined>();

/** Preserve the received frame's trace independently of a custom propagator's context. */
export function setReceivedEventTrace(context: ReceivedEventContext, trace: Trace | undefined): void {
  receivedEventTraces.set(context, trace);
}

/** The trace the event's frame arrived with, whatever a propagator placed on its context. */
export function receivedEventTrace(context: ReceivedEventContext): Trace | undefined {
  return receivedEventTraces.has(context) ? receivedEventTraces.get(context) : context.trace;
}

const eventContextCarrier: AddressedWire = Object.freeze({
  send: () => {
    throw new PublicError('invalid_message', 'An event context is not a return address.');
  },
});

/** Inspect only runtime-associated event context, never caller data. */
export function eventContext(message: Message): ReceivedEventContext | undefined {
  return message.return ? eventContexts.get(message.return) : undefined;
}

/** Carry received event context across local asynchronous composition. */
export function withEventContext(message: Message, context: ReceivedEventContext): Message {
  const address: ReturnAddress = { wire: eventContextCarrier };
  eventContexts.set(address, context);
  return { frame: message.frame, return: address };
}
