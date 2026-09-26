/** Trace members as frames carry them. Module-private: no package subpath exports it. */
import type { Trace } from '../trace.ts';

/** The members an incoming frame carries, verbatim: what a propagator extracts. */
export function traceOf(frame: { readonly traceparent?: unknown; readonly tracestate?: unknown }): Trace | undefined {
  const traceparent = typeof frame.traceparent === 'string' ? frame.traceparent : '';
  const tracestate = typeof frame.tracestate === 'string' ? frame.tracestate : '';
  if (!traceparent && !tracestate) return undefined;
  return tracestate ? { traceparent, tracestate } : { traceparent };
}

/** The members a structured frame carries for a trace; an empty member is none. */
export function traceMembers(trace: Trace | undefined): { traceparent?: string; tracestate?: string } {
  const members: { traceparent?: string; tracestate?: string } = {};
  if (trace?.traceparent) members.traceparent = trace.traceparent;
  if (trace?.tracestate) members.tracestate = trace.tracestate;
  return members;
}

/** Stamps onto an outgoing frame what a propagator minted; an empty member is none. */
export function traced(envelope: Record<string, unknown>, trace: Trace | undefined): Record<string, unknown> {
  if (trace?.traceparent) envelope.traceparent = trace.traceparent;
  if (trace?.tracestate) envelope.tracestate = trace.tracestate;
  return envelope;
}
