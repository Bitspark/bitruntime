/**
 * W3C Trace Context as the wire carries it: the two members verbatim, nothing
 * invented and nothing normalised. An absent member is the empty string, which
 * a frame never carries; a traceparent is the one of the two the peer holds to
 * a form, and the propagator decides what either of them means.
 */
export interface Trace {
  traceparent: string;
  tracestate?: string;
}

/** The trace-bearing part shared by physical-peer and opaque-wire contexts. */
export interface TraceContext {
  trace?: Trace;
}

/**
 * Where an incoming trace goes and what an outgoing frame carries. The runtime
 * imports no tracing library: an adapter for one replaces this hook, and the
 * default below correlates without it.
 */
export interface Propagator {
  /** Places an incoming frame's trace on the context its handler runs with. */
  extract(context: TraceContext, trace: Trace | undefined): void;
  /** What an outgoing frame carries: a child of the context's trace, or a new trace. */
  inject(context: TraceContext | undefined): Trace;
}

/** `version-traceid-spanid-flags`, the one form the profile accepts, in its parts. */
const TRACEPARENT = /^([0-9a-f]{2})-([0-9a-f]{32})-[0-9a-f]{16}-([0-9a-f]{2})$/;

/**
 * Correlation with no tracing library installed: a new trace is a random
 * 16-byte trace id and 8-byte span id, sampled; a child keeps its parent's
 * version, trace id and flags and mints a span id of its own, and carries the
 * parent's tracestate verbatim.
 */
export const defaultPropagator: Propagator = {
  extract(context, trace) {
    // A tracestate that arrives without a traceparent continues no trace, as
    // the specification says; the response to its frame still carries it back.
    if (trace?.traceparent) context.trace = trace;
  },
  inject(context) {
    const trace = context?.trace;
    if (trace) {
      const parent = TRACEPARENT.exec(trace.traceparent);
      if (parent) {
        const child: Trace = { traceparent: `${parent[1]}-${parent[2]}-${randomHex(8)}-${parent[3]}` };
        if (trace.tracestate !== undefined) child.tracestate = trace.tracestate;
        return child;
      }
    }
    return { traceparent: `00-${randomHex(16)}-${randomHex(8)}-01` };
  },
};

/** The members an incoming frame carries, verbatim: what a propagator extracts. */
export function traceOf(frame: { readonly traceparent?: unknown; readonly tracestate?: unknown }): Trace | undefined {
  const traceparent = typeof frame.traceparent === 'string' ? frame.traceparent : '';
  const tracestate = typeof frame.tracestate === 'string' ? frame.tracestate : '';
  if (!traceparent && !tracestate) return undefined;
  return tracestate ? { traceparent, tracestate } : { traceparent };
}

/** Stamps onto an outgoing frame what a propagator minted; an empty member is none. */
export function traced(envelope: Record<string, unknown>, trace: Trace | undefined): Record<string, unknown> {
  if (trace?.traceparent) envelope.traceparent = trace.traceparent;
  if (trace?.tracestate) envelope.tracestate = trace.tracestate;
  return envelope;
}

/** Web Crypto is the only source; the runtime takes no dependency for it. */
function randomHex(bytes: number): string {
  return Array.from(crypto.getRandomValues(new Uint8Array(bytes)), (byte) => byte.toString(16).padStart(2, '0')).join(
    '',
  );
}
