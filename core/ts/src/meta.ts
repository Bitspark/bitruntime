/**
 * What a frame carries about a call rather than of it: a flat map of strings —
 * a tenant, an idempotency key, a credential that is per request — which the
 * profile carries verbatim and reads nothing into. A handler's outgoing call
 * carries the meta it received only where the handler says so.
 */
export type Meta = Record<string, string>;
