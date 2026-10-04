export interface PairOptions { readonly maxEnvelopeBytes?: number; readonly maxQueuedBytes?: number; readonly maxQueuedEnvelopes?: number }
export function positiveLimit(n: number): number {
  if (!Number.isSafeInteger(n) || n <= 0) throw new RangeError('wire limit must be a positive safe integer'); return n;
}
export function pairOptions(options: PairOptions = {}) {
  return { maxEnvelopeBytes: positiveLimit(options.maxEnvelopeBytes ?? 16 * 1024 * 1024),
    maxQueuedBytes: positiveLimit(options.maxQueuedBytes ?? 64 * 1024 * 1024),
    maxQueuedEnvelopes: positiveLimit(options.maxQueuedEnvelopes ?? 1024) };
}
