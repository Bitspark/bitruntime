export interface PairOptions { readonly maxMessageBytes?: number; readonly maxQueuedBytes?: number; readonly maxQueuedMessages?: number }
export function positiveLimit(n: number): number {
  if (!Number.isSafeInteger(n) || n <= 0) throw new RangeError('wire limit must be a positive safe integer'); return n;
}
export function pairOptions(options: PairOptions = {}) {
  return { maxMessageBytes: positiveLimit(options.maxMessageBytes ?? 16 * 1024 * 1024),
    maxQueuedBytes: positiveLimit(options.maxQueuedBytes ?? 64 * 1024 * 1024),
    maxQueuedMessages: positiveLimit(options.maxQueuedMessages ?? 1024) };
}
