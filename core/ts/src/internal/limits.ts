/** Limit validation shared by the local pair and the protocol engine. Module-private. */
import { PublicError } from '../error.ts';

/** The limits a local pair and a peer run with unless their options say otherwise. */
export const LIMIT_DEFAULTS = Object.freeze({
  maxConcurrentHandlers: 64,
  maxPendingRequests: 128,
  queueCapacity: 128,
  maxFrameBytes: 1_048_576,
  requestTimeoutMs: 30_000,
  writeTimeoutMs: 10_000,
});

/** Validates a component limit, returning it or throwing invalid_options; safe also requires exact integer representation. */
export function positiveInteger(value: unknown, name: string, safe = false): number {
  if (typeof value !== 'number' || !(safe ? Number.isSafeInteger(value) : Number.isInteger(value)) || value <= 0) {
    throw new PublicError('invalid_options', `${name} must be a positive ${safe ? 'safe ' : ''}integer.`);
  }
  return value;
}

/** Applies validated overrides to a set of defaults. */
export function limits<T extends Record<string, number>>(defaults: T, options: Partial<Record<keyof T, unknown>>): T {
  const result = { ...defaults };
  for (const key of Object.keys(defaults) as (keyof T)[]) {
    const value = options[key];
    if (value !== undefined) {
      positiveInteger(value, key as string, true);
      (result as Record<keyof T, number>)[key] = value as number;
    }
  }
  return result;
}
