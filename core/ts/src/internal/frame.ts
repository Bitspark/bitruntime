/**
 * Structured profile frames at local admission: snapshots, validation against
 * the bitwire/1 envelope, the private identity of an outgoing trace, and the
 * public form of an error. Module-private: no package subpath exports it.
 */
import type { ProfileFrame } from '@bitspark/bitwire';
import { PublicError, UnpublishedError } from '../error.ts';
import type { Trace } from '../trace.ts';
import { decodeEnvelope, isObject } from './envelope.ts';
import { traceOf } from './trace.ts';
import { scalarJSON } from './unicode.ts';

// Outgoing propagators may privately associate their trace object with an
// active consumer context. Keep that identity across local frame snapshots;
// only the two public trace strings cross a physical connection.
const outgoingTraces = new WeakMap<ProfileFrame, Trace>();

/** The trace an outgoing frame carries, with the identity its propagator gave it. */
export function outgoingTrace(frame: ProfileFrame): Trace | undefined {
  return outgoingTraces.get(frame) ?? traceOf(frame);
}

/** Associate the trace object an outgoing frame was minted with. */
export function setOutgoingTrace(frame: ProfileFrame, trace: Trace): void {
  outgoingTraces.set(frame, trace);
}

/** A JSON copy of a value, refused when it is not JSON or not Unicode scalar text. */
export function snapshot<T>(value: T): T {
  try {
    const encoded = JSON.stringify(value, (_key, member: unknown) => {
      if (
        typeof member === 'function' ||
        typeof member === 'symbol' ||
        (typeof member === 'number' && !Number.isFinite(member))
      )
        throw new Error('Not JSON.');
      return member;
    });
    scalarJSON(encoded);
    return JSON.parse(encoded) as T;
  } catch {
    throw new PublicError('invalid_message', 'Frame must contain serializable JSON values.');
  }
}

/** Reuse the physical profile validator at structured admission. */
export function profileFrame(value: ProfileFrame, name: string, maxFrameBytes?: number): ProfileFrame {
  const saved: unknown = snapshot(value);
  if (!isObject(saved) || Object.hasOwn(saved, 'method') || Object.hasOwn(saved, 'event'))
    throw new PublicError('invalid_message', 'Invalid structured profile frame.');
  // The path is the sole operation name. Reuse the physical profile validator
  // after translating that name, without silently replacing an extra member.
  const envelope = {
    ...saved,
    ...(saved.kind === 'request' ? { method: name } : saved.kind === 'event' ? { event: name } : {}),
  };
  const text = JSON.stringify(envelope);
  if (maxFrameBytes !== undefined && new TextEncoder().encode(text).byteLength > maxFrameBytes)
    throw new PublicError('frame_too_large', 'Outgoing frame exceeds the size limit.');
  // Logical request ids belong to local return addresses, not physical roles.
  // Both accepted prefixes still use the existing canonical numeric grammar.
  const prefix = typeof saved.id === 'string' && saved.id.startsWith('s:') ? 's:' : 'c:';
  try {
    decodeEnvelope(text, prefix, prefix);
  } catch {
    throw new PublicError('invalid_message', 'Invalid structured profile frame.');
  }
  const frame = saved as unknown as ProfileFrame;
  const associated = outgoingTraces.get(value);
  if (associated) outgoingTraces.set(frame, associated);
  return frame;
}

/**
 * The public form of an error: a public error keeps its code, message and data,
 * reconstructed so that no local publication proof survives dispatch, and any
 * other error is the generic internal one.
 */
export function publicError(error: unknown): PublicError {
  return error instanceof PublicError &&
    typeof error.code === 'string' &&
    error.code.length > 0 &&
    typeof error.message === 'string' &&
    error.message.length > 0
    ? new PublicError(error.code, error.message, error.data)
    : new PublicError('internal', 'Request handler failed.');
}

/** What an ended carrier reports when nothing more specific said so. */
export const ENDED_MESSAGE = 'Connection ended; outcome may be unknown.';

/**
 * The one closed classification: an ended carrier reports `disconnected`,
 * keeping what ended it as the error's cause.
 */
export function ended(cause?: unknown): PublicError {
  // A send attempt's own proof is never lent to what else the ending settles.
  if (cause instanceof PublicError && !(cause instanceof UnpublishedError) && cause.code === 'disconnected')
    return cause;
  const error = new PublicError('disconnected', ENDED_MESSAGE);
  if (cause !== undefined) Object.defineProperty(error, 'cause', { value: cause });
  return error;
}
