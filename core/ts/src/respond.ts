import type { Message, ProfileFrame } from '@bitspark/bitwire';
import { PublicError } from './error.ts';
import { publicError, snapshot } from './internal/frame.ts';
import { traceMembers, traceOf } from './internal/trace.ts';

/**
 * Answers a request through its return capability: the result, or the error
 * normalized to what the protocol carries — a public error as it is, anything
 * else as `internal`. An unencodable or oversized result is answered with a
 * bounded internal error instead, so the caller is never left waiting.
 *
 * It returns the outcome it reported: undefined for a delivered success, the
 * refusal it sent, or the return capability's own refusal. A message that is
 * no request, or has no return capability, is answered `disconnected`.
 */
export function respond(request: Message, result?: unknown, error?: unknown): PublicError | undefined {
  if (request.frame.kind !== 'request' || !request.return)
    return new PublicError('disconnected', 'The request has no return address.');
  let outcome: PublicError | undefined;
  let payload: { result: unknown } | { error: { code: string; message: string; data?: unknown } };
  try {
    if (error !== undefined) {
      const refused = publicError(error);
      // Retain a valid local cancellation identity for the helper's outcome;
      // only normalized public fields below enter the response frame.
      outcome =
        error instanceof PublicError && error.code === refused.code && error.message === refused.message
          ? error
          : refused;
      payload = snapshot({
        error: {
          code: refused.code,
          message: refused.message,
          ...(refused.data === undefined ? {} : { data: refused.data }),
        },
      });
    } else payload = { result: snapshot(result === undefined ? null : result) };
  } catch {
    outcome = new PublicError('internal', 'Response could not be encoded');
    payload = { error: { code: 'internal', message: 'Response could not be encoded' } };
  }
  const frame = {
    version: 1,
    kind: 'response',
    id: request.frame.id,
    ...payload,
    ...traceMembers(traceOf(request.frame)),
  } as ProfileFrame;
  try {
    request.return.wire.send([], { frame });
  } catch (error) {
    if (error instanceof PublicError && ['invalid_message', 'frame_too_large'].includes(error.code)) {
      outcome = new PublicError('internal', 'Response could not be encoded');
      try {
        request.return.wire.send([], {
          frame: {
            version: 1,
            kind: 'response',
            id: request.frame.id,
            error: { code: 'internal', message: 'Response could not be encoded' },
            ...traceMembers(traceOf(request.frame)),
          },
        });
      } catch {
        /* The return address cannot admit even the bounded error response. */
      }
    } else outcome ??= publicError(error);
    /* The caller may already have cancelled or ended. */
  }
  return outcome;
}
