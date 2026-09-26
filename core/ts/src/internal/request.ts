/**
 * The request primitive shared by the public call helper and the protocol
 * engine's inbound bridge: one request sent through addressed access with a
 * fresh return capability that carries its invocation lifecycle, and the wait
 * for its one response. Module-private: no package subpath exports it.
 */
import type { AddressedWire, Path, ProfileFrame, ReturnAddress } from '@bitspark/bitwire';
import { PublicError, UnpublishedError } from '../error.ts';
import { Invocation, defaultInvocationLimits } from '../invocation.ts';
import type { Meta } from '../meta.ts';
import type { Trace } from '../trace.ts';
import { clearDispatchContext, setDispatchContext, type DispatchContext } from './context.ts';
import { carrying } from './envelope.ts';
import { ended, profileFrame, setOutgoingTrace, snapshot } from './frame.ts';
import { encodePath } from './path.ts';
import { traceMembers } from './trace.ts';

/** How long a call waits for its response when it states no deadline of its own. */
export const DEFAULT_TIMEOUT_MS = 30_000;

/** The completion and deadline of one call. */
export function requestCompletion<T>() {
  let settled = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let detach: (() => void) | undefined;
  let resolve!: (value: T) => void;
  let reject!: (error: PublicError) => void;
  const promise = new Promise<T>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  const cleanup = () => {
    clearTimeout(timer);
    detach?.();
    detach = undefined;
  };
  return {
    promise,
    get settled() {
      return settled;
    },
    cleanup,
    resolve: (value: T) => {
      if (!settled) {
        settled = true;
        cleanup();
        resolve(value);
      }
    },
    reject: (error: PublicError) => {
      if (!settled) {
        settled = true;
        cleanup();
        reject(error);
      }
    },
    wait: (
      signal: AbortSignal | undefined,
      timeoutMs: number,
      method: string,
      cancel: (error: PublicError, outcome: 'timeout' | 'cancelled') => void,
    ) => {
      if (settled) return;
      timer = setTimeout(
        () =>
          cancel(
            new PublicError('request_timeout', `Call ${method} timed out; its outcome may be unknown.`),
            'timeout',
          ),
        timeoutMs,
      );
      if (signal) {
        const abort = () =>
          cancel(new PublicError('cancelled', 'Call was cancelled; its outcome may be unknown.'), 'cancelled');
        signal.addEventListener('abort', abort, { once: true });
        detach = () => signal.removeEventListener('abort', abort);
        if (signal.aborted) abort();
      }
    },
  };
}

/** What one call may carry. */
export interface RequestOptions {
  signal?: AbortSignal;
  timeoutMs?: number;
  meta?: Meta;
}

/**
 * Sends one request at path through access and waits for its response.
 * `dispatch` is the context a carrier established when this call forwards a
 * request it admitted; it is absent for an application's own call, which a
 * trace minted for it identifies.
 */
export function request<T>(
  access: AddressedWire,
  path: Path,
  params: unknown,
  options: RequestOptions,
  trace?: Trace,
  dispatch?: DispatchContext,
): Promise<T> {
  let name: string;
  let frame: ProfileFrame;
  try {
    name = encodePath(path);
    if (options.timeoutMs !== undefined && (!Number.isSafeInteger(options.timeoutMs) || options.timeoutMs <= 0))
      throw new PublicError('invalid_options', 'timeoutMs must be a positive safe integer.');
    if (options.signal?.aborted) throw new PublicError('cancelled', 'Call was cancelled before sending.');
    frame = snapshot(
      carrying({ version: 1, kind: 'request', id: 'c:1', params, ...traceMembers(trace) }, options.meta),
    ) as unknown as ProfileFrame;
  } catch (error) {
    return Promise.reject(new UnpublishedError(error));
  }
  if (!dispatch && trace) setOutgoingTrace(frame, trace);
  const completion = requestCompletion<T>();
  if (dispatch) dispatch = { ...dispatch, completion: { cancelled: false } };
  const invocation = new Invocation(defaultInvocationLimits());
  const returning: AddressedWire = {
    send: (suffix, message) => {
      if (suffix.length) {
        invocation.deliver(suffix, message);
        return;
      }
      let frame: ProfileFrame;
      try {
        frame = profileFrame(message.frame, '', dispatch?.maxFrameBytes);
      } catch (error) {
        // A request a carrier admitted must not be left waiting: a reply that
        // cannot travel settles as the bounded internal error, which the
        // carrier sends itself or ends its connection over, as its own
        // response path does.
        if (!dispatch || message.frame.kind !== 'response' || message.frame.id !== 'c:1') throw error;
        if (completion.settled) throw ended();
        completion.reject(new PublicError('internal', 'Response could not be encoded'));
        invocation.settle();
        return;
      }
      if (frame.kind !== 'response' || frame.id !== 'c:1')
        throw new PublicError('invalid_message', 'Invalid wire response.');
      if (completion.settled) throw ended();
      if (frame.error?.code === 'cancelled' && dispatch?.context.signal.aborted && dispatch.completion?.cancelled)
        completion.resolve(undefined as T);
      else if (frame.error) completion.reject(new PublicError(frame.error.code, frame.error.message, frame.error.data));
      else completion.resolve(frame.result as T);
      invocation.settle();
    },
  };
  const address: ReturnAddress = { wire: returning };
  const retire = () => {
    clearDispatchContext(address);
    invocation.settle();
    invocation.dispatchDone();
  };
  if (dispatch) setDispatchContext(address, dispatch);
  void completion.promise.then(retire, retire);
  try {
    access.send(path, { frame, return: address });
  } catch (error) {
    completion.reject(new UnpublishedError(error));
  }
  completion.wait(options.signal, options.timeoutMs ?? DEFAULT_TIMEOUT_MS, name, (error) => {
    if (completion.settled) return;
    completion.cleanup();
    // An incoming dispatch occupies the carrier's handler budget until the
    // receiver replies. Its cancellation ends the caller's wait elsewhere;
    // settling this promise here would release a still-executing body.
    if (!dispatch) completion.reject(error);
    try {
      access.send(path, { frame: { version: 1, kind: 'cancel', id: 'c:1', ...traceMembers(trace) }, return: address });
    } catch {
      /* Cancellation is best effort and never extends the caller's wait. */
    }
  });
  return completion.promise;
}
