import type { AddressedWire, Endpoint, Message, Path, ProfileFrame, Receiver, ReturnAddress } from '@bitspark/bitwire';
import { PublicError, ReceiverExistsError } from './error.ts';
import { Invocation, defaultInvocationLimits } from './invocation.ts';
import {
  dispatchContext,
  eventContext,
  setDispatchContext,
  withEventContext,
  type DispatchContext,
  type ReceivedEventContext,
  type ReceivedRequestContext,
} from './internal/context.ts';
import { ended, profileFrame, publicError } from './internal/frame.ts';
import { LIMIT_DEFAULTS, limits } from './internal/limits.ts';
import { encodePath } from './internal/path.ts';
import { traceMembers, traceOf } from './internal/trace.ts';
import { respond } from './respond.ts';
import { defaultPropagator, type Propagator } from './trace.ts';

/**
 * Bounds a local pair. Absent members take the defaults, which are the
 * protocol engine's: 64 concurrent handlers and 128 pending requests and queued
 * messages per direction, frames of at most 1 MiB, a 30-second request
 * deadline and a 10-second stall deadline for event consumers.
 */
export interface PairOptions {
  /** The requests a direction runs at once; the one past it is refused busy. */
  maxConcurrentHandlers?: number;
  /** The requests a direction holds admitted and unanswered; the one past it is refused busy. */
  maxPendingRequests?: number;
  /** The requests and events a direction holds queued. A full queue ends the pair. */
  queueCapacity?: number;
  /** The envelope each message would travel as. */
  maxFrameBytes?: number;
  /** Answers a request whose handler has not answered by then. */
  requestTimeoutMs?: number;
  /** How long an event consumer may take before the pair ends as stalled. */
  writeTimeoutMs?: number;
  /** Moves a trace between a frame and a handler's context. */
  propagator?: Propagator;
  /** Told why the pair failed, before it ends. */
  onError?: (error: PublicError) => void;
}

interface Registration {
  receiver: Receiver;
}
interface LocalCall {
  id: string;
  original: Message;
  path: Path;
  returning: ReturnAddress;
  invocation: Invocation;
  source?: DispatchContext;
  cleanup: () => void;
  controller: AbortController;
  registration?: Registration;
  timer?: ReturnType<typeof setTimeout>;
  completed: boolean;
  responded: boolean;
  active: boolean;
  cancelQueued: boolean;
  cancelled: boolean;
}
interface Delivery {
  path: Path;
  message: Message;
  call?: LocalCall;
  refusal?: PublicError;
}
// One end of a bounded local pair: the Endpoint it presents, what it owes the
// other end, and the state its own admission keeps.
interface PairEnd {
  wire: Endpoint;
  other: PairEnd;
  queue: Delivery[];
  queued: number;
  retained: number;
  active: number;
  eventTimer?: ReturnType<typeof setTimeout>;
  draining: boolean;
  calls: Map<ReturnAddress, Map<string, LocalCall>>;
  attachment?: Registration;
}

/**
 * A bounded local carrier with two relative origins. Sending on either
 * endpoint delivers asynchronously to the receiver on the other. It allocates
 * no peer, serializes nothing, and preserves structured frames, the original
 * return capability's identity and received context. Each admitted request
 * gets a fresh return capability that carries its invocation lifecycle.
 */
export function pair(options: PairOptions = {}): [Endpoint, Endpoint] {
  const bounds = limits(LIMIT_DEFAULTS, options);
  const propagator = options.propagator ?? defaultPropagator;
  let closed = false;
  const ends: PairEnd[] = [];
  // Every request admitted and not yet answered is answered disconnected, and
  // every refusal still queued is answered with the refusal it was admitted
  // with, so no caller is left to its own deadline.
  const end = (code = 1000, reason = '') => {
    if (closed) return;
    closed = true;
    const receivers: Receiver[] = [],
      requests: Message[] = [],
      refusals: Delivery[] = [];
    for (const endpoint of ends) {
      clearTimeout(endpoint.eventTimer);
      if (endpoint.attachment) receivers.push(endpoint.attachment.receiver);
      for (const queued of endpoint.queue) if (queued.refusal) refusals.push(queued);
      for (const calls of endpoint.calls.values())
        for (const call of calls.values()) {
          clearTimeout(call.timer);
          call.controller.abort();
          call.cleanup();
          call.invocation.settle();
          call.invocation.dispatchDone();
          if (!call.responded) requests.push(call.original);
          call.responded = call.completed = true;
        }
      delete endpoint.attachment;
      endpoint.calls.clear();
      endpoint.queue.length = endpoint.queued = endpoint.retained = endpoint.active = 0;
    }
    queueMicrotask(() => {
      for (const receiver of receivers) {
        try {
          receiver.closed?.(code, reason);
        } catch {
          /* Every receiver gets closure. */
        }
      }
      for (const refused of refusals) respond(refused.message, undefined, refused.refusal);
      for (const request of requests) respond(request, undefined, ended());
    });
  };
  const fail = (error: PublicError) => {
    try {
      options.onError?.(error);
    } catch {
      /* Diagnostics do not interrupt closure. */
    }
    end(4011, error.message);
  };
  // Frees a call's pending slot once it is answered and no already queued
  // cancellation still owns its reservation. It never removes a newer
  // admission that reused the same return identity and identifier.
  const retire = (endpoint: PairEnd, call: LocalCall) => {
    if (!call.completed || call.cancelQueued) return;
    const calls = endpoint.calls.get(call.original.return!);
    if (calls?.get(call.id) !== call) return;
    calls.delete(call.id);
    if (!calls.size) endpoint.calls.delete(call.original.return!);
    endpoint.retained--;
    call.cleanup();
    call.invocation.settle();
    call.invocation.dispatchDone();
  };
  const complete = (endpoint: PairEnd, call: LocalCall) => {
    if (!call.completed) {
      call.completed = true;
      clearTimeout(call.timer);
      call.controller.abort();
      if (call.active) {
        endpoint.active--;
        call.active = false;
      }
    }
    retire(endpoint, call);
  };

  const invoke = (registration: Registration, path: Path, message: Message): void | Promise<void> => {
    try {
      return registration.receiver.message!(path, message);
    } catch (error) {
      if (message.frame.kind === 'request') respond(message, undefined, publicError(error));
      else fail(publicError(error));
    }
  };
  const deliver = async (endpoint: PairEnd) => {
    try {
      while (endpoint.queue.length && !closed) {
        const { path, message, call, refusal } = endpoint.queue.shift()!;
        const frame = message.frame;
        if (frame.kind === 'cancel') {
          call!.cancelQueued = false;
          call!.cancelled = true;
          call!.controller.abort();
          if (!call!.completed && call!.registration) {
            const pending = invoke(call!.registration, path, message);
            if (pending) void pending.catch((error: unknown) => fail(publicError(error)));
          }
          retire(endpoint, call!);
          continue;
        }
        endpoint.queued--;
        if (refusal) {
          respond(message, undefined, refusal);
          continue;
        }
        const registration = endpoint.attachment?.receiver.message ? endpoint.attachment : undefined;
        if (frame.kind === 'event') {
          if (registration) {
            let delivered = message;
            if (!eventContext(message)) {
              const context: ReceivedEventContext = {};
              propagator.extract(context, traceOf(frame));
              delivered = withEventContext(message, context);
            }
            endpoint.eventTimer = setTimeout(() => {
              fail(new PublicError('stalled_consumer', 'Local wire event handler deadline exceeded.'));
            }, bounds.writeTimeoutMs);
            try {
              await invoke(registration, path, delivered);
            } catch (error) {
              fail(publicError(error));
            } finally {
              clearTimeout(endpoint.eventTimer);
              endpoint.eventTimer = undefined;
            }
          }
          continue;
        }
        if (frame.kind !== 'request') continue;
        if (!registration || endpoint.active >= bounds.maxConcurrentHandlers) {
          respond(
            message,
            undefined,
            new PublicError(
              registration ? 'busy' : 'method_not_found',
              registration ? 'Incoming request limit reached.' : 'Unknown method.',
            ),
          );
          continue;
        }
        endpoint.active++;
        call!.active = true;
        call!.registration = registration;
        const context = Object.create(call!.source?.context ?? null) as ReceivedRequestContext;
        Object.defineProperties(context, {
          signal: {
            value: call!.source
              ? AbortSignal.any([call!.controller.signal, call!.source.context.signal])
              : call!.controller.signal,
            enumerable: true,
          },
          requestId: { value: call!.source?.context.requestId ?? frame.id, enumerable: true },
          ...(frame.meta ? { meta: { value: { ...frame.meta }, enumerable: true } } : {}),
        });
        if (!call!.source) propagator.extract(context, traceOf(frame));
        call!.cleanup = setDispatchContext(call!.returning, {
          context,
          completion: call!.source?.completion,
          maxFrameBytes: bounds.maxFrameBytes,
        });
        call!.timer = setTimeout(() => {
          if (closed || call!.completed) return;
          call!.controller.abort();
          if (!call!.cancelQueued && !call!.cancelled) {
            call!.cancelQueued = true;
            endpoint.queue.push({
              path,
              message: { frame: { version: 1, kind: 'cancel', id: frame.id }, return: call!.returning },
              call,
            });
            schedule(endpoint);
          }
          // A timeout answers once but retains the handler slot until its
          // actual response, bounding applications that ignore cancellation.
          if (!call!.responded) {
            call!.responded = true;
            respond(call!.original, undefined, new PublicError('cancelled', 'Request deadline exceeded.'));
          }
        }, bounds.requestTimeoutMs);
        const pending = invoke(registration, path, message);
        if (pending) void pending.catch((error: unknown) => respond(message, undefined, publicError(error)));
      }
    } finally {
      endpoint.draining = false;
      if (endpoint.queue.length && !closed) schedule(endpoint);
    }
  };
  const schedule = (endpoint: PairEnd) => {
    if (endpoint.draining || closed) return;
    endpoint.draining = true;
    queueMicrotask(() => {
      void deliver(endpoint);
    });
  };
  const admit = (endpoint: PairEnd, path: Path, original: Message) => {
    if (closed) throw ended();
    const name = encodePath(path),
      frame = profileFrame(original.frame, name, bounds.maxFrameBytes);
    if (frame.kind !== 'request' && frame.kind !== 'event' && frame.kind !== 'cancel')
      throw new PublicError('invalid_message', "A response is sent to its request's return address.");
    if (frame.kind !== 'event' && !original.return?.wire)
      throw new PublicError('invalid_message', 'A wire request or cancellation requires a return address.');
    const message: Message = { frame, return: original.return };
    let call: LocalCall | undefined, refusal: PublicError | undefined;
    if (frame.kind === 'cancel') {
      call = endpoint.calls.get(message.return!)?.get(frame.id);
      if (!call || call.completed || call.cancelQueued || call.cancelled) return;
      call.cancelQueued = true;
    } else {
      if (endpoint.queued >= bounds.queueCapacity) {
        // A full queue ends the carrier; the refused send reports it ended.
        const error = new PublicError('busy', 'Local wire queue limit reached.');
        fail(error);
        throw ended(error);
      }
      endpoint.queued++;
      if (frame.kind === 'request') {
        let calls = endpoint.calls.get(message.return!);
        if (calls?.has(frame.id)) refusal = new PublicError('invalid_message', 'Duplicate active request identifier.');
        else if (endpoint.retained >= bounds.maxPendingRequests)
          refusal = new PublicError('busy', 'Outstanding call limit reached.');
        else {
          const invocation = new Invocation(defaultInvocationLimits());
          const returning: AddressedWire = {
            send: (suffix, reply) => {
              if (suffix.length) {
                invocation.deliver(suffix, reply);
                return;
              }
              if (reply.frame.kind !== 'response' || reply.frame.id !== frame.id)
                throw new PublicError('invalid_message', 'Invalid wire response.');
              // A refused encoding may be retried as the shared bounded
              // internal-error fallback; only an admitted response completes.
              const checked = profileFrame(reply.frame, '', bounds.maxFrameBytes);
              if (call!.responded || call!.completed) {
                // A deadline already answered the caller. This is the handler's
                // actual response, which is what releases its budget.
                complete(endpoint, call!);
                throw ended();
              }
              call!.responded = true;
              // Retire before the caller can hold its answer: a caller that
              // issues its next call as soon as this one returns must find the
              // slot free. A queued cancellation keeps the reservation until
              // it drains.
              complete(endpoint, call!);
              invocation.settle();
              try {
                message.return!.wire.send([], { frame: checked });
              } catch (error) {
                // The caller's own return capability can refuse what this pair
                // admitted, such as a reply over a smaller frame limit further
                // back. The call is already answered here, so the caller gets
                // the bounded internal error directly rather than waiting for
                // its deadline.
                try {
                  message.return!.wire.send([], {
                    frame: {
                      version: 1,
                      kind: 'response',
                      id: frame.id,
                      error: { code: 'internal', message: 'Response could not be encoded' },
                      ...traceMembers(traceOf(checked)),
                    } as ProfileFrame,
                  });
                } catch {
                  /* The caller cannot take even the bounded answer. */
                }
                throw error instanceof PublicError ? publicError(error) : error;
              }
            },
          };
          call = {
            id: frame.id,
            original: message,
            path: [...path],
            returning: { wire: returning },
            invocation,
            source: dispatchContext(message.return),
            cleanup: () => {},
            controller: new AbortController(),
            completed: false,
            responded: false,
            active: false,
            cancelQueued: false,
            cancelled: false,
          };
          if (!calls) {
            calls = new Map();
            endpoint.calls.set(message.return!, calls);
          }
          calls.set(frame.id, call);
          endpoint.retained++;
        }
      }
    }
    endpoint.queue.push({
      path: [...path],
      message: call ? { frame, return: call.returning } : message,
      call,
      refusal,
    });
    schedule(endpoint);
  };
  for (let i = 0; i < 2; i++) {
    const endpoint = {
      queue: [],
      queued: 0,
      retained: 0,
      active: 0,
      draining: false,
      calls: new Map(),
    } as unknown as PairEnd;
    endpoint.wire = {
      send: (path, message) => admit(endpoint.other, path, message),
      receive: (receiver) => {
        if (closed) throw ended();
        if (endpoint.attachment) throw new ReceiverExistsError();
        const registration = { receiver };
        endpoint.attachment = registration;
        return () => {
          if (endpoint.attachment === registration) delete endpoint.attachment;
        };
      },
      // Nothing is transmitted in-process, so any code closes the pair as an
      // abort would: its receivers are told the code and the reason.
      close: end,
    };
    ends.push(endpoint);
  }
  ends[0]!.other = ends[1]!;
  ends[1]!.other = ends[0]!;
  return [ends[0]!.wire, ends[1]!.wire];
}
