import { encodePath, WireError } from '@nightseam/duplex';
import type { Message, Path, Receiver, ReturnAddress, Wire, Endpoint } from '@nightseam/duplex';
import { Invocation, defaultInvocationLimits } from './invocation.ts';
import { DUPLEX_DEFAULTS, positiveInteger } from './peer.ts';
import type { PeerOptions } from './peer.ts';
import { DuplexError } from './error.ts';
import { defaultPropagator, traceOf } from './trace.ts';
import type { ObserverEvent } from './observer.ts';
import {
  profileFrame,
  publicError,
  response,
  wireContext,
  setWireContext,
  wireEventContext,
  withWireEventContext,
} from './wire.ts';
import type { WireDispatchContext, WireRequestContext, WireEventContext } from './wire.ts';

interface Registration {
  receiver: Receiver;
}
interface LocalCall {
  id: string;
  original: Message;
  path: Path;
  returning: ReturnAddress;
  invocation: Invocation;
  source?: WireDispatchContext;
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
  refusal?: DuplexError;
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
 * A bounded local carrier. Sending on one endpoint delivers asynchronously to
 * receivers on the other. No Peer, transport connection, or request protocol
 * is constructed; the runtime's existing return capability carries responses.
 */
export function wirePair(options: PeerOptions = {}): [Endpoint, Endpoint] {
  const limits = { ...DUPLEX_DEFAULTS };
  for (const key of Object.keys(DUPLEX_DEFAULTS) as (keyof typeof DUPLEX_DEFAULTS)[]) {
    if (options[key] !== undefined) {
      positiveInteger(options[key], key, true);
      (limits as Record<string, number>)[key] = options[key]!;
    }
  }
  const propagator = options.propagator ?? defaultPropagator;
  let closed = false;
  const ends: PairEnd[] = [];
  const disconnected = () => new DuplexError('disconnected', 'Connection ended; outcome may be unknown.');
  const observe = (event: ObserverEvent) => {
    try {
      options.observer?.observe(event);
    } catch {
      /* Observers own their failures. */
    }
  };
  const end = (code = 1000, reason = '') => {
    if (closed) return;
    closed = true;
    const receivers: Receiver[] = [],
      requests: Message[] = [];
    for (const endpoint of ends) {
      clearTimeout(endpoint.eventTimer);
      if (endpoint.attachment) receivers.push(endpoint.attachment.receiver);
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
    observe({ type: 'connection.closed', at: new Date(), code, reason, local: true });
    queueMicrotask(() => {
      for (const receiver of receivers) {
        try {
          receiver.closed?.(code, reason);
        } catch {
          /* Every receiver gets closure. */
        }
      }
      for (const request of requests) response(request, undefined, disconnected());
    });
  };
  const fail = (error: DuplexError) => {
    try {
      options.onError?.(error);
    } catch {
      /* Diagnostics do not interrupt closure. */
    }
    end(4011, error.message);
  };
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
      if (message.frame.kind === 'request') response(message, undefined, publicError(error));
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
          response(message, undefined, refusal);
          continue;
        }
        const registration = endpoint.attachment?.receiver.message ? endpoint.attachment : undefined;
        if (frame.kind === 'event') {
          if (registration) {
            let delivered = message;
            if (!wireEventContext(message)) {
              const context: WireEventContext = { wire: endpoint.wire };
              propagator.extract(context, traceOf(frame));
              delivered = withWireEventContext(message, context);
            }
            endpoint.eventTimer = setTimeout(() => {
              observe({
                type: 'backpressure',
                at: new Date(),
                queued: endpoint.queued,
                stalled: true,
                deadlineMs: limits.writeTimeoutMs,
              });
              fail(new DuplexError('stalled_consumer', 'Local wire event handler deadline exceeded.'));
            }, limits.writeTimeoutMs);
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
        if (!registration || endpoint.active >= limits.maxConcurrentHandlers) {
          response(
            message,
            undefined,
            new DuplexError(
              registration ? 'busy' : 'method_not_found',
              registration ? 'Incoming request limit reached.' : 'Unknown method.',
            ),
          );
          continue;
        }
        endpoint.active++;
        call!.active = true;
        call!.registration = registration;
        const context = Object.create(call!.source?.context ?? null) as WireRequestContext;
        Object.defineProperties(context, {
          wire: { value: endpoint.wire, enumerable: true },
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
        call!.cleanup = setWireContext(call!.returning, {
          context,
          completion: call!.source?.completion,
          maxFrameBytes: limits.maxFrameBytes,
          panic:
            call!.source?.panic ??
            ((error) =>
              observe({
                type: 'handler.panic',
                at: new Date(),
                method: encodePath(path),
                value: String(error),
                trace: traceOf(frame),
                family: options.families?.[encodePath(path)] ?? '',
              })),
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
            response(call!.original, undefined, new DuplexError('cancelled', 'Request deadline exceeded.'));
          }
        }, limits.requestTimeoutMs);
        const pending = invoke(registration, path, message);
        if (pending) void pending.catch((error: unknown) => response(message, undefined, publicError(error)));
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
    if (closed) throw disconnected();
    const name = encodePath(path),
      frame = profileFrame(original.frame, name, limits.maxFrameBytes);
    if (frame.kind !== 'request' && frame.kind !== 'event' && frame.kind !== 'cancel')
      throw new DuplexError('invalid_message', "A response is sent to its request's return address.");
    if (frame.kind !== 'event' && !original.return?.wire)
      throw new DuplexError('invalid_message', 'A wire request or cancellation requires a return address.');
    const message: Message = { frame, return: original.return };
    let call: LocalCall | undefined, refusal: DuplexError | undefined;
    if (frame.kind === 'cancel') {
      call = endpoint.calls.get(message.return!)?.get(frame.id);
      if (!call || call.completed || call.cancelQueued || call.cancelled) return;
      call.cancelQueued = true;
    } else {
      if (endpoint.queued >= limits.queueCapacity) {
        const error = new DuplexError('busy', 'Local wire queue limit reached.');
        observe({
          type: 'backpressure',
          at: new Date(),
          queued: endpoint.queued,
          stalled: true,
          deadlineMs: limits.writeTimeoutMs,
        });
        fail(error);
        throw error;
      }
      endpoint.queued++;
      if (frame.kind === 'request') {
        let calls = endpoint.calls.get(message.return!);
        if (calls?.has(frame.id)) refusal = new DuplexError('invalid_message', 'Duplicate active request identifier.');
        else if (endpoint.retained >= limits.maxPendingRequests)
          refusal = new DuplexError('busy', 'Outstanding call limit reached.');
        else {
          const invocation = new Invocation(defaultInvocationLimits());
          const returning: ReturnAddress = {
            wire: {
              send: (suffix, reply) => {
                if (suffix.length) {
                  invocation.deliver(suffix, reply);
                  return;
                }
                if (reply.frame.kind !== 'response' || reply.frame.id !== frame.id)
                  throw new DuplexError('invalid_message', 'Invalid wire response.');
                // A refused encoding may be retried as the shared bounded
                // internal-error fallback; only an admitted response completes.
                const checked = profileFrame(reply.frame, '', limits.maxFrameBytes);
                try {
                  if (call!.responded || call!.completed) throw disconnected();
                  call!.responded = true;
                  try {
                    message.return!.wire.send([], { frame: checked });
                  } catch (error) {
                    throw error instanceof DuplexError ? publicError(error) : error;
                  }
                  invocation.settle();
                } finally {
                  complete(endpoint, call!);
                }
              },
            },
          };
          call = {
            id: frame.id,
            original: message,
            path: [...path],
            returning,
            invocation,
            source: wireContext(message.return!),
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
        if (closed) throw disconnected();
        if (endpoint.attachment) throw new WireError('receiver_exists');
        const registration = { receiver };
        endpoint.attachment = registration;
        return () => {
          if (endpoint.attachment === registration) delete endpoint.attachment;
        };
      },
      close: end,
    };
    ends.push(endpoint);
  }
  ends[0]!.other = ends[1]!;
  ends[1]!.other = ends[0]!;
  return [ends[0]!.wire, ends[1]!.wire];
}
