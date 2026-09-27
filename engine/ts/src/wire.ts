import type { AddressedWire, Endpoint, Message, Path, Receiver, ReturnAddress } from '@bitspark/bitwire';
import { PublicError, ReceiverExistsError } from '../../../core/ts/src/error.ts';
import type { Meta } from '../../../core/ts/src/meta.ts';
import { respond } from '../../../core/ts/src/respond.ts';
import type { Trace } from '../../../core/ts/src/trace.ts';
import {
  receivedEventTrace,
  withEventContext,
  type ReceivedEventContext,
  type ReceivedRequestContext,
} from '../../../core/ts/src/internal/context.ts';
import { ended, outgoingTrace, profileFrame, publicError } from '../../../core/ts/src/internal/frame.ts';
import { decodePath, encodePath } from '../../../core/ts/src/internal/path.ts';
import { isApplicationReturn, request } from '../../../core/ts/src/internal/request.ts';
import { traceMembers } from '../../../core/ts/src/internal/trace.ts';
import { validCloseReason } from '../../../transports/ts/src/index.ts';
import type { Peer } from './peer.ts';

/**
 * The body the peer runs for an incoming request whose method names a path,
 * given the trace members its frame arrived with: what a propagator placed on
 * the context is its own, and never rewrites the forwarded frame.
 */
export type RequestHandler = (params: unknown, context: ReceivedRequestContext, received?: Trace) => Promise<unknown>;
/** The listener the peer runs for an incoming event whose name names a path. */
export type EventHandler = (name: string, data: unknown, context: ReceivedEventContext) => void | Promise<void>;

/** Hooks that retain all carrier ownership in the peer. */
export interface RootOptions {
  queueCapacity: number;
  maxPendingRequests: number;
  maxFrameBytes: number;
  requestTimeoutMs: number;
  call: (method: string, params: unknown, options: { signal?: AbortSignal; meta?: Meta }, trace?: Trace) => Promise<unknown>;
  emit: (name: string, data: unknown, options: { meta?: Meta }, trace?: Trace) => Promise<void>;
  dispatch: (request: (method: string) => RequestHandler | undefined, event: (name: string) => EventHandler | undefined) => void;
  fail: (error: PublicError) => void;
  close: (code: number, reason: string) => void;
}

interface RoutedCall {
  address: ReturnAddress;
  id: string;
  controller: AbortController;
  completed: boolean;
  cancelQueued: boolean;
  cancelled: boolean;
}
interface RoutedDelivery {
  path: Path;
  message: Message;
  call?: RoutedCall;
  refusal?: PublicError;
}
interface Registration {
  receiver: Receiver;
  request: (path: Path, params: unknown, context: ReceivedRequestContext, received?: Trace) => Promise<unknown>;
  detach: () => void;
}

/**
 * The peer's addressed origin; one per peer, and selection never constructs
 * another. Outgoing requests, events and cancellations queue here in admission
 * order and are handed to the peer one at a time. What the remote side sends
 * is delivered to the one receiver with the path the frame's name encodes; a
 * name that encodes no path, or arrives while nothing is attached, is what a
 * peer without that handler answers.
 */
export function rootWire(peer: Peer, options: RootOptions): Endpoint {
  const queued: RoutedDelivery[] = [];
  const incoming = new Map<ReturnAddress, Map<string, RoutedCall>>();
  let attachment: Registration | undefined;
  const lookup = (name: string): { path: Path; registration: Registration } | undefined => {
    let path: Path;
    try {
      path = decodePath(name);
    } catch {
      return;
    }
    return attachment ? { path, registration: attachment } : undefined;
  };
  // The receiver chosen when a frame arrives stays with it, so a later detach
  // or replacement cannot redirect its cancellation.
  options.dispatch(
    (name) => {
      const found = lookup(name);
      return found
        ? (params, context, received) => found.registration.request(found.path, params, context, received)
        : undefined;
    },
    (name) => {
      const found = lookup(name);
      return found
        ? (_name, data, context) => {
            const received = receivedEventTrace(context);
            return found.registration.receiver.message?.(
              found.path,
              withEventContext(
                {
                  frame: {
                    version: 1,
                    kind: 'event',
                    data,
                    ...traceMembers(received),
                    ...(context.meta ? { meta: context.meta } : {}),
                  },
                },
                context,
              ),
            );
          }
        : undefined;
    },
  );
  let retained = 0,
    dataQueued = 0,
    scheduled = false,
    closed = false;
  const retire = (call: RoutedCall) => {
    // A completed call still owns a queued control slot. Reusing its budget
    // early would let fast completions accumulate unbounded stale cancels.
    if (!call.completed || call.cancelQueued) return;
    const calls = incoming.get(call.address);
    if (calls?.get(call.id) !== call) return;
    calls.delete(call.id);
    if (!calls.size) incoming.delete(call.address);
    retained--;
  };
  peer.onClose((reason) => {
    closed = true;
    // Every request still queued is owed an answer: a queued refusal its
    // refusal, an admitted request that never reached the peer disconnected.
    // A request already handed to the peer is answered by its own waiter,
    // which the peer's end releases.
    for (const delivery of queued.splice(0))
      if (delivery.message.frame.kind === 'request')
        respond(delivery.message, undefined, delivery.refusal ?? ended(reason));
    for (const calls of incoming.values()) for (const call of calls.values()) call.controller.abort();
    incoming.clear();
    retained = 0;
    dataQueued = 0;
    const ending = attachment ? [attachment] : [];
    for (const { detach } of ending) detach();
    for (const { receiver } of ending) {
      try {
        receiver.closed?.(1001, 'peer ended');
      } catch {
        /* One receiver cannot interrupt another's cleanup. */
      }
    }
  });
  const drain = () => {
    scheduled = false;
    while (queued.length && !closed) {
      const { path, message, call, refusal } = queued.shift()!;
      const frame = message.frame;
      if (frame.kind === 'cancel') {
        call!.cancelQueued = false;
        call!.cancelled = true;
        if (!call!.completed) call!.controller.abort();
        retire(call!);
        continue;
      }
      dataQueued--;
      if (refusal) {
        respond(message, undefined, refusal);
        continue;
      }
      const name = encodePath(path);
      if (frame.kind === 'event') {
        // Invoke admission now, in wire order; only completion is asynchronous.
        void options
          .emit(name, frame.data, { meta: frame.meta ? { ...frame.meta } : undefined }, outgoingTrace(frame))
          .catch((error: unknown) => {
            // A peer whose connection is closing is ending already; its close
            // brings the remote's code and reason, which failing here would lose.
            const refused = publicError(error);
            if (refused.code !== 'disconnected') options.fail(refused);
          });
        continue;
      }
      if (frame.kind !== 'request') continue;
      // The peer allocates the carrier id and enqueues before it returns.
      const pending = options.call(
        name,
        frame.params,
        { signal: call!.controller.signal, meta: frame.meta ? { ...frame.meta } : undefined },
        outgoingTrace(frame),
      );
      const finish = (value?: unknown, error?: unknown) => {
        // Retire before delivering the response: its callback can admit
        // another request, but a queued cancellation still owns budget.
        call!.completed = true;
        retire(call!);
        // The peer's deadline is its caller's (bitwire/1): an application's
        // own call gets request_timeout, and a return that may cross a wire is
        // answered cancelled, since a request_timeout is never a frame.
        if (error instanceof PublicError && error.code === 'request_timeout' && !isApplicationReturn(message.return?.wire))
          error = new PublicError('cancelled', 'Request cancelled');
        respond(message, value, error);
      };
      void pending.then(
        (value) => finish(value),
        (error: unknown) => finish(undefined, error),
      );
    }
  };
  const connected = () => !closed && peer.status === 'connected';
  const endpoint: Endpoint = {
    send: (path, message) => {
      const name = encodePath(path);
      if (!connected()) throw ended();
      const frame = profileFrame(message.frame, name, options.maxFrameBytes);
      if (!name && (frame.kind === 'request' || frame.kind === 'event'))
        throw new PublicError('invalid_message', 'A root wire operation requires a nonempty path.');
      if ((frame.kind === 'request' || frame.kind === 'cancel') && !message.return?.wire)
        throw new PublicError('invalid_message', 'A wire request or cancellation requires a return address.');
      if (frame.kind !== 'request' && frame.kind !== 'event' && frame.kind !== 'cancel')
        throw new PublicError('invalid_message', "A response is sent to its request's return address.");
      let call: RoutedCall | undefined;
      if (frame.kind === 'cancel') {
        call = incoming.get(message.return!)?.get(frame.id);
        // Cancellation belongs to an already admitted request. Its one control
        // reservation is bounded by the existing pending-request budget.
        if (!call || call.completed || call.cancelQueued || call.cancelled) return;
      }
      if (!connected()) throw ended();
      if (frame.kind !== 'cancel' && dataQueued >= options.queueCapacity) {
        // bitwire/1: a full root queue ends the carrier, and the refused send
        // reports the carrier ended.
        const error = new PublicError('busy', 'Output consumer is stalled; queue limit reached.');
        options.fail(error);
        throw ended(error);
      }
      let refusal: PublicError | undefined;
      if (frame.kind === 'cancel') call!.cancelQueued = true;
      else {
        dataQueued++;
        if (frame.kind === 'request') {
          let calls = incoming.get(message.return!);
          if (calls?.has(frame.id) || retained >= options.maxPendingRequests) {
            refusal = new PublicError(calls?.has(frame.id) ? 'invalid_message' : 'busy', 'Outstanding wire call refused.');
          } else {
            if (!calls) {
              calls = new Map();
              incoming.set(message.return!, calls);
            }
            call = {
              address: message.return!,
              id: frame.id,
              controller: new AbortController(),
              completed: false,
              cancelQueued: false,
              cancelled: false,
            };
            calls.set(frame.id, call);
            retained++;
          }
        }
      }
      // Data and reserved control entries share one FIFO. A cancel cannot jump
      // ahead of an earlier event, request, or cancellation on this wire.
      queued.push({ path: [...path], message: { frame, return: message.return }, call, refusal });
      if (!scheduled) {
        scheduled = true;
        queueMicrotask(drain);
      }
    },
    receive: (receiver) => {
      if (closed) throw ended();
      if (attachment) throw new ReceiverExistsError();
      const target: AddressedWire = {
        send: (suffix, message) => {
          try {
            if (!receiver.message) {
              respond(message, undefined, new PublicError('method_not_found', 'Unknown method.'));
              return;
            }
            const result = receiver.message(suffix, message);
            if (result) void result.catch((error: unknown) => respond(message, undefined, publicError(error)));
          } catch (error) {
            respond(message, undefined, publicError(error));
          }
        },
      };
      const handler = (path: Path, params: unknown, context: ReceivedRequestContext, received?: Trace) =>
        request(
          target,
          path,
          params,
          { signal: context.signal, timeoutMs: options.requestTimeoutMs, meta: context.meta },
          received,
          { context, maxFrameBytes: options.maxFrameBytes },
        );
      const registration: Registration = {
        receiver,
        request: handler,
        detach: () => {
          if (attachment === registration) attachment = undefined;
        },
      };
      attachment = registration;
      return registration.detach;
    },
    close: (code = 1000, reason = '') => {
      // Refused before the peer ends: its connection would refuse the reason
      // too, and leave the peer ended over a connection still open.
      if (!validCloseReason(reason))
        throw new RangeError('bitruntime: a close reason is valid UTF-8 of at most 123 bytes');
      options.close(code, reason);
    },
  };
  return endpoint;
}
