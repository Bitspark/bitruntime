import type { AddressedWire, Path, ProfileFrame, ReturnAddress } from '@bitspark/bitwire';
import { PublicError, UnpublishedError } from '../../../core/ts/src/error.ts';
import { InvocationError, beginInvocationBody } from '../../../core/ts/src/invocation.ts';
import type { Meta } from '../../../core/ts/src/meta.ts';
import { respond } from '../../../core/ts/src/respond.ts';
import { defaultPropagator, type Propagator, type TraceContext } from '../../../core/ts/src/trace.ts';
import { dispatchContext, eventContext } from '../../../core/ts/src/internal/context.ts';
import { carrying } from '../../../core/ts/src/internal/envelope.ts';
import { setOutgoingTrace, snapshot } from '../../../core/ts/src/internal/frame.ts';
import { encodePath } from '../../../core/ts/src/internal/path.ts';
import { request } from '../../../core/ts/src/internal/request.ts';
import { traceMembers, traceOf } from '../../../core/ts/src/internal/trace.ts';
import type { Registry } from './dispatcher.ts';

/**
 * What a request handler is given beside the params, independent of the
 * carrier: the registry it was registered on, a signal that fires when the
 * caller withdraws or a deadline passes, the request's id, and the trace and
 * meta the request brought. Values a carrier established privately for the
 * request — an authenticated identity a propagator placed there — are reached
 * through the context's prototype; the carrier itself is not.
 */
export interface RequestContext extends TraceContext {
  wire: AddressedWire;
  signal: AbortSignal;
  requestId: string;
  readonly meta?: Meta;
}
/** What an event listener is told about the frame that carried its event. */
export interface EventContext extends TraceContext {
  wire: AddressedWire;
  readonly meta?: Meta;
}
/** What one call may carry: its propagator, a withdrawing signal, a deadline, the context it is made under, and its meta. */
export interface CallOptions {
  propagator?: Propagator;
  signal?: AbortSignal;
  timeoutMs?: number;
  context?: TraceContext;
  meta?: Meta;
}
/** What one emit may carry: its propagator, the context it is made under, and its meta. */
export interface EmitOptions {
  propagator?: Propagator;
  context?: TraceContext;
  meta?: Meta;
}
/** Answers one request: the result, or a thrown PublicError that crosses with its code; any other error crosses as `internal`. */
export type Handler = (params: unknown, context: RequestContext) => unknown | Promise<unknown>;
/** Takes one event's data; an event has no answer, and a failure ends the registry. */
export type EventListener = (data: unknown, context: EventContext) => void | Promise<void>;
/** A request handler and an event listener that share one path. */
export interface Handlers {
  request?: Handler;
  event?: EventListener;
}

/**
 * Sends one request at a relative path and waits for its response. Its return
 * capability is fresh for this call and carries the invocation lifecycle, so it
 * is independent of every other call's identifier. A caller that withdraws —
 * its signal fires or its deadline passes — sends a best-effort cancellation.
 * It never retries.
 *
 * A refusal before anything was sent is an UnpublishedError; a response error
 * is a PublicError.
 */
export function call<T = unknown>(
  access: AddressedWire,
  path: Path,
  params: unknown = {},
  options: CallOptions = {},
): Promise<T> {
  return request<T>(access, path, params, options, (options.propagator ?? defaultPropagator).inject(options.context));
}

/**
 * Admits one event at a relative path. Return means the destination accepted
 * it, never that it was consumed; a refusal is an UnpublishedError.
 */
export function emit(access: AddressedWire, path: Path, data: unknown = null, options: EmitOptions = {}): void {
  try {
    encodePath(path);
    const trace = (options.propagator ?? defaultPropagator).inject(options.context);
    const frame = snapshot(
      carrying({ version: 1, kind: 'event', data, ...traceMembers(trace) }, options.meta),
    ) as unknown as ProfileFrame;
    setOutgoingTrace(frame, trace);
    access.send(path, { frame });
  } catch (error) {
    throw new UnpublishedError(error);
  }
}

/** Registers one request handler; application code runs after the delivering turn. */
export function handle(registry: Registry, path: Path, handler: Handler): () => void {
  return register(registry, path, { request: handler });
}

/** Registers one event listener; the delivering carrier awaits an asynchronous one. */
export function onEvent(registry: Registry, path: Path, listener: EventListener): () => void {
  return register(registry, path, { event: listener });
}

/**
 * Registers a request handler, an event listener, or both at one path, with
 * one cancellation map; the one detach removes the group. An event-only path
 * refuses requests with method_not_found, and an event listener's failure
 * closes the registry as a protocol error.
 */
export function register(registry: Registry, path: Path, handlers: Handlers): () => void {
  const incoming = new Map<ReturnAddress, Map<string, AbortController>>();
  const stop = () => {
    for (const calls of incoming.values()) for (const controller of calls.values()) controller.abort();
  };
  const detach = registry.register(path, {
    closed: stop,
    message: (_path, message) => {
      const frame = message.frame;
      if (frame.kind === 'event') {
        if (!handlers.event) return;
        const received = eventContext(message);
        const context = (received ? Object.create(received) : {}) as EventContext;
        Object.defineProperties(context, {
          wire: { value: registry, enumerable: true },
          meta: { value: frame.meta ? { ...frame.meta } : undefined, enumerable: true },
        });
        if (!received) defaultPropagator.extract(context, traceOf(frame));
        const failed = () => {
          registry.close(1002, 'wire event rejected');
        };
        try {
          const pending = handlers.event(frame.data, context);
          if (pending) return pending.catch(failed);
        } catch {
          failed();
        }
        return;
      }
      if ((frame.kind !== 'request' && frame.kind !== 'cancel') || !message.return) return;
      let calls = incoming.get(message.return);
      if (frame.kind === 'cancel') {
        calls?.get(frame.id)?.abort();
        return;
      }
      if (!handlers.request) {
        respond(message, undefined, new PublicError('method_not_found', 'An event has no request handler.'));
        return;
      }
      if (calls?.has(frame.id)) {
        respond(message, undefined, new PublicError('invalid_message', 'Duplicate active request identifier.'));
        return;
      }
      if (!calls) {
        calls = new Map();
        incoming.set(message.return, calls);
      }
      const controller = new AbortController();
      calls.set(frame.id, controller);
      const dispatch = dispatchContext(message.return);
      const context = (
        dispatch ? Object.create(dispatch.context) : { ...(frame.meta ? { meta: { ...frame.meta } } : {}) }
      ) as RequestContext;
      Object.defineProperties(context, {
        wire: { value: registry, enumerable: true },
        signal: {
          value: dispatch ? AbortSignal.any([controller.signal, dispatch.context.signal]) : controller.signal,
          enumerable: true,
        },
        requestId: { value: dispatch?.context.requestId ?? frame.id, enumerable: true },
      });
      if (!dispatch) defaultPropagator.extract(context, traceOf(frame));
      // The body runs after this receiver returns, so returning is not
      // completion. The lease says so to whoever admitted the request: an
      // early answer to the caller cannot retire an invocation whose body is
      // still running. A bound reached is a refusal; any other refusal means
      // this return capability carries no lifecycle, and ordinary addressed
      // delivery goes on without one.
      let body: { done(): void } | undefined;
      try {
        body = beginInvocationBody(message);
      } catch (error) {
        if (error instanceof InvocationError && error.code === 'limit') {
          calls.delete(frame.id);
          if (!calls.size) incoming.delete(message.return);
          controller.abort();
          respond(message, undefined, new PublicError('busy', 'Invocation participation limit reached.'));
          return;
        }
      }
      let cancelledBeforeHandler = false;
      void Promise.resolve()
        .then(() => {
          if (context.signal.aborted) {
            cancelledBeforeHandler = true;
            throw new PublicError('cancelled', 'Request was cancelled.');
          }
          return handlers.request!(frame.params, context);
        })
        .then(
          (result) => {
            const error = context.signal.aborted ? new PublicError('cancelled', 'Request was cancelled.') : undefined;
            if (error && dispatch?.completion) dispatch.completion.cancelled = true;
            respond(message, result, error);
          },
          (error: unknown) => {
            // A public handler refusal stays an error even if cancellation
            // raced its completion. Only this helper's withdrawal is local.
            if (cancelledBeforeHandler && dispatch?.completion) dispatch.completion.cancelled = true;
            respond(message, undefined, error);
          },
        )
        .finally(() => {
          body?.done();
          calls!.delete(frame.id);
          if (!calls!.size) incoming.delete(message.return!);
        });
    },
  });
  return () => {
    detach();
    stop();
  };
}
