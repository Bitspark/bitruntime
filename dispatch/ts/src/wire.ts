import { decodePath, encodePath, WireError } from '@nightseam/duplex';
import type { Message, Path, ProfileFrame, Receiver, ReturnAddress, Wire, Endpoint } from '@nightseam/duplex';
import { type HandlerRegistry } from './dispatcher.ts';
import { Invocation, InvocationError, beginInvocationBody, defaultInvocationLimits } from './invocation.ts';
import { DuplexError, UnpublishedError } from './error.ts';
import { carrying, decodeEnvelope, isObject } from './envelope.ts';
import { scalarJSON } from './unicode.ts';
import { defaultPropagator, traceOf } from './trace.ts';
import type { ValueEnvironment } from './value-adapter.ts';
import type { Propagator, Trace, TraceContext } from './trace.ts';
import type { Observer } from './observer.ts';
import { observeWire, observeWireRequest } from './wire-observer.ts';
import type {
  CallOptions,
  DuplexPeer,
  EmitOptions,
  EventContext,
  EventListener,
  Meta,
  PeerOptions,
  RequestContext,
  RequestHandler,
} from './peer.ts';

// Outgoing propagators may privately associate their trace object with an
// active consumer context. Keep that identity across local frame snapshots;
// only the two public trace strings cross a physical connection.
const outgoingTraces = new WeakMap<ProfileFrame, Trace>();
function outgoingTrace(frame: ProfileFrame): Trace | undefined {
  return outgoingTraces.get(frame) ?? traceOf(frame);
}

/** @internal Received values retained beside a local return capability. */
export interface WireDispatchContext {
  context: RequestContext | WireRequestContext;
  panic: (error: unknown) => void;
  maxFrameBytes: number;
  completion?: { cancelled: boolean };
}
// This is a local capability association, never a field a caller can serialize
// or supply as ambient outgoing metadata. Keep non-enumerable verified values.
const dispatchContexts = new WeakMap<ReturnAddress, WireDispatchContext>();

interface WireEventDispatchContext {
  context: EventContext | WireEventContext;
  panic?: (error: unknown) => void;
}
// An event's local context capability has no waiter, id or callable return.
// Weak ownership lets queued deliveries outlive the source receiver's return.
const eventContexts = new WeakMap<ReturnAddress, WireEventDispatchContext>();
const receivedEventTraces = new WeakMap<EventContext, Trace | undefined>();
/** @internal Preserve the received frame independently of a custom propagator's context. */
export function setReceivedEventTrace(context: EventContext, trace: Trace | undefined): void {
  receivedEventTraces.set(context, trace);
}
const eventContextCarrier: Wire = Object.freeze({
  send: () => {
    throw new DuplexError('invalid_message', 'An event context is not a return address.');
  },
});
/** @internal Inspect only runtime-associated event context, never caller data. */
export function wireEventContext(message: Message): WireEventDispatchContext | undefined {
  return message.return ? eventContexts.get(message.return) : undefined;
}
/** @internal Carry received event context across local asynchronous composition. */
export function withWireEventContext(
  message: Message,
  context: EventContext | WireEventContext,
  panic?: (error: unknown) => void,
): Message {
  const address: ReturnAddress = { wire: eventContextCarrier };
  eventContexts.set(address, { context, panic });
  return { frame: message.frame, return: address };
}

/** @internal Read the verified context associated with a local return capability. */
export function wireContext(address: ReturnAddress): WireDispatchContext | undefined {
  return dispatchContexts.get(address);
}

/** @internal Associate received context without adding it to serialized frame data. */
export function setWireContext(address: ReturnAddress, context: WireDispatchContext): () => void {
  dispatchContexts.set(address, context);
  return () => {
    if (dispatchContexts.get(address) === context) dispatchContexts.delete(address);
  };
}

/** @internal Preserve authenticated receive context across a local root's return remapping. */
export function inheritWireContext(source: ReturnAddress, target: ReturnAddress): () => void {
  const context = dispatchContexts.get(source);
  return context ? setWireContext(target, context) : () => {};
}

/** Context beside a wire request, independent of its concrete carrier. */
export interface WireModelContext extends TraceContext {
  signal?: AbortSignal;
  timeoutMs?: number;
  readonly meta?: Meta;
  outgoingMeta?: Meta;
  requestId?: string;
}
/** Runtime construction options shared by derived family adapters. */
export interface AdapterContext {
  options?: PeerOptions;
  valueEnvironment?: ValueEnvironment;
}
/** Context beside a wire request, independent of its concrete carrier. */
export interface WireRequestContext extends WireModelContext {
  wire: Wire;
  signal: AbortSignal;
  requestId: string;
  meta?: Meta;
}
export interface WireCallOptions {
  propagator?: Propagator;
  observer?: Observer;
  family?: string;
  signal?: AbortSignal;
  timeoutMs?: number;
  context?: TraceContext;
  meta?: Meta;
}
export interface WireEmitOptions {
  propagator?: Propagator;
  observer?: Observer;
  family?: string;
  context?: TraceContext;
  meta?: Meta;
}
export interface WireEventContext extends TraceContext {
  wire: Wire;
  meta?: Meta;
}
export type WireHandler = (params: unknown, context: WireRequestContext) => unknown | Promise<unknown>;
export type WireEventListener = (data: unknown, context: WireEventContext) => void | Promise<void>;
export interface WireHandlers {
  request?: WireHandler;
  event?: WireEventListener;
  observer?: Observer;
  family?: string;
}

/** @internal The completion/deadline primitive shared by Peer.call and CallWire. */
export function requestCompletion<T>() {
  let settled = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let detach: (() => void) | undefined;
  let resolve!: (value: T) => void;
  let reject!: (error: DuplexError) => void;
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
    reject: (error: DuplexError) => {
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
      cancel: (error: DuplexError, outcome: 'timeout' | 'cancelled') => void,
    ) => {
      if (settled) return;
      timer = setTimeout(
        () =>
          cancel(
            new DuplexError('request_timeout', `Call ${method} timed out; its outcome may be unknown.`),
            'timeout',
          ),
        timeoutMs,
      );
      if (signal) {
        const abort = () =>
          cancel(new DuplexError('cancelled', 'Call was cancelled; its outcome may be unknown.'), 'cancelled');
        signal.addEventListener('abort', abort, { once: true });
        detach = () => signal.removeEventListener('abort', abort);
        if (signal.aborted) abort();
      }
    },
  };
}

function snapshot<T>(value: T): T {
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
    throw new DuplexError('invalid_message', 'Frame must contain serializable JSON values.');
  }
}

/** @internal Reuse the physical profile validator at structured root admission. */
export function profileFrame(value: ProfileFrame, name: string, maxFrameBytes?: number): ProfileFrame {
  const saved: unknown = snapshot(value);
  if (!isObject(saved) || Object.hasOwn(saved, 'method') || Object.hasOwn(saved, 'event'))
    throw new DuplexError('invalid_message', 'Invalid structured profile frame.');
  // The path is the sole operation name. Reuse the physical profile validator
  // after translating that name, without silently replacing an extra member.
  const envelope = {
    ...saved,
    ...(saved.kind === 'request' ? { method: name } : saved.kind === 'event' ? { event: name } : {}),
  };
  const text = JSON.stringify(envelope);
  if (maxFrameBytes !== undefined && new TextEncoder().encode(text).byteLength > maxFrameBytes)
    throw new DuplexError('frame_too_large', 'Outgoing frame exceeds the size limit.');
  // Logical request ids belong to local return addresses, not physical roles.
  // Both accepted prefixes still use the existing canonical numeric grammar.
  const prefix = typeof saved.id === 'string' && saved.id.startsWith('s:') ? 's:' : 'c:';
  try {
    decodeEnvelope(text, prefix, prefix);
  } catch {
    throw new DuplexError('invalid_message', 'Invalid structured profile frame.');
  }
  const frame = saved as unknown as ProfileFrame;
  const associated = outgoingTraces.get(value);
  if (associated) outgoingTraces.set(frame, associated);
  return frame;
}

/** @internal Remove local publication proof from a dispatched refusal. */
export function publicError(error: unknown): DuplexError {
  // Reconstructing public data strips local publication proof after dispatch.
  return error instanceof DuplexError &&
    typeof error.code === 'string' &&
    error.code.length > 0 &&
    typeof error.message === 'string' &&
    error.message.length > 0
    ? new DuplexError(error.code, error.message, error.data)
    : new DuplexError('internal', 'Request handler failed.');
}
/** @internal Return a validated public result or a bounded internal-error fallback. */
export function response(request: Message, result?: unknown, error?: unknown): DuplexError | undefined {
  if (request.frame.kind !== 'request' || !request.return)
    return new DuplexError('disconnected', 'The request has no return address.');
  let outcome: DuplexError | undefined;
  let payload: { result: unknown } | { error: { code: string; message: string; data?: unknown } };
  try {
    if (error !== undefined) {
      const refused = publicError(error);
      // Retain a valid local cancellation identity for the helper's outcome;
      // only normalized public fields below enter the response frame.
      outcome =
        error instanceof DuplexError && error.code === refused.code && error.message === refused.message
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
    outcome = new DuplexError('internal', 'Response could not be encoded');
    payload = { error: { code: 'internal', message: 'Response could not be encoded' } };
  }
  const frame: ProfileFrame = {
    version: 1,
    kind: 'response',
    id: request.frame.id,
    ...payload,
    ...traceOf(request.frame),
  };
  try {
    request.return.wire.send([], { frame });
  } catch (error) {
    if (error instanceof DuplexError && ['invalid_message', 'frame_too_large'].includes(error.code)) {
      outcome = new DuplexError('internal', 'Response could not be encoded');
      try {
        request.return.wire.send([], {
          frame: {
            version: 1,
            kind: 'response',
            id: request.frame.id,
            error: { code: 'internal', message: 'Response could not be encoded' },
            ...traceOf(request.frame),
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

/** Calls through the shared request primitive; no new peer or channel is made. */
export function callWire<T = unknown>(
  wire: Wire,
  path: Path,
  params: unknown = {},
  options: WireCallOptions = {},
): Promise<T> {
  return callWireTraced(wire, path, params, options, (options.propagator ?? defaultPropagator).inject(options.context));
}
function callWireTraced<T>(
  wire: Wire,
  path: Path,
  params: unknown,
  options: WireCallOptions,
  trace?: Trace,
  dispatch?: WireDispatchContext,
): Promise<T> {
  let name: string;
  let frame: ProfileFrame;
  try {
    name = encodePath(path);
    if (options.timeoutMs !== undefined && (!Number.isSafeInteger(options.timeoutMs) || options.timeoutMs <= 0))
      throw new DuplexError('invalid_options', 'timeoutMs must be a positive safe integer.');
    if (options.signal?.aborted) throw new DuplexError('cancelled', 'Call was cancelled before sending.');
    frame = snapshot(
      carrying({ version: 1, kind: 'request', id: 'c:1', params, ...trace }, options.meta),
    ) as unknown as ProfileFrame;
  } catch (error) {
    return Promise.reject(new UnpublishedError(error));
  }
  if (!dispatch && trace) outgoingTraces.set(frame, trace);
  const completion = requestCompletion<T>();
  if (dispatch) dispatch = { ...dispatch, completion: { cancelled: false } };
  const invocation = new Invocation(defaultInvocationLimits());
  const finish = observeWireRequest(options.observer, options.family, name, false, trace);
  let localOutcome: 'cancelled' | 'timeout' | undefined;
  void completion.promise.then(
    () => finish(),
    (error: unknown) => finish(error, localOutcome ?? 'error'),
  );
  const returning: Wire = {
    send: (suffix, message) => {
      if (suffix.length) {
        invocation.deliver(suffix, message);
        return;
      }
      const frame = profileFrame(message.frame, '', dispatch?.maxFrameBytes);
      if (frame.kind !== 'response' || frame.id !== 'c:1')
        throw new DuplexError('invalid_message', 'Invalid wire response.');
      if (completion.settled) throw new WireError('closed');
      if (frame.error?.code === 'cancelled' && dispatch?.context.signal.aborted && dispatch.completion?.cancelled)
        completion.resolve(undefined as T);
      else if (frame.error) completion.reject(new DuplexError(frame.error.code, frame.error.message, frame.error.data));
      else completion.resolve(frame.result as T);
      invocation.settle();
    },
  };
  const address: ReturnAddress = { wire: returning };
  const retire = () => {
    dispatchContexts.delete(address);
    invocation.settle();
    invocation.dispatchDone();
  };
  if (dispatch) dispatchContexts.set(address, dispatch);
  void completion.promise.then(retire, retire);
  try {
    wire.send(path, { frame, return: address });
  } catch (error) {
    completion.reject(new UnpublishedError(error));
  }
  completion.wait(options.signal, options.timeoutMs ?? 30_000, name, (error, outcome) => {
    if (completion.settled) return;
    completion.cleanup();
    if (!dispatch) {
      localOutcome = outcome;
      completion.reject(error);
      finish(error, outcome);
    }
    // An incoming dispatch occupies the carrier's handler budget until the
    // receiver replies. Its cancellation ends the caller's wait elsewhere;
    // settling this promise here would release a still-executing body.
    try {
      wire.send(path, { frame: { version: 1, kind: 'cancel', id: 'c:1', ...trace }, return: address });
    } catch {
      /* Cancellation is best effort and never extends the caller's wait. */
    }
  });
  return completion.promise;
}

/** Registers one operation; application code runs after the delivering turn. */
export function handleWire(wire: HandlerRegistry, path: Path, handler: WireHandler): () => void {
  return registerWire(wire, path, { request: handler });
}

/** Registers request and event facets at one operation with one cancellation map. */
export function registerWire(wire: HandlerRegistry, path: Path, handlers: WireHandlers): () => void {
  const incoming = new Map<ReturnAddress, Map<string, AbortController>>();
  const stop = () => {
    for (const calls of incoming.values()) for (const controller of calls.values()) controller.abort();
  };
  const detach = wire.register(path, {
    closed: stop,
    message: (_path, message) => {
      const frame = message.frame;
      if (frame.kind === 'event') {
        if (!handlers.event) return;
        if (handlers.observer)
          observeWire(handlers.observer, {
            type: 'event.delivered',
            at: new Date(),
            name: encodePath(path),
            bytes: new TextEncoder().encode(JSON.stringify(frame.data)).byteLength,
            trace: traceOf(frame),
            family: handlers.family ?? '',
          });
        const dispatch = wireEventContext(message);
        const context = (dispatch ? Object.create(dispatch.context) : {}) as WireEventContext;
        Object.defineProperties(context, {
          wire: { value: wire, enumerable: true },
          meta: { value: frame.meta ? { ...frame.meta } : undefined, enumerable: true },
        });
        if (!dispatch) defaultPropagator.extract(context, traceOf(frame));
        const failed = (error: unknown) => {
          if (!(error instanceof DuplexError)) dispatch?.panic?.(error);
          wire.close(1002, 'wire event rejected');
        };
        try {
          const pending = handlers.event(frame.data, context);
          if (pending) return pending.catch(failed);
        } catch (error) {
          failed(error);
        }
        return;
      }
      if ((frame.kind !== 'request' && frame.kind !== 'cancel') || !message.return) return;
      let calls = incoming.get(message.return);
      if (frame.kind === 'cancel') {
        calls?.get(frame.id)?.abort();
        return;
      }
      const finish = observeWireRequest(handlers.observer, handlers.family, encodePath(path), true, traceOf(frame));
      if (!handlers.request) {
        const error = new DuplexError('method_not_found', 'An event has no request handler.');
        finish(response(message, undefined, error));
        return;
      }
      if (calls?.has(frame.id)) {
        const error = new DuplexError('invalid_message', 'Duplicate active request identifier.');
        finish(response(message, undefined, error));
        return;
      }
      if (!calls) {
        calls = new Map();
        incoming.set(message.return, calls);
      }
      const controller = new AbortController();
      calls.set(frame.id, controller);
      const dispatch = dispatchContexts.get(message.return);
      const context: WireRequestContext = dispatch
        ? (Object.create(dispatch.context) as WireRequestContext)
        : ({
            ...(frame.meta ? { meta: { ...frame.meta } } : {}),
          } as WireRequestContext);
      Object.defineProperties(context, {
        wire: { value: wire, enumerable: true },
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
      // still running. A return capability that carries no lifecycle still
      // gets ordinary addressed delivery.
      // A bound reached is a refusal; any other refusal means this return
      // capability carries no lifecycle, and ordinary addressed delivery goes
      // on without one.
      let body: { done(): void } | undefined;
      try {
        body = beginInvocationBody(message);
      } catch (error) {
        if (error instanceof InvocationError && error.code === 'limit') {
          calls.delete(frame.id);
          if (!calls.size) incoming.delete(message.return);
          controller.abort();
          finish(response(message, undefined, new DuplexError('busy', 'Invocation participation limit reached.')));
          return;
        }
      }
      let cancelledBeforeHandler = false;
      void Promise.resolve()
        .then(() => {
          if (context.signal.aborted) {
            cancelledBeforeHandler = true;
            throw new DuplexError('cancelled', 'Request was cancelled.');
          }
          return handlers.request!(frame.params, context);
        })
        .then(
          (result) => {
            const error = context.signal.aborted ? new DuplexError('cancelled', 'Request was cancelled.') : undefined;
            if (error && dispatch?.completion) dispatch.completion.cancelled = true;
            const outcome = response(message, result, error);
            finish(outcome, error && outcome === error ? 'cancelled' : undefined);
          },
          (error: unknown) => {
            if (!(error instanceof DuplexError)) dispatch?.panic(error);
            // A public handler refusal stays an error even if cancellation
            // raced its completion. Only this helper's withdrawal is local.
            if (cancelledBeforeHandler && dispatch?.completion) dispatch.completion.cancelled = true;
            const outcome = response(message, undefined, error);
            finish(outcome, cancelledBeforeHandler && outcome === error ? 'cancelled' : 'error');
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

/** Emits a relative event; return means accepted, never consumed. */
export function emitWire(wire: Wire, path: Path, data: unknown = null, options: WireEmitOptions = {}): void {
  try {
    const name = encodePath(path);
    const trace = (options.propagator ?? defaultPropagator).inject(options.context);
    const frame = snapshot(
      carrying({ version: 1, kind: 'event', data, ...trace }, options.meta),
    ) as unknown as ProfileFrame;
    outgoingTraces.set(frame, trace);
    if (options.observer)
      observeWire(options.observer, {
        type: 'event.emitted',
        at: new Date(),
        name,
        bytes: new TextEncoder().encode(JSON.stringify(frame.kind === 'event' ? frame.data : null)).byteLength,
        trace,
        family: options.family ?? '',
      });
    wire.send(path, { frame });
  } catch (error) {
    throw new UnpublishedError(error);
  }
}

/** The root's existing serial event dispatcher awaits an async listener. */
export function onWireEvent(wire: HandlerRegistry, path: Path, listener: WireEventListener): () => void {
  return registerWire(wire, path, { event: listener });
}

/** Forwards both relative origins without owning either endpoint. */
export function forwardWire(a: Endpoint, b: Endpoint): () => void {
  const removals: (() => void)[] = [];
  let detached = false;
  const stop = () => {
    if (detached) return;
    detached = true;
    for (const remove of removals) remove();
  };
  const receiver = (destination: Wire): Receiver => ({
    closed: stop,
    message: (path, message) => {
      // Detach stops new dispatch, not controls for an already captured call.
      try {
        destination.send(path, message);
      } catch (error) {
        stop();
        response(message, undefined, publicError(error));
      }
    },
  });
  try {
    for (const [source, destination] of [
      [a, b],
      [b, a],
    ] as const) {
      const remove = source.receive(receiver(destination));
      if (detached) remove();
      else removals.push(remove);
    }
  } catch (error) {
    stop();
    throw error;
  }
  return stop;
}

/** @internal Hooks retain all carrier ownership in the peer. */
export interface PeerWireOptions {
  queueCapacity: number;
  maxPendingRequests: number;
  maxFrameBytes: number;
  requestTimeoutMs: number;
  call: (method: string, params: unknown, options: CallOptions, trace?: Trace) => Promise<unknown>;
  emit: (name: string, data: unknown, options: EmitOptions, trace?: Trace) => Promise<void>;
  dispatch: (
    request: (method: string) => RequestHandler | undefined,
    event: (name: string) => EventListener | undefined,
  ) => void;
  fail: (error: DuplexError) => void;
  pressure: (waiting: number) => void;
  panic: (method: string, error: unknown, trace?: Trace) => void;
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
  refusal?: DuplexError;
}

interface WireRegistration {
  receiver: Receiver;
  request: (path: Path, params: unknown, context: RequestContext) => Promise<unknown>;
  detach: () => void;
}

/** @internal One bridge per peer; selection never constructs another. */
export function peerWire(peer: DuplexPeer, options: PeerWireOptions): Endpoint {
  const queued: RoutedDelivery[] = [];
  const incoming = new Map<ReturnAddress, Map<string, RoutedCall>>();
  let attachment: WireRegistration | undefined;
  const lookup = (name: string): { path: Path; registration: WireRegistration } | undefined => {
    let path: Path;
    try {
      path = decodePath(name);
    } catch {
      return;
    }
    return attachment ? { path, registration: attachment } : undefined;
  };
  options.dispatch(
    (name) => {
      const found = lookup(name);
      return found ? (params, context) => found.registration.request(found.path, params, context) : undefined;
    },
    (name) => {
      const found = lookup(name);
      return found
        ? (_name, data, context) => {
            const receivedTrace = receivedEventTraces.has(context) ? receivedEventTraces.get(context) : context.trace;
            return found.registration.receiver.message?.(
              found.path,
              withWireEventContext(
                {
                  frame: {
                    version: 1,
                    kind: 'event',
                    data,
                    ...receivedTrace,
                    ...(context.meta ? { meta: context.meta } : {}),
                  },
                },
                context,
                (error) => options.panic(name, error, receivedTrace),
              ),
            );
          }
        : undefined;
    },
  );
  let retained = 0,
    dataQueued = 0,
    scheduled = false,
    ended = false;
  const endError = () => new DuplexError('disconnected', 'Connection ended; outcome may be unknown.');
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
  peer.onClose(() => {
    ended = true;
    for (const delivery of queued.splice(0)) response(delivery.message, undefined, endError());
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
    while (queued.length && !ended) {
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
        response(message, undefined, refusal);
        continue;
      }
      const name = encodePath(path);
      if (frame.kind === 'event') {
        // Invoke admission now, in wire order; only completion is asynchronous.
        void options
          .emit(
            name,
            frame.data,
            {
              meta: frame.meta ? { ...frame.meta } : undefined,
            },
            outgoingTrace(frame),
          )
          .catch((error: unknown) => options.fail(publicError(error)));
        continue;
      }
      if (frame.kind !== 'request') continue;
      // Peer.call allocates the carrier id and enqueues before it returns.
      const pending = options.call(
        name,
        frame.params,
        {
          signal: call!.controller.signal,
          meta: frame.meta ? { ...frame.meta } : undefined,
        },
        outgoingTrace(frame),
      );
      const finish = (value?: unknown, error?: unknown) => {
        call!.completed = true;
        retire(call!);
        response(message, value, error);
      };
      void pending.then(
        (value) => finish(value),
        (error: unknown) => finish(undefined, error),
      );
    }
  };
  const wire: Endpoint = {
    send: (path, message) => {
      const name = encodePath(path);
      if (ended || peer.status !== 'connected') throw endError();
      const frame = profileFrame(message.frame, name, options.maxFrameBytes);
      if (!name && (frame.kind === 'request' || frame.kind === 'event'))
        throw new DuplexError('invalid_message', 'A root wire operation requires a nonempty path.');
      if ((frame.kind === 'request' || frame.kind === 'cancel') && !message.return?.wire)
        throw new DuplexError('invalid_message', 'A wire request or cancellation requires a return address.');
      if (frame.kind !== 'request' && frame.kind !== 'event' && frame.kind !== 'cancel')
        throw new DuplexError('invalid_message', "A response is sent to its request's return address.");
      let call: RoutedCall | undefined;
      if (frame.kind === 'cancel') {
        call = incoming.get(message.return!)?.get(frame.id);
        // Cancellation belongs to an already admitted request. Its one control
        // reservation is bounded by the existing pending-request budget.
        if (!call || call.completed || call.cancelQueued || call.cancelled) return;
      }
      if (ended || peer.status !== 'connected') throw endError();
      if (frame.kind !== 'cancel' && dataQueued >= options.queueCapacity) {
        const error = new DuplexError('busy', 'Output consumer is stalled; queue limit reached.');
        options.pressure(dataQueued);
        options.fail(error);
        throw error;
      }
      let refusal: DuplexError | undefined;
      if (frame.kind === 'cancel') call!.cancelQueued = true;
      else {
        dataQueued++;
        if (frame.kind === 'request') {
          let calls = incoming.get(message.return!);
          if (calls?.has(frame.id) || retained >= options.maxPendingRequests) {
            refusal = new DuplexError(
              calls?.has(frame.id) ? 'invalid_message' : 'busy',
              'Outstanding wire call refused.',
            );
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
      if (ended) throw endError();
      if (attachment) throw new WireError('receiver_exists');
      const target: Wire = {
        send: (suffix, message) => {
          try {
            if (!receiver.message) {
              response(message, undefined, new DuplexError('method_not_found', 'Unknown method.'));
              return;
            }
            const result = receiver.message(suffix, message);
            if (result) void result.catch((error: unknown) => response(message, undefined, publicError(error)));
          } catch (error) {
            response(message, undefined, publicError(error));
          }
        },
      };
      const request = (received: Path, params: unknown, context: RequestContext) =>
        callWireTraced(
          target,
          received,
          params,
          {
            signal: context.signal,
            timeoutMs: options.requestTimeoutMs,
            meta: context.meta,
          },
          context.trace,
          {
            context,
            panic: (error) => options.panic(encodePath(received), error, context.trace),
            maxFrameBytes: options.maxFrameBytes,
          },
        );
      const registration: WireRegistration = {
        receiver,
        request,
        detach: () => {
          if (attachment === registration) attachment = undefined;
        },
      };
      attachment = registration;
      return registration.detach;
    },
    close: (code = 1000, reason = '') => options.close(code, reason),
  };
  return wire;
}
