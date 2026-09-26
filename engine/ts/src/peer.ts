import { DuplexError, UnpublishedError } from './error.ts';
export { DuplexError, UnpublishedError } from './error.ts';
import { decodeEnvelope, carrying, requireName, isObject, type Envelope } from './envelope.ts';
import { webSocketConnection } from '@nightseam/duplex';
import type { Frame, FrameConnection, WebSocketLike, Endpoint } from '@nightseam/duplex';
import { defaultPropagator, traceOf, traced } from './trace.ts';
import type { Propagator, Trace, TraceContext } from './trace.ts';
import { peerWire, requestCompletion, setReceivedEventTrace } from './wire.ts';
import type { Observer, ObserverEvent } from './observer.ts';
import { scalarJSON } from './unicode.ts';

export type { WebSocketLike } from '@nightseam/duplex';

/** The endpoint selects this profile; it is offered as no subprotocol by default. */
export const DUPLEX_PROFILE = 'nightseam.duplex/1';
/** The limits a peer runs with unless its options say otherwise; docs/runtime/peer.md tables them. */
export const DUPLEX_DEFAULTS = Object.freeze({
  maxConcurrentHandlers: 64,
  maxPendingRequests: 128,
  queueCapacity: 128,
  maxFrameBytes: 1_048_576,
  requestTimeoutMs: 30_000,
  writeTimeoutMs: 10_000,
  connectTimeoutMs: 30_000,
});

/** Where a peer is between construction and its end; `connected` is the only state that carries frames. */
export type PeerStatus = 'disconnected' | 'connecting' | 'connected';
/**
 * What a frame carries about a call rather than of it: a flat map of strings —
 * a tenant, an idempotency key, a credential that is per request — which the
 * profile carries verbatim and reads nothing into.
 */
export type Meta = Record<string, string>;
/** What a call may carry: a signal that withdraws it, a deadline of its own, the context it is made under (so its trace parents on the request being served), and its meta. */
export interface CallOptions {
  signal?: AbortSignal;
  timeoutMs?: number;
  context?: TraceContext;
  meta?: Meta;
}
/** What an emit may carry: the context it is made under, and its meta. */
export interface EmitOptions {
  context?: TraceContext;
  meta?: Meta;
}
/** What a handler is given beside the params: a signal that fires when the caller withdraws the request or its deadline passes, the peer it arrived on, the request's id, and the trace and meta the frame brought. */
export interface RequestContext {
  signal: AbortSignal;
  peer: DuplexPeer;
  requestId: string;
  /** What the propagator read from the frame that started this request. */
  trace?: Trace;
  /**
   * What the frame that started this request carried, and absent where it
   * carried none. Passing it on is the handler's to say — `{ meta: context.meta }`
   * — since a trace is the peer's to propagate and a credential is not.
   */
  meta?: Meta;
}
/** What an event's listeners are told about the frame that carried it. */
export interface EventContext {
  peer: DuplexPeer;
  trace?: Trace;
  meta?: Meta;
}
/** Answers one request: the result, or a thrown DuplexError that crosses the wire with its code — any other error reaches the caller as `internal`. */
export type RequestHandler = (params: unknown, context: RequestContext) => unknown | Promise<unknown>;
/** Answers every request the peer has no handler for, by method name; the generated binding installs one. */
export type Dispatcher = (method: string, params: unknown, context: RequestContext) => unknown | Promise<unknown>;
/** Takes one event's data; events have no answer, and listeners run one at a time in arrival order. */
export type EventListener = (event: string, data: unknown, context: EventContext) => void | Promise<void>;
/** How a peer is made: its role, its dispatcher, the socket factory, its limits, its propagator, its observer and the family each name belongs to. Every member is optional and takes DUPLEX_DEFAULTS. */
export interface PeerOptions {
  /** Installs handlers once after validation, before any connection can read. Must be synchronous. */
  prepare?: (peer: DuplexPeer) => void;
  role?: 'client' | 'server';
  dispatch?: Dispatcher;
  webSocketFactory?: (url: string, protocols?: string[]) => WebSocketLike;
  /**
   * Offered to the server at the handshake, in order of preference; none by
   * default. A server that selects none leaves the connection with none and
   * the profile is spoken over it either way — but a browser refuses a
   * handshake whose offer went unselected, so a client that offers must be
   * met by a server that selects (docs/wire/profile.md).
   */
  subprotocols?: string[];
  maxConcurrentHandlers?: number;
  maxPendingRequests?: number;
  queueCapacity?: number;
  maxFrameBytes?: number;
  requestTimeoutMs?: number;
  writeTimeoutMs?: number;
  connectTimeoutMs?: number;
  /** Absent, the default mints W3C ids; an adapter for a tracing library replaces it. */
  propagator?: Propagator;
  onError?: (error: DuplexError) => void;
  observer?: Observer;
  /** Method or event name to family label; the generated install fills it. */
  families?: Record<string, string>;
}

type Timer = ReturnType<typeof setTimeout>;
/** How a request ended, as the observer's event spells it. */
type Outcome = Extract<ObserverEvent, { type: 'request.ended' }>['outcome'];
interface Pending {
  resolve: (value: unknown) => void;
  reject: (error: DuplexError) => void;
  /** What an observer is told the call was, wherever and whenever it ends. */
  method: string;
  started: number;
  trace?: Trace;
  cleanup: () => void;
}
interface Incoming {
  controller: AbortController;
  timer: Timer;
  responded: boolean;
  /** The request's trace; its response, and nothing else, carries it back. */
  trace?: Trace;
  method: string;
  started: number;
}
interface Outgoing {
  text: string;
  started: number;
  sent: boolean;
  waited: boolean;
  /** What the observer is told of this frame, called by the writer just before the bytes leave. */
  observeSent?: () => void;
}
interface QueuedEvent {
  name: string;
  data: unknown;
  bytes: number;
  trace?: Trace;
  meta?: Meta;
}

/**
 * A bounded full-duplex peer. Message routing never awaits application handlers.
 * Calls are not retried, and connections are never reopened automatically.
 * The caller owns endpoint authentication and authorization of incoming methods.
 */
export class DuplexPeer {
  private readonly options: PeerOptions;
  private readonly limits: typeof DUPLEX_DEFAULTS;
  private readonly propagator: Propagator;
  private readonly observer?: Observer;
  private readonly localPrefix: string;
  private readonly remotePrefix: string;
  private connection?: FrameConnection;
  private detach?: () => void;
  private state: PeerStatus = 'disconnected';
  private nextID = 0;
  // Only a request advances the mark: a response or a control names a serial
  // that was taken before it.
  private admittedSerial = 0;
  // Requests publish in the order they were reserved. A request takes its
  // place here when it enters the outgoing queue's gate and leaves when it has
  // published or given up, so a sender that arrives while others are waiting
  // for room takes its turn behind them rather than jumping in.
  private readonly publishing: number[] = [];
  private nextTicket = 0;
  private opening?: { resolve: () => void; reject: (error: DuplexError) => void; timer: Timer };
  private readonly pending = new Map<string, Pending>();
  private readonly incoming = new Map<string, Incoming>();
  private readonly handlers = new Map<string, RequestHandler>();
  private readonly listeners = new Set<EventListener>();
  private readonly closedListeners = new Set<(error: DuplexError) => void>();
  private readonly outgoing: Outgoing[] = [];
  private writeTimer?: Timer;
  /** Public senders paced by a full output queue; wire handoffs never join it. */
  private readonly waitingForRoom = new Set<(room: boolean) => void>();
  private readonly events: QueuedEvent[] = [];
  private eventActive = false;
  private eventTimer?: Timer;
  /** Set while the event queue is over capacity: the deadline it has to drain in. */
  private stallTimer?: Timer;
  private generation = 0;
  private negotiated = '';
  private relativeWire?: Endpoint;
  private wireRequest?: (method: string) => RequestHandler | undefined;
  private wireEvent?: (name: string) => EventListener | undefined;

  constructor(options: PeerOptions = {}) {
    this.options = options;
    if (options.role !== undefined && options.role !== 'client' && options.role !== 'server') {
      throw new DuplexError('invalid_options', 'Peer role must be client or server.');
    }
    this.limits = { ...DUPLEX_DEFAULTS };
    for (const key of Object.keys(DUPLEX_DEFAULTS) as (keyof typeof DUPLEX_DEFAULTS)[]) {
      const value = options[key];
      if (value !== undefined) {
        positiveInteger(value, key, true);
        // Values deliberately remain configurable without introducing unbounded queues.
        (this.limits as Record<string, number>)[key] = value;
      }
    }
    this.propagator = options.propagator ?? defaultPropagator;
    this.observer = options.observer;
    this.localPrefix = options.role === 'server' ? 's:' : 'c:';
    this.remotePrefix = options.role === 'server' ? 'c:' : 's:';
    try {
      const preparation: unknown = options.prepare?.(this);
      if (preparation && typeof (preparation as PromiseLike<unknown>).then === 'function') {
        void Promise.resolve(preparation).catch(() => {});
        throw new DuplexError('invalid_options', 'prepare must complete synchronously.');
      }
    } catch (error) {
      // Preparation owns no transport yet, but it can already own wire
      // registrations and scopes. Notify their existing close hooks once.
      this.handlers.clear();
      this.listeners.clear();
      for (const listener of [...this.closedListeners]) {
        try {
          listener(asError(error));
        } catch {
          /* Cleanup cannot replace the construction error. */
        }
      }
      this.closedListeners.clear();
      throw error;
    }
  }

  /** Where the peer is now; `connected` is the only status in which a call or an event travels. */
  get status(): PeerStatus {
    return this.state;
  }

  /** This peer's relative origin; selections share its existing carrier. */
  wire(): Endpoint {
    return (this.relativeWire ??= peerWire(this, {
      queueCapacity: this.limits.queueCapacity,
      maxPendingRequests: this.limits.maxPendingRequests,
      maxFrameBytes: this.limits.maxFrameBytes,
      requestTimeoutMs: this.limits.requestTimeoutMs,
      call: (method, params, options, trace) => this.callWithTrace(method, params, options, () => trace, true),
      emit: (name, data, options, trace) => this.emitWithTrace(name, data, options, trace, true),
      dispatch: (request, event) => {
        this.wireRequest = request;
        this.wireEvent = event;
      },
      fail: (error) => this.fail(error),
      pressure: (waiting) => {
        if (this.observer) this.pressure(waiting, true);
      },
      panic: (method, error, trace) => {
        if (this.observer)
          this.observe({
            type: 'handler.panic',
            at: new Date(),
            method,
            value: describe(error),
            trace,
            family: this.family(method),
          });
      },
      close: (code, reason) =>
        this.fail(new DuplexError('disconnected', 'Connection closed by caller.'), true, code, reason),
    }));
  }

  /** The side of the connection this peer is; a tunnel over it chooses channel ids by it. */
  get role(): 'client' | 'server' {
    return this.options.role ?? 'client';
  }

  /**
   * What the WebSocket handshake beneath this peer selected, and '' when it
   * selected none or the peer does not run over a WebSocket. The profile
   * reads nothing into it.
   */
  get subprotocol(): string {
    return this.negotiated;
  }

  /** Absolute ws/wss URLs are required. Factories may supply platform-specific auth. */
  connect(url: string): Promise<void> {
    if (this.connection) return Promise.reject(new DuplexError('already_connected', 'Peer already has a connection.'));
    let endpoint: URL;
    try {
      endpoint = new URL(url);
    } catch {
      return Promise.reject(new DuplexError('invalid_url', 'An absolute WebSocket URL is required.'));
    }
    if (!['ws:', 'wss:'].includes(endpoint.protocol) || endpoint.hash || endpoint.username || endpoint.password) {
      return Promise.reject(
        new DuplexError('invalid_url', 'Use an absolute ws/wss URL without credentials or a fragment.'),
      );
    }
    let socket: WebSocketLike;
    const protocols = this.options.subprotocols;
    try {
      socket =
        this.options.webSocketFactory?.(endpoint.href, protocols) ??
        (protocols ? new WebSocket(endpoint.href, protocols) : new WebSocket(endpoint.href));
    } catch {
      return Promise.reject(new DuplexError('connection_failed', 'Unable to create WebSocket.'));
    }
    return this.attach(socket);
  }

  /**
   * Attach an externally authenticated, connecting or open connection. A
   * WebSocket is wrapped by the adapter; the peer itself never touches one.
   */
  attach(connection: FrameConnection | WebSocketLike): Promise<void> {
    if (this.connection) return Promise.reject(new DuplexError('already_connected', 'Peer already has a connection.'));
    const socket = isWebSocketLike(connection) ? connection : undefined;
    const frames = socket === undefined ? (connection as FrameConnection) : webSocketConnection(socket);
    if (frames.state !== 'connecting' && frames.state !== 'open') {
      return Promise.reject(new DuplexError('disconnected', 'Cannot attach a closing or closed WebSocket.'));
    }
    this.connection = frames;
    this.generation++;
    this.state = frames.state === 'open' ? 'connected' : 'connecting';
    // The handshake has selected by the time the socket opens, and not before.
    this.negotiated = this.state === 'connected' ? subprotocolOf(socket) : '';
    if (this.observer && this.state === 'connected')
      this.observe({ type: 'connection.opened', at: new Date(), role: this.role });
    const current = () => this.connection === frames;
    this.detach = frames.listen({
      open: () => {
        if (!current()) return;
        this.state = 'connected';
        this.negotiated = subprotocolOf(socket);
        if (this.observer) this.observe({ type: 'connection.opened', at: new Date(), role: this.role });
        if (this.opening) {
          clearTimeout(this.opening.timer);
          this.opening.resolve();
          this.opening = undefined;
        }
      },
      frame: (frame) => {
        if (current()) this.receive(frame);
      },
      close: (code, reason) => {
        if (current())
          this.fail(
            new DuplexError('disconnected', 'Connection closed; outstanding call outcomes may be unknown.'),
            false,
            code,
            reason,
          );
      },
      error: () => {
        if (current()) this.fail(new DuplexError('connection_failed', 'WebSocket connection failed.'));
      },
    });
    if (this.state === 'connected') return Promise.resolve();
    return new Promise<void>((resolve, reject) => {
      const timer = setTimeout(
        () => this.fail(new DuplexError('connect_timeout', 'Connection timed out.')),
        this.limits.connectTimeoutMs,
      );
      this.opening = { resolve, reject, timer };
    });
  }

  /** Ends the connection with a normal close; every pending call rejects with `disconnected`. */
  close(): void {
    this.fail(new DuplexError('disconnected', 'Connection closed by caller.'), true, 1000);
  }

  /** Tells the listener once, when the peer ends, why it ended; returns what removes the listener. */
  onClose(listener: (error: DuplexError) => void): () => void {
    this.closedListeners.add(listener);
    return () => {
      this.closedListeners.delete(listener);
    };
  }

  /**
   * Serves a method: one handler per name, given the params and a request context, its return the
   * result and a thrown DuplexError the error the caller receives. Returns what unregisters it.
   */
  handle(method: string, handler: RequestHandler): () => void {
    requireName(method, 'method');
    if (this.handlers.has(method))
      throw new DuplexError('duplicate_handler', `Handler already registered for ${method}.`);
    this.handlers.set(method, handler);
    return () => {
      if (this.handlers.get(method) === handler) this.handlers.delete(method);
    };
  }

  /** Listens to every event the remote emits, or to one by name; returns what removes the listener. */
  onEvent(listener: EventListener): () => void;
  onEvent(event: string, listener: (data: unknown, context: EventContext) => void | Promise<void>): () => void;
  onEvent(
    eventOrListener: string | EventListener,
    listener?: (data: unknown, context: EventContext) => void | Promise<void>,
  ): () => void {
    let callback: EventListener;
    if (typeof eventOrListener === 'string') {
      requireName(eventOrListener, 'event');
      if (!listener) throw new DuplexError('invalid_listener', 'An event listener is required.');
      callback = (event, data, context) => {
        if (event === eventOrListener) return listener(data, context);
      };
    } else {
      callback = eventOrListener;
    }
    this.listeners.add(callback);
    return () => {
      this.listeners.delete(callback);
    };
  }

  /**
   * Calls a method on the remote and resolves with its result, or rejects with the DuplexError the
   * remote answered — or the peer's own: `request_timeout` past the deadline, `cancelled` when the
   * caller's signal fired, `busy` when too many calls are outstanding, `disconnected` when the
   * connection ended first.
   */
  call<T = unknown>(method: string, params: unknown = {}, options: CallOptions = {}): Promise<T> {
    return this.callWithTrace(method, params, options, () => this.propagator.inject(options.context));
  }

  private callWithTrace<T>(
    method: string,
    params: unknown,
    options: CallOptions,
    traceSource: () => Trace | undefined,
    immediate = false,
  ): Promise<T> {
    try {
      requireName(method, 'method');
      if (options.timeoutMs !== undefined) positiveInteger(options.timeoutMs, 'timeoutMs', true);
    } catch (error) {
      return Promise.reject(new UnpublishedError(error));
    }
    if (!this.isOpen())
      return Promise.reject(new UnpublishedError(new DuplexError('not_connected', 'Peer is not connected.')));
    if (options.signal?.aborted)
      return Promise.reject(new UnpublishedError(new DuplexError('cancelled', 'Call was cancelled before sending.')));
    if (this.pending.size >= this.limits.maxPendingRequests) {
      return Promise.reject(new UnpublishedError(new DuplexError('busy', 'Outstanding call limit reached.')));
    }
    if (this.nextID >= Number.MAX_SAFE_INTEGER) {
      return Promise.reject(
        new UnpublishedError(
          new DuplexError('identifier_exhausted', 'Create a new peer before issuing further calls.'),
        ),
      );
    }
    const id = this.localPrefix + (++this.nextID).toString(10);
    // One trace for the exchange: the request carries it and its cancel repeats it.
    const trace = traceSource();
    let request: Envelope;
    try {
      request = carrying(traced({ version: 1, kind: 'request', id, method, params }, trace), options.meta);
    } catch (error) {
      return Promise.reject(new UnpublishedError(error));
    }
    const completion = requestCompletion<T>();
    const admission = new AbortController();
    let accepted = false;
    const pending: Pending = {
      resolve: (value) => completion.resolve(value as T),
      reject: completion.reject,
      cleanup: () => {
        completion.cleanup();
        admission.abort();
      },
      method,
      started: Date.now(),
      trace,
    };
    this.pending.set(id, pending);
    const cancel = (error: DuplexError, outcome: Outcome) => {
      if (!this.takePending(id)) return;
      this.ended(id, pending, outcome, error.code);
      completion.reject(accepted ? error : new UnpublishedError(error));
      // Cancellation is best effort, as in Go. It never waits for room and
      // an already cancelled caller cannot end a healthy carrier merely
      // because its cancellation frame has no room in the output queue.
      if (accepted && this.outgoing.length < this.limits.queueCapacity)
        void this.send(traced({ version: 1, kind: 'cancel', id }, trace), method).catch(() => {});
    };
    if (this.observer)
      this.observe({
        type: 'request.started',
        at: new Date(),
        id,
        method,
        incoming: false,
        trace,
        family: this.family(method),
      });
    completion.wait(options.signal, options.timeoutMs ?? this.limits.requestTimeoutMs, method, cancel);
    const refused = (failure: unknown) => {
      const unsent = this.takePending(id);
      if (!unsent) return;
      const error = asError(failure, 'send_failed');
      this.ended(id, unsent, 'error', error.code);
      unsent.reject(error);
    };
    if (!completion.settled)
      void this.send(request, method, refused, undefined, immediate, admission.signal, () => {
        accepted = true;
      }).catch(refused);
    return completion.promise;
  }

  /**
   * Emits one event and resolves when its frame was accepted for sending, which is queued for this
   * connection and no more: an event says nothing about receipt, and a caller that wants delivery
   * has a call. The queue's own deadline continues behind it and ends a connection that never drains.
   */
  emit(event: string, data: unknown = null, options: EmitOptions = {}): Promise<void> {
    try {
      return this.emitWithTrace(event, data, options, this.propagator.inject(options.context));
    } catch (error) {
      return Promise.reject(new UnpublishedError(error));
    }
  }

  private emitWithTrace(
    event: string,
    data: unknown,
    options: EmitOptions,
    trace?: Trace,
    immediate = false,
  ): Promise<void> {
    try {
      requireName(event, 'event');
      return this.send(
        carrying(traced({ version: 1, kind: 'event', event, data }, trace), options.meta),
        event,
        undefined,
        trace,
        immediate,
      );
    } catch (error) {
      return Promise.reject(new UnpublishedError(error));
    }
  }

  /** Gives up this request's place in the publication order, once. Others
   * waiting for room re-check their turn when the queue next drains. */
  private release(ticket: number | undefined): void {
    if (ticket === undefined) return;
    const at = this.publishing.indexOf(ticket);
    if (at < 0) return;
    this.publishing.splice(at, 1);
    if (at === 0) for (const wake of [...this.waitingForRoom]) wake(true);
  }

  private isOpen(): boolean {
    return this.state === 'connected' && this.connection?.state === 'open';
  }

  private takePending(id: string): Pending | undefined {
    const pending = this.pending.get(id);
    if (!pending) return;
    this.pending.delete(id);
    pending.cleanup();
    return pending;
  }

  private async send(
    envelope: Envelope,
    name = '',
    refused?: (error: UnpublishedError) => void,
    carriedTrace?: Trace,
    immediate = false,
    abandoned?: AbortSignal,
    accepted?: () => void,
  ): Promise<void> {
    let queued = false;
    let endOnRefusal = false;
    // A response, a control or an event takes no serial and waits behind no
    // request: only a request's publication order is a promise.
    const ticket = envelope.kind === 'request' ? this.nextTicket++ : undefined;
    if (ticket !== undefined) this.publishing.push(ticket);
    try {
      if (!this.isOpen()) throw new DuplexError('not_connected', 'Peer is not connected.');
      let text: string;
      try {
        text = JSON.stringify(envelope, (_key, value: unknown) => {
          if (
            typeof value === 'function' ||
            typeof value === 'symbol' ||
            (typeof value === 'number' && !Number.isFinite(value))
          ) {
            throw new Error('Not a JSON value.');
          }
          return value;
        });
        scalarJSON(text);
      } catch {
        throw new DuplexError('invalid_message', 'Frame must contain serializable JSON values.');
      }
      const bytes = new TextEncoder().encode(text).byteLength;
      if (bytes > this.limits.maxFrameBytes) {
        throw new DuplexError('frame_too_large', 'Outgoing frame exceeds the size limit.');
      }
      // A wire handoff must decide bounded admission immediately. Public peer
      // sends instead pace transient bursts for at most one write deadline.
      const connection = this.connection;
      const deadline = Date.now() + this.limits.writeTimeoutMs;
      let paced = false;
      for (;;) {
        if (abandoned?.aborted) return;
        if (!this.isOpen() || this.connection !== connection)
          throw new DuplexError('not_connected', 'Peer is not connected.');
        // The queue is the one ordering gate: room alone is not enough, the
        // sender must also be the one whose turn it is. That is what keeps
        // publication in the order senders reserved, which the request serial
        // is a promise about.
        if (this.outgoing.length < this.limits.queueCapacity && (ticket === undefined || this.publishing[0] === ticket))
          break;
        if (immediate || Date.now() >= deadline) {
          if (this.observer) this.pressure(this.outgoing.length, true);
          endOnRefusal = true;
          throw new DuplexError('busy', 'Output consumer is stalled; queue limit reached.');
        }
        if (!paced) {
          paced = true;
          if (this.observer) this.pressure(this.outgoing.length, false);
        }
        // An observer may synchronously withdraw the call or close the peer.
        if (abandoned?.aborted || !this.isOpen() || this.connection !== connection) continue;
        const room = await new Promise<boolean>((resolve) => {
          const wake = (available: boolean) => {
            clearTimeout(timer);
            this.waitingForRoom.delete(wake);
            abandoned?.removeEventListener('abort', cancel);
            resolve(available);
          };
          const cancel = () => wake(true);
          const timer = setTimeout(() => wake(false), Math.max(0, deadline - Date.now()));
          this.waitingForRoom.add(wake);
          abandoned?.addEventListener('abort', cancel, { once: true });
        });
        if (!room) {
          if (abandoned?.aborted) return;
          if (!this.isOpen() || this.connection !== connection)
            throw new DuplexError('not_connected', 'Peer is not connected.');
          if (this.observer) this.pressure(this.outgoing.length, true);
          endOnRefusal = true;
          throw new DuplexError('busy', 'Output consumer is stalled; queue limit reached.');
        }
      }
      const kind = envelope.kind as string;
      const trace = carriedTrace ?? traceOf(envelope);
      const family = this.family(name);
      // What the peer did comes before the frame that carried it, as the Go
      // peer tells it: an event is emitted, then its frame is sent. The frame
      // itself is observed by the writer, immediately before the bytes leave —
      // one serialization point per peer, so that nothing a frame draws can be
      // observed received ahead of it (docs/runtime/observer.md).
      if (this.observer && kind === 'event')
        this.observe({ type: 'event.emitted', at: new Date(), name, bytes, trace, family });
      const observeSent = this.observer
        ? () =>
            this.observe({
              type: 'frame.sent',
              at: new Date(),
              kind,
              name,
              bytes,
              id: envelope.id as string | undefined,
              trace,
              family,
            })
        : undefined;
      // Accepted for sending is queued, as the profile says and as the Go peer
      // returns: what the transport does with the frame after that is the
      // transport's, held to the write deadline the flush keeps, and a sender
      // that waited on the drain would hold a composition to this consumer.
      this.outgoing.push({ text, started: Date.now(), sent: false, waited: false, observeSent });
      queued = true;
      accepted?.();
      this.release(ticket);
      this.flush();
    } catch (error) {
      this.release(ticket);
      if (!queued) {
        const proof = new UnpublishedError(error);
        // Settle this unqueued attempt before a terminal admission failure
        // broadcasts an uncertain outcome to unrelated accepted requests.
        refused?.(proof);
        if (endOnRefusal) this.fail(asError(error));
        throw proof;
      }
      if (error instanceof UnpublishedError) {
        const dispatched = new DuplexError(error.code, error.message, error.data);
        Object.defineProperty(dispatched, 'cause', { value: error });
        throw dispatched;
      }
      throw error;
    }
  }

  private flush(): void {
    if (this.writeTimer || !this.isOpen()) return;
    const connection = this.connection!;
    while (this.outgoing.length) {
      const item = this.outgoing[0];
      if (Date.now() - item.started >= this.limits.writeTimeoutMs) {
        if (this.observer) this.pressure(this.outgoing.length, true);
        this.fail(new DuplexError('write_timeout', 'Socket output did not drain before the write deadline.'));
        return;
      }
      if (!item.sent && connection.buffered === 0) {
        item.observeSent?.();
        try {
          connection.send({ kind: 'text', data: item.text });
          item.sent = true;
        } catch {
          this.fail(new DuplexError('send_failed', 'WebSocket send failed.'));
          return;
        }
      }
      if (item.sent && connection.buffered === 0) {
        this.outgoing.shift();
        for (const wake of [...this.waitingForRoom]) wake(true);
      } else {
        item.waited = true;
        this.writeTimer = setTimeout(() => {
          this.writeTimer = undefined;
          this.flush();
        }, 5);
        return;
      }
    }
  }

  private receive(incoming: Frame): void {
    if (!this.isOpen()) return;
    if (incoming.kind !== 'text') {
      this.fail(new DuplexError('invalid_message', 'Only JSON text frames are supported.'));
      return;
    }
    const data = incoming.data;
    const bytes = new TextEncoder().encode(data).byteLength;
    if (bytes > this.limits.maxFrameBytes) {
      this.fail(new DuplexError('frame_too_large', 'Incoming frame exceeds the size limit.'));
      return;
    }
    let frame: Envelope;
    try {
      frame = decodeEnvelope(data, this.localPrefix, this.remotePrefix);
    } catch {
      this.fail(new DuplexError('invalid_message', 'Invalid duplex frame.'));
      return;
    }
    const trace = traceOf(frame);
    if (this.observer) {
      const name = this.nameOf(frame);
      this.observe({
        type: 'frame.received',
        at: new Date(),
        kind: frame.kind as string,
        name,
        bytes,
        id: frame.id as string | undefined,
        trace,
        family: this.family(name),
      });
    }
    // Within one connection instance and one direction, each request's serial
    // is greater than every request's published before it. Gaps are allowed; a
    // serial that does not increase is a protocol violation, as a malformed
    // frame is, because the peer could not then say which invocation a later
    // control names.
    if (frame.kind === 'request') {
      const serial = Number((frame.id as string).slice(this.remotePrefix.length));
      if (!Number.isSafeInteger(serial) || serial <= this.admittedSerial) {
        this.fail(new DuplexError('invalid_message', 'Duplex request serial did not increase.'));
        return;
      }
      this.admittedSerial = serial;
    }
    switch (frame.kind) {
      case 'response': {
        const id = frame.id as string;
        const pending = this.takePending(id);
        if (!pending) return; // A cancellation or deadline may precede a late response.
        if (isObject(frame.error)) {
          const error = new DuplexError(frame.error.code as string, frame.error.message as string, frame.error.data);
          this.ended(id, pending, 'error', error.code);
          pending.reject(error);
        } else {
          this.ended(id, pending, 'ok');
          pending.resolve(frame.result);
        }
        break;
      }
      case 'cancel': {
        // A cancel withdraws the request and answers nothing itself: the
        // receiver aborts the handler's signal, and the response — cancelled,
        // whatever the handler goes on to return — is the handler's return,
        // as the profile says and the Go peer does. What frees the
        // correlation is the work ending, not the asking to end it.
        this.incoming.get(frame.id as string)?.controller.abort();
        break;
      }
      case 'request':
        this.request(frame.id as string, frame.method as string, frame.params, trace, frame.meta as Meta | undefined);
        break;
      case 'event':
        this.event(frame.event as string, frame.data, bytes, trace, frame.meta as Meta | undefined);
        break;
    }
  }

  private request(id: string, method: string, params: unknown, trace?: Trace, meta?: Meta): void {
    if (this.incoming.has(id)) {
      this.fail(new DuplexError('invalid_message', 'An incoming request ID is already active.'));
      return;
    }
    if (this.incoming.size >= this.limits.maxConcurrentHandlers) {
      void this.send(
        traced(
          { version: 1, kind: 'response', id, error: { code: 'busy', message: 'Incoming request limit reached.' } },
          trace,
        ),
        method,
      ).catch((error) => this.fail(asError(error)));
      return;
    }
    const controller = new AbortController();
    const incoming: Incoming = {
      controller,
      responded: false,
      trace,
      method,
      started: Date.now(),
      timer: setTimeout(() => {
        controller.abort();
        // What crosses the wire when a receiver's own deadline passes is
        // `cancelled`: the request was abandoned, which is what the caller can
        // act on, and is what the profile and the Go peer both answer.
        // `request_timeout` is a caller's own error and never a frame.
        this.respond(id, incoming, undefined, new DuplexError('cancelled', 'Request deadline exceeded.'), 'timeout');
      }, this.limits.requestTimeoutMs),
    };
    this.incoming.set(id, incoming);
    if (this.observer)
      this.observe({
        type: 'request.started',
        at: new Date(),
        id,
        method,
        incoming: true,
        trace,
        family: this.family(method),
      });
    const context: RequestContext = { peer: this, signal: controller.signal, requestId: id };
    if (meta) context.meta = meta;
    this.propagator.extract(context, trace);
    const attachedHandler = this.wireRequest?.(method);
    void Promise.resolve()
      .then(() => {
        // The peer can close or cancel before the handler's first microtask.
        if (controller.signal.aborted) {
          this.respond(id, incoming, undefined, new DuplexError('cancelled', 'Request was cancelled.'), 'cancelled');
          return;
        }
        if (attachedHandler) return attachedHandler(params, context);
        const handler = this.handlers.get(method);
        if (handler) return handler(params, context);
        if (this.options.dispatch) return this.options.dispatch(method, params, context);
        throw new DuplexError('method_not_found', `Unknown method ${method}.`);
      })
      .then(
        (result) => this.respond(id, incoming, result === undefined ? null : result),
        (error: unknown) => {
          // Anything a handler threw but a public error is this runtime's panic.
          if (this.observer && !(error instanceof DuplexError)) {
            this.observe({
              type: 'handler.panic',
              at: new Date(),
              method,
              value: describe(error),
              trace,
              family: this.family(method),
            });
          }
          this.respond(
            id,
            incoming,
            undefined,
            error instanceof DuplexError ? error : new DuplexError('internal', 'Request handler failed.'),
          );
        },
      )
      .finally(() => {
        clearTimeout(incoming.timer);
        if (this.incoming.get(id) === incoming) this.incoming.delete(id);
      });
  }

  private respond(id: string, incoming: Incoming, result?: unknown, error?: DuplexError, outcome?: Outcome): void {
    if (incoming.responded || this.incoming.get(id) !== incoming || !this.isOpen()) return;
    // A result returned after withdrawal becomes a local cancellation. A
    // handler's public refusal stays an error, whatever its code says.
    if (!error && incoming.controller.signal.aborted) {
      error = new DuplexError('cancelled', 'Request was cancelled.');
      outcome ??= 'cancelled';
    }
    outcome ??= error ? 'error' : 'ok';
    incoming.responded = true;
    clearTimeout(incoming.timer);
    const frame: Envelope = { version: 1, kind: 'response', id };
    if (error)
      frame.error = {
        code: error.code,
        message: error.message,
        ...(error.data === undefined ? {} : { data: error.data }),
      };
    else frame.result = result;
    // The request ends before its response is sent, as the Go peer tells it;
    // a response carries its request's trace, mints none of its own, and is
    // named by nothing — its id says which request it answers.
    if (this.observer) {
      this.observe({
        type: 'request.ended',
        at: new Date(),
        id,
        method: incoming.method,
        incoming: true,
        durationMs: Date.now() - incoming.started,
        outcome,
        // The observer names this peer's deadline; the response still says cancelled.
        errorCode: outcome === 'timeout' ? 'request_timeout' : error?.code,
        trace: incoming.trace,
        family: this.family(incoming.method),
      });
    }
    void this.send(traced(frame, incoming.trace), '').catch(async (error: unknown) => {
      // An unencodable response must settle the call, as the Go peer does,
      // without publishing a replacement for malformed handler output.
      if (error instanceof DuplexError && ['invalid_message', 'frame_too_large'].includes(error.code)) {
        try {
          await this.send(
            traced(
              {
                version: 1,
                kind: 'response',
                id,
                error: { code: 'internal', message: 'Response could not be encoded' },
              },
              incoming.trace,
            ),
          );
          return;
        } catch (fallbackError) {
          this.fail(asError(fallbackError));
          return;
        }
      }
      this.fail(asError(error));
    });
  }

  private event(name: string, data: unknown, bytes: number, trace?: Trace, meta?: Meta): void {
    const queued = this.events.length + Number(this.eventActive);
    if (queued >= this.limits.queueCapacity && !this.stallTimer) {
      // A full queue can be a healthy transient burst, so the producer is paced
      // for one write deadline before the consumer is declared stalled, as the
      // Go peer paces it. The producer is the remote, and a peer here cannot
      // pause what it is handed — a socket delivers when it delivers — so the
      // events are held rather than the reading stopped. The deadline is the
      // same, and so is what happens at it.
      if (this.observer) this.pressure(queued, false);
      this.stallTimer = setTimeout(() => {
        this.stallTimer = undefined;
        if (this.observer) this.pressure(this.events.length + Number(this.eventActive), true);
        this.fail(new DuplexError('busy', 'Event consumer is stalled; queue limit reached.'));
      }, this.limits.writeTimeoutMs);
    }
    this.events.push({ name, data, bytes, trace, meta });
    this.drainEvents();
  }

  /** The queue came back under capacity within its deadline: the burst drained. */
  private drained(): void {
    if (!this.stallTimer || this.events.length + Number(this.eventActive) >= this.limits.queueCapacity) return;
    clearTimeout(this.stallTimer);
    this.stallTimer = undefined;
  }

  private drainEvents(): void {
    if (this.eventActive || !this.isOpen()) return;
    const event = this.events.shift();
    if (!event) return;
    this.eventActive = true;
    // Delivered when it reaches the listeners, not when its frame arrived.
    if (this.observer)
      this.observe({
        type: 'event.delivered',
        at: new Date(),
        name: event.name,
        bytes: event.bytes,
        trace: event.trace,
        family: this.family(event.name),
      });
    const generation = this.generation;
    this.eventTimer = setTimeout(() => {
      if (this.observer) this.pressure(this.events.length, true);
      this.fail(new DuplexError('stalled_consumer', 'Event handler deadline exceeded.'));
    }, this.limits.writeTimeoutMs);
    const listeners = [...this.listeners];
    const wireListener = this.wireEvent?.(event.name);
    if (wireListener) listeners.push(wireListener);
    const context: EventContext = { peer: this };
    setReceivedEventTrace(context, event.trace);
    this.propagator.extract(context, event.trace);
    if (event.meta) context.meta = event.meta;
    let index = 0;
    const finish = () => {
      if (generation !== this.generation) return;
      clearTimeout(this.eventTimer);
      this.eventTimer = undefined;
      this.eventActive = false;
      // Where the backlog is judged: an event finishing is the only thing that
      // brings the queue under capacity, so a burst that drained within its
      // deadline stops being one here.
      this.drained();
      this.drainEvents();
    };
    const next = () => {
      while (index < listeners.length) {
        if (!this.isOpen() || generation !== this.generation) return;
        try {
          const result = listeners[index++](event.name, event.data, context);
          if (result && typeof result.then === 'function') {
            void result.then(next, () => {
              this.notifyError(new DuplexError('event_handler_failed', 'An event handler failed.'));
              next();
            });
            return;
          }
        } catch {
          this.notifyError(new DuplexError('event_handler_failed', 'An event handler failed.'));
        }
      }
      finish();
    };
    // A synchronous listener must finish synchronously: a native WebSocket can
    // deliver a large replay burst within one turn, before any microtask runs.
    next();
  }

  private fail(error: DuplexError, closeConnection = true, code = 4011, reason = CLOSE_REASON): void {
    // Ending a connection settles unrelated, possibly delivered requests too.
    // A failed reply's local proof must not be broadcast as their send outcome.
    if (error instanceof UnpublishedError) {
      const cause = error;
      error = new DuplexError(cause.code, cause.message, cause.data);
      Object.defineProperty(error, 'cause', { value: cause });
    }
    const connection = this.connection;
    if (!connection) return;
    this.connection = undefined;
    this.state = 'disconnected';
    this.generation++;
    this.negotiated = '';
    this.detach?.();
    this.detach = undefined;
    clearTimeout(this.writeTimer);
    this.writeTimer = undefined;
    clearTimeout(this.eventTimer);
    this.eventTimer = undefined;
    clearTimeout(this.stallTimer);
    this.stallTimer = undefined;
    this.eventActive = false;
    this.events.length = 0;
    for (const wake of [...this.waitingForRoom]) wake(true);
    if (this.opening) {
      clearTimeout(this.opening.timer);
      this.opening.reject(error);
      this.opening = undefined;
    }
    for (const id of [...this.pending.keys()]) {
      const pending = this.takePending(id);
      if (!pending) continue;
      this.ended(id, pending, 'error', error.code);
      pending.reject(error);
    }
    for (const [id, request] of this.incoming) {
      clearTimeout(request.timer);
      request.controller.abort();
      if (this.observer && !request.responded) {
        this.observe({
          type: 'request.ended',
          at: new Date(),
          id,
          method: request.method,
          incoming: true,
          durationMs: Date.now() - request.started,
          outcome: 'error',
          errorCode: error.code,
          trace: request.trace,
          family: this.family(request.method),
        });
      }
    }
    this.incoming.clear();
    // Nothing here is a caller's promise: a send resolved when it was queued.
    this.outgoing.length = 0;
    if (closeConnection) {
      // Browser close() restricts application codes to 3000–4999 (or 1000).
      try {
        connection.close(code, reason);
      } catch {
        /* Already closed. */
      }
    }
    // Reported once everything it ended has been. The peer closes the
    // connection exactly when the close is its own.
    if (this.observer)
      this.observe({ type: 'connection.closed', at: new Date(), code, reason, local: closeConnection });
    this.notifyError(error);
    for (const listener of this.closedListeners) {
      try {
        listener(error);
      } catch {
        /* Observers cannot interrupt cleanup. */
      }
    }
  }

  /**
   * Tells this peer's observer one event, the runtime's own or one of a layer
   * running over the peer, which is how a tunnel and a live scope observe — through
   * the peer they run over, rather than through an observer of their own. An
   * observer that is absent costs nothing, and one that throws interrupts nothing.
   */
  observe(event: ObserverEvent): void {
    try {
      this.observer?.observe(event);
    } catch {
      /* Observers cannot interrupt routing. */
    }
  }

  private pressure(queued: number, stalled: boolean): void {
    this.observe({ type: 'backpressure', at: new Date(), queued, stalled, deadlineMs: this.limits.writeTimeoutMs });
  }

  /** Every outgoing call ends once, wherever it settles. */
  private ended(id: string, pending: Pending, outcome: Outcome, errorCode?: string): void {
    if (!this.observer) return;
    this.observe({
      type: 'request.ended',
      at: new Date(),
      id,
      method: pending.method,
      incoming: false,
      durationMs: Date.now() - pending.started,
      outcome,
      errorCode,
      trace: pending.trace,
      family: this.family(pending.method),
    });
  }

  /** What a frame is named on the wire: a request its method, an event its event; a response or a cancel nothing, its id says which request it concerns. */
  private nameOf(frame: Envelope): string {
    switch (frame.kind) {
      case 'request':
        return frame.method as string;
      case 'event':
        return frame.event as string;
      default:
        return '';
    }
  }

  /** The label the caller gave this method or event name; an unlabelled name has none. */
  private family(name: string): string {
    const label = this.options.families?.[name];
    return typeof label === 'string' ? label : '';
  }

  private notifyError(error: DuplexError): void {
    try {
      this.options.onError?.(error);
    } catch {
      /* Observers cannot interrupt routing. */
    }
  }
}

function isWebSocketLike(value: FrameConnection | WebSocketLike): value is WebSocketLike {
  return typeof (value as WebSocketLike).readyState === 'number';
}
/** What the handshake selected, as WebSocket.protocol spells it; '' when none. */
function subprotocolOf(socket: WebSocketLike | undefined): string {
  const selected = (socket as { protocol?: unknown } | undefined)?.protocol;
  return typeof selected === 'string' ? selected : '';
}
/** The reason a peer gives for a close of its own. */
const CLOSE_REASON = 'Duplex connection closed';
/** What a handler threw, as a string: never its params, and never a payload. */
function describe(value: unknown): string {
  try {
    return String(value);
  } catch {
    return '[unprintable value]';
  }
}
/** Validates a component limit, returning it or throwing invalid_options; safe also requires exact integer representation. */
export function positiveInteger(value: unknown, name: string, safe = false): number {
  if (typeof value !== 'number' || !(safe ? Number.isSafeInteger(value) : Number.isInteger(value)) || value <= 0) {
    throw new DuplexError('invalid_options', `${name} must be a positive ${safe ? 'safe ' : ''}integer.`);
  }
  return value;
}
function asError(error: unknown, code = 'internal'): DuplexError {
  return error instanceof DuplexError ? error : new DuplexError(code, 'Duplex operation failed.');
}
