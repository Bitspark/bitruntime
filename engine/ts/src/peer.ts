import type { Endpoint } from '@bitspark/bitwire';
import { PublicError, UnpublishedError } from '../../../core/ts/src/error.ts';
import type { Meta } from '../../../core/ts/src/meta.ts';
import { defaultPropagator, type Propagator, type Trace } from '../../../core/ts/src/trace.ts';
import {
  setReceivedEventTrace,
  type ReceivedEventContext,
  type ReceivedRequestContext,
} from '../../../core/ts/src/internal/context.ts';
import { carrying, decodeEnvelope, isObject, requireName, type Envelope } from '../../../core/ts/src/internal/envelope.ts';
import { ended } from '../../../core/ts/src/internal/frame.ts';
import { LIMIT_DEFAULTS, limits } from '../../../core/ts/src/internal/limits.ts';
import { requestCompletion } from '../../../core/ts/src/internal/request.ts';
import { traceOf, traced } from '../../../core/ts/src/internal/trace.ts';
import { scalarJSON } from '../../../core/ts/src/internal/unicode.ts';
import {
  sendable,
  webSocketConnection,
  type Frame,
  type FrameConnection,
  type WebSocketLike,
} from '../../../transports/ts/src/index.ts';
import { rootWire, type EventHandler, type RequestHandler } from './wire.ts';

/**
 * The protocol revision this engine speaks: the behavior of Nightseam
 * v0.6.0's `nightseam.duplex/1` profile. The name never travels on a
 * connection and is offered as no subprotocol by default.
 */
export const PROTOCOL = 'bitwire/1';
/** The limits a peer runs with unless its options say otherwise. */
export const PEER_DEFAULTS = Object.freeze({ ...LIMIT_DEFAULTS, connectTimeoutMs: 30_000 });

/** Where a peer is between construction and its end; `connected` is the only state that carries frames. */
export type PeerStatus = 'disconnected' | 'connecting' | 'connected';

/** How a peer is made: its role, its preparation, the socket factory, its limits and its propagator. Every member is optional and takes PEER_DEFAULTS. */
export interface PeerOptions {
  /**
   * Runs once after validation, before any connection can read: a receiver it
   * attaches to the peer's wire() is there before anything can arrive. Must be
   * synchronous; a throw fails the construction.
   */
  prepare?: (peer: Peer) => void;
  /** The side of the connection; it decides the prefix of the request ids the peer mints, c: or s:. */
  role?: 'client' | 'server';
  webSocketFactory?: (url: string, protocols?: string[]) => WebSocketLike;
  /**
   * Offered to the server at the handshake, in order of preference; none by
   * default. A server that selects none leaves the connection with none and
   * the protocol is spoken over it either way — but a browser refuses a
   * handshake whose offer went unselected, so a client that offers must be
   * met by a server that selects.
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
  /** Told of failures the peer does not surface elsewhere: why it ended, a listener that failed. */
  onError?: (error: PublicError) => void;
}

type Timer = ReturnType<typeof setTimeout>;
interface Pending {
  resolve: (value: unknown) => void;
  reject: (error: PublicError) => void;
  cleanup: () => void;
}
interface Incoming {
  controller: AbortController;
  timer: Timer;
  responded: boolean;
  /** The request's trace; its response, and nothing else, carries it back. */
  trace?: Trace;
}
interface Outgoing {
  text: string;
  started: number;
  sent: boolean;
  waited: boolean;
}
interface QueuedEvent {
  name: string;
  data: unknown;
  trace?: Trace;
  meta?: Meta;
}

/**
 * A bounded full-duplex bitwire/1 peer over a frames duplex connection. It
 * presents the protocol only through its root Endpoint, wire(): a request or
 * an event sent there travels with its path's canonical encoding as its method
 * or event name, and what the remote side sends is delivered to the root's
 * receiver by the path its name encodes. Message routing never awaits
 * application handlers. Calls are not retried, and connections are never
 * reopened automatically. The caller owns endpoint authentication and
 * authorization of incoming requests.
 */
export class Peer {
  private readonly options: PeerOptions;
  private readonly limits: { -readonly [K in keyof typeof PEER_DEFAULTS]: number };
  private readonly propagator: Propagator;
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
  private opening?: { resolve: () => void; reject: (error: PublicError) => void; timer: Timer };
  private readonly pending = new Map<string, Pending>();
  private readonly incoming = new Map<string, Incoming>();
  private readonly closedListeners = new Set<(error: PublicError) => void>();
  private readonly outgoing: Outgoing[] = [];
  private writeTimer?: Timer;
  /** Senders paced by a full output queue; wire handoffs never join it. */
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
  private wireEvent?: (name: string) => EventHandler | undefined;

  constructor(options: PeerOptions = {}) {
    this.options = options;
    if (options.role !== undefined && options.role !== 'client' && options.role !== 'server') {
      throw new PublicError('invalid_options', 'Peer role must be client or server.');
    }
    // Values deliberately remain configurable without introducing unbounded queues.
    this.limits = limits(PEER_DEFAULTS, options);
    this.propagator = options.propagator ?? defaultPropagator;
    this.localPrefix = options.role === 'server' ? 's:' : 'c:';
    this.remotePrefix = options.role === 'server' ? 'c:' : 's:';
    try {
      const preparation: unknown = options.prepare?.(this);
      if (preparation && typeof (preparation as PromiseLike<unknown>).then === 'function') {
        void Promise.resolve(preparation).catch(() => {});
        throw new PublicError('invalid_options', 'prepare must complete synchronously.');
      }
    } catch (error) {
      // Preparation owns no transport yet, but it can already own wire
      // registrations. Notify their existing close hooks once.
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

  /**
   * This peer's root origin: send access to the remote side's paths, receive
   * attachment for the requests and events the remote side sends, and the
   * connection's closure. Repeated calls return the same endpoint.
   */
  wire(): Endpoint {
    return (this.relativeWire ??= rootWire(this, {
      queueCapacity: this.limits.queueCapacity,
      maxPendingRequests: this.limits.maxPendingRequests,
      maxFrameBytes: this.limits.maxFrameBytes,
      requestTimeoutMs: this.limits.requestTimeoutMs,
      call: (method, params, options, trace) => this.callWithTrace(method, params, options, trace),
      emit: (name, data, options, trace) => this.emitWithTrace(name, data, options, trace),
      dispatch: (request, event) => {
        this.wireRequest = request;
        this.wireEvent = event;
      },
      fail: (error) => this.fail(error),
      close: (code, reason) =>
        this.fail(new PublicError('disconnected', 'Connection closed by caller.'), true, code, reason),
    }));
  }

  /** The side of the connection this peer is. */
  get role(): 'client' | 'server' {
    return this.options.role ?? 'client';
  }

  /**
   * What the WebSocket handshake beneath this peer selected, and '' when it
   * selected none or the peer does not run over a WebSocket. The protocol
   * reads nothing into it.
   */
  get subprotocol(): string {
    return this.negotiated;
  }

  /** Absolute ws/wss URLs are required. Factories may supply platform-specific auth. */
  connect(url: string): Promise<void> {
    if (this.connection) return Promise.reject(new PublicError('already_connected', 'Peer already has a connection.'));
    let endpoint: URL;
    try {
      endpoint = new URL(url);
    } catch {
      return Promise.reject(new PublicError('invalid_url', 'An absolute WebSocket URL is required.'));
    }
    if (!['ws:', 'wss:'].includes(endpoint.protocol) || endpoint.hash || endpoint.username || endpoint.password) {
      return Promise.reject(
        new PublicError('invalid_url', 'Use an absolute ws/wss URL without credentials or a fragment.'),
      );
    }
    let socket: WebSocketLike;
    const protocols = this.options.subprotocols;
    try {
      socket =
        this.options.webSocketFactory?.(endpoint.href, protocols) ??
        (protocols ? new WebSocket(endpoint.href, protocols) : new WebSocket(endpoint.href));
    } catch {
      return Promise.reject(new PublicError('connection_failed', 'Unable to create WebSocket.'));
    }
    return this.attach(socket);
  }

  /**
   * Attach an externally authenticated, connecting or open connection. A
   * WebSocket is wrapped by the adapter; the peer itself never touches one.
   */
  attach(connection: FrameConnection | WebSocketLike): Promise<void> {
    if (this.connection) return Promise.reject(new PublicError('already_connected', 'Peer already has a connection.'));
    const socket = isWebSocketLike(connection) ? connection : undefined;
    const frames = socket === undefined ? (connection as FrameConnection) : webSocketConnection(socket);
    if (frames.state !== 'connecting' && frames.state !== 'open') {
      return Promise.reject(new PublicError('disconnected', 'Cannot attach a closing or closed WebSocket.'));
    }
    this.connection = frames;
    this.generation++;
    this.state = frames.state === 'open' ? 'connected' : 'connecting';
    // The handshake has selected by the time the socket opens, and not before.
    this.negotiated = this.state === 'connected' ? subprotocolOf(socket) : '';
    const current = () => this.connection === frames;
    this.detach = frames.listen({
      open: () => {
        if (!current()) return;
        this.state = 'connected';
        this.negotiated = subprotocolOf(socket);
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
            new PublicError('disconnected', 'Connection closed; outstanding call outcomes may be unknown.'),
            false,
            code,
            reason,
          );
      },
      error: () => {
        if (current()) this.fail(new PublicError('connection_failed', 'WebSocket connection failed.'));
      },
    });
    if (this.state === 'connected') return Promise.resolve();
    return new Promise<void>((resolve, reject) => {
      const timer = setTimeout(
        () => this.fail(new PublicError('connect_timeout', 'Connection timed out.')),
        this.limits.connectTimeoutMs,
      );
      this.opening = { resolve, reject, timer };
    });
  }

  /** Ends the connection with a normal close; every pending call rejects with `disconnected`. */
  close(): void {
    this.fail(new PublicError('disconnected', 'Connection closed by caller.'), true, 1000);
  }

  /** Tells the listener once, when the peer ends, why it ended; returns what removes the listener. */
  onClose(listener: (error: PublicError) => void): () => void {
    this.closedListeners.add(listener);
    return () => {
      this.closedListeners.delete(listener);
    };
  }

  /**
   * Performs the bounded admission of one outgoing request immediately: the
   * root hands requests over in its delivery order, and this reserves the
   * serial and enqueues before it returns. It resolves with the result, or
   * rejects with the remote's public error or the peer's own: `request_timeout`
   * past the deadline, `cancelled` when the signal fired, `busy` when too many
   * calls are outstanding, `disconnected` when the connection ended first.
   */
  private callWithTrace<T>(
    method: string,
    params: unknown,
    options: { signal?: AbortSignal; meta?: Meta },
    trace: Trace | undefined,
  ): Promise<T> {
    try {
      requireName(method, 'method');
    } catch (error) {
      return Promise.reject(new UnpublishedError(error));
    }
    if (!this.isOpen())
      return Promise.reject(new UnpublishedError(new PublicError('not_connected', 'Peer is not connected.')));
    if (options.signal?.aborted)
      return Promise.reject(new UnpublishedError(new PublicError('cancelled', 'Call was cancelled before sending.')));
    if (this.pending.size >= this.limits.maxPendingRequests) {
      return Promise.reject(new UnpublishedError(new PublicError('busy', 'Outstanding call limit reached.')));
    }
    if (this.nextID >= Number.MAX_SAFE_INTEGER) {
      return Promise.reject(
        new UnpublishedError(
          new PublicError('identifier_exhausted', 'Create a new peer before issuing further calls.'),
        ),
      );
    }
    const id = this.localPrefix + (++this.nextID).toString(10);
    // One trace for the exchange: the request carries it and its cancel repeats it.
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
    };
    this.pending.set(id, pending);
    const cancel = (error: PublicError) => {
      if (!this.takePending(id)) return;
      completion.reject(accepted ? error : new UnpublishedError(error));
      // Cancellation is best effort, as in Go. It never waits for room and
      // an already cancelled caller cannot end a healthy carrier merely
      // because its cancellation frame has no room in the output queue.
      if (accepted && this.outgoing.length < this.limits.queueCapacity)
        void this.send(traced({ version: 1, kind: 'cancel', id }, trace)).catch(() => {});
    };
    completion.wait(options.signal, this.limits.requestTimeoutMs, method, cancel);
    const refused = (failure: unknown) => {
      const unsent = this.takePending(id);
      if (!unsent) return;
      unsent.reject(asError(failure, 'send_failed'));
    };
    if (!completion.settled)
      void this.send(request, refused, true, admission.signal, () => {
        accepted = true;
      }).catch(refused);
    return completion.promise;
  }

  /**
   * Queues one outgoing event immediately. It resolves when its frame was
   * accepted for sending, which is queued for this connection and no more: an
   * event says nothing about receipt. The queue's own deadline continues
   * behind it and ends a connection that never drains.
   */
  private emitWithTrace(event: string, data: unknown, options: { meta?: Meta }, trace?: Trace): Promise<void> {
    try {
      requireName(event, 'event');
      return this.send(carrying(traced({ version: 1, kind: 'event', event, data }, trace), options.meta), undefined, true);
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
    refused?: (error: UnpublishedError) => void,
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
      if (!this.isOpen()) throw new PublicError('not_connected', 'Peer is not connected.');
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
        throw new PublicError('invalid_message', 'Frame must contain serializable JSON values.');
      }
      const bytes = new TextEncoder().encode(text).byteLength;
      if (bytes > this.limits.maxFrameBytes) {
        throw new PublicError('frame_too_large', 'Outgoing frame exceeds the size limit.');
      }
      // A wire handoff must decide bounded admission immediately. A response
      // instead paces a transient burst for at most one write deadline.
      const connection = this.connection;
      const deadline = Date.now() + this.limits.writeTimeoutMs;
      for (;;) {
        if (abandoned?.aborted) return;
        if (!this.isOpen() || this.connection !== connection)
          throw new PublicError('not_connected', 'Peer is not connected.');
        // The queue is the one ordering gate: room alone is not enough, the
        // sender must also be the one whose turn it is. That is what keeps
        // publication in the order senders reserved, which the request serial
        // is a promise about.
        if (this.outgoing.length < this.limits.queueCapacity && (ticket === undefined || this.publishing[0] === ticket))
          break;
        if (immediate || Date.now() >= deadline) {
          endOnRefusal = true;
          throw new PublicError('busy', 'Output consumer is stalled; queue limit reached.');
        }
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
            throw new PublicError('not_connected', 'Peer is not connected.');
          endOnRefusal = true;
          throw new PublicError('busy', 'Output consumer is stalled; queue limit reached.');
        }
      }
      // Accepted for sending is queued, as the protocol says: what the
      // transport does with the frame after that is the transport's, held to
      // the write deadline the flush keeps, and a sender that waited on the
      // drain would hold a composition to this consumer.
      this.outgoing.push({ text, started: Date.now(), sent: false, waited: false });
      queued = true;
      accepted?.();
      this.release(ticket);
      this.flush();
    } catch (error) {
      this.release(ticket);
      if (!queued) {
        // A refusal that ends the carrier reports the carrier ended, keeping
        // why as its cause (R26); the peer itself ends with that cause.
        const proof = new UnpublishedError(endOnRefusal ? ended(error) : error);
        // Settle this unqueued attempt before a terminal admission failure
        // broadcasts an uncertain outcome to unrelated accepted requests.
        refused?.(proof);
        if (endOnRefusal) this.fail(asError(error));
        throw proof;
      }
      if (error instanceof UnpublishedError) {
        const dispatched = new PublicError(error.code, error.message, error.data);
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
      const item = this.outgoing[0]!;
      if (Date.now() - item.started >= this.limits.writeTimeoutMs) {
        this.fail(new PublicError('write_timeout', 'Socket output did not drain before the write deadline.'));
        return;
      }
      if (!item.sent && connection.buffered === 0) {
        try {
          connection.send({ kind: 'text', data: item.text });
          item.sent = true;
        } catch {
          this.fail(new PublicError('send_failed', 'WebSocket send failed.'));
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
      this.fail(new PublicError('invalid_message', 'Only JSON text frames are supported.'));
      return;
    }
    const data = incoming.data;
    const bytes = new TextEncoder().encode(data).byteLength;
    if (bytes > this.limits.maxFrameBytes) {
      this.fail(new PublicError('frame_too_large', 'Incoming frame exceeds the size limit.'));
      return;
    }
    let frame: Envelope;
    try {
      frame = decodeEnvelope(data, this.localPrefix, this.remotePrefix);
    } catch {
      this.fail(new PublicError('invalid_message', 'Invalid duplex frame.'));
      return;
    }
    const trace = traceOf(frame);
    // Within one connection instance and one direction, each request's serial
    // is greater than every request's published before it. Gaps are allowed; a
    // serial that does not increase is a protocol violation, as a malformed
    // frame is, because the peer could not then say which invocation a later
    // control names.
    if (frame.kind === 'request') {
      const serial = Number((frame.id as string).slice(this.remotePrefix.length));
      if (!Number.isSafeInteger(serial) || serial <= this.admittedSerial) {
        this.fail(new PublicError('invalid_message', 'Duplex request serial did not increase.'));
        return;
      }
      this.admittedSerial = serial;
    }
    switch (frame.kind) {
      case 'response': {
        const pending = this.takePending(frame.id as string);
        if (!pending) return; // A cancellation or deadline may precede a late response.
        if (isObject(frame.error)) {
          pending.reject(new PublicError(frame.error.code as string, frame.error.message as string, frame.error.data));
        } else {
          pending.resolve(frame.result);
        }
        break;
      }
      case 'cancel': {
        // A cancel withdraws the request and answers nothing itself: the
        // receiver aborts the handler's signal, and the response — cancelled,
        // whatever the handler goes on to return — is the handler's return.
        // What frees the correlation is the work ending, not the asking to end it.
        this.incoming.get(frame.id as string)?.controller.abort();
        break;
      }
      case 'request':
        this.request(frame.id as string, frame.method as string, frame.params, trace, frame.meta as Meta | undefined);
        break;
      case 'event':
        this.event(frame.event as string, frame.data, trace, frame.meta as Meta | undefined);
        break;
    }
  }

  private request(id: string, method: string, params: unknown, trace?: Trace, meta?: Meta): void {
    if (this.incoming.has(id)) {
      this.fail(new PublicError('invalid_message', 'An incoming request ID is already active.'));
      return;
    }
    if (this.incoming.size >= this.limits.maxConcurrentHandlers) {
      void this.send(
        traced(
          { version: 1, kind: 'response', id, error: { code: 'busy', message: 'Incoming request limit reached.' } },
          trace,
        ),
      ).catch((error) => this.fail(asError(error)));
      return;
    }
    const controller = new AbortController();
    const incoming: Incoming = {
      controller,
      responded: false,
      trace,
      timer: setTimeout(() => {
        controller.abort();
        // What crosses the wire when a receiver's own deadline passes is
        // `cancelled`: the request was abandoned, which is what the caller can
        // act on. `request_timeout` is a caller's own error and never a frame.
        this.respond(id, incoming, undefined, new PublicError('cancelled', 'Request deadline exceeded.'));
      }, this.limits.requestTimeoutMs),
    };
    this.incoming.set(id, incoming);
    // What the handler is given: the signal, the request's id, and the trace
    // and meta the frame brought. The peer itself is not reachable from it.
    const context: ReceivedRequestContext = { signal: controller.signal, requestId: id };
    if (meta) context.meta = meta;
    this.propagator.extract(context, trace);
    // The receiver chosen now stays with this request.
    const attachedHandler = this.wireRequest?.(method);
    void Promise.resolve()
      .then(() => {
        // The peer can close or cancel before the handler's first microtask.
        if (controller.signal.aborted) {
          this.respond(id, incoming, undefined, new PublicError('cancelled', 'Request was cancelled.'));
          return;
        }
        if (attachedHandler) return attachedHandler(params, context, trace);
        throw new PublicError('method_not_found', `Unknown method ${method}.`);
      })
      .then(
        (result) => this.respond(id, incoming, result === undefined ? null : result),
        (error: unknown) =>
          this.respond(
            id,
            incoming,
            undefined,
            error instanceof PublicError ? error : new PublicError('internal', 'Request handler failed.'),
          ),
      )
      .finally(() => {
        clearTimeout(incoming.timer);
        if (this.incoming.get(id) === incoming) this.incoming.delete(id);
      });
  }

  private respond(id: string, incoming: Incoming, result?: unknown, error?: PublicError): void {
    if (incoming.responded || this.incoming.get(id) !== incoming || !this.isOpen()) return;
    // A result returned after withdrawal becomes a local cancellation. A
    // handler's public refusal stays an error, whatever its code says.
    if (!error && incoming.controller.signal.aborted) error = new PublicError('cancelled', 'Request was cancelled.');
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
    // A response carries its request's trace and mints none of its own.
    void this.send(traced(frame, incoming.trace)).catch(async (error: unknown) => {
      // An unencodable response must settle the call without publishing a
      // replacement for malformed handler output.
      if (error instanceof PublicError && ['invalid_message', 'frame_too_large'].includes(error.code)) {
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

  private event(name: string, data: unknown, trace?: Trace, meta?: Meta): void {
    const queued = this.events.length + Number(this.eventActive);
    if (queued >= this.limits.queueCapacity && !this.stallTimer) {
      // A full queue can be a healthy transient burst, so the producer is paced
      // for one write deadline before the consumer is declared stalled. The
      // producer is the remote, and a peer here cannot pause what it is handed
      // — a socket delivers when it delivers — so the events are held rather
      // than the reading stopped. The deadline is the same, and so is what
      // happens at it.
      this.stallTimer = setTimeout(() => {
        this.stallTimer = undefined;
        this.fail(new PublicError('busy', 'Event consumer is stalled; queue limit reached.'));
      }, this.limits.writeTimeoutMs);
    }
    this.events.push({ name, data, trace, meta });
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
    const generation = this.generation;
    this.eventTimer = setTimeout(() => {
      this.fail(new PublicError('stalled_consumer', 'Event handler deadline exceeded.'));
    }, this.limits.writeTimeoutMs);
    const listeners: EventHandler[] = [];
    const wireListener = this.wireEvent?.(event.name);
    if (wireListener) listeners.push(wireListener);
    const context: ReceivedEventContext = {};
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
          const result = listeners[index++]!(event.name, event.data, context);
          if (result && typeof result.then === 'function') {
            void result.then(next, () => {
              this.notifyError(new PublicError('event_handler_failed', 'An event handler failed.'));
              next();
            });
            return;
          }
        } catch {
          this.notifyError(new PublicError('event_handler_failed', 'An event handler failed.'));
        }
      }
      finish();
    };
    // A synchronous listener must finish synchronously: a native WebSocket can
    // deliver a large replay burst within one turn, before any microtask runs.
    next();
  }

  private fail(error: PublicError, closeConnection = true, code = 4011, reason = CLOSE_REASON): void {
    // Ending a connection settles unrelated, possibly delivered requests too.
    // A failed reply's local proof must not be broadcast as their send outcome.
    if (error instanceof UnpublishedError) {
      const cause = error;
      error = new PublicError(cause.code, cause.message, cause.data);
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
    // Every pending call learns the carrier ended, and why, as its cause.
    const outcome = ended(error);
    for (const id of [...this.pending.keys()]) this.takePending(id)?.reject(outcome);
    for (const request of this.incoming.values()) {
      clearTimeout(request.timer);
      request.controller.abort();
    }
    this.incoming.clear();
    // Nothing here is a caller's promise: a send resolved when it was queued.
    this.outgoing.length = 0;
    if (closeConnection) {
      try {
        // A code that may only be observed is never sent. A connection of the
        // seam ends only by a close, so it ends with a normal one instead.
        if (sendable(code)) connection.close(code, reason);
        else connection.close();
      } catch {
        /* Already closed. */
      }
    }
    this.notifyError(error);
    for (const listener of this.closedListeners) {
      try {
        listener(error);
      } catch {
        /* Listeners cannot interrupt cleanup. */
      }
    }
  }

  private notifyError(error: PublicError): void {
    try {
      this.options.onError?.(error);
    } catch {
      /* Diagnostics cannot interrupt routing. */
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
/** The reason a peer gives for a close of its own, as bitwire/1 sends it. */
const CLOSE_REASON = 'Duplex connection closed';
function asError(error: unknown, code = 'internal'): PublicError {
  return error instanceof PublicError ? error : new PublicError(code, 'Duplex operation failed.');
}
