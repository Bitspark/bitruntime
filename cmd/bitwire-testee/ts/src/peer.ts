// Ported from nightseam v0.6.0 conformance/ts/src/peer.ts (5cc9723).
/**
 * The peer under control: bitruntime's protocol engine, reached only through
 * its root endpoint and a dispatcher, as a hand-written adapter reaches it.
 *
 * A driver-1 `method` or event `name` is the one-segment path [name]: the
 * engine carries it on the wire as that path's canonical encoding, and
 * delivers to it only a name that decodes to it. bitruntime has no observer,
 * so `observe`, `families` and peer.observed are unsupported.
 */
import type { Server } from 'node:http';
import { WebSocket, WebSocketServer } from 'ws';
import type { Path } from '@bitspark/bitwire';
import { PublicError, defaultPropagator, respond, type Meta } from '@bitspark/bitruntime/core';
import {
  call,
  createDispatcher,
  emit,
  register,
  type Dispatcher,
  type EventListener,
  type Handler,
} from '@bitspark/bitruntime/dispatch';
import { PEER_DEFAULTS, Peer, type PeerOptions } from '@bitspark/bitruntime/engine';
import type { FrameConnection } from '@bitspark/bitruntime/transports';
import {
  Inbox,
  fail,
  intOf,
  invalid,
  stringOf,
  unsupported,
  withinOf,
  type Args,
  type Op,
  type Testee,
} from './driver.ts';
import { asLike, isConn, serve } from './seam.ts';

/** The path a driver-1 method or event name addresses. */
const pathOf = (name: string): Path => [name];

/** One phase of one request a canned handler served, and what it carried. */
interface Lifecycle {
  id: string;
  method: string;
  phase: 'started' | 'ended';
  outcome?: string;
  meta?: Meta;
}

/** A request handler and an event listener sharing one name's route. */
interface Route {
  request?: Handler;
  event: EventListener;
  detach: () => void;
}

/**
 * A peer under control: the engine's peer, the dispatcher that owns its
 * root's one receiver, what it received while the runner was not asking, and
 * the code its connection ended under.
 */
export class Controlled {
  readonly events = new Inbox<{ name: string; data: unknown; meta?: Meta }>();
  readonly requests = new Inbox<Lifecycle>();
  readonly peer: Peer;
  readonly dispatcher: Dispatcher;
  /** The peer's own deadline for a call, which its options set. */
  readonly requestTimeoutMs: number;
  private readonly routes = new Map<string, Route>();
  private readonly holds = new Set<{ name: string; release: () => void }>();
  private ended = false;
  private code?: number;
  private readonly closeWaiters = new Set<() => void>();

  constructor(options: PeerOptions) {
    this.peer = new Peer(options);
    this.requestTimeoutMs = options.requestTimeoutMs ?? PEER_DEFAULTS.requestTimeoutMs;
    // Attached before the peer has a connection, so nothing can arrive first.
    this.dispatcher = createDispatcher(this.peer.wire());
    // Every name nobody registered: an event is held for peer.await_event, a
    // request is refused by name as a dispatcher refuses it.
    this.dispatcher.registerPrefix([], {
      message: (path, message) => {
        const frame = message.frame;
        if (frame.kind === 'event') {
          if (path.length === 1) this.record(path[0]!, frame.data, frame.meta);
        } else if (frame.kind === 'request') {
          respond(message, undefined, new PublicError('method_not_found', 'Unknown method.'));
        }
      },
    });
    this.peer.onClose(() => {
      this.ended = true;
      this.events.close();
      this.requests.close();
      for (const hold of [...this.holds]) hold.release();
    });
  }

  /** Holds one event for peer.await_event, and releases what waits on its name. */
  record(name: string, data: unknown, meta: Readonly<Meta> | undefined): void {
    this.events.put({ name, data, ...(meta ? { meta: { ...meta } } : {}) });
    for (const hold of [...this.holds]) if (hold.name === name) hold.release();
  }

  /** Replaces what serves one name; an event there is held unless a behaviour says otherwise. */
  route(name: string, change: { request?: Handler; event?: EventListener }): void {
    const current = this.routes.get(name);
    const request = change.request ?? current?.request;
    const event = change.event ?? current?.event ?? ((data, context) => this.record(name, data, context.meta));
    current?.detach();
    this.routes.delete(name);
    const detach = register(this.dispatcher, pathOf(name), { ...(request ? { request } : {}), event });
    this.routes.set(name, { ...(request ? { request } : {}), event, detach });
  }

  /** Settles when the remote emits name after this, or the peer ends. */
  until(name: string): Promise<void> {
    if (this.ended) return Promise.resolve();
    return new Promise((resolve) => {
      const hold = {
        name,
        release: () => {
          this.holds.delete(hold);
          resolve();
        },
      };
      this.holds.add(hold);
    });
  }

  /** The WebSocket the peer runs over tells the code the connection ended under. */
  watch(socket: WebSocket): void {
    socket.on('error', (error: Error & { code?: string }) => {
      // The socket's maxPayload is the peer's frame limit. A frame over it is
      // refused with 1009, which this side ends the connection under; the
      // socket's own close reports the remote's reply instead, or 1006 when
      // none arrives. Any other error's close reports it.
      if (error.code === 'WS_ERR_UNSUPPORTED_MESSAGE_LENGTH') this.closedWith(1009);
    });
    socket.on('close', (code: number) => this.closedWith(code));
  }

  /** So does a frames connection handed to the peer, where no socket is in sight. */
  watchConnection(connection: FrameConnection): void {
    connection.listen({
      close: (code) => this.closedWith(code),
      error: () => this.closedWith(1006),
    });
  }

  private closedWith(code: number): void {
    if (this.code !== undefined) return;
    this.code = code;
    for (const waiter of [...this.closeWaiters]) waiter();
  }

  /** The code the connection ended under, or undefined when it did not end in time. */
  closed(withinMs: number): Promise<number | undefined> {
    if (this.code !== undefined) return Promise.resolve(this.code);
    return new Promise((resolve) => {
      const waiter = () => {
        clearTimeout(timer);
        this.closeWaiters.delete(waiter);
        resolve(this.code);
      };
      const timer = setTimeout(waiter, withinMs);
      this.closeWaiters.add(waiter);
    });
  }

  shutdown(): void {
    this.peer.close();
  }
}

export const isPeer = (object: unknown): object is Controlled => object instanceof Controlled;

/** A listener accepting one peer at a URL, as the server. */
class PeerListener {
  readonly accepted: Inbox<Controlled>;
  readonly made: Controlled[];
  readonly server: Server;
  readonly sockets: WebSocketServer;
  readonly url: string;
  constructor(
    accepted: Inbox<Controlled>,
    made: Controlled[],
    served: { server: Server; sockets: WebSocketServer; url: string },
  ) {
    this.accepted = accepted;
    this.made = made;
    this.server = served.server;
    this.sockets = served.sockets;
    this.url = served.url;
  }
  shutdown(): void {
    this.accepted.close();
    this.sockets.close();
    this.server.close();
    // A peer nobody accepted is no handle's; the reset ends it here.
    for (const peer of this.made) peer.shutdown();
    for (const client of this.sockets.clients) client.terminate();
  }
}

const isPeerListener = (object: unknown): object is PeerListener => object instanceof PeerListener;

/** One call in flight. */
class Call {
  readonly promise: Promise<unknown>;
  readonly controller: AbortController;
  constructor(promise: Promise<unknown>, controller: AbortController) {
    this.promise = promise;
    this.controller = controller;
    promise.catch(() => {
      /* Awaited by call.await, or never. */
    });
  }
}

const isCall = (object: unknown): object is Call => object instanceof Call;

/** How a call ended, as the driver's codes. */
const callError = (error: unknown): Record<string, unknown> => {
  if (!(error instanceof PublicError)) return { code: 'internal', message: String(error) };
  switch (error.code) {
    case 'cancelled':
    case 'request_timeout':
    case 'disconnected':
      return { code: error.code, message: error.message };
    case 'not_connected':
    case 'connection_failed':
      return { code: 'disconnected', message: error.message };
    case 'send_failed':
      return { code: 'failed', message: error.message };
  }
  const out: Record<string, unknown> = { code: error.code, message: error.message };
  if (error.data !== undefined) out.data = error.data;
  return out;
};

interface Behavior {
  kind: string;
  value?: unknown;
  code?: string;
  message?: string;
  data?: unknown;
  method?: string;
  params?: unknown;
  event?: string;
  then?: unknown;
  until?: string;
}

const BEHAVIORS = new Set(['echo', 'return', 'fail', 'wait', 'hold', 'panic', 'reverse', 'emit']);

const behaviorOf = (args: Args): Behavior => {
  const raw = args.behavior;
  if (raw === undefined) return { kind: '' };
  if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) throw invalid('behavior is an object');
  return raw as Behavior;
};

/** The carriage a step gave a call or an event, and undefined where it gave none. */
const metaOf = (args: Args): Meta | undefined => {
  const raw = args.meta;
  if (raw === undefined) return undefined;
  if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) throw invalid('meta is an object of strings');
  for (const value of Object.values(raw as Record<string, unknown>)) {
    if (typeof value !== 'string') throw invalid('meta is an object of strings');
  }
  return raw as Meta;
};

/** What the panic behaviours give up with. */
const panicValue = (value: unknown): string =>
  typeof value === 'string' ? value : JSON.stringify(value ?? 'the handler gave up');

/** A handler that does what its behaviour says and records its lifecycle. */
const canned =
  (p: Controlled, method: string, b: Behavior): Handler =>
  async (params, context) => {
    p.requests.put({
      id: context.requestId,
      method,
      phase: 'started',
      ...(context.meta ? { meta: { ...context.meta } } : {}),
    });
    const ended = (outcome: string) => p.requests.put({ id: context.requestId, method, phase: 'ended', outcome });
    try {
      let result: unknown;
      switch (b.kind) {
        case 'echo':
          result = params;
          break;
        case 'return':
          result = b.value ?? null;
          break;
        case 'fail':
          throw new PublicError(b.code ?? 'internal', b.message ?? '', b.data);
        case 'wait':
          await new Promise<void>((resolve) => {
            if (context.signal.aborted) resolve();
            else context.signal.addEventListener('abort', () => resolve(), { once: true });
          });
          // Returning with an aborted signal is the handler seeing its own
          // cancellation; the dispatcher answers it cancelled. A PublicError
          // would be a public refusal instead.
          ended('cancelled');
          return null;
        case 'hold':
          // The one handler that does not stop when it is told to: it holds
          // the request until the remote emits what releases it, cancelled or
          // not, which is how a scenario holds a withdrawn request open.
          await p.until(b.until ?? '');
          result = b.value ?? null;
          break;
        case 'panic':
          throw new Error(panicValue(b.value));
        case 'reverse':
          result = await call(context.wire, pathOf(b.method ?? ''), b.params ?? params, {
            context,
            signal: context.signal,
          });
          break;
        case 'emit':
          emit(context.wire, pathOf(b.event ?? ''), b.data ?? null, { context });
          result = b.then ?? null;
          break;
        default:
          throw new PublicError('internal', `no such behaviour: ${b.kind}`);
      }
      ended('ok');
      return result;
    } catch (error) {
      if (error instanceof PublicError) ended(context.signal.aborted ? 'cancelled' : 'error');
      else ended('panic');
      throw error;
    }
  };

/**
 * What a peer.listen selects from or a peer.dial offers at the handshake;
 * absent, none is offered and none selected.
 */
const subprotocolsOf = (args: Args): string[] => {
  const value = args.subprotocols;
  if (value === undefined) return [];
  if (!Array.isArray(value) || value.some((token) => typeof token !== 'string'))
    throw invalid('subprotocols is an array of strings');
  return value as string[];
};

/** The peer options a step names, as the engine's; the engine validates the bounds. */
const optionsOf = (args: Args): PeerOptions => {
  const raw = args.options;
  const options: PeerOptions = {};
  if (raw === undefined) return options;
  if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) throw invalid('options is an object');
  for (const [key, value] of Object.entries(raw as Record<string, unknown>)) {
    switch (key) {
      case 'max_frame_bytes':
        options.maxFrameBytes = value as number;
        break;
      case 'max_pending_requests':
        options.maxPendingRequests = value as number;
        break;
      case 'queue_capacity':
        options.queueCapacity = value as number;
        break;
      case 'request_timeout_ms':
        options.requestTimeoutMs = value as number;
        break;
      case 'write_timeout_ms':
        options.writeTimeoutMs = value as number;
        break;
      case 'propagate':
        if (typeof value !== 'boolean') throw invalid('propagate is a boolean');
        // Every request carries a trace and a handler's calls are its children:
        // what the default propagator does, which a peer uses when none is named.
        if (value) options.propagator = defaultPropagator;
        break;
      case 'observe':
        if (value === true) throw unsupported('bitruntime has no observer');
        break;
      case 'families':
        if (value !== null && typeof value === 'object' && Object.keys(value).length > 0)
          throw unsupported('bitruntime has no observer, and no family labels');
        break;
      default:
        throw unsupported(`option ${key}`);
    }
  }
  return options;
};

/** A peer made with the step's options; bounds the engine refuses are the step's error. */
const make = (options: PeerOptions): Controlled => {
  try {
    return new Controlled(options);
  } catch (error) {
    if (error instanceof PublicError) throw invalid(`${error.code}: ${error.message}`);
    throw error;
  }
};

/** The receive limit a WebSocket beneath a peer has: the peer's own. */
const limitOf = (options: PeerOptions): number => options.maxFrameBytes ?? PEER_DEFAULTS.maxFrameBytes;

export function peerOps(t: Testee): Record<string, Op> {
  const peerOf = (args: Args) => t.lookup(args.on, isPeer, 'a peer');
  return {
    'peer.listen': async (args) => {
      const options: PeerOptions = { ...optionsOf(args), role: 'server' };
      const offered = subprotocolsOf(args);
      // Refused bounds are this step's answer, not a connection that never comes.
      make(options);
      const accepted = new Inbox<Controlled>();
      const made: Controlled[] = [];
      let first = true;
      // The selection is the server's, in its own order of preference, and
      // none where the lists do not meet; ws would otherwise echo the client's
      // first offer back, which is not what a server naming none does.
      const handleProtocols = (protocols: Set<string>): string | false =>
        offered.find((token) => protocols.has(token)) ?? false;
      const served = await serve(
        (server) => new WebSocketServer({ server, maxPayload: limitOf(options), handleProtocols }),
        (socket) => {
          if (!first) {
            socket.close(1008, 'one connection is accepted');
            return;
          }
          first = false;
          const p = new Controlled(options);
          made.push(p);
          p.watch(socket);
          // The socket itself, not a connection already wrapped around it: the
          // peer wraps it the same way and reads the selected subprotocol off it.
          p.peer.attach(asLike(socket)).then(
            () => accepted.put(p),
            () => socket.terminate(),
          );
        },
      );
      const listener = new PeerListener(accepted, made, served);
      return { handle: t.mint('pl', listener), url: listener.url };
    },
    'peer.accept': async (args) => {
      const l = t.lookup(args.on, isPeerListener, 'a peer listener');
      const { item } = await l.accepted.await(withinOf(args), () => true);
      if (!item) throw fail('timeout', 'nobody connected');
      return { handle: t.mint('p', item), subprotocol: item.peer.subprotocol };
    },
    'peer.dial': async (args) => {
      const url = stringOf(args, 'url', true);
      const base = optionsOf(args);
      const offered = subprotocolsOf(args);
      const limit = limitOf(base);
      const dialed: { socket?: WebSocket } = {};
      const p = make({
        ...base,
        role: 'client',
        ...(offered.length > 0 ? { subprotocols: offered } : {}),
        webSocketFactory: (target, protocols) => {
          const socket =
            protocols && protocols.length > 0
              ? new WebSocket(target, protocols, { maxPayload: limit })
              : new WebSocket(target, { maxPayload: limit });
          dialed.socket = socket;
          return asLike(socket);
        },
      });
      const connecting = p.peer.connect(url);
      if (dialed.socket) p.watch(dialed.socket);
      try {
        await connecting;
      } catch (error) {
        p.shutdown();
        dialed.socket?.terminate();
        throw fail('failed', error instanceof Error ? `${error.name}: ${error.message}` : String(error));
      }
      return { handle: t.mint('p', p), subprotocol: p.peer.subprotocol };
    },
    'peer.over': async (args) => {
      const c = t.lookup(args.on, isConn, 'a connection');
      const role = stringOf(args, 'role', true);
      if (role !== 'client' && role !== 'server') throw invalid('role is client or server');
      if (!c.lazy) throw invalid('a peer is made over a lazily consumed connection');
      const p = make({ ...optionsOf(args), role });
      const connection = c.release();
      const attaching = p.peer.attach(connection);
      // After the peer's own reader, so that the peer is handed every frame.
      // A WebSocket tells the code this side ended under, as for a dialled or
      // accepted peer; a pipe has only its connection's close.
      if (c.socket) p.watch(c.socket);
      else p.watchConnection(connection);
      await attaching.catch((error: unknown) => {
        throw fail('failed', error instanceof Error ? `${error.name}: ${error.message}` : String(error));
      });
      return { handle: t.mint('p', p) };
    },
    'peer.handle': (args) => {
      const p = peerOf(args);
      const method = stringOf(args, 'method', true);
      const b = behaviorOf(args);
      if (b.kind === 'through') throw unsupported('the through behaviour belongs to the live layer');
      if (!BEHAVIORS.has(b.kind)) throw invalid(`no such behaviour: ${b.kind}`);
      try {
        p.route(method, { request: canned(p, method, b) });
      } catch (error) {
        throw invalid(String(error));
      }
      return {};
    },
    'peer.on_event': (args) => {
      const p = peerOf(args);
      const name = stringOf(args, 'name', true);
      const b = behaviorOf(args);
      let event: EventListener;
      switch (b.kind) {
        case '':
        case 'record':
          event = (data, context) => p.record(name, data, context.meta);
          break;
        case 'block':
          event = () =>
            new Promise<void>(() => {
              /* never */
            });
          break;
        case 'panic':
          event = () => {
            throw new Error(panicValue(b.value));
          };
          break;
        default:
          throw invalid('an event handler records, blocks or panics');
      }
      try {
        p.route(name, { event });
      } catch (error) {
        throw invalid(String(error));
      }
      return {};
    },
    'peer.call': (args) => {
      const p = peerOf(args);
      const method = stringOf(args, 'method', true);
      const timeout = intOf(args, 'timeout_ms', 0);
      if (timeout < 0) throw invalid('timeout_ms is not negative');
      const meta = metaOf(args);
      const controller = new AbortController();
      // Absent, the call has only the peer's deadline, which is the one it
      // waits for as well.
      const promise = call(p.dispatcher, pathOf(method), args.params === undefined ? null : args.params, {
        signal: controller.signal,
        timeoutMs: timeout > 0 ? timeout : p.requestTimeoutMs,
        ...(meta ? { meta } : {}),
      });
      return { handle: t.mint('call', new Call(promise, controller)) };
    },
    'call.await': async (args) => {
      const c = t.lookup(args.on, isCall, 'a call');
      let timer: ReturnType<typeof setTimeout> | undefined;
      const settled = await Promise.race([
        c.promise.then(
          (result) => ({ result }),
          (error: unknown) => ({ error }),
        ),
        new Promise<undefined>((resolve) => {
          timer = setTimeout(() => resolve(undefined), withinOf(args));
        }),
      ]);
      clearTimeout(timer);
      if (settled === undefined) throw fail('timeout', 'no response');
      if ('error' in settled) return { error: callError(settled.error) };
      return { result: settled.result ?? null };
    },
    'call.cancel': (args) => {
      const c = t.lookup(args.on, isCall, 'a call');
      c.controller.abort();
      return {};
    },
    'peer.emit': (args) => {
      const p = peerOf(args);
      const event = stringOf(args, 'event', true);
      const meta = metaOf(args);
      withinOf(args);
      // Accepted for sending is the whole of an emit: it never waits.
      try {
        emit(p.dispatcher, pathOf(event), args.data === undefined ? null : args.data, meta ? { meta } : {});
      } catch (error) {
        if (!(error instanceof PublicError)) throw error;
        if (error.code === 'disconnected' || error.code === 'not_connected') throw fail('disconnected', error.message);
        throw fail(error.code, error.message);
      }
      return {};
    },
    'peer.await_event': async (args) => {
      const p = peerOf(args);
      const name = stringOf(args, 'name', true);
      const { item, ended } = await p.events.await(withinOf(args), (e) => e.name === name);
      if (ended && !item) throw fail('disconnected', `the peer ended before ${name} arrived`);
      if (!item) throw fail('timeout', `no ${name}`);
      return { data: item.data ?? null, ...(item.meta ? { meta: item.meta } : {}) };
    },
    'peer.await_request': async (args) => {
      const p = peerOf(args);
      const method = stringOf(args, 'method', true);
      const phase = stringOf(args, 'phase', true);
      if (phase !== 'started' && phase !== 'ended') throw invalid('phase is started or ended');
      const { item } = await p.requests.await(withinOf(args), (l) => l.method === method && l.phase === phase);
      if (!item) throw fail('timeout', `no ${method} ${phase}`);
      return item;
    },
    'peer.close': (args) => {
      peerOf(args).peer.close();
      return {};
    },
    'peer.await_close': async (args) => {
      const p = peerOf(args);
      const code = await p.closed(withinOf(args));
      if (code === undefined) throw fail('timeout', 'the peer did not end');
      // Clean is a close somebody chose, whichever side: a peer closes with
      // 1000 by choice and with a code of its own when it refuses a frame,
      // and 1006 is what a side that aborted leaves behind.
      return { clean: code === 1000, code };
    },
  };
}
