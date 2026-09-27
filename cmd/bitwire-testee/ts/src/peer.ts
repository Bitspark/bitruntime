/** The peer under control: DuplexPeer, its canned handlers, its observer. */
import { createServer } from 'node:http';
import { WebSocketServer } from 'ws';
import {
  DuplexError,
  DuplexPeer,
  identityHandler,
  checkIdentity,
  IDENTITY_METHOD,
  type EventContext,
  type Meta,
  type Observer,
  type ObserverEvent,
  type PeerOptions,
  type RequestContext,
  type Trace,
} from '@nightseam/runtime';
import {
  Inbox,
  fail,
  invalid,
  unsupported,
  boolOf,
  intOf,
  stringOf,
  withinOf,
  type Args,
  type Op,
  type Testee,
} from './testee.ts';
import { Conn, asLike, isConn } from './seam.ts';

/** One phase of one request a canned handler served, and what it carried. */
interface Lifecycle {
  id: string;
  method: string;
  phase: 'started' | 'ended';
  outcome?: string;
  meta?: Meta;
}

/** An observer that keeps what it is told, for peer.observed. */
class Recorder implements Observer {
  events: ObserverEvent[] = [];
  closed?: { code: number; local: boolean };
  private readonly closedWaiters: Array<() => void> = [];
  observe(event: ObserverEvent): void {
    this.events.push(event);
    if (event.type === 'connection.closed') {
      this.closed = { code: event.code, local: event.local };
      for (const waiter of this.closedWaiters.splice(0)) waiter();
    }
  }
  whenClosed(withinMs: number): Promise<boolean> {
    if (this.closed) return Promise.resolve(true);
    return new Promise((resolve) => {
      const timer = setTimeout(() => resolve(false), withinMs);
      this.closedWaiters.push(() => {
        clearTimeout(timer);
        resolve(true);
      });
    });
  }
  report(withTrace: boolean, drain: boolean): Record<string, unknown>[] {
    const out = this.events.map((event) => normalize(event, withTrace));
    if (drain) this.events = [];
    return out;
  }
}

const splitTrace = (trace: Trace | undefined): Record<string, unknown> | undefined => {
  if (!trace?.traceparent) return undefined;
  const parts = trace.traceparent.split('-');
  if (parts.length !== 4) return { traceparent: trace.traceparent };
  const split: Record<string, unknown> = { trace_id: parts[1], span_id: parts[2], flags: parts[3] };
  if (trace.tracestate) split.state = trace.tracestate;
  return split;
};

/** Renders one event as DRIVER.md says every language reports it. */
const normalize = (event: ObserverEvent, withTrace: boolean): Record<string, unknown> => {
  const out: Record<string, unknown> = {};
  const record = event as unknown as Record<string, unknown>;
  for (const [key, value] of Object.entries(record)) {
    if (key === 'at' || value === undefined || value === '') continue;
    switch (key) {
      case 'trace':
        if (withTrace) {
          const split = splitTrace(value as Trace);
          if (split) out.trace = split;
        }
        break;
      case 'bytes':
        out.bytes = (value as number) > 0;
        break;
      case 'durationMs':
        out.duration = (value as number) >= 0;
        break;
      case 'deadlineMs':
        out.deadline = (value as number) >= 0;
        break;
      case 'errorCode':
        out.error_code = value;
        break;
      default:
        out[key] = value;
    }
  }
  return out;
};

/** A peer under control. */
export class Peer {
  readonly events = new Inbox<{ name: string; data: unknown; meta?: Meta }>();
  readonly requests = new Inbox<Lifecycle>();
  readonly peer: DuplexPeer;
  readonly recorder: Recorder;
  readonly observable: boolean;
  constructor(peer: DuplexPeer, recorder: Recorder, observable: boolean) {
    this.peer = peer;
    this.recorder = recorder;
    this.observable = observable;
    peer.onEvent((name, data, context: EventContext) => {
      this.events.put({ name, data, ...(context.meta ? { meta: context.meta } : {}) });
    });
    peer.onClose(() => {
      this.events.close();
      this.requests.close();
    });
  }
  shutdown(): void {
    this.peer.close();
  }
}

export const isPeer = (object: unknown): object is Peer => object instanceof Peer;

class PeerListener {
  readonly accepted = new Inbox<Peer>();
  readonly close: () => void;
  url = '';
  constructor(close: () => void) {
    this.close = close;
  }
  shutdown(): void {
    this.close();
  }
}

const isPeerListener = (object: unknown): object is PeerListener => object instanceof PeerListener;

/** One call in flight. A live invocation is one of these too: it is a call like any other. */
export class Call {
  readonly peer: Peer;
  readonly promise: Promise<unknown>;
  readonly controller: AbortController;
  constructor(peer: Peer, promise: Promise<unknown>, controller: AbortController) {
    this.peer = peer;
    this.promise = promise;
    this.controller = controller;
    promise.catch(() => {
      /* Awaited by call.await, or never. */
    });
  }
}

export const isCall = (object: unknown): object is Call => object instanceof Call;

/** How a call ended, as the driver's codes. */
const callError = (error: DuplexError, peer: Peer): Record<string, unknown> => {
  switch (error.code) {
    case 'cancelled':
      return { code: 'cancelled', message: error.message };
    case 'request_timeout':
      return { code: 'request_timeout', message: error.message };
    case 'disconnected':
    case 'not_connected':
    case 'connection_failed':
      return { code: 'disconnected', message: error.message };
  }
  const out: Record<string, unknown> = { code: error.code, message: error.message };
  if (error.data !== undefined) out.data = error.data;
  void peer;
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

const behaviorOf = (args: Args): Behavior => {
  const raw = args.behavior;
  if (raw === undefined) return { kind: '' };
  if (typeof raw !== 'object' || raw === null) throw invalid('behavior is an object');
  return raw as Behavior;
};

/**
 * The carriage a step gave a call or an event, and undefined where it gave
 * none: an object of strings, as the profile's member is.
 */
const metaOf = (args: Args): Meta | undefined => {
  const raw = args.meta;
  if (raw === undefined) return undefined;
  if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) throw invalid('meta is an object of strings');
  for (const value of Object.values(raw as Record<string, unknown>)) {
    if (typeof value !== 'string') throw invalid('meta is an object of strings');
  }
  return raw as Meta;
};

/** A handler that does what its behaviour says and records its lifecycle. */
const canned =
  (p: Peer, method: string, b: Behavior) =>
  async (params: unknown, context: RequestContext): Promise<unknown> => {
    p.requests.put({
      id: context.requestId,
      method,
      phase: 'started',
      ...(context.meta ? { meta: context.meta } : {}),
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
          throw new DuplexError(b.code ?? 'internal', b.message ?? '', b.data);
        case 'wait':
          await new Promise<void>((resolve) => {
            if (context.signal.aborted) resolve();
            else context.signal.addEventListener('abort', () => resolve(), { once: true });
          });
          // Completing with an aborted signal lets the peer observe its own
          // cancellation; throwing DuplexError would be a public refusal.
          ended('cancelled');
          return null;
        case 'hold':
          // The one handler that does not stop when it is told to: it holds the
          // request until the remote emits what releases it, cancelled or not,
          // which is how a scenario holds when a withdrawn request is answered.
          await new Promise<void>((resolve) => {
            const released = () => {
              off();
              ended();
              resolve();
            };
            const off = context.peer.onEvent(b.until ?? '', released);
            const ended = context.peer.onClose(released);
          });
          result = b.value ?? null;
          break;
        case 'panic':
          throw new Error(typeof b.value === 'string' ? b.value : JSON.stringify(b.value ?? 'the handler gave up'));
        case 'reverse':
          result = await context.peer.call(b.method ?? '', b.params ?? params, { context });
          break;
        case 'emit':
          await context.peer.emit(b.event ?? '', b.data ?? null, { context });
          result = b.then ?? null;
          break;
        default:
          throw new DuplexError('internal', `no such behaviour: ${b.kind}`);
      }
      ended('ok');
      return result;
    } catch (error) {
      if (error instanceof DuplexError) ended(context.signal.aborted ? 'cancelled' : 'error');
      else ended('panic');
      throw error;
    }
  };

/**
 * What a peer.listen selects from or a peer.dial offers at the handshake;
 * absent, none is offered and none selected, as the runtimes default.
 */
const subprotocolsOf = (args: Args): string[] => {
  const value = args.subprotocols;
  if (value === undefined) return [];
  if (!Array.isArray(value) || value.some((token) => typeof token !== 'string'))
    throw invalid('subprotocols is an array of strings');
  return value as string[];
};

const optionsOf = (args: Args): { options: PeerOptions; recorder: Recorder; observable: boolean } => {
  const raw = args.options;
  const recorder = new Recorder();
  const options: PeerOptions = { observer: recorder };
  let observable = false;
  if (raw === undefined) return { options, recorder, observable };
  if (typeof raw !== 'object' || raw === null) throw invalid('options is an object');
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
      case 'families':
        options.families = value as Record<string, string>;
        break;
      case 'observe':
        observable = value === true;
        break;
      case 'propagate':
        break;
      default:
        throw unsupported(`option ${key}`);
    }
  }
  return { options, recorder, observable };
};

export function peerOps(t: Testee): Record<string, Op> {
  const peerOf = (args: Args, name = 'on') => t.lookup(args[name], isPeer, 'a peer');
  return {
    'peer.listen': (args) =>
      new Promise((resolve, reject) => {
        const { options, recorder, observable } = optionsOf(args);
        const offered = subprotocolsOf(args);
        const server = createServer();
        // The selection is the server's, in its own order of preference, and
        // none where the lists do not meet — ws would otherwise echo the
        // client's first offer back, which is not what a runtime naming none
        // does. The reference testee selects none by default; this one too.
        const handleProtocols = (protocols: Set<string>): string | false =>
          offered.find((token) => protocols.has(token)) ?? false;
        const sockets = new WebSocketServer({ server, maxPayload: options.maxFrameBytes ?? 1 << 20, handleProtocols });
        const listener = new PeerListener(() => {
          sockets.close();
          server.close();
        });
        let first = true;
        sockets.on('connection', (socket) => {
          if (!first) {
            socket.close(1008, 'one connection is accepted');
            return;
          }
          first = false;
          const peer = new DuplexPeer({ ...options, role: 'server' });
          const wrapped = new Peer(peer, recorder, observable);
          // The socket itself, not a connection already wrapped around it: the
          // peer wraps it the same way and reads the selected subprotocol off it.
          peer.attach(asLike(socket)).then(
            () => listener.accepted.put(wrapped),
            () => socket.terminate(),
          );
        });
        server.on('error', reject);
        server.listen(0, '127.0.0.1', () => {
          const address = server.address();
          if (!address || typeof address === 'string') {
            reject(new Error('no address'));
            return;
          }
          listener.url = `ws://127.0.0.1:${address.port}`;
          resolve({ handle: t.mint('pl', listener), url: listener.url });
        });
      }),
    'peer.accept': async (args) => {
      const l = t.lookup(args.on, isPeerListener, 'a peer listener');
      const { item } = await l.accepted.await(withinOf(args), () => true);
      if (!item) throw fail('timeout', 'nobody connected');
      return { handle: t.mint('p', item), subprotocol: item.peer.subprotocol };
    },
    'peer.dial': async (args) => {
      const { options, recorder, observable } = optionsOf(args);
      const offered = subprotocolsOf(args);
      const peer = new DuplexPeer({
        ...options,
        role: 'client',
        ...(offered.length > 0 ? { subprotocols: offered } : {}),
      });
      const wrapped = new Peer(peer, recorder, observable);
      await peer.connect(stringOf(args, 'url', true)).catch((error) => {
        throw fail('failed', String(error));
      });
      return { handle: t.mint('p', wrapped), subprotocol: peer.subprotocol };
    },
    'peer.over': async (args) => {
      const c = t.lookup(args.on, isConn, 'a connection');
      const role = stringOf(args, 'role', true);
      if (role !== 'client' && role !== 'server') throw invalid('role is client or server');
      const { options, recorder, observable } = optionsOf(args);
      const peer = new DuplexPeer({ ...options, role });
      const wrapped = new Peer(peer, recorder, observable);
      await peer.attach(c.release()).catch((error) => {
        throw fail('failed', String(error));
      });
      return { handle: t.mint('p', wrapped) };
    },
    'peer.identity': (args) => {
      const p = peerOf(args);
      const path = stringOf(args, 'path', true);
      const digest = stringOf(args, 'digest');
      try {
        p.peer.handle(IDENTITY_METHOD, identityHandler({ path, ...(digest ? { digest } : {}) }));
      } catch (error) {
        if (error instanceof DuplexError) throw fail(error.code, error.message);
        throw error;
      }
      return {};
    },
    'peer.check_identity': async (args) => {
      const p = peerOf(args);
      const path = stringOf(args, 'path', true);
      const digest = stringOf(args, 'digest');
      try {
        await checkIdentity(
          p.peer.call.bind(p.peer),
          { path, ...(digest ? { digest } : {}) },
          { timeoutMs: withinOf(args) },
        );
      } catch (error) {
        if (error instanceof DuplexError) throw fail(error.code, error.message);
        throw error;
      }
      return {};
    },
    'peer.handle': (args) => {
      const p = peerOf(args);
      const method = stringOf(args, 'method', true);
      try {
        p.peer.handle(method, canned(p, method, behaviorOf(args)));
      } catch (error) {
        throw invalid(String(error));
      }
      return {};
    },
    'peer.on_event': (args) => {
      const p = peerOf(args);
      const name = stringOf(args, 'name', true);
      const b = behaviorOf(args);
      switch (b.kind) {
        case '':
        case 'record':
          return {};
        case 'block':
          p.peer.onEvent(
            name,
            () =>
              new Promise<void>(() => {
                /* never */
              }),
          );
          return {};
        case 'panic':
          p.peer.onEvent(name, () => {
            throw new Error(typeof b.value === 'string' ? b.value : 'the handler gave up');
          });
          return {};
      }
      throw invalid('an event handler records, blocks or panics');
    },
    'peer.call': (args) => {
      const p = peerOf(args);
      const method = stringOf(args, 'method', true);
      const timeout = intOf(args, 'timeout_ms', 0);
      const controller = new AbortController();
      const meta = metaOf(args);
      const promise = p.peer.call(method, args.params ?? null, {
        signal: controller.signal,
        ...(timeout > 0 ? { timeoutMs: timeout } : {}),
        ...(meta ? { meta } : {}),
      });
      return { handle: t.mint('call', new Call(p, promise, controller)) };
    },
    'call.await': async (args) => {
      const c = t.lookup(args.on, isCall, 'a call');
      const settled = await Promise.race([
        c.promise.then(
          (result) => ({ result }),
          (error: DuplexError) => ({ error }),
        ),
        new Promise<undefined>((resolve) => setTimeout(() => resolve(undefined), withinOf(args))),
      ]);
      if (settled === undefined) throw fail('timeout', 'no response');
      if ('error' in settled) return { error: callError(settled.error, c.peer) };
      return { result: settled.result ?? null };
    },
    'call.cancel': (args) => {
      const c = t.lookup(args.on, isCall, 'a call');
      c.controller.abort();
      return {};
    },
    'peer.emit': async (args) => {
      const p = peerOf(args);
      const event = stringOf(args, 'event', true);
      const settled = await Promise.race([
        p.peer.emit(event, args.data ?? null, { ...(metaOf(args) ? { meta: metaOf(args) as Meta } : {}) }).then(
          () => 'ok' as const,
          (error: DuplexError) => error,
        ),
        new Promise<'late'>((resolve) => setTimeout(() => resolve('late'), withinOf(args))),
      ]);
      if (settled === 'late') throw fail('timeout', 'the event was not sent');
      if (settled !== 'ok') throw fail('disconnected', settled.message);
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
      const { item } = await p.requests.await(withinOf(args), (l) => l.method === method && l.phase === phase);
      if (!item) throw fail('timeout', `no ${method} ${phase}`);
      return item;
    },
    'peer.observed': (args) => {
      const p = peerOf(args);
      if (!p.observable) throw invalid('the peer was made without observe');
      return p.recorder.report(boolOf(args, 'trace'), boolOf(args, 'drain', true));
    },
    'peer.close': (args) => {
      peerOf(args).peer.close();
      return {};
    },
    'peer.await_close': async (args) => {
      const p = peerOf(args);
      if (!(await p.recorder.whenClosed(withinOf(args)))) throw fail('timeout', 'the peer did not end');
      // Clean is a close somebody chose, whichever side: a peer closes with
      // 1000 by choice and with a code of its own when it refuses a frame, and
      // 1006 is what a side that aborted leaves behind.
      return { clean: p.recorder.closed!.code === 1000, code: p.recorder.closed!.code };
    },
  };
}

// Keep the seam's Conn in this module's type graph for peer.over.
