// Ported from nightseam v0.6.0 conformance/ts/src/seam.ts (5cc9723).
/**
 * The seam under control: bitruntime's frames duplex connections over a
 * WebSocket, the testee's one transport, and over the in-process pipe.
 */
import { createServer, type Server } from 'node:http';
import { WebSocket, WebSocketServer } from 'ws';
import {
  pipe,
  sendable,
  webSocketConnection,
  type Frame,
  type FrameConnection,
  type WebSocketLike,
} from '@bitspark/bitruntime/transports';
import {
  Inbox,
  fail,
  intOf,
  invalid,
  stringOf,
  unsupported,
  within,
  withinOf,
  type Args,
  type Op,
  type Testee,
} from './driver.ts';

/** How a connection ended: a close frame's code and reason, or a failure. */
type Ended = { code: number; reason: string } | { failed: true };

/** The receive limit a connection has when the step names none: 1 MiB. */
const DEFAULT_LIMIT = 1 << 20;

/**
 * A connection under control: what it received while the runner was not
 * asking, and how it ended. A lazily consumed connection has no reader until
 * conn.receive asks. The pipe holds what nobody reads; a WebSocket is paused,
 * so that it reads nothing off the socket either until then.
 */
export class Conn {
  readonly frames = new Inbox<{ frame?: Frame; ended?: Ended }>();
  readonly connection: FrameConnection;
  readonly socket?: WebSocket;
  readonly lazy: boolean;
  ended?: Ended;
  private detach?: () => void;
  constructor(connection: FrameConnection, socket: WebSocket | undefined, lazy: boolean) {
    this.connection = connection;
    this.socket = socket;
    this.lazy = lazy;
    if (!lazy) this.attachReader();
  }

  attachReader(): void {
    if (this.detach) return;
    this.detach = this.connection.listen({
      frame: (frame) => this.frames.put({ frame }),
      close: (code, reason) => this.finish({ code, reason }),
      error: () => this.finish({ failed: true }),
    });
    if (this.socket?.isPaused) this.socket.resume();
  }

  /**
   * Hands the connection to a peer: the wrapper reads no more. A paused
   * socket resumes once the peer, which attaches in the same turn, listens.
   */
  release(): FrameConnection {
    this.detach?.();
    this.detach = undefined;
    const socket = this.socket;
    if (socket?.isPaused) queueMicrotask(() => socket.resume());
    return this.connection;
  }

  finish(ended: Ended): void {
    if (this.ended) return;
    this.ended = ended;
    this.frames.put({ ended });
    this.frames.close();
  }

  shutdown(): void {
    this.detach?.();
    if (this.socket) this.socket.terminate();
    else if (this.connection.state === 'open') this.connection.close(1001, 'reset');
    this.finish({ code: 1006, reason: '' });
  }
}

export const isConn = (object: unknown): object is Conn => object instanceof Conn;

const closeError = (ended: Ended) =>
  'failed' in ended
    ? fail('failed', 'the connection failed')
    : fail('closed', `the connection closed with ${ended.code}`, { close_code: ended.code, reason: ended.reason });

/** A listener accepting one WebSocket at a URL. */
class Listener {
  readonly accepted: Inbox<WebSocket>;
  readonly server: Server;
  readonly sockets: WebSocketServer;
  readonly url: string;
  constructor(accepted: Inbox<WebSocket>, served: { server: Server; sockets: WebSocketServer; url: string }) {
    this.accepted = accepted;
    this.server = served.server;
    this.sockets = served.sockets;
    this.url = served.url;
  }
  shutdown(): void {
    this.accepted.close();
    this.sockets.close();
    this.server.close();
    // A socket nobody accepted is no handle's; the reset ends it here.
    for (const client of this.sockets.clients) client.terminate();
  }
}

const isListener = (object: unknown): object is Listener => object instanceof Listener;

/** Serves a WebSocket server on a loopback port; each connection is handed to accept. */
export const serve = (
  sockets: (server: Server) => WebSocketServer,
  accept: (socket: WebSocket) => void,
): Promise<{ server: Server; sockets: WebSocketServer; url: string }> =>
  new Promise((resolve, reject) => {
    const server = createServer();
    const wss = sockets(server);
    wss.on('error', () => {
      /* A failed handshake is the dialer's to report. */
    });
    wss.on('connection', (socket) => {
      socket.on('error', () => {
        /* Its close reports it. */
      });
      accept(socket);
    });
    server.on('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const address = server.address();
      if (!address || typeof address === 'string') {
        reject(new Error('no address'));
        return;
      }
      resolve({ server, sockets: wss, url: `ws://127.0.0.1:${address.port}` });
    });
  });

const listen = async (limit: number): Promise<Listener> => {
  const accepted = new Inbox<WebSocket>();
  let first = true;
  const served = await serve(
    (server) => new WebSocketServer({ server, maxPayload: limit }),
    (socket) => {
      if (!first) {
        socket.close(1008, 'one connection is accepted');
        return;
      }
      first = false;
      // Nothing is read until conn.accept says how the connection is consumed.
      socket.pause();
      accepted.put(socket);
    },
  );
  return new Listener(accepted, served);
};

const dial = (url: string, limit: number): Promise<WebSocket> =>
  new Promise((resolve, reject) => {
    const socket = new WebSocket(url, { maxPayload: limit });
    socket.on('error', () => {
      /* Before the open it is the dial's refusal, after it the close's. */
    });
    socket.once('open', () => {
      socket.pause();
      resolve(socket);
    });
    socket.once('error', reject);
  });

/**
 * A ws socket as the seam's WebSocketLike: the same surface, its events typed
 * by ws rather than by the DOM, and binary delivered as ArrayBuffer so the
 * seam reads it as it reads a browser's.
 */
export const asLike = (socket: WebSocket): WebSocketLike => {
  socket.binaryType = 'arraybuffer';
  return socket as unknown as WebSocketLike;
};

const kindOf = (args: Args): 'text' | 'binary' => {
  const kind = stringOf(args, 'kind', true);
  if (kind !== 'text' && kind !== 'binary') throw invalid('kind is text or binary');
  return kind;
};

const lazyOf = (args: Args): boolean => {
  const mode = stringOf(args, 'consume');
  if (mode === '' || mode === 'eager') return false;
  if (mode === 'lazy') return true;
  throw invalid('consume is eager or lazy');
};

const bytesOf = (data: ArrayBuffer | Uint8Array): Buffer =>
  data instanceof Uint8Array ? Buffer.from(data.buffer, data.byteOffset, data.byteLength) : Buffer.from(data);

export function seamOps(t: Testee): Record<string, Op> {
  const conn = (args: Args) => t.lookup(args.on, isConn, 'a connection');
  return {
    'conn.listen': async (args) => {
      const l = await listen(intOf(args, 'limit', DEFAULT_LIMIT));
      return { handle: t.mint('l', l), url: l.url };
    },
    'conn.accept': async (args) => {
      const l = t.lookup(args.on, isListener, 'a listener');
      const lazy = lazyOf(args);
      const { item } = await l.accepted.await(withinOf(args), () => true);
      if (!item) throw fail('timeout', 'nobody connected');
      return { handle: t.mint('c', new Conn(webSocketConnection(asLike(item)), item, lazy)) };
    },
    'conn.dial': async (args) => {
      const lazy = lazyOf(args);
      const socket = await dial(stringOf(args, 'url', true), intOf(args, 'limit', DEFAULT_LIMIT)).catch((error) => {
        throw fail('failed', String(error));
      });
      return { handle: t.mint('c', new Conn(webSocketConnection(asLike(socket)), socket, lazy)) };
    },
    'conn.pipe': (args) => {
      const lazy = lazyOf(args);
      const [a, b] = pipe(intOf(args, 'limit', DEFAULT_LIMIT));
      return { a: t.mint('c', new Conn(a, undefined, lazy)), b: t.mint('c', new Conn(b, undefined, lazy)) };
    },
    'conn.send': async (args) => {
      const c = conn(args);
      const kind = kindOf(args);
      const frame: Frame =
        kind === 'text'
          ? { kind, data: stringOf(args, 'text') }
          : { kind, data: new Uint8Array(Buffer.from(stringOf(args, 'base64'), 'base64')) };
      if (c.ended) throw closeError(c.ended);
      try {
        c.connection.send(frame);
      } catch (error) {
        throw c.ended ? closeError(c.ended) : fail('closed', String(error));
      }
      // A send never blocks; it has settled when nothing of it is buffered,
      // which on a pipe is when the far end took a frame and made room.
      const deadline = Date.now() + withinOf(args);
      while (c.connection.buffered > 0 && !c.ended) {
        if (Date.now() >= deadline) throw fail('timeout', 'the send did not settle: the frame waits on the other side');
        await new Promise((resolve) => setTimeout(resolve, 5));
      }
      return {};
    },
    'conn.receive': async (args) => {
      const c = conn(args);
      c.attachReader();
      const { item } = await c.frames.await(withinOf(args), () => true);
      if (!item) throw fail('timeout', 'nothing received');
      if (item.ended) {
        // A close stays: every later receive sees it.
        c.frames.put(item);
        throw closeError(item.ended);
      }
      const frame = item.frame!;
      if (frame.kind === 'binary') return { kind: 'binary', base64: bytesOf(frame.data).toString('base64') };
      return { kind: 'text', text: frame.data };
    },
    'conn.close': async (args) => {
      const c = conn(args);
      const code = intOf(args, 'code', 1000);
      const reason = stringOf(args, 'reason');
      if (!sendable(code)) throw invalid(`close code ${code} may only be observed`);
      if (c.ended) throw closeError(c.ended);
      // The close handshake is read off the socket, so a paused one resumes.
      c.attachReader();
      const socket = c.socket;
      const closed = new Promise<void>((resolve) => {
        if (socket && socket.readyState !== WebSocket.CLOSED) socket.once('close', () => resolve());
        else resolve();
      });
      c.connection.close(code, reason);
      await within(withinOf(args), closed, 'the close');
      c.finish({ code, reason });
      return {};
    },
    'conn.abort': (args) => {
      const c = conn(args);
      // bitruntime's FrameConnection has no abort; only a socket can end
      // without a close.
      if (!c.socket) throw unsupported('a pipe cannot be aborted, only closed');
      c.socket.terminate();
      c.finish({ code: 1006, reason: '' });
      return {};
    },
    'conn.await_close': async (args) => {
      const c = conn(args);
      c.attachReader();
      const { item } = await c.frames.await(withinOf(args), (i) => i.ended !== undefined);
      if (!item?.ended) throw fail('timeout', 'the connection did not end');
      c.frames.put(item);
      return 'failed' in item.ended ? { code: 1006, reason: '' } : { code: item.ended.code, reason: item.ended.reason };
    },
  };
}
