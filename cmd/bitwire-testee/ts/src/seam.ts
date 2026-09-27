/** The seam under control: FrameConnections over WebSockets and pipes. */
import { createServer, type Server } from 'node:http';
import { WebSocket, WebSocketServer } from 'ws';
import { pipe, webSocketConnection, type Frame, type FrameConnection, type WebSocketLike } from '@nightseam/duplex';
import { Inbox, fail, invalid, intOf, stringOf, withinOf, within, type Args, type Op, type Testee } from './testee.ts';

/** How a connection ended: a close frame's code and reason, or a failure. */
type Ended = { code: number; reason: string } | { failed: true };

/**
 * A connection under control: what it received while the runner was not
 * asking, and how it ended. A lazily consumed connection has no reader until
 * conn.receive asks; on a tunnel's channel that is what holds credit back,
 * which is the one way a sender is made to wait.
 */
export class Conn {
  readonly frames = new Inbox<{ frame?: Frame; ended?: Ended }>();
  readonly connection: FrameConnection;
  readonly socket?: WebSocket;
  readonly lazy: boolean;
  ended?: Ended;
  private detach?: () => void;
  constructor(connection: FrameConnection, socket?: WebSocket, lazy = false) {
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
  }

  /** Hands the connection to a peer: the wrapper reads no more. */
  release(): FrameConnection {
    this.detach?.();
    this.detach = undefined;
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
  readonly accepted = new Inbox<WebSocket>();
  readonly server: Server;
  url: string;
  constructor(server: Server, url: string) {
    this.server = server;
    this.url = url;
  }
  shutdown(): void {
    this.server.close();
  }
}

const isListener = (object: unknown): object is Listener => object instanceof Listener;

const listen = (limit: number): Promise<Listener> =>
  new Promise((resolve, reject) => {
    const server = createServer();
    const sockets = new WebSocketServer({ server, maxPayload: limit });
    const listener = new Listener(server, '');
    let first = true;
    sockets.on('connection', (socket) => {
      if (!first) {
        socket.close(1008, 'one connection is accepted');
        return;
      }
      first = false;
      listener.accepted.put(socket);
    });
    server.on('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const address = server.address();
      if (!address || typeof address === 'string') {
        reject(new Error('no address'));
        return;
      }
      listener.url = `ws://127.0.0.1:${address.port}`;
      resolve(listener);
    });
  });

const dial = (url: string, limit: number): Promise<WebSocket> =>
  new Promise((resolve, reject) => {
    const socket = new WebSocket(url, { maxPayload: limit });
    socket.once('open', () => resolve(socket));
    socket.once('error', reject);
  });

/**
 * A ws socket as the seam's WebSocketLike: the same surface, its events
 * typed by ws rather than by the DOM, and binary delivered as ArrayBuffer so
 * the seam reads it as it reads a browser's.
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

export function seamOps(t: Testee): Record<string, Op> {
  const conn = (args: Args) => t.lookup(args.on, isConn, 'a connection');
  return {
    'conn.listen': async (args) => {
      const l = await listen(intOf(args, 'limit', 1 << 20));
      return { handle: t.mint('l', l), url: l.url };
    },
    'conn.accept': async (args) => {
      const l = t.lookup(args.on, isListener, 'a listener');
      const { item } = await l.accepted.await(withinOf(args), () => true);
      if (!item) throw fail('timeout', 'nobody connected');
      return { handle: t.mint('c', new Conn(webSocketConnection(asLike(item)), item, lazyOf(args))) };
    },
    'conn.dial': async (args) => {
      const socket = await dial(stringOf(args, 'url', true), intOf(args, 'limit', 1 << 20)).catch((error) => {
        throw fail('failed', String(error));
      });
      return { handle: t.mint('c', new Conn(webSocketConnection(asLike(socket)), socket, lazyOf(args))) };
    },
    'conn.pipe': (args) => {
      const [a, b] = pipe();
      const lazy = lazyOf(args);
      return { a: t.mint('c', new Conn(a, undefined, lazy)), b: t.mint('c', new Conn(b, undefined, lazy)) };
    },
    'conn.send': async (args) => {
      const c = conn(args);
      if (c.ended) throw closeError(c.ended);
      const kind = kindOf(args);
      const frame: Frame =
        kind === 'text'
          ? { kind, data: stringOf(args, 'text') }
          : { kind, data: new Uint8Array(Buffer.from(stringOf(args, 'base64'), 'base64')) };
      try {
        c.connection.send(frame);
      } catch (error) {
        throw c.ended ? closeError(c.ended) : fail('closed', String(error));
      }
      // A send here never blocks; it has settled when nothing of it is
      // buffered — on a channel, when the other side's credit took it.
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
        c.frames.put(item);
        throw closeError(item.ended);
      }
      const frame = item.frame!;
      if (frame.kind === 'binary')
        return { kind: 'binary', base64: Buffer.from(frame.data as ArrayBuffer).toString('base64') };
      return { kind: 'text', text: frame.data };
    },
    'conn.close': async (args) => {
      const c = conn(args);
      const code = intOf(args, 'code', 1000);
      const reason = stringOf(args, 'reason');
      if (c.ended) return {};
      const closed = new Promise<void>((resolve) => {
        if (c.socket) c.socket.once('close', () => resolve());
        else resolve();
      });
      c.connection.close(code, reason);
      await within(withinOf(args), closed, 'the close');
      c.finish({ code, reason });
      return {};
    },
    'conn.abort': (args) => {
      const c = conn(args);
      if (c.socket) c.socket.terminate();
      else if (c.connection.state === 'open') c.connection.close(1006, '');
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
