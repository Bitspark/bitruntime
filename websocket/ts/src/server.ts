import { WEBSOCKET_PROTOCOL } from '@bitspark/bitwire';
import { createServer } from 'node:http';
import type { IncomingMessage } from 'node:http';
import type { Duplex } from 'node:stream';
import { WebSocketServer } from 'ws';
import type { WebSocketConnectionHandler, WebSocketHttpServer, WebSocketListenOptions,
  WebSocketServerOptions, WebSocketWireListener, WebSocketWireServer } from './options.ts';
import type { Termination } from '@bitspark/bitwire';
import { WebSocketWire, wireOptions } from './endpoint.ts';

const heldServers = new WeakSet<WebSocketHttpServer>();
function pathname(path: string): string {
  if (!path.startsWith('/') || /[?#\s]/u.test(path) || new URL(path, 'http://wire.invalid').pathname !== path) {
    throw new TypeError('wire path must be an absolute URL pathname');
  }
  return path;
}
function refuse(socket: Duplex, status: 400 | 403 | 404 | 503): void {
  socket.on('error', () => {});
  socket.end(`HTTP/1.1 ${status} Rejected\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`, () => socket.destroy());
}
export function bindWebSocketServer(server: WebSocketHttpServer, onConnection: WebSocketConnectionHandler,
  options: WebSocketServerOptions = {}): WebSocketWireServer {
  const limits = wireOptions(options), path = pathname(options.path ?? '/wire');
  const origins = new Set(options.allowedOrigins ?? []);
  for (const origin of origins) {
    const parsed = new URL(origin);
    if (!['http:', 'https:'].includes(parsed.protocol) || parsed.origin !== origin) throw new TypeError('allowed origins must be exact HTTP origins');
  }
  if (typeof onConnection !== 'function') throw new TypeError('wire connection handler must be a function');
  if (options.authorize !== undefined && typeof options.authorize !== 'function') throw new TypeError('wire authorization predicate must be a function');
  if (heldServers.has(server)) throw new Error('HTTP server already has a wire binding');
  heldServers.add(server);
  const sockets = new WebSocketServer({ noServer: true, maxPayload: limits.maxEnvelopeBytes, perMessageDeflate: false,
    handleProtocols: (protocols) => protocols.has(WEBSOCKET_PROTOCOL) ? WEBSOCKET_PROTOCOL : false });
  const wires = new Set<WebSocketWire>();
  let resolve!: (end: Termination) => void;
  const closed = new Promise<Termination>((finish) => { resolve = finish; });
  let completion: Promise<void> | undefined;
  function stop(end: Termination): Promise<void> {
    if (completion) return completion;
    server.off('upgrade', upgrade); server.off('close', serverClosed); server.off('error', serverFailed);
    completion = (async () => {
      const transportClosed = new Promise<void>((finish) => sockets.close(() => finish()));
      await Promise.all([transportClosed, ...[...wires].map((wire) => wire.close())]);
      sockets.off('error', serverFailed); heldServers.delete(server);
      resolve(Object.freeze(end));
    })();
    return completion;
  }
  function serverClosed() { void stop({ kind: 'closed' }); }
  function serverFailed() { void stop({ kind: 'failed', message: 'wire listener failed' }); }
  function upgrade(request: IncomingMessage, socket: Duplex, head: Buffer): void {
    if (completion) { refuse(socket, 503); return; }
    try {
      if (new URL(request.url ?? '/', 'http://wire.invalid').pathname !== path) { refuse(socket, 404); return; }
      const offered = request.headers['sec-websocket-protocol'];
      if (request.headers['sec-websocket-version'] !== '13' || typeof offered !== 'string' ||
        !offered.split(',').map((token) => token.trim()).includes(WEBSOCKET_PROTOCOL)) { refuse(socket, 400); return; }
      const origin = request.headers.origin;
      if (origin && !origins.has(origin)) {
        const parsed = new URL(origin);
        if (!['http:', 'https:'].includes(parsed.protocol) || parsed.origin !== origin || parsed.host !== request.headers.host) { refuse(socket, 403); return; }
      }
      if (options.authorize && options.authorize(request) !== true) { refuse(socket, 403); return; }
      sockets.handleUpgrade(request, socket, head, (websocket) => {
        const wire = new WebSocketWire(websocket, limits); wires.add(wire);
        void wire.closed.then(() => wires.delete(wire));
        try { void Promise.resolve(onConnection(wire, request)).catch(() => wire.fail('wire connection setup failed')); }
        catch { wire.fail('wire connection setup failed'); }
      });
    } catch { refuse(socket, 403); }
  }
  server.on('upgrade', upgrade); server.on('close', serverClosed); server.on('error', serverFailed);
  sockets.on('error', serverFailed);
  return { closed, close: () => stop({ kind: 'closed' }) };
}

function closeHttp(server: WebSocketHttpServer): Promise<void> {
  if (!server.listening) return Promise.resolve();
  return new Promise((resolve) => { server.close(() => resolve()); server.closeAllConnections(); });
}
export async function listenWebSocket(options: WebSocketListenOptions, onConnection: WebSocketConnectionHandler): Promise<WebSocketWireListener> {
  const port = options.port ?? 0, host = options.host ?? '127.0.0.1';
  if (!Number.isInteger(port) || port < 0 || port > 65535) throw new RangeError('wire port must be between 0 and 65535');
  if (typeof host !== 'string' || !host.length) throw new TypeError('wire host must be nonempty');
  wireOptions(options); pathname(options.path ?? '/wire');
  const server = createServer((_request, response) => { response.writeHead(426); response.end(); });
  const binding = bindWebSocketServer(server, onConnection, options);
  try {
    await new Promise<void>((resolve, reject) => {
      const error = () => { server.off('listening', listening); reject(new Error('wire listener could not bind')); };
      const listening = () => { server.off('error', error); resolve(); };
      server.once('error', error); server.once('listening', listening); server.listen(port, host);
    });
  } catch (error) { await binding.close(); await closeHttp(server); throw error; }
  const address = server.address();
  if (!address || typeof address === 'string') { await binding.close(); await closeHttp(server); throw new Error('wire listener has no network address'); }
  const url = `ws://${address.address.includes(':') ? '[' + address.address + ']' : address.address}:${address.port}${options.path ?? '/wire'}`;
  const closed = binding.closed.then(async (end) => { await closeHttp(server); return end; });
  const completion = closed.then(() => {});
  return { url, closed, close: () => { void binding.close(); return completion; } };
}
