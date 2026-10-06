import { WEBSOCKET_PROTOCOL } from '@bitspark/bitwire';
import WebSocket from 'ws';
import type { Endpoint } from '@bitspark/bitwire';
import type { WebSocketConnectOptions } from './options.ts';
import { WebSocketEndpoint, wireOptions, timerLimit } from './endpoint.ts';

export async function connectWebSocket(url: string, options: WebSocketConnectOptions = {}): Promise<Endpoint> {
  const address = new URL(url), limits = wireOptions(options);
  if (!['ws:', 'wss:'].includes(address.protocol) || address.username || address.password || address.hash) {
    throw new TypeError('wire URL must be ws: or wss: without credentials or fragment');
  }
  const handshakeTimeout = timerLimit(options.handshakeTimeoutMs ?? 10000);
  if (options.signal?.aborted) throw new Error('wire connection aborted');
  const socket = new WebSocket(address, WEBSOCKET_PROTOCOL, {
    maxPayload: limits.maxMessageBytes, handshakeTimeout, perMessageDeflate: false, followRedirects: false,
    headers: options.headers, ca: typeof options.ca === 'string' ? options.ca : options.ca && Buffer.from(options.ca),
  });
  return new Promise((resolve, reject) => {
    function cleanup() {
      socket.off('open', opened); socket.off('error', failed); socket.off('close', failed);
      options.signal?.removeEventListener('abort', aborted);
    }
    function refuse(message: string) {
      cleanup(); socket.on('error', () => {}); socket.terminate(); reject(new Error(message));
    }
    function failed() { refuse('wire WebSocket connection failed'); }
    function aborted() { refuse('wire connection aborted'); }
    function opened() {
      if (socket.protocol !== WEBSOCKET_PROTOCOL) { refuse('wire subprotocol negotiation failed'); return; }
      const wire = new WebSocketEndpoint(socket, limits); cleanup(); resolve(wire);
    }
    socket.once('open', opened); socket.once('error', failed); socket.once('close', failed);
    options.signal?.addEventListener('abort', aborted, { once: true });
    if (options.signal?.aborted) aborted();
  });
}
