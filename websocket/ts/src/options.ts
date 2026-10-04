// Node WebSocket carrier configuration. The generic Wire remains carrier independent.
import type { IncomingMessage, Server as HttpServer } from 'node:http';
import type { Server as HttpsServer } from 'node:https';
import type { Wire, Termination } from '@bitspark/bitwire';

export interface WebSocketWireOptions {
  readonly maxEnvelopeBytes?: number;
  readonly maxQueuedBytes?: number;
  readonly maxQueuedEnvelopes?: number;
  readonly closeTimeoutMs?: number;
}
export interface WebSocketConnectOptions extends WebSocketWireOptions {
  readonly handshakeTimeoutMs?: number;
  readonly signal?: AbortSignal;
  readonly headers?: Readonly<Record<string, string>>;
  readonly ca?: string | Uint8Array;
}
export interface WebSocketServerOptions extends WebSocketWireOptions {
  readonly path?: string;
  readonly allowedOrigins?: readonly string[];
  readonly authorize?: (request: IncomingMessage) => boolean;
}
export interface WebSocketListenOptions extends WebSocketServerOptions {
  readonly host?: string;
  readonly port?: number;
}
export interface WebSocketWireServer {
  readonly closed: Promise<Termination>;
  close(): Promise<void>;
}
export interface WebSocketWireListener extends WebSocketWireServer {
  readonly url: string;
}
export type WebSocketConnectionHandler = (wire: Wire, request: IncomingMessage) => void | Promise<void>;
export type WebSocketHttpServer = HttpServer | HttpsServer;
