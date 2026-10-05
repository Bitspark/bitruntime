// Node WebSocket carrier configuration. The generic Endpoint remains carrier independent.
import type { IncomingMessage, Server as HttpServer } from 'node:http';
import type { Server as HttpsServer } from 'node:https';
import type { Endpoint, Termination } from '@bitspark/bitwire';

export interface WebSocketEndpointOptions {
  readonly maxMessageBytes?: number;
  readonly maxQueuedBytes?: number;
  readonly maxQueuedMessages?: number;
  readonly closeTimeoutMs?: number;
}
export interface WebSocketConnectOptions extends WebSocketEndpointOptions {
  readonly handshakeTimeoutMs?: number;
  readonly signal?: AbortSignal;
  readonly headers?: Readonly<Record<string, string>>;
  readonly ca?: string | Uint8Array;
}
export interface WebSocketServerOptions extends WebSocketEndpointOptions {
  readonly path?: string;
  readonly allowedOrigins?: readonly string[];
  readonly authorize?: (request: IncomingMessage) => boolean;
}
export interface WebSocketListenOptions extends WebSocketServerOptions {
  readonly host?: string;
  readonly port?: number;
}
export interface WebSocketServerBinding {
  readonly closed: Promise<Termination>;
  close(): Promise<void>;
}
export interface WebSocketListener extends WebSocketServerBinding {
  readonly url: string;
}
export type WebSocketConnectionHandler = (wire: Endpoint, request: IncomingMessage) => void | Promise<void>;
export type WebSocketHttpServer = HttpServer | HttpsServer;
