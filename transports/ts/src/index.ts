/**
 * The seam beneath every carrier: a frames duplex connection. It is ordered,
 * message-framed, bidirectional and closed explicitly with a code and a
 * reason, and it is nothing else — no JSON, no requests, no correlation, no
 * events, no reconnection. The bitwire/1 protocol engine runs over it; beneath
 * it the transport is an in-memory pipe or a WebSocket, and neither side of
 * the seam knows which.
 */

/**
 * A frames duplex connection: ordered, message-framed, bidirectional, with an
 * explicit close carrying a code and a reason. Nothing about JSON, requests,
 * correlation or events belongs here; those stay in the protocol engine.
 */
export type Frame = { kind: 'text'; data: string } | { kind: 'binary'; data: ArrayBuffer | Uint8Array };
/** Where a connection is in its life; frames flow only while `open`. */
export type ConnectionState = 'connecting' | 'open' | 'closing' | 'closed';
/** What a connection tells the one listening: it opened, a frame arrived, it closed with a code and a reason, or it failed. */
export interface ConnectionHandlers {
  open?: () => void;
  frame?: (frame: Frame) => void;
  close?: (code: number, reason: string) => void;
  error?: () => void;
}

/**
 * Close codes are the WebSocket registry's numbers (1000 normal, 1008 policy,
 * 1009 too big, 1011 internal, 4000–4999 application) on every transport, so
 * that close semantics travel with the peer. A code that may only be observed
 * (see `sendable`) is refused: `close` throws and sends nothing.
 */
export interface FrameConnection {
  readonly state: ConnectionState;
  /**
   * What a send left with the connection and the transport has not taken yet,
   * in whatever the transport counts: a WebSocket's bytes, a pipe's or a
   * tunnel channel's frames. The peer paces on it and reads only whether it is
   * zero, so a transport that can take no more says so by counting what waits
   * rather than by taking without bound.
   */
  readonly buffered: number;
  /** Throws when the connection is not open. */
  send(frame: Frame): void;
  /** Throws a RangeError, and sends nothing, for a code that is not `sendable`. */
  close(code?: number, reason?: string): void;
  /** Registers handlers; returns a function that detaches all of them. */
  listen(handlers: ConnectionHandlers): () => void;
}

/** The browser WebSocket surface, also implemented by Node's native WebSocket. */
export interface WebSocketLike {
  readonly readyState: number;
  readonly bufferedAmount: number;
  send(data: string): void;
  close(code?: number, reason?: string): void;
  addEventListener(type: string, listener: globalThis.EventListener): void;
  removeEventListener(type: string, listener: globalThis.EventListener): void;
}

const STATES: readonly ConnectionState[] = ['connecting', 'open', 'closing', 'closed'];

/** A close both sides meant. */
export const CODE_NORMAL = 1000;
/** A side shutting down. */
export const CODE_GOING_AWAY = 1001;
/** A frame the receiver could not take as the protocol above the seam defines one. */
export const CODE_PROTOCOL_ERROR = 1002;
/** A frame of a kind the receiver does not speak, such as binary where JSON text was expected. */
export const CODE_UNSUPPORTED_DATA = 1003;
/** The registry's "no status code present": what a side reads when the other closed with no code, never sent. */
export const NO_STATUS = 1005;
/** What a side reads when the other ended with no close at all, never sent. */
export const CODE_ABNORMAL_CLOSURE = 1006;
/** A frame that parses and is refused anyway. */
export const CODE_POLICY_VIOLATION = 1008;
/** A frame over the receiver's limit. */
export const CODE_TOO_LARGE = 1009;
/** A failure of the receiver's own. */
export const CODE_INTERNAL_ERROR = 1011;
/** What a side reads when a TLS handshake failed, never sent. */
export const CODE_TLS_HANDSHAKE = 1015;
/** The first code of the range a protocol above the seam may use for its own reasons. */
export const CODE_APPLICATION_FIRST = 4000;
/** The last code of that range. */
export const CODE_APPLICATION_LAST = 4999;
/** The code bitwire/1 closes with when the other side broke the protocol. */
export const CODE_PROTOCOL = 4011;

/**
 * Whether a close code may be sent. Codes that only describe what a side
 * observed — no status, an abnormal closure, a failed TLS handshake — are
 * never transmitted, and neither is 1004 or a code outside the registry's
 * usable ranges, 1000–1014 and 3000–4999.
 */
export function sendable(code: number): boolean {
  if (!Number.isInteger(code)) return false;
  if (code === NO_STATUS || code === CODE_ABNORMAL_CLOSURE || code === CODE_TLS_HANDSHAKE || code === 1004) return false;
  return (code >= 1000 && code <= 1014) || (code >= 3000 && code <= 4999);
}

/** Refuses a close whose code may only be observed; nothing is sent. */
function refuseUnsendable(code: number): void {
  if (!sendable(code)) throw new RangeError(`bitruntime: close code ${code} may only be observed`);
}

/**
 * Adapts a WebSocket to a frames duplex connection: readyState maps to state,
 * bufferedAmount to buffered, a string message to a text frame, an ArrayBuffer,
 * Uint8Array or Blob to a binary frame, and the close event's code and reason
 * to the close handler. Text frames are delivered synchronously, in the turn
 * the socket delivers them.
 */
export function webSocketConnection(socket: WebSocketLike): FrameConnection {
  const listeners = new Set<ConnectionHandlers>();
  const each = <K extends keyof ConnectionHandlers>(
    name: K,
    ...args: Parameters<NonNullable<ConnectionHandlers[K]>>
  ) => {
    for (const handlers of [...listeners]) {
      (handlers[name] as ((...args: unknown[]) => void) | undefined)?.(...args);
    }
  };
  // A Blob is read asynchronously; frames after it wait behind it so order holds.
  let tail: Promise<void> | undefined;
  const after = (task: () => void | Promise<void>) => {
    const next: Promise<void> = (tail ?? Promise.resolve())
      .then(task)
      .catch(() => {})
      .then(() => {
        if (tail === next) tail = undefined;
      });
    tail = next;
  };
  const deliver = (frame: Frame) => {
    if (tail) after(() => each('frame', frame));
    else each('frame', frame);
  };
  const open = () => each('open');
  const message = (event: globalThis.Event) => {
    const data: unknown = (event as MessageEvent).data;
    if (typeof data === 'string') deliver({ kind: 'text', data });
    else if (data instanceof ArrayBuffer || data instanceof Uint8Array) deliver({ kind: 'binary', data });
    else if (isBlob(data)) {
      after(() =>
        data.arrayBuffer().then(
          (buffer) => each('frame', { kind: 'binary', data: buffer }),
          () => each('error'),
        ),
      );
    } else each('error');
  };
  const close = (event: globalThis.Event) => {
    const { code, reason } = event as Partial<CloseEvent>;
    detach();
    each('close', typeof code === 'number' ? code : NO_STATUS, typeof reason === 'string' ? reason : '');
  };
  const error = () => each('error');
  socket.addEventListener('open', open);
  socket.addEventListener('message', message);
  socket.addEventListener('close', close);
  socket.addEventListener('error', error);
  const detach = () => {
    socket.removeEventListener('open', open);
    socket.removeEventListener('message', message);
    socket.removeEventListener('close', close);
    socket.removeEventListener('error', error);
  };
  // Browsers deliver binary as Blob unless told otherwise; ArrayBuffer keeps delivery synchronous.
  if ('binaryType' in socket) {
    try {
      (socket as { binaryType: string }).binaryType = 'arraybuffer';
    } catch {
      /* Not settable here. */
    }
  }
  return {
    get state() {
      return STATES[socket.readyState] ?? 'closed';
    },
    get buffered() {
      return socket.bufferedAmount;
    },
    send(frame) {
      if (socket.readyState !== 1) throw new Error('Connection is not open.');
      // WebSocketLike declares the text surface the peer needs; real sockets also accept binary.
      (socket.send as (data: string | ArrayBuffer | Uint8Array) => void)(frame.data);
    },
    close(code = CODE_NORMAL, reason = '') {
      refuseUnsendable(code);
      try {
        socket.close(code, reason);
      } catch {
        // The browser WebSocket API accepts only 1000 and 3000–4999 and a
        // reason of at most 123 bytes. A code or reason it refuses must not
        // leave the connection open: close without them, and the far side
        // observes no status (1005).
        socket.close();
      }
    },
    listen(handlers) {
      listeners.add(handlers);
      return () => {
        listeners.delete(handlers);
      };
    },
  };
}

/**
 * How many frames a pipe holds in flight per direction before a send is held:
 * the bound the Go pipe has, as a socket holds some bytes and no more.
 */
const IN_FLIGHT = 8;

/**
 * Two connected ends in memory: what one sends, the other receives, in order,
 * in a later microtask; a close on one end is the close on the other, with its
 * code and reason. Each direction holds at most eight frames in flight, as the
 * Go pipe does: a send past the bound is held until the far end takes a frame,
 * `buffered` counts what is held and reads zero once the far end took it, and
 * a peer over the pipe paces on it exactly as it paces on a WebSocket's
 * `bufferedAmount`. The far end takes a frame when the frame is handed to a
 * listener, so an end nobody listens to holds what was sent rather than losing
 * it, and is given it in order once someone listens. It carries the profile in
 * tests without a socket, as the Go pipe does.
 *
 * `limit`, in bytes (UTF-8 for text), is each end's receive limit; 0, the
 * default, is none. A frame over it is refused before delivery and ends the
 * pipe with 1009 on both ends, as a WebSocket does at its read limit and the
 * Go pipe does at its own.
 */
export function pipe(limit = 0): [FrameConnection, FrameConnection] {
  if (!Number.isSafeInteger(limit) || limit < 0) throw new RangeError('A pipe limit is a nonnegative safe integer.');
  const encoder = new TextEncoder();
  const size = (frame: Frame): number =>
    frame.kind === 'text' ? encoder.encode(frame.data).byteLength : frame.data.byteLength;
  class End implements FrameConnection {
    state: ConnectionState = 'open';
    partner!: End;
    /** What the transport took and the far end has not been handed: at most IN_FLIGHT. */
    private readonly inFlight: Frame[] = [];
    /** What a send left past the bound, waiting for room; what buffered counts. */
    private readonly held: Frame[] = [];
    private draining = false;
    private readonly listeners = new Set<ConnectionHandlers>();
    get buffered(): number {
      return this.held.length;
    }
    send(frame: Frame): void {
      if (this.state !== 'open') throw new Error('Connection is not open.');
      (this.inFlight.length < IN_FLIGHT ? this.inFlight : this.held).push(frame);
      this.drainLater();
    }
    close(code = CODE_NORMAL, reason = ''): void {
      refuseUnsendable(code);
      if (this.state === 'closed') return;
      this.state = 'closed';
      // What neither end has been handed goes nowhere, so nothing is buffered
      // on a connection that has ended.
      this.inFlight.length = 0;
      this.held.length = 0;
      for (const handlers of [...this.listeners]) handlers.close?.(code, reason);
      this.listeners.clear();
      this.partner.close(code, reason);
    }
    listen(handlers: ConnectionHandlers): () => void {
      this.listeners.add(handlers);
      // What the partner sent while nobody listened has a taker now.
      this.partner.drainLater();
      return () => {
        this.listeners.delete(handlers);
      };
    }
    /** Hands on what the far end can take, in a later turn and never inside a send. */
    private drainLater(): void {
      if (this.draining) return;
      this.draining = true;
      queueMicrotask(() => {
        this.draining = false;
        this.drain();
      });
    }
    private drain(): void {
      while (this.inFlight.length > 0) {
        const partner = this.partner;
        // A frame is taken when a listener is handed it; until there is one it
        // waits, and so does everything a send left behind it.
        if (this.state !== 'open' || partner.state !== 'open' || partner.listeners.size === 0) return;
        const frame = this.inFlight.shift()!;
        if (this.held.length > 0) this.inFlight.push(this.held.shift()!);
        if (limit > 0 && size(frame) > limit) {
          // The receiving end refuses it, and the close reaches both ends.
          partner.close(CODE_TOO_LARGE, 'frame exceeds the receive limit');
          return;
        }
        for (const handlers of [...partner.listeners]) handlers.frame?.(frame);
      }
    }
  }
  const left = new End();
  const right = new End();
  left.partner = right;
  right.partner = left;
  return [left, right];
}

function isBlob(value: unknown): value is Blob {
  return typeof Blob !== 'undefined' && value instanceof Blob;
}
