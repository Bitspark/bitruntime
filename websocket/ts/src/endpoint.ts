// A single WebSocket endpoint realizing the generic Wire lifecycle.
import WebSocket from 'ws';
import type { RawData } from 'ws';
import type { Envelope, Wire, Termination } from '@bitspark/bitwire';
import type { WebSocketWireOptions } from './options.ts';
import { captureEnvelope, encodeEnvelope, decodeEnvelope, DEFAULT_MAX_ENVELOPE_BYTES } from '@bitspark/bitwire';
import { positiveLimit } from '../../../core/ts/src/limits.ts';

export function timerLimit(value: number): number {
  positiveLimit(value);
  if (value > 2147483647) throw new RangeError('wire timer exceeds the Node timer range');
  return value;
}
export function wireOptions(options: WebSocketWireOptions = {}) {
  const maxEnvelopeBytes = positiveLimit(options.maxEnvelopeBytes ?? DEFAULT_MAX_ENVELOPE_BYTES);
  if (maxEnvelopeBytes > 2147483647) throw new RangeError('wire envelope limit exceeds the WebSocket receiver range');
  return {
    maxEnvelopeBytes,
    maxQueuedBytes: positiveLimit(options.maxQueuedBytes ?? 64 * 1024 * 1024),
    maxQueuedEnvelopes: positiveLimit(options.maxQueuedEnvelopes ?? 1024),
    closeTimeoutMs: timerLimit(options.closeTimeoutMs ?? 5000),
  };
}
export class WebSocketWire implements Wire {
  readonly closed: Promise<Termination>;
  readonly #done: Promise<void>;
  readonly #socket: WebSocket;
  readonly #options: ReturnType<typeof wireOptions>;
  #resolve!: (termination: Termination) => void;
  #termination: Termination | undefined;
  #released = false;
  #timer: ReturnType<typeof setTimeout> | undefined;
  #handler: ((envelope: Envelope) => void) | undefined;
  #queue: { envelope: Envelope; size: number }[] = [];
  #queuedBytes = 0;
  #scheduled = false;
  #writes = 0;
  #writeBytes = 0;
  constructor(socket: WebSocket, options: WebSocketWireOptions = {}) {
    if (socket.readyState !== WebSocket.OPEN) throw new Error('WebSocket is not open');
    this.#socket = socket; this.#options = wireOptions(options);
    this.closed = new Promise((resolve) => { this.#resolve = resolve; });
    this.#done = this.closed.then(() => {});
    socket.binaryType = 'nodebuffer';
    socket.on('message', this.#message);
    socket.on('error', this.#error);
    socket.on('close', this.#close);
  }
  async send(envelope: Envelope): Promise<void> {
    if (this.#termination || this.#socket.readyState !== WebSocket.OPEN) throw new Error('wire is closed');
    const captured = captureEnvelope(envelope), bytes = encodeEnvelope(captured, this.#options.maxEnvelopeBytes);
    if (this.#writes >= this.#options.maxQueuedEnvelopes || this.#writeBytes + bytes.byteLength > this.#options.maxQueuedBytes) {
      throw new Error('wire output queue is full');
    }
    this.#writes++; this.#writeBytes += bytes.byteLength;
    try {
      this.#socket.send(bytes, { binary: true, compress: false }, (error) => {
        this.#writes--; this.#writeBytes -= bytes.byteLength;
        if (error) this.fail('wire socket write failed');
      });
    } catch (error) { this.#writes--; this.#writeBytes -= bytes.byteLength; throw error; }
    // Local admission, deliberately before the write callback or any peer reply.
  }
  receive(handler: (envelope: Envelope) => void): () => void {
    if (this.#termination || this.#socket.readyState !== WebSocket.OPEN) throw new Error('wire is closed');
    if (this.#handler) throw new Error('wire already has a receive handler');
    if (typeof handler !== 'function') throw new TypeError('wire handler must be a function');
    this.#handler = handler; this.#schedule();
    let detached = false;
    return () => { if (!detached) { detached = true; if (this.#handler === handler) this.#handler = undefined; } };
  }
  close(): Promise<void> {
    if (!this.#termination) {
      this.#begin({ kind: 'closed' });
      this.#socket.close(1000);
      this.#timer = setTimeout(() => this.#socket.terminate(), this.#options.closeTimeoutMs);
      this.#timer.unref();
    }
    return this.#done;
  }
  fail(message: string): void {
    if (this.#termination) return;
    this.#begin({ kind: 'failed', message }); this.#socket.terminate();
  }
  #begin(termination: Termination): void {
    this.#termination = Object.freeze(termination); this.#handler = undefined;
    this.#queue.length = 0; this.#queuedBytes = 0;
  }
  #error = (): void => { this.fail('wire socket failed'); };
  #close = (code: number): void => {
    if (this.#released) return;
    if (!this.#termination) this.#begin(code === 1000 || code === 1001 ? { kind: 'closed' } : { kind: 'failed', message: 'wire socket ended abnormally' });
    this.#released = true;
    if (this.#timer) clearTimeout(this.#timer);
    this.#socket.off('message', this.#message); this.#socket.off('error', this.#error); this.#socket.off('close', this.#close);
    this.#resolve(this.#termination!);
  };
  #message = (data: RawData, binary: boolean): void => {
    if (this.#termination) return;
    try {
      if (!binary || !Buffer.isBuffer(data)) throw new Error('binary envelope required');
      const envelope = decodeEnvelope(data, this.#options.maxEnvelopeBytes);
      if (this.#queue.length >= this.#options.maxQueuedEnvelopes || this.#queuedBytes + data.byteLength > this.#options.maxQueuedBytes) {
        throw new Error('wire input queue is full');
      }
      this.#queue.push({ envelope, size: data.byteLength });
      this.#queuedBytes += data.byteLength; this.#schedule();
    } catch { this.fail('invalid wire envelope or input limit exceeded'); }
  };
  #schedule(): void {
    if (this.#scheduled || !this.#handler || this.#termination) return;
    this.#scheduled = true;
    queueMicrotask(() => {
      this.#scheduled = false;
      while (this.#handler && this.#queue.length && !this.#termination) {
        const selected = this.#queue.shift()!; this.#queuedBytes -= selected.size;
        try { this.#handler(selected.envelope); } catch { this.fail('wire receive handler failed'); }
      }
    });
  }
}
