/** Internal frame validation and carriage shared by the peer and its conformance tests. */
import { DuplexError } from './error.ts';
import type { Meta } from './peer.ts';
import { scalarJSON } from './unicode.ts';

/** A decoded JSON envelope; the connection beneath carries it as a text frame. */
export type Envelope = Record<string, unknown>;

/**
 * One frame of the profile, validated by kind. Every kind may carry W3C trace
 * context, and a request and an event a `meta` of strings; the members are
 * kept on the envelope for a caller that propagates them, and the peer itself
 * reads none of them. Not part of the package surface.
 */
export function decodeEnvelope(data: string, localPrefix: string, remotePrefix: string): Envelope {
  scalarJSON(data);
  const value: unknown = JSON.parse(data);
  if (!isObject(value) || value.version !== 1) throw new Error();
  // JSON.parse keeps the last of two members of one name; a frame that spells
  // a member twice is ambiguous and refused, as the Go peer refuses it.
  if (topLevelMembers(data) !== Object.keys(value).length) throw new Error();
  const frame: Envelope = value;
  switch (frame.kind) {
    case 'request':
      keys(frame, ['version', 'kind', 'id', 'method', 'params', 'meta', ...TRACE]);
      requestID(frame.id, remotePrefix);
      requireName(frame.method, 'method');
      if (!Object.hasOwn(frame, 'params')) throw new Error();
      break;
    case 'response':
      keys(frame, ['version', 'kind', 'id', 'result', 'error', ...TRACE]);
      requestID(frame.id, localPrefix);
      if (Object.hasOwn(frame, 'result') === Object.hasOwn(frame, 'error')) throw new Error();
      if (Object.hasOwn(frame, 'error')) {
        if (!isObject(frame.error)) throw new Error();
        keys(frame.error, ['code', 'message', 'data']);
        requireName(frame.error.code, 'code');
        // code and message are both non-empty, as the Go peer refuses them.
        requireName(frame.error.message, 'message');
      }
      break;
    case 'cancel':
      keys(frame, ['version', 'kind', 'id', ...TRACE]);
      requestID(frame.id, remotePrefix);
      break;
    case 'event':
      keys(frame, ['version', 'kind', 'event', 'data', 'meta', ...TRACE]);
      requireName(frame.event, 'event');
      if (!Object.hasOwn(frame, 'data')) throw new Error();
      break;
    default:
      throw new Error();
  }
  trace(frame);
  carriage(frame);
  return frame;
}

export function isObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
/** How many members the text spells at the top level, duplicates counted. */
function topLevelMembers(text: string): number {
  let depth = 0;
  let inString = false;
  let members = 0;
  let expectKey = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (inString) {
      if (c === '\\') i++;
      else if (c === '"') inString = false;
      continue;
    }
    switch (c) {
      case '"':
        inString = true;
        if (depth === 1 && expectKey) {
          members++;
          expectKey = false;
        }
        break;
      case '{':
      case '[':
        depth++;
        if (depth === 1) expectKey = true;
        break;
      case '}':
      case ']':
        depth--;
        break;
      case ',':
        if (depth === 1) expectKey = true;
        break;
    }
  }
  return members;
}
function keys(frame: Envelope, allowed: string[]): void {
  if (Object.keys(frame).some((key) => !allowed.includes(key))) throw new Error('Unknown frame property.');
}
/**
 * The meta keys the profile and its components keep for themselves — a
 * deadline, a cause — so that a consumer's key and one defined later never
 * collide. This version defines none, so every key under it is refused.
 */
const META_RESERVED = 'nightseam.';
/** W3C Trace Context, verbatim: an optional member of every kind, never of an error. */
const TRACE = ['traceparent', 'tracestate'];
const TRACEPARENT = /^[0-9a-f]{2}-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$/;
function trace(frame: Envelope): void {
  if (
    Object.hasOwn(frame, 'traceparent') &&
    (typeof frame.traceparent !== 'string' || !TRACEPARENT.test(frame.traceparent))
  ) {
    throw new Error('Invalid traceparent.');
  }
  if (Object.hasOwn(frame, 'tracestate') && typeof frame.tracestate !== 'string')
    throw new Error('Invalid tracestate.');
}
/**
 * What a frame sent from here carries. The map is copied, so a later write to
 * the caller's does not reach a frame already sent; keys under META_RESERVED
 * are the profile's and are dropped rather than sent, since the peer at the
 * far end refuses a frame carrying one, and a carriage left with nothing in it
 * is not sent at all.
 */
export function carrying(envelope: Envelope, meta: Meta | undefined): Envelope {
  if (!meta) return envelope;
  const carried: Meta = {};
  for (const [key, value] of Object.entries(meta)) {
    if (!key.startsWith(META_RESERVED)) carried[key] = value;
  }
  if (Object.keys(carried).length > 0) envelope.meta = carried;
  return envelope;
}
/**
 * A call's metadata, verbatim: `meta` maps names to strings and may be empty,
 * and nothing here reads a value of it. Keys under META_RESERVED are the
 * profile's to define and it defines none in this version, so a frame carrying
 * one is refused rather than read as a consumer's.
 */
function carriage(frame: Envelope): void {
  if (!Object.hasOwn(frame, 'meta')) return;
  const meta = frame.meta;
  if (!isObject(meta)) throw new Error('Invalid meta.');
  for (const [key, value] of Object.entries(meta)) {
    if (typeof value !== 'string' || key.startsWith(META_RESERVED)) throw new Error('Invalid meta.');
  }
}
export function requireName(value: unknown, field: string): asserts value is string {
  if (typeof value !== 'string' || value.length === 0)
    throw new DuplexError('invalid_message', `${field} must be a nonempty string.`);
}
function requestID(value: unknown, prefix: string): void {
  if (typeof value !== 'string' || !value.startsWith(prefix) || !/^[1-9][0-9]{0,19}$/.test(value.slice(prefix.length)))
    throw new Error('Invalid request ID.');
}
