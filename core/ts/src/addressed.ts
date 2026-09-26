import type { Endpoint, Path, Receiver, Wire } from '@bitspark/bitwire';

// The public Nightseam names present the shared contract's actual declarations.
export type {
  Path,
  ProfileKind,
  ProfileFrame,
  ProfileError,
  ReturnAddress,
  Message,
  Receiver,
  Wire,
  Endpoint,
} from '@bitspark/bitwire';

export class WireError extends Error {
  readonly code: 'closed' | 'no_route' | 'receiver_exists' | 'invalid_path';
  constructor(code: WireError['code']) {
    super(`Wire ${code}.`);
    this.name = 'WireError';
    this.code = code;
  }
}

function scalar(value: string): void {
  for (let i = 0; i < value.length; i++) {
    const unit = value.charCodeAt(i);
    if (unit >= 0xd800 && unit <= 0xdbff) {
      const low = value.charCodeAt(++i);
      if (!(low >= 0xdc00 && low <= 0xdfff)) throw new WireError('invalid_path');
    } else if (unit >= 0xdc00 && unit <= 0xdfff) throw new WireError('invalid_path');
  }
}

/** UTF-8 byte-length-prefixed segments; [] is '', whereas [''] is '0:'. */
export function encodePath(path: Path): string {
  const encoder = new TextEncoder();
  return path
    .map((segment) => {
      scalar(segment);
      return `${encoder.encode(segment).length}:${segment}`;
    })
    .join('');
}
/** Accepts only the canonical encoding, retaining dots, empty strings and BOMs. */
export function decodePath(encoded: string): string[] {
  scalar(encoded);
  const bytes = new TextEncoder().encode(encoded);
  const decoder = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true });
  const path: string[] = [];
  for (let offset = 0; offset < bytes.length;) {
    const start = offset;
    let length = 0;
    while (offset < bytes.length && bytes[offset] !== 58) {
      const digit = bytes[offset++]! - 48;
      if (digit < 0 || digit > 9) throw new WireError('invalid_path');
      length = length * 10 + digit;
      if (!Number.isSafeInteger(length)) throw new WireError('invalid_path');
    }
    if (offset === start || offset === bytes.length || (offset - start > 1 && bytes[start] === 48))
      throw new WireError('invalid_path');
    offset++;
    if (length > bytes.length - offset) throw new WireError('invalid_path');
    try {
      path.push(decoder.decode(bytes.subarray(offset, offset + length)));
    } catch {
      throw new WireError('invalid_path');
    }
    offset += length;
  }
  return path;
}

/** Selects send access without granting receive attachment or closure authority. */
export function at(root: Wire, path: Path): Wire {
  const prefix = [...path];
  return {
    send: (suffix, message) => root.send([...prefix, ...suffix], message),
  };
}

interface ChildAttachment {
  active: boolean;
  detach?: () => void;
}
interface Attachment {
  receiver: Receiver;
  active: boolean;
  children: ChildAttachment[];
}
/**
 * Consumes one path segment; [] has no leaf, and [''] can select an empty key.
 * One owning receiver spans the borrowed children and sees their keys restored.
 * Copies the map. Closing detaches this mount's attachment, never children.
 */
export function mount(children: ReadonlyMap<string, Endpoint>): Endpoint {
  const routes = new Map(children);
  let attachment: Attachment | undefined;
  let closed = false;
  const destination = (path: Path): Endpoint => {
    if (closed) throw new WireError('closed');
    encodePath(path);
    const child = path.length ? routes.get(path[0]!) : undefined;
    if (!child) throw new WireError('no_route');
    return child;
  };
  const release = (child: ChildAttachment): void => {
    child.active = false;
    const detach = child.detach;
    child.detach = undefined;
    detach?.();
  };
  const remove = (held: Attachment, ending?: { code: number; reason: string }): void => {
    if (!held.active) return;
    held.active = false;
    if (attachment === held) attachment = undefined;
    for (const child of held.children) release(child);
    if (ending) held.receiver.closed?.(ending.code, ending.reason);
  };
  const receive = (receiver: Receiver): (() => void) => {
    if (closed) throw new WireError('closed');
    if (attachment) throw new WireError('receiver_exists');
    const held: Attachment = { receiver, active: true, children: [] };
    attachment = held;
    let remaining = routes.size;
    try {
      for (const [key, child] of routes) {
        const slot: ChildAttachment = { active: true };
        held.children.push(slot);
        const detach = child.receive({
          // The child's runtime owns admission and captured cancellation. Keep
          // its captured receiver even after this attachment is detached.
          message: (suffix, message) => receiver.message?.([key, ...suffix], message),
          closed: (code, reason) => {
            if (!held.active || !slot.active) return;
            remaining--;
            release(slot);
            if (remaining === 0) remove(held, { code, reason });
          },
        });
        // A child may synchronously end or close this mount while receiving.
        // Its returned disposer still belongs to this acquisition attempt.
        if (!held.active || !slot.active) {
          detach();
          throw new WireError('closed');
        }
        slot.detach = detach;
      }
    } catch (error) {
      remove(held);
      throw error;
    }
    return () => remove(held);
  };
  return {
    send: (path, message) => destination(path).send(path.slice(1), message),
    receive,
    close: (code = 1000, reason = '') => {
      if (closed) return;
      closed = true;
      if (attachment) remove(attachment, { code, reason });
    },
  };
}
