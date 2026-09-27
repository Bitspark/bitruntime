import type { AddressedWire, Endpoint, Message, Path, Receiver, Wire } from '@bitspark/bitwire';
import { MissingPathError, ReceiverExistsError } from './error.ts';
import { ended } from './internal/frame.ts';
import { encodePath } from './internal/path.ts';

/**
 * Binds a relative path prefix to addressed access without allocating a peer,
 * channel or queue: at(w, []) ≃ w and at(at(w, a), b) ≃ at(w, a ++ b). The
 * result grants only send access, even when path is empty. This is addressed
 * prefix binding, not structural selection: whether the root admits what is
 * sent through it is the root's to decide.
 */
export function at(root: AddressedWire, path: Path): AddressedWire {
  const prefix = [...path];
  return {
    send: (suffix, message) => root.send([...prefix, ...suffix], message),
  };
}

/**
 * Addressless access that sends at one fixed addressed path:
 * bind(access, path).send(m) is access.send(path, m), with the message and its
 * return capability unchanged and a refusal thrown as access throws it. The
 * path is copied. The Wire grants only sending: no receive attachment, closure
 * or structure.
 *
 * A carrier path names a position, not a node: addressed access cannot reveal
 * that two far positions share one node. A tree whose own values live across a
 * carrier therefore binds each position it names.
 */
export function bind(access: AddressedWire, path: Path): Wire {
  const fixed = [...path];
  return Object.freeze({ send: (message: Message) => access.send([...fixed], message) });
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
 * A path that selects no child is refused with MissingPathError, an invalid
 * segment with InvalidPathError, a second receiver with ReceiverExistsError,
 * and anything after close as `disconnected`.
 */
export function mount(children: ReadonlyMap<string, Endpoint>): Endpoint {
  const routes = new Map(children);
  let attachment: Attachment | undefined;
  let closed = false;
  const destination = (path: Path): Endpoint => {
    if (closed) throw ended();
    encodePath(path);
    if (!path.length) throw new MissingPathError();
    const child = routes.get(path[0]!);
    if (!child) throw new MissingPathError();
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
    if (closed) throw ended();
    if (attachment) throw new ReceiverExistsError();
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
          throw ended();
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
