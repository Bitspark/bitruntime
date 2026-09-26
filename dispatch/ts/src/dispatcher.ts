import type { AddressedWire, Endpoint, Message, Path, Receiver } from '@bitspark/bitwire';
import { PublicError, ReceiverExistsError } from '../../../core/ts/src/error.ts';
import { InvocationError, captureInvocation, relayInvocationControl } from '../../../core/ts/src/invocation.ts';
import { respond } from '../../../core/ts/src/respond.ts';
import { ended as disconnected } from '../../../core/ts/src/internal/frame.ts';
import { encodePath } from '../../../core/ts/src/internal/path.ts';

/**
 * The registration capability the handler helpers need: send access plus
 * explicit route registration. Closing it releases its registrations, never
 * a borrowed carrier.
 */
export interface Registry extends AddressedWire {
  register(path: Path, receiver: Receiver): () => void;
  close(code?: number, reason?: string): void;
}
interface Registration {
  path: Path;
  receiver: Receiver;
}

/** One attachment's options. */
export interface DispatcherOptions {
  /** Explicit closure authority over an endpoint owned by the caller. */
  ownEndpoint?: boolean;
}
/**
 * One attachment, with explicit exact-before-longest-prefix dispatch. Each
 * request's traversal is captured on the invocation its return capability
 * carries, through the public vocabulary alone; a request whose return
 * capability carries none is refused rather than routed with weaker detach and
 * cancellation guarantees. An opaque wrapper is therefore as good as a native
 * endpoint: the lifecycle travels with the unchanged return capability, and
 * nothing here recognizes a concrete type.
 */
export class Dispatcher implements Registry {
  private readonly exact = new Map<string, Registration>();
  private readonly prefixes = new Map<string, Registration>();
  private ended = false;
  private detach: (() => void) | undefined;
  private readonly root: Endpoint;
  private readonly ownEndpoint: boolean;

  constructor(root: Endpoint, options: DispatcherOptions = {}) {
    this.root = root;
    this.ownEndpoint = options.ownEndpoint ?? false;
    const detach = root.receive({
      message: (path, message) => this.deliver(path, message),
      closed: (code, reason) => this.close(code, reason),
    });
    if (this.ended) {
      detach();
      throw disconnected();
    }
    this.detach = detach;
  }
  send(path: Path, message: Message): void {
    if (this.ended) throw disconnected();
    this.root.send(path, message);
  }
  /** Routes exactly path to receiver. A path has one registration. */
  register(path: Path, receiver: Receiver): () => void {
    return this.install(path, receiver, this.exact);
  }
  /** Routes path and every path beneath it, the longest registered prefix winning, unless an exact registration matches. */
  registerPrefix(path: Path, receiver: Receiver): () => void {
    return this.install(path, receiver, this.prefixes);
  }
  private install(path: Path, receiver: Receiver, routes: Map<string, Registration>): () => void {
    const name = encodePath(path);
    if (this.ended) throw disconnected();
    if (routes.has(name)) throw new ReceiverExistsError();
    const registration = { path: [...path], receiver };
    routes.set(name, registration);
    return () => {
      if (routes.get(name) === registration) routes.delete(name);
    };
  }
  private match(path: Path, name: string): Registration | undefined {
    const exact = this.exact.get(name);
    if (exact) return exact;
    let selected: Registration | undefined;
    for (const candidate of this.prefixes.values()) {
      if (
        candidate.path.length <= path.length &&
        (!selected || candidate.path.length > selected.path.length) &&
        candidate.path.every((part, index) => part === path[index])
      )
        selected = candidate;
    }
    return selected;
  }
  private deliver(path: Path, message: Message): void | Promise<void> {
    const name = encodePath(path);
    // A control belongs to the traversal that captured it, never to the
    // registration in force now. Handing it to the invocation is what keeps a
    // detach or a rebind from retargeting an admitted request.
    if (message.frame.kind === 'cancel') {
      try {
        relayInvocationControl(message);
      } catch {
        /* A carrier with no lifecycle has nothing to route it to. */
      }
      return;
    }
    const registration = this.ended ? undefined : this.match(path, name);
    if (!registration?.receiver.message) {
      if (message.frame.kind === 'request')
        respond(message, undefined, new PublicError('method_not_found', 'Unknown method.'));
      return;
    }
    const delivered = [...path];
    if (message.frame.kind !== 'request') return registration.receiver.message(delivered, message);
    let capture;
    try {
      capture = captureInvocation(message, (control) => {
        void registration.receiver.message?.([...delivered], control);
      });
    } catch (error) {
      // A bound reached is a refusal to try again at; a capability that
      // carries no lifecycle is a request this dispatcher cannot route with
      // the guarantees it advertises.
      respond(
        message,
        undefined,
        error instanceof InvocationError && error.code === 'limit'
          ? new PublicError('busy', 'Invocation participation limit reached.')
          : new PublicError('invalid_message', 'Invocation requires the lifecycle its return capability carries.'),
      );
      return;
    }
    let pending: void | Promise<void>;
    try {
      pending = registration.receiver.message(delivered, message);
    } catch (error) {
      capture.ready();
      throw error;
    }
    if (!pending) {
      capture.ready();
      return;
    }
    return pending.finally(() => capture.ready());
  }

  /** A receiving view of this shared dispatcher at a prefix, with no closure authority over the root. */
  select(path: Path): SelectedEndpoint {
    return new SelectedEndpoint(this, [...path]);
  }
  /** Releases the routes and the root attachment, and closes the root only when this dispatcher owns it. */
  close(code = 1000, reason = ''): void {
    if (this.ended) return;
    this.ended = true;
    const detach = this.detach;
    this.detach = undefined;
    const ending = [...this.exact.values(), ...this.prefixes.values()];
    this.exact.clear();
    this.prefixes.clear();
    detach?.();
    for (const registration of ending) {
      try {
        registration.receiver.closed?.(code, reason);
      } catch {
        /* Each owner receives its end. */
      }
    }
    if (this.ownEndpoint) this.root.close(code, reason);
  }
}
/** Attaches a dispatcher to an endpoint, borrowed unless the options transfer its closure. */
export function createDispatcher(endpoint: Endpoint, options: DispatcherOptions = {}): Dispatcher {
  return new Dispatcher(endpoint, options);
}

interface Attachment {
  receiver: Receiver;
  detach?: () => void;
}
/** A route owned by one dispatcher, with no borrowed-root closure authority. */
export class SelectedEndpoint implements Endpoint {
  private ended = false;
  private attachment: Attachment | undefined;
  private readonly owner: Dispatcher;
  private readonly prefix: Path;
  constructor(owner: Dispatcher, prefix: Path) {
    this.owner = owner;
    this.prefix = prefix;
  }
  select(path: Path): SelectedEndpoint {
    return this.owner.select([...this.prefix, ...path]);
  }
  send(path: Path, message: Message): void {
    if (this.ended) throw disconnected();
    this.owner.send([...this.prefix, ...path], message);
  }
  receive(receiver: Receiver): () => void {
    if (this.ended) throw disconnected();
    if (this.attachment) throw new ReceiverExistsError();
    const attachment: Attachment = { receiver };
    this.attachment = attachment;
    try {
      const detach = this.owner.registerPrefix(this.prefix, {
        message: (path, message) => {
          if (receiver.message) return receiver.message(path.slice(this.prefix.length), message);
          if (message.frame.kind === 'request')
            respond(message, undefined, new PublicError('method_not_found', 'Unknown method.'));
        },
        closed: (code, reason) => this.remove(attachment, { code, reason }),
      });
      if (this.attachment !== attachment) {
        detach();
        throw disconnected();
      }
      attachment.detach = detach;
    } catch (error) {
      if (this.attachment === attachment) this.attachment = undefined;
      throw error;
    }
    return () => this.remove(attachment);
  }
  private remove(attachment: Attachment, ending?: { code: number; reason: string }): void {
    if (this.attachment !== attachment) return;
    this.attachment = undefined;
    attachment.detach?.();
    if (ending) attachment.receiver.closed?.(ending.code, ending.reason);
  }
  /** Ends the view's route; it never closes the shared root. */
  close(code = 1000, reason = ''): void {
    this.ended = true;
    if (this.attachment) this.remove(this.attachment, { code, reason });
  }
}
