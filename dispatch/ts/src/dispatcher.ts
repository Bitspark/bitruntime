import { encodePath, WireError } from '@nightseam/duplex';
import type { Endpoint, Message, Path, Receiver, Wire } from '@nightseam/duplex';
import { DuplexError } from './error.ts';
import { InvocationError, captureInvocation, relayInvocationControl } from './invocation.ts';
import { response } from './wire.ts';

/** Registration authority; close releases this registry, never its borrowed carrier. */
export interface HandlerRegistry extends Wire {
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
export class WireDispatcher implements HandlerRegistry {
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
      throw new WireError('closed');
    }
    this.detach = detach;
  }
  send(path: Path, message: Message): void {
    if (this.ended) throw new WireError('closed');
    this.root.send(path, message);
  }
  register(path: Path, receiver: Receiver): () => void {
    return this.install(path, receiver, this.exact);
  }
  registerPrefix(path: Path, receiver: Receiver): () => void {
    return this.install(path, receiver, this.prefixes);
  }
  private install(path: Path, receiver: Receiver, routes: Map<string, Registration>): () => void {
    const name = encodePath(path);
    if (this.ended) throw new WireError('closed');
    if (routes.has(name)) throw new WireError('receiver_exists');
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
        response(message, undefined, new DuplexError('method_not_found', 'Unknown method.'));
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
      response(
        message,
        undefined,
        error instanceof InvocationError && error.code === 'limit'
          ? new DuplexError('busy', 'Invocation participation limit reached.')
          : new DuplexError('invalid_message', 'Invocation requires the lifecycle its return capability carries.'),
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

  select(path: Path): SelectedEndpoint {
    return new SelectedEndpoint(this, [...path]);
  }
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
export function createDispatcher(endpoint: Endpoint, options: DispatcherOptions = {}): WireDispatcher {
  return new WireDispatcher(endpoint, options);
}

interface Attachment {
  receiver: Receiver;
  detach?: () => void;
}
/** A route owned by one dispatcher, with no borrowed-root closure authority. */
export class SelectedEndpoint implements Endpoint {
  private ended = false;
  private attachment: Attachment | undefined;
  private readonly owner: WireDispatcher;
  private readonly prefix: Path;
  constructor(owner: WireDispatcher, prefix: Path) {
    this.owner = owner;
    this.prefix = prefix;
  }
  select(path: Path): SelectedEndpoint {
    return this.owner.select([...this.prefix, ...path]);
  }
  send(path: Path, message: Message): void {
    if (this.ended) throw new WireError('closed');
    this.owner.send([...this.prefix, ...path], message);
  }
  receive(receiver: Receiver): () => void {
    if (this.ended) throw new WireError('closed');
    if (this.attachment) throw new WireError('receiver_exists');
    const attachment: Attachment = { receiver };
    this.attachment = attachment;
    try {
      const detach = this.owner.registerPrefix(this.prefix, {
        message: (path, message) => {
          if (receiver.message) return receiver.message(path.slice(this.prefix.length), message);
          if (message.frame.kind === 'request')
            response(message, undefined, new DuplexError('method_not_found', 'Unknown method.'));
        },
        closed: (code, reason) => this.remove(attachment, { code, reason }),
      });
      if (this.attachment !== attachment) {
        detach();
        throw new WireError('closed');
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
  close(code = 1000, reason = ''): void {
    this.ended = true;
    if (this.attachment) this.remove(this.attachment, { code, reason });
  }
}
