import type { Path, Receiver, TreePath, Wire, WireTree } from '@bitspark/bitwire';
import { InvalidPathError } from '../../../core/ts/src/error.ts';
import { respond } from '../../../core/ts/src/respond.ts';
import { compose } from '../../../core/ts/src/tree.ts';
import type { Dispatcher, Route } from './dispatcher.ts';

/** A WireTree served on a dispatcher beneath a prefix. */
export interface Served {
  /**
   * Serves tree in place of the tree served now, in one step: each delivery is
   * routed by the old tree or by the new one, never by neither, and a request
   * admitted before the update keeps the node that admitted it, its
   * cancellation included. A refused update leaves the served tree unchanged.
   */
  update(tree: WireTree): void;
  /**
   * The positions, relative to the tree, of children whose key is not UTF-8
   * and which are therefore not served, with their subtrees.
   */
  unreachable(): ReadonlyArray<TreePath>;
  /** Stops serving. Requests already admitted still reach their nodes. */
  close(): void;
}

// Fatal: an ill-formed key is not a name. ignoreBOM: a leading U+FEFF is part
// of the key, not a byte order mark to strip.
const utf8 = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true });

function segment(key: Uint8Array): string | undefined {
  try {
    return utf8.decode(key);
  } catch {
    return undefined;
  }
}

function serving(own: Wire): Receiver {
  return {
    message: (_path, message) => {
      try {
        own.send(message);
      } catch (error) {
        if (message.frame.kind === 'request') respond(message, undefined, error);
      }
    },
  };
}

function positions(prefix: Path, tree: WireTree): { routes: Route[]; unreachable: TreePath[] } {
  if (tree === null || typeof tree !== 'object') throw new TypeError('Serve requires a tree');
  // Composing validates a foreign tree's complete child graph: duplicate keys,
  // non-node children and cycles, as well as the root's own Wire.
  const root = compose(tree.own(), tree.children());
  const routes: Route[] = [];
  const unreachable: TreePath[] = [];
  const stack: Array<{ node: WireTree; path: string[]; keys: Uint8Array[] }> = [{ node: root, path: [], keys: [] }];
  while (stack.length > 0) {
    const { node, path, keys } = stack.pop()!;
    const own = node.own();
    if (own === null || typeof own !== 'object' || typeof own.send !== 'function')
      throw new TypeError('A served node requires an own Wire');
    routes.push({ path: [...prefix, ...path], receiver: serving(own) });
    const reachable: Array<{ node: WireTree; path: string[]; keys: Uint8Array[] }> = [];
    for (const [key, child] of node.children()) {
      const name = segment(key);
      if (name === undefined) unreachable.push([...keys, Uint8Array.from(key)]);
      else reachable.push({ node: child, path: [...path, name], keys: [...keys, Uint8Array.from(key)] });
    }
    stack.push(...reachable.reverse());
  }
  return { routes, unreachable };
}

/**
 * Routes each position of tree through one exact route beneath prefix, bound
 * to that position's own Wire, so what a peer sends at prefix ++ path reaches
 * select(tree, path).own(). A node shared by two positions is served at both:
 * a carrier path names a position, not a node.
 *
 * A child whose key is not UTF-8 cannot be named by a bitwire/1 path; it and
 * its whole subtree are left unserved and listed by `unreachable`. A request
 * its node's own refuses is answered with that refusal, as `internal` unless it
 * is a PublicError. A refused event is dropped, as an event nothing handles is;
 * it never ends the carrier. Cancellation is not routed here: the dispatcher
 * hands it to the route that admitted its request.
 *
 * The prefix is nonempty, since bitwire/1 names no request or event at the
 * empty path. Serve validates the tree's complete child graph and refuses a
 * missing own Wire. Neither serving nor closing closes the dispatcher or its
 * endpoint.
 */
export function serve(dispatcher: Dispatcher, prefix: Path, tree: WireTree): Served {
  if (prefix.length === 0) throw new InvalidPathError();
  const fixed = [...prefix];
  const routes = dispatcher.routeSet();
  let unreachable: TreePath[] = [];
  const update = (next: WireTree): void => {
    const served = positions(fixed, next);
    routes.set(served.routes);
    unreachable = served.unreachable;
  };
  try {
    update(tree);
  } catch (error) {
    routes.close();
    throw error;
  }
  return Object.freeze({
    update,
    unreachable: () => unreachable.map((path) => path.map((key) => Uint8Array.from(key))),
    close: () => routes.close(),
  });
}
