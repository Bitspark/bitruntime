import type {
  AddressedWire, Child, DeixisNode, Key, Message, Parts, Path, TreePath, WireTree,
} from '@bitspark/bitwire';

import { InvalidPathError, MissingPathError } from './error.ts';

function keyName(key: Key): string {
  if (!(key instanceof Uint8Array)) throw new TypeError('Tree keys must be Uint8Array values');
  let name = '';
  for (const byte of key) name += byte.toString(16).padStart(2, '0');
  return name;
}

function copyChildren<T>(children: Iterable<Child<T>>): ReadonlyArray<Child<T>> {
  const result: Child<T>[] = [];
  const names = new Set<string>();
  for (const [key, child] of children) {
    const name = keyName(key);
    if (names.has(name)) throw new TypeError('Duplicate byte key');
    if (child === null || (typeof child !== 'object' && typeof child !== 'function') ||
        typeof child.own !== 'function' || typeof child.children !== 'function' ||
        typeof child.at !== 'function' || typeof child.decompose !== 'function') {
      throw new TypeError('A child must implement DeixisNode');
    }
    names.add(name);
    // Uint8Array.from also copies a Node Buffer; Buffer.slice would alias it.
    result.push(Object.freeze([Uint8Array.from(key), child] as const));
  }
  return Object.freeze(result);
}

const constructed = new WeakSet<object>();

/** Validate foreign subtrees without recursion or changing their identity. */
function validateChildren<T>(children: ReadonlyArray<Child<T>>): void {
  const active = new WeakSet<object>();
  const visited = new WeakSet<object>();
  const pending: Array<readonly [DeixisNode<T>, boolean]> = children.map(([, child]) => [child, false]);
  while (pending.length > 0) {
    const [node, leaving] = pending.pop()!;
    if (leaving) {
      active.delete(node);
      visited.add(node);
      continue;
    }
    if (active.has(node)) throw new TypeError('Structural cycle');
    if (visited.has(node) || constructed.has(node)) continue;
    active.add(node);
    pending.push([node, true]);
    for (const [, child] of copyChildren(node.children())) pending.push([child, false]);
  }
}

class Node<T> implements DeixisNode<T> {
  readonly #value: T;
  readonly #children: ReadonlyArray<Child<T>>;
  readonly #byKey: ReadonlyMap<string, DeixisNode<T>>;

  constructor(own: T, children: ReadonlyArray<Child<T>>) {
    this.#value = own;
    this.#children = children;
    this.#byKey = new Map(children.map(([key, child]) => [keyName(key), child]));
    constructed.add(this);
    Object.freeze(this);
  }

  own(): T { return this.#value; }

  children(): ReadonlyArray<Child<T>> { return copyChildren(this.#children); }

  at(path: TreePath): DeixisNode<T> | undefined { return select(this, path); }

  decompose(): Parts<T> {
    return Object.freeze({ own: this.#value, children: this.children() });
  }

  static child<T>(node: Node<T>, key: Key): DeixisNode<T> | undefined {
    return node.#byKey.get(keyName(key));
  }
}

/**
 * Construct a complete immutable node, retaining own-value and child identity.
 * Keys and the child collection are copied. A missing own value (undefined or
 * null; every node has one, bitwire decision 0012), duplicate keys and
 * structural cycles are rejected. Foreign nodes must themselves honor
 * DeixisNode's finite, stable, immutable topology contract; validation does not
 * freeze another implementation.
 */
export function compose<T>(own: T, children: Iterable<Child<T>> = []): DeixisNode<T> {
  if (own === undefined || own === null) throw new TypeError('A node requires an own value');
  const retained = copyChildren(children);
  validateChildren(retained);
  return new Node(own, retained);
}

/** Follow exact byte-key edges. Empty path is self; missing edges have no fallback. */
export function select<T>(tree: DeixisNode<T>, path: TreePath): DeixisNode<T> | undefined {
  let current = tree;
  for (const key of path) {
    const wanted = keyName(key);
    const child = current instanceof Node
      ? Node.child(current, key)
      : current.children().find(([candidate]) => keyName(candidate) === wanted)?.[1];
    if (child === undefined) return undefined;
    current = child;
  }
  return current;
}

/** Select a node and call exactly its own primitive, preserving message identity. */
export function send(tree: WireTree, path: TreePath, message: Message): void {
  const selected = select(tree, path);
  if (selected === undefined) throw new MissingPathError();
  selected.own().send(message);
}

const encoder = new TextEncoder();

function treePath(path: Path): TreePath {
  return path.map(segment => {
    if (typeof segment !== 'string') throw new InvalidPathError();
    for (let i = 0; i < segment.length; i++) {
      const unit = segment.charCodeAt(i);
      if (unit >= 0xd800 && unit <= 0xdbff) {
        const next = segment.charCodeAt(++i);
        if (!(next >= 0xdc00 && next <= 0xdfff)) throw new InvalidPathError();
      } else if (unit >= 0xdc00 && unit <= 0xdfff) {
        throw new InvalidPathError();
      }
    }
    return encoder.encode(segment);
  });
}

/**
 * Expose a full tree through the existing Unicode-string addressed access.
 * Segments become exact UTF-8 keys without normalization or slash interpretation.
 * Non-UTF-8 tree keys remain valid, but cannot be named through this bridge.
 * This grants no receive/close capability and cannot infer a tree from a router.
 */
export function asAddressed(tree: WireTree): AddressedWire {
  return Object.freeze({ send(path: Path, message: Message): void {
    send(tree, treePath(path), message);
  } });
}
