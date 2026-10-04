import { Atom, type Children, type DeixisNode, type Envelope, type Path } from '@bitspark/bitwire';
export function compose<T>(own: T, children: Children<T> = []): DeixisNode<T> {
  const captured: Children<T> = Object.freeze(children.map(([key,node])=> {
    if(!(key instanceof Atom)||!node)throw new TypeError('invalid child');
    return Object.freeze([key,node] as const);
  }));
  for(let i=0;i<captured.length;i++)for(let j=0;j<i;j++)if(captured[i]![0].equals(captured[j]![0]))throw new Error('duplicate byte key');
  const active = new Set<DeixisNode<T>>(), complete = new Set<DeixisNode<T>>();
  const stack = captured.map(([, node]) => ({ node, depth: 1, leaving: false }));
  while (stack.length) {
    const { node, depth, leaving } = stack.pop()!;
    if (leaving) { active.delete(node); complete.add(node); continue; }
    if (depth > 4096) throw new RangeError('tree validation depth limit exceeded');
    if (!node || typeof node.children !== 'function') throw new TypeError('invalid child');
    if (active.has(node)) throw new Error('cyclic tree');
    if (complete.has(node)) continue;
    active.add(node); stack.push({ node, depth, leaving: true });
    const keys = new Set<string>();
    for (const [key, child] of node.children()) {
      if (!(key instanceof Atom)) throw new TypeError('invalid child key');
      const bytes = Array.from(key.bytes(), byte => byte.toString(16).padStart(2, '0')).join('');
      if (keys.has(bytes)) throw new Error('duplicate byte key');
      keys.add(bytes); stack.push({ node: child, depth: depth + 1, leaving: false });
    }
  }
  const node:DeixisNode<T>={own:()=>own,children:()=>captured,
    at:(path)=>{let selected:DeixisNode<T>|undefined=node;for(const key of path){if(!(key instanceof Atom))throw new TypeError('path key must be an atom');selected=selected.children().find(([k])=>k.equals(key))?.[1];if(!selected)return undefined;}return selected;},
    decompose:()=>Object.freeze({own,children:captured})};
  return Object.freeze(node);
}
export function select<T>(node:DeixisNode<T>,path:Path):DeixisNode<T>|undefined{return node.at(path);}
// Exact leaf routing preserves the original whole envelope and never falls back.
export function route(tree:DeixisNode<(e:Envelope)=>void>,envelope:Envelope):boolean {
  const target=tree.at(envelope.destination);if(!target)return false;target.own()(envelope);return true;
}
