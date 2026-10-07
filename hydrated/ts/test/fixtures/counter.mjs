import { invoke, memoAdapter, provide, ProtocolError, string, text } from './domain.mjs';

function integer(value) {
  const encoded = string(value), number = Number(encoded);
  if (!Number.isSafeInteger(number) || String(number) !== encoded) throw new ProtocolError('expected integer');
  return number;
}

// Counter = { read(): Promise<number>; add(delta: number): Promise<number> }.
export function counterAdapter(owner) {
  return memoAdapter(
    source => provide(owner, async (operation, args) => {
      if (operation === 'read' && args.length === 0) return text(String(await source.read()));
      if (operation === 'add' && args.length === 1) return text(String(await source.add(integer(args[0]))));
      throw new ProtocolError('invalid Counter request');
    }),
    target => ({
      read: () => invoke(owner, target, 'read', [], integer),
      add: delta => invoke(owner, target, 'add', [text(String(delta))], integer),
    }),
  );
}
