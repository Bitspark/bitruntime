import { invoke, memoAdapter, provide, ProtocolError, string, text } from './domain.mjs';

// Text = { render(prefix: string): Promise<string> }.
export function textAdapter(owner) {
  return memoAdapter(
    source => provide(owner, async (operation, args) => {
      if (operation !== 'render' || args.length !== 1) throw new ProtocolError('invalid Text request');
      return text(await source.render(string(args[0])));
    }),
    target => ({ render: prefix => invoke(owner, target, 'render', [text(prefix)], string) }),
  );
}
