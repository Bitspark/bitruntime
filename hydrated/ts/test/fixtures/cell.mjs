import { invoke, memoAdapter, provide, ProtocolError, sendingWire, string, text } from './domain.mjs';

// Mixed variance: get uses the forward conversion; set uses the backward one.
export function adaptCell(source, adapters) {
  return {
    get: async () => adapters.to(await source.get()),
    set: async value => source.set(adapters.from(value)),
  };
}

/** The only Cell operation protocol, independent of the inner domain. */
export function wireCellAdapter(owner) {
  return memoAdapter(
    source => provide(owner, async (operation, args) => {
      if (operation === 'get' && args.length === 0) return sendingWire(await source.get());
      if (operation === 'set' && args.length === 1) {
        await source.set(sendingWire(args[0]));
        return text('done');
      }
      throw new ProtocolError('invalid Cell request');
    }),
    target => ({
      get: () => invoke(owner, target, 'get', [], sendingWire),
      set: value => invoke(owner, target, 'set', [value], result => {
        if (string(result) !== 'done') throw new ProtocolError('invalid Cell completion');
      }),
    }),
  );
}

/** adapter3<T> = adapter3_ composed with lift3(adapterT), in both directions. */
export function cellAdapter(outer, inner) {
  return memoAdapter(
    source => outer.toWire(adaptCell(source, { to: inner.toWire, from: inner.fromWire })),
    target => adaptCell(outer.fromWire(target), { to: inner.fromWire, from: inner.toWire }),
  );
}
