import { capturePath, packAddressed, unpackAddressed,
  type Endpoint, type AddressedEndpoint, type Wire, type AddressedWire,
  type WireTree, type Path, type Value } from '@bitspark/bitwire';

// Inert facade: receiving is attached only by receive(), with the raw owner's lifetime.
export function addressed(endpoint: Endpoint): AddressedEndpoint {
  return Object.freeze({
    send: async (path: Path, message: Value) => endpoint.send(packAddressed(path, message)),
    receive(handler: (path: Path, message: Value) => void) {
      if (typeof handler !== 'function') throw new TypeError('addressed handler must be a function');
      return endpoint.receive(value => {
        const {path, message} = unpackAddressed(value);
        handler(path, message);
      });
    },
    closed: endpoint.closed,
    close: () => endpoint.close(),
  });
}
export function bind(wire: AddressedWire, path: Path): Wire {
  const captured = capturePath(path);
  return Object.freeze({send: async (message: Value) => wire.send(captured, message)});
}
export function under(wire: AddressedWire, prefix: Path): AddressedWire {
  const captured = capturePath(prefix);
  return Object.freeze({send: async (path: Path, message: Value) =>
    wire.send(Object.freeze([...captured, ...capturePath(path)]), message)});
}
export class MissingPathError extends Error {
  constructor() { super('wire tree path is absent'); this.name = 'MissingPathError'; }
}
export function asAddressed(tree: WireTree): AddressedWire {
  return Object.freeze({async send(path: Path, message: Value): Promise<void> {
    const selected = tree.at(capturePath(path));
    if (!selected) throw new MissingPathError();
    await selected.own().send(message);
  }});
}
