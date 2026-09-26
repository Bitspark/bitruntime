import type { AddressedWire, DeixisNode, Message, Wire, WireTree } from '@bitspark/bitwire';
import { asAddressed, compose, select, send } from '../src/index.js';

const wire: Wire = { send(_message: Message): void {} };
const tree: WireTree = compose(wire);
const selected: DeixisNode<Wire> | undefined = select(tree, []);
const addressed: AddressedWire = asAddressed(tree);
const message: Message = { frame: { version: 1, kind: 'event', data: null } };
send(tree, [], message);
addressed.send([], message);
selected?.own().send(message);

// @ts-expect-error Addressed access cannot manufacture a full tree.
const opaqueIsNotATree: WireTree = addressed;
// @ts-expect-error A primitive accepts only a message, not a path.
wire.send([], message);
// @ts-expect-error Structural paths are exact bytes, not carrier strings.
select(tree, ['child']);
void opaqueIsNotATree;
