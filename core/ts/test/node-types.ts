import {compose as composeBytes, type DeixisNode as BytesNode} from '@bitspark/deixis-core';
import {atom, type DeixisNode, type Wire, type WireNode} from '@bitspark/bitwire';
import {compose, asAddressed} from '../src/index.js';

// Check each independently declared structural surface, rather than concealing
// their different key representations behind an unchecked cast.
const own: Wire = {send: async () => {}};
const bytesNode: BytesNode<Wire> = composeBytes(own, []);
const atomNode: DeixisNode<Wire> = compose(own, [[atom([]), compose(own)]]);
const wireNode: WireNode = atomNode;
bytesNode.at([new Uint8Array()]);
asAddressed(wireNode);
