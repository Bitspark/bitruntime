import assert from 'node:assert/strict';
import test from 'node:test';
import { Atom } from '@bitspark/bitwire';
import { wire, LiveWire } from '../dist/live.js';
import { slotOfToWire, slotOfFromWire } from '../dist/slot.js';
import { tree, bytes } from './harness.mjs';

// Leaf API: Text = { read(): Promise<Atom> }. These adapters know only Text.
function textToWire(source) {
  return wire(async reply => {
    assert.ok(reply instanceof LiveWire);
    await reply.send(await source.read());
  });
}
function textFromWire(target) {
  return {
    read: () => new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('Text outcome unknown')), 3000);
      const reply = wire(value => {
        clearTimeout(timer);
        if (value instanceof Atom) resolve(value); else reject(new Error('invalid Text value'));
      });
      void target.send(reply).catch(error => { clearTimeout(timer); reject(error); });
    }),
  };
}
const textAdapters = { to: textToWire, from: textFromWire };

for (const carrier of ['pair', 'websocket']) {
  test(`${carrier}: Slot<Text> composes Slot<Wire> and Text adapters in both directions`, async t => {
    const network = await tree(t, carrier);
    let selected = { read: async () => bytes('provider Text') };
    const source = {
      read: async () => selected,
      write: async value => { selected = value; },
    };
    const service = slotOfToWire(source, textAdapters);
    const ref = network.right.expose(service); // composition bootstrap only
    const remote = slotOfFromWire(network.left.connect(ref), textAdapters);

    // Covariant occurrence: read returns a Text adapter over the nested Wire.
    assert.ok((await (await remote.read()).read()).equals(await (await source.read()).read()));

    // Contravariant occurrence: write imports a client-provided Text at the far end.
    const clientText = { read: async () => bytes('caller Text') };
    await remote.write(clientText);
    assert.ok((await (await source.read()).read()).equals(await clientText.read()));
    assert.ok((await (await remote.read()).read()).equals(await clientText.read()));
    assert.deepEqual(network.faults, []);
  });
}
