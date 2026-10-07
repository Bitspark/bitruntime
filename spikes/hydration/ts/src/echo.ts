// Domain adapter: no hydration, reference, registry, carrier, or routing imports.
import { atom, Atom } from '@bitspark/bitwire';
import { live, wire, items, LiveWire, type LiveValue } from './live.js';

const ECHO = atom(new TextEncoder().encode('echo'));
const expectWire = (v: LiveValue | undefined) => {
  if (!(v instanceof LiveWire)) throw new TypeError('Expected reply wire');
  return v;
};

export interface EchoResult { value: Atom; continuation: LiveWire; returned: LiveWire }
export interface Echo { echo(value: Atom, callback: LiveWire): Promise<EchoResult> }

/** Tiny domain fixture whose response carries a further Wire. */
export function provideEcho(): LiveWire {
  return wire(async message => {
    const [operation, argument, nested] = items(message);
    if (!(operation instanceof Atom) || !operation.equals(ECHO) || !(argument instanceof Atom) || !nested) throw new Error('bad echo');
    const [replyValue, callbackValue] = items(nested);
    const reply = expectWire(replyValue), callback = expectWire(callbackValue);
    const continuation = wire(async next => {
      const [value, returnedReply] = items(next);
      if (!value) throw new Error('bad continuation');
      await expectWire(returnedReply).send(value);
    });
    await reply.send(live(argument, live(continuation, callback)));
  });
}

export function echoFromWire(target: LiveWire): Echo {
  return {
    echo(value, callback) {
      // This timeout is a fixture/domain policy, never a generic Wire deadline.
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error('echo outcome unknown')), 3000);
        const reply = wire(message => {
          try {
            const [answer, nested] = items(message);
            if (!(answer instanceof Atom) || !nested) throw new Error('bad response');
            const [continuation, returned] = items(nested);
            resolve({ value: answer, continuation: expectWire(continuation), returned: expectWire(returned) });
          } catch (error) { reject(error); }
          finally { clearTimeout(timer); }
        });
        void target.send(live(ECHO, value, live(reply, callback))).catch(error => {
          clearTimeout(timer); reject(error);
        });
      });
    }
  };
}
