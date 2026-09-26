import type { AddressedWire, Endpoint, Receiver } from '@bitspark/bitwire';
import { publicError } from './internal/frame.ts';
import { respond } from './respond.ts';

/**
 * Joins two existing origins without allocating a peer or channel: what either
 * endpoint delivers is sent through the other unchanged, with its return
 * capability and context, in the order the source delivers it. The returned
 * detach removes only the forwarding attachments; both endpoints stay owned by
 * their callers, and each root remains responsible for ending its failed
 * carrier.
 *
 * A message the destination refuses fails only that message: a refused
 * request is answered through its return capability, without lending it the
 * refusal's local publication proof, and forwarding goes on. Forwarding ends
 * when either endpoint ends or detach is called.
 */
export function forward(inbound: Endpoint, outbound: Endpoint): () => void {
  const removals: (() => void)[] = [];
  let detached = false;
  const stop = () => {
    if (detached) return;
    detached = true;
    for (const remove of removals) remove();
  };
  const receiver = (destination: AddressedWire): Receiver => ({
    closed: stop,
    message: (path, message) => {
      // Detach stops new dispatch, not controls for an already captured call.
      try {
        destination.send(path, message);
      } catch (error) {
        if (message.frame.kind === 'request') respond(message, undefined, publicError(error));
      }
    },
  });
  try {
    for (const [source, destination] of [
      [inbound, outbound],
      [outbound, inbound],
    ] as const) {
      const remove = source.receive(receiver(destination));
      if (detached) remove();
      else removals.push(remove);
    }
  } catch (error) {
    stop();
    throw error;
  }
  return stop;
}
