// Ported from nightseam v0.6.0 conformance/ts/src/testee.ts (5cc9723).
/**
 * The TypeScript testee of bitwire's `bitwire/1` conformance contract:
 * bitruntime's seam and protocol engine under the runner's control over
 * driver 1. One request per line on stdin, one answer per line on stdout, a
 * table of handles, an inbox per handle for what arrived unasked, and nothing
 * on stdout but answers. Its transport is a WebSocket on the loopback
 * interface; the in-process pipe serves conn.pipe.
 */
import { createInterface } from 'node:readline';
import { Failure, Testee, fail, invalid, unsupported, type Op } from './driver.ts';
import { peerOps } from './peer.ts';
import { seamOps } from './seam.ts';

const DRIVER = 1;

/** Ops the contract names that this testee answers unsupported, and why. */
const refused = (reason: string): Op => () => {
  throw unsupported(reason);
};

const testee = new Testee();
const ops: Record<string, Op> = {
  hello: () => ({
    driver: DRIVER,
    language: 'typescript',
    layers: ['seam', 'peer'],
    features: ['listen', 'pipe', 'lazy', 'propagator'],
  }),
  reset: () => {
    testee.reset();
    return {};
  },
  bye: () => {
    testee.bye = true;
    testee.reset();
    return {};
  },
  ...seamOps(testee),
  ...peerOps(testee),
  'peer.observed': refused('bitruntime has no observer'),
  'peer.identity': refused('the identity exchange is outside bitwire/1'),
  'peer.check_identity': refused('the identity exchange is outside bitwire/1'),
  'peer.recorded_wire_witness': refused('the recorded-wire witness is outside bitwire/1'),
  'tunnel.over': refused('bitruntime implements no tunnel yet'),
  'tunnel.open': refused('bitruntime implements no tunnel yet'),
  'tunnel.accept': refused('bitruntime implements no tunnel yet'),
};

const serve = async (line: string): Promise<string> => {
  let request: Record<string, unknown>;
  try {
    request = JSON.parse(line) as Record<string, unknown>;
  } catch (error) {
    return JSON.stringify({ id: 0, error: invalid(`not a request: ${String(error)}`) });
  }
  if (typeof request !== 'object' || request === null || Array.isArray(request))
    return JSON.stringify({ id: 0, error: invalid('a request is an object') });
  const id = request.id;
  const op = request.op;
  if (typeof id !== 'number') return JSON.stringify({ id: 0, error: invalid('a request carries an integer id') });
  if (typeof op !== 'string' || op === '') return JSON.stringify({ id, error: invalid('a request names its op') });
  const { id: _id, op: _op, ...args } = request;
  const handler = Object.hasOwn(ops, op) ? ops[op] : undefined;
  if (!handler) return JSON.stringify({ id, error: unsupported(`no such op: ${op}`) });
  try {
    const ok = (await handler(args)) ?? {};
    return JSON.stringify({ id, ok });
  } catch (error) {
    if (error instanceof Failure) return JSON.stringify({ id, error });
    return JSON.stringify({
      id,
      error: fail('internal', error instanceof Error ? `${error.name}: ${error.message}` : String(error)),
    });
  }
};

const lines = createInterface({ input: process.stdin, crlfDelay: Infinity });
for await (const line of lines) {
  if (line.trim() === '') continue;
  const answer = await serve(line);
  await new Promise<void>((resolve) => process.stdout.write(answer + '\n', () => resolve()));
  if (testee.bye) break;
}
testee.reset();
process.exit(0);
