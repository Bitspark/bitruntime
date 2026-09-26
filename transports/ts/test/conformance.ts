/**
 * The suite every transport of the seam is held to in TypeScript: what a
 * `FrameConnection` promises and nothing of the transport that keeps it, so
 * that a peer written against the seam can trust the same things wherever it
 * runs — frames arrive in order and whole, in either direction, every
 * listener is given every one of them and detaching one leaves the others,
 * a close carries its code and its reason to both sides and refuses what is
 * sent after it, and `buffered` counts what a send left with the transport
 * and nothing once the transport has taken it.
 *
 * It is the twin of `transports/go/transporttest` and holds the seam as this
 * language has it, which is not in every respect as Go has it. Two of the Go
 * suite's promises are therefore not asked for here. A receive limit is not
 * the seam's: a connection takes none, and an oversized frame is refused by
 * the peer and by the tunnel, each with 1009 and each held to it by its own
 * tests. An abort is not the seam's either — the Go `Conn` has one and a
 * `FrameConnection` has not, a close being how a connection ends here. Nor
 * is the order of a last frame against the close behind it: a transport may
 * deliver the close first, so a peer that needs a frame seen sends it and
 * waits rather than closing on top of it.
 *
 * The Go suite's remaining promise, that a send waits while the receiver does
 * not receive, is asked here of every transport that holds anything at all:
 * `send` cannot wait in this language, so what a transport cannot hand on it
 * holds, and `buffered` counts it until the far end takes it — the pipe past
 * its bound, a channel past the other side's window. A transport that hands
 * every frame to something else inside the send holds nothing and reads zero
 * throughout; it says so with `holds: false` and the case then asks it only
 * what it can keep.
 *
 * A transport's own test calls `run` with a name and a way to make a
 * connected pair: `transports/ts` runs it over the in-memory pipe and over the
 * WebSocket adapter, as the Go packages run the Go suite.
 */
import assert from 'node:assert/strict';
import test from 'node:test';
import type { Frame, FrameConnection } from '../src/index.ts';

/** A connected pair and what ends it, whatever the suite left it in. */
export interface Pair {
  a: FrameConnection;
  b: FrameConnection;
  /** Ends both ends and whatever carries them; the suite calls it once, closed or not. */
  end(): void;
}

/** Makes a fresh connected pair. The suite ends what it opened. */
export type Connect = () => Promise<Pair>;

/** What a transport keeps, where the suite cannot ask it of every one. */
export interface Keeps {
  /**
   * Whether a frame the far end has not taken is held by the transport and
   * counted by `buffered`. True of the pipe, which bounds what it has in
   * flight, and of a tunnel channel, which bounds it by the other side's
   * window; false of an adapter that hands every frame on inside the send, as
   * a socket does, which holds nothing and reads zero throughout.
   */
  holds?: boolean;
}

/** How long a promise of the seam is given before the suite calls it broken. */
const DEADLINE = 5_000;

/** Run holds a transport to the seam, naming it in every test it fails. */
export function run(what: string, connect: Connect, keeps: Keeps = {}): void {
  test(`${what}: frames arrive in order and whole, text and binary alike`, async () => {
    const pair = await connect();
    try {
      const seen = collect(pair.b);
      const sent: Frame[] = [];
      for (let i = 0; i < 64; i++) {
        const text = `frame ${String(i).padStart(2, '0')}`;
        const frame: Frame =
          i % 3 === 0 ? { kind: 'binary', data: new TextEncoder().encode(text) } : { kind: 'text', data: text };
        sent.push(frame);
        pair.a.send(frame);
      }
      await eventually('64 frames arrive', () => seen.frames.length >= 64);
      assert.equal(seen.closed, undefined, 'the connection closed while the frames were crossing');
      assert.equal(seen.frames.length, 64, 'more frames arrived than were sent');
      for (let i = 0; i < 64; i++) same(seen.frames[i]!, sent[i]!, `frame ${i}`);
    } finally {
      pair.end();
    }
  });

  test(`${what}: a frame arrives as what it carried, empty or multi-byte or every byte there is`, async () => {
    const pair = await connect();
    try {
      const seen = collect(pair.b);
      const sent: Frame[] = [
        { kind: 'text', data: '' },
        { kind: 'text', data: 'ä ☃ 𝄞 — one frame, not the pieces of one' },
        { kind: 'binary', data: new Uint8Array(0) },
        { kind: 'binary', data: Uint8Array.from({ length: 256 }, (_, byte) => byte) },
      ];
      for (const frame of sent) pair.a.send(frame);
      await eventually('four frames arrive', () => seen.frames.length >= sent.length);
      assert.equal(seen.frames.length, sent.length, 'more frames arrived than were sent');
      for (let i = 0; i < sent.length; i++) same(seen.frames[i]!, sent[i]!, `frame ${i}`);
    } finally {
      pair.end();
    }
  });

  test(`${what}: both sides send, and each is given the other's frames alone`, async () => {
    const pair = await connect();
    try {
      const atA = collect(pair.a);
      const atB = collect(pair.b);
      for (let i = 0; i < 3; i++) {
        pair.a.send({ kind: 'text', data: `a to b ${i}` });
        pair.b.send({ kind: 'text', data: `b to a ${i}` });
      }
      await eventually('three frames arrive each way', () => atA.frames.length >= 3 && atB.frames.length >= 3);
      assert.deepEqual(
        atB.frames,
        [0, 1, 2].map((i) => ({ kind: 'text', data: `a to b ${i}` })),
        'what b was given',
      );
      assert.deepEqual(
        atA.frames,
        [0, 1, 2].map((i) => ({ kind: 'text', data: `b to a ${i}` })),
        'what a was given',
      );
    } finally {
      pair.end();
    }
  });

  test(`${what}: every listener is given every frame, and the function listen returns detaches that one alone`, async () => {
    const pair = await connect();
    try {
      const first: Frame[] = [];
      const second: Frame[] = [];
      const detach = pair.b.listen({
        frame: (frame) => {
          first.push(frame);
        },
      });
      pair.b.listen({
        frame: (frame) => {
          second.push(frame);
        },
      });
      pair.a.send({ kind: 'text', data: 'to both' });
      await eventually('the frame reaches both listeners', () => first.length >= 1 && second.length >= 1);
      detach();
      pair.a.send({ kind: 'text', data: 'to the one still listening' });
      await eventually('the second frame reaches the listener that stayed', () => second.length >= 2);
      assert.deepEqual(
        second.map((frame) => frame.data),
        ['to both', 'to the one still listening'],
      );
      assert.deepEqual(
        first.map((frame) => frame.data),
        ['to both'],
        'a detached listener was given a frame',
      );
    } finally {
      pair.end();
    }
  });

  test(`${what}: a close carries its code and its reason to both sides, and nothing crosses after it`, async () => {
    const pair = await connect();
    try {
      assert.equal(pair.a.state, 'open', 'a fresh connection was not open');
      assert.equal(pair.b.state, 'open', 'the other end of a fresh connection was not open');
      const closing = collect(pair.a);
      const far = collect(pair.b);
      const closed = { code: 4010, reason: 'an observer sent a deciding frame' };
      pair.a.close(closed.code, closed.reason);
      await eventually('the close reaches the far side', () => far.closed !== undefined);
      assert.deepEqual(far.closed, closed, 'the close arrived as something else');
      await eventually('the close reaches the side that closed', () => closing.closed !== undefined);
      assert.deepEqual(closing.closed, closed, 'the side that closed was told something else');
      assert.equal(pair.a.state, 'closed', 'the side that closed is not closed');
      await eventually('the far side is closed', () => pair.b.state === 'closed');
      assert.throws(() => pair.a.send({ kind: 'text', data: 'late' }), 'the side that closed sent a frame');
      assert.throws(() => pair.b.send({ kind: 'text', data: 'late' }), 'the side that was closed sent a frame');
      // A close is one close: closing again tells nobody a second time, so a
      // protocol above may close what it has already closed without minding.
      pair.a.close(1000, 'again');
      pair.b.close(1000, 'again');
      await settled();
      assert.deepEqual(closing.closed, closed, 'a second close was a second close');
      assert.deepEqual(far.closed, closed, 'a second close reached the far side');
      assert.equal(far.frames.length, 0, 'a frame arrived that nobody sent');
    } finally {
      pair.end();
    }
  });

  test(`${what}: buffered counts what the transport has not taken, and nothing once it has`, async () => {
    const pair = await connect();
    try {
      const seen = collect(pair.b);
      assert.equal(pair.a.buffered, 0, 'a fresh connection had something buffered');
      const payload = 'x'.repeat(1024);
      for (let i = 0; i < 16; i++) pair.a.send({ kind: 'text', data: payload });
      assert.ok(Number.isFinite(pair.a.buffered) && pair.a.buffered >= 0, `buffered is ${pair.a.buffered}`);
      await eventually('16 frames arrive', () => seen.frames.length >= 16);
      await eventually('what arrived is no longer buffered', () => pair.a.buffered === 0);
    } finally {
      pair.end();
    }
  });

  test(`${what}: what the far end has not taken is held and counted, and handed on in order once it takes it`, async () => {
    const pair = await connect();
    try {
      // Nobody listens on b, so nothing of what a sends is taken: a transport
      // that bounds what it has in flight holds the rest and says how much in
      // buffered, rather than taking without bound as a queue of its own.
      const sent: Frame[] = [];
      for (let i = 0; i < 40; i++) {
        const frame: Frame = { kind: 'text', data: `held ${String(i).padStart(2, '0')}` };
        sent.push(frame);
        pair.a.send(frame);
      }
      const held = pair.a.buffered;
      if (keeps.holds ?? true) {
        assert.ok(held > 0, `40 frames nothing took left ${held} buffered: the transport takes without bound`);
      }
      // The far end takes them: what was held is handed on, in order and whole,
      // and buffered reads zero once it has.
      const seen = collect(pair.b);
      await eventually(`the ${held} frames the transport held are handed on`, () => seen.frames.length >= sent.length);
      await eventually('what the far end took is no longer buffered', () => pair.a.buffered === 0);
      assert.equal(seen.frames.length, sent.length, 'more frames arrived than were sent');
      for (let i = 0; i < sent.length; i++) same(seen.frames[i]!, sent[i]!, `frame ${i}`);
    } finally {
      pair.end();
    }
  });
}

/** The frames a connection delivered and the close it was given, in order. */
function collect(connection: FrameConnection) {
  const frames: Frame[] = [];
  let closed: { code: number; reason: string } | undefined;
  connection.listen({
    frame: (frame) => {
      frames.push(frame);
    },
    close: (code, reason) => {
      closed = { code, reason };
    },
  });
  return {
    frames,
    get closed() {
      return closed;
    },
  };
}

/** Waits for what the seam promises, and fails naming it where it does not happen. */
async function eventually(what: string, ready: () => boolean): Promise<void> {
  const deadline = Date.now() + DEADLINE;
  while (!ready()) {
    if (Date.now() > deadline) assert.fail(`${what}: not within ${DEADLINE}ms`);
    await new Promise((resolve) => {
      setTimeout(resolve, 1);
    });
  }
}

/** Gives a transport every turn it could need to deliver what it should not. */
async function settled(): Promise<void> {
  for (let i = 0; i < 5; i++)
    await new Promise((resolve) => {
      setTimeout(resolve, 2);
    });
}

/** Holds a frame to the one that was sent: the kind, and the string or the bytes. */
function same(got: Frame, want: Frame, at: string): void {
  assert.equal(got.kind, want.kind, `${at} arrived as a ${got.kind} frame`);
  if (got.kind === 'text' && want.kind === 'text') assert.equal(got.data, want.data, `${at} arrived as ${got.data}`);
  else if (got.kind === 'binary' && want.kind === 'binary')
    assert.deepEqual(bytes(got.data), bytes(want.data), `${at} arrived as other bytes`);
}

/** The bytes of a binary frame, whichever of the two shapes a transport delivers. */
function bytes(data: ArrayBuffer | Uint8Array): Uint8Array {
  return data instanceof Uint8Array ? data : new Uint8Array(data);
}
