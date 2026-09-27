// The seam's two transports in TypeScript: the in-memory pipe and the
// WebSocket adapter, each held to the suite every transport is held to, and
// then the adapter held to what it alone does — the shapes a socket delivers
// a message in, the close event's code and reason, and the socket surface it
// reads state and buffered off. The suite is the twin of transporttest, which
// transports/go runs over the pipe and transports/websocket/go over a real socket.
// Ported from nightseam v0.6.0 duplex/ts/src/duplex.test.ts.
import assert from 'node:assert/strict';
import test from 'node:test';
import { run } from './conformance.ts';
import { CODE_PROTOCOL, NO_STATUS, pipe, sendable, webSocketConnection } from '../src/index.ts';
import type { Frame, WebSocketLike } from '../src/index.ts';

run('the in-memory pipe', async () => {
  const [a, b] = pipe();
  return {
    a,
    b,
    end: () => {
      a.close();
    },
  };
});

// A socket takes every frame inside the send and holds none of it back, so
// the suite asks it nothing of what a transport holds; bufferedAmount is the
// socket's own and the adapter is held to reading it below.
run(
  'a WebSocket',
  async () => {
    const [left, right] = Socket.pair();
    return {
      a: webSocketConnection(left),
      b: webSocketConnection(right),
      end: () => {
        left.close();
      },
    };
  },
  { holds: false },
);

test('the pipe delivers in a later turn, never inside the send', async () => {
  const [a, b] = pipe();
  const frames: Frame[] = [];
  b.listen({
    frame: (frame) => {
      frames.push(frame);
    },
  });
  a.send({ kind: 'text', data: 'now' });
  assert.deepEqual(frames, [], 'a pipe delivered a frame inside the send');
  await settled();
  assert.deepEqual(frames, [{ kind: 'text', data: 'now' }]);
  a.close();
});

test('the pipe takes eight frames in flight and holds what a send leaves past them until the far end takes one', async () => {
  const [a, b] = pipe();
  // Nobody listens on b, so nothing is taken: the bound is what the transport
  // itself takes, and it is the Go pipe's eight.
  for (let i = 0; i < 8; i++) a.send({ kind: 'text', data: `frame ${i}` });
  assert.equal(a.buffered, 0, 'eight frames in flight left something buffered');
  a.send({ kind: 'text', data: 'frame 8' });
  assert.equal(a.buffered, 1, 'a ninth frame was taken as though the transport had room for it');
  a.send({ kind: 'text', data: 'frame 9' });
  assert.equal(a.buffered, 2, 'what a send leaves past the bound is not counted');
  await settled();
  assert.equal(a.buffered, 2, 'what nobody takes stopped being buffered on its own');
  const frames: Frame[] = [];
  b.listen({
    frame: (frame) => {
      frames.push(frame);
    },
  });
  await settled();
  assert.equal(a.buffered, 0, 'what the far end took is still counted as buffered');
  assert.deepEqual(
    frames.map((frame) => frame.data),
    Array.from({ length: 10 }, (_, i) => `frame ${i}`),
    'what was held was handed on out of order, or lost',
  );
  a.close();
});

test('a send on a closed pipe throws, and nothing it held is counted', async () => {
  const [a, b] = pipe();
  a.send({ kind: 'text', data: 'never taken' });
  a.close(4010, 'a policy of the family');
  assert.equal(a.buffered, 0, 'a closed connection buffers what it can never hand on');
  assert.equal(b.state, 'closed', 'the far end of a closed pipe is open');
  assert.throws(() => a.send({ kind: 'text', data: 'late' }), /not open/);
});

test('a socket that opens says so, and a string, an ArrayBuffer and a Uint8Array are the two kinds of frame', async () => {
  const socket = new Socket();
  const connection = webSocketConnection(socket);
  const frames: Frame[] = [];
  let opened = 0;
  connection.listen({
    open: () => {
      opened++;
    },
    frame: (frame) => {
      frames.push(frame);
    },
  });
  socket.dispatchEvent(new Event('open'));
  socket.receive('a string');
  socket.receive(new Uint8Array([1, 2, 3]).buffer);
  socket.receive(new Uint8Array([4, 5]));
  await settled();
  assert.equal(opened, 1, 'the open event reached the handler once');
  assert.deepEqual(
    frames.map((frame) => frame.kind),
    ['text', 'binary', 'binary'],
  );
  assert.equal(frames[0]!.data, 'a string');
  assert.deepEqual(new Uint8Array(frames[1]!.data as ArrayBuffer), new Uint8Array([1, 2, 3]));
  assert.deepEqual(frames[2]!.data, new Uint8Array([4, 5]));
});

test('a Blob is read as the bytes it holds, and what a socket delivered after it waits behind it', async () => {
  const socket = new Socket();
  const connection = webSocketConnection(socket);
  const frames: Frame[] = [];
  connection.listen({
    frame: (frame) => {
      frames.push(frame);
    },
  });
  socket.receive(new Blob([new Uint8Array([7, 8, 9])]));
  socket.receive('behind the blob');
  await settled();
  assert.deepEqual(
    frames.map((frame) => frame.kind),
    ['binary', 'text'],
    'a frame overtook the blob before it',
  );
  assert.deepEqual(new Uint8Array(frames[0]!.data as ArrayBuffer), new Uint8Array([7, 8, 9]));
  assert.equal(frames[1]!.data, 'behind the blob');
});

test('binaryType is asked for arraybuffer, and a socket that refuses it is adapted all the same', () => {
  const socket = new Socket();
  webSocketConnection(socket);
  assert.equal(socket.binaryType, 'arraybuffer', 'binary would arrive as a Blob, a turn behind everything else');
  const refusing = new Socket();
  Object.defineProperty(refusing, 'binaryType', {
    get: () => 'blob',
    set: () => {
      throw new Error('Not settable here.');
    },
  });
  const connection = webSocketConnection(refusing);
  assert.equal(connection.state, 'open', 'a socket that refuses binaryType was not adapted');
});

test("a message of no shape the seam knows is an error and no frame, as is the socket's own error", async () => {
  const socket = new Socket();
  const connection = webSocketConnection(socket);
  const frames: Frame[] = [];
  let errors = 0;
  connection.listen({
    frame: (frame) => {
      frames.push(frame);
    },
    error: () => {
      errors++;
    },
  });
  socket.receive(42);
  socket.dispatchEvent(new Event('error'));
  await settled();
  assert.equal(errors, 2, 'a message of no shape and an error event are two errors');
  assert.deepEqual(frames, [], 'a message of no shape became a frame');
});

test("the close event's code and reason reach the handler, and a close that carries neither is 1005", async () => {
  const told = new Socket();
  const toldConnection = webSocketConnection(told);
  let closed: { code: number; reason: string } | undefined;
  toldConnection.listen({
    close: (code, reason) => {
      closed = { code, reason };
    },
  });
  told.close(4011, 'the duplex profile closed it');
  await settled();
  assert.deepEqual(closed, { code: 4011, reason: 'the duplex profile closed it' });

  const silent = new Socket();
  const silentConnection = webSocketConnection(silent);
  let none: { code: number; reason: string } | undefined;
  silentConnection.listen({
    close: (code, reason) => {
      none = { code, reason };
    },
  });
  silent.readyState = 3;
  silent.dispatchEvent(new Event('close'));
  await settled();
  assert.deepEqual(none, { code: 1005, reason: '' }, "a close event with no code is the registry's no status present");
});

test('the handlers are detached when the connection closes, so a message after it reaches nobody', async () => {
  const socket = new Socket();
  const connection = webSocketConnection(socket);
  const frames: Frame[] = [];
  connection.listen({
    frame: (frame) => {
      frames.push(frame);
    },
  });
  socket.close(1000, '');
  socket.readyState = 1;
  socket.receive('after the close');
  await settled();
  assert.deepEqual(frames, [], 'a message after the close reached a handler');
});

test("state is the socket's readyState and buffered its bufferedAmount, and a send it cannot take throws", () => {
  const socket = new Socket();
  const connection = webSocketConnection(socket);
  for (const [readyState, state] of [
    [0, 'connecting'],
    [1, 'open'],
    [2, 'closing'],
    [3, 'closed'],
    [9, 'closed'],
  ] as const) {
    socket.readyState = readyState;
    assert.equal(connection.state, state, `readyState ${readyState}`);
  }
  socket.readyState = 1;
  socket.bufferedAmount = 4096;
  assert.equal(connection.buffered, 4096);
  socket.readyState = 2;
  assert.throws(() => connection.send({ kind: 'text', data: 'not now' }), /not open/);
});

test('close passes the code and the reason it was given, and normal closure where it was given none', () => {
  const socket = new Socket();
  const connection = webSocketConnection(socket);
  connection.close();
  assert.deepEqual(socket.closes, [{ code: 1000, reason: '' }]);
  const second = new Socket();
  webSocketConnection(second).close(4010, 'a policy of the family');
  assert.deepEqual(second.closes, [{ code: 4010, reason: 'a policy of the family' }]);
});

// R27: a close code that only describes what a side observed is never sent.

test('only codes the registry lets a side send are sendable', () => {
  for (const code of [1000, 1001, 1002, 1003, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 1014, 3000, 4000, 4011, 4999])
    assert.equal(sendable(code), true, `${code}`);
  for (const code of [0, 999, 1004, NO_STATUS, 1006, 1015, 1016, 2999, 5000, 1000.5, Number.NaN])
    assert.equal(sendable(code), false, `${code}`);
});

test('a pipe refuses an observe-only close code, sends nothing and stays open', async () => {
  const [a, b] = pipe();
  const frames: Frame[] = [];
  let closed: { code: number; reason: string } | undefined;
  b.listen({
    frame: (frame) => {
      frames.push(frame);
    },
    close: (code, reason) => {
      closed = { code, reason };
    },
  });
  for (const code of [NO_STATUS, 1006, 1015, 1004, 5000]) {
    assert.throws(() => a.close(code, 'observed'), RangeError, `${code}`);
    assert.throws(() => b.close(code, 'observed'), RangeError, `${code}`);
  }
  assert.equal(a.state, 'open');
  assert.equal(b.state, 'open');
  await settled();
  assert.equal(closed, undefined, 'an observe-only code reached the far side');
  a.send({ kind: 'text', data: 'still open' });
  await settled();
  assert.deepEqual(frames, [{ kind: 'text', data: 'still open' }]);
  a.close(CODE_PROTOCOL, 'sent');
  assert.deepEqual(closed, { code: CODE_PROTOCOL, reason: 'sent' });
});

test('the WebSocket adapter refuses an observe-only close code before the socket sees it', () => {
  const socket = new Socket();
  const connection = webSocketConnection(socket);
  for (const code of [NO_STATUS, 1006, 1015]) assert.throws(() => connection.close(code, 'observed'), RangeError);
  assert.deepEqual(socket.closes, [], 'the socket was asked to send an observe-only code');
  assert.equal(connection.state, 'open');
  connection.close(1001, 'going');
  assert.deepEqual(socket.closes, [{ code: 1001, reason: 'going' }]);
});

/**
 * A socket pair in memory, as faithful to a WebSocket as the adapter reads
 * one: a message is delivered in a later turn, binary in the shape
 * binaryType asks for, and a close ends both ends with the code and the
 * reason the closing side gave. No WebSocket is involved anywhere, as in the
 * runtime's own suites.
 */
class Socket extends EventTarget implements WebSocketLike {
  readyState = 1;
  bufferedAmount = 0;
  binaryType = 'blob';
  readonly closes: { code: number; reason: string }[] = [];
  partner?: Socket;

  static pair(): [Socket, Socket] {
    const left = new Socket();
    const right = new Socket();
    left.partner = right;
    right.partner = left;
    return [left, right];
  }

  send(data: string | ArrayBuffer | Uint8Array): void {
    if (this.readyState !== 1) throw new Error('The socket is not open.');
    const partner = this.partner;
    if (!partner) return;
    const delivered = typeof data === 'string' ? data : partner.shape(data);
    queueMicrotask(() => {
      if (partner.readyState === 1) partner.receive(delivered);
    });
  }

  /** Delivers one message as a socket does, whatever the test made of it. */
  receive(data: unknown): void {
    this.dispatchEvent(new MessageEvent('message', { data }));
  }

  close(code = 1000, reason = ''): void {
    this.closes.push({ code, reason });
    if (this.readyState === 3) return;
    this.readyState = 3;
    this.dispatchEvent(Object.assign(new Event('close'), { code, reason }));
    this.partner?.close(code, reason);
  }

  /** Binary arrives as binaryType asks: an ArrayBuffer of its own bytes, or a Blob. */
  private shape(data: ArrayBuffer | Uint8Array): ArrayBuffer | Blob {
    const bytes = data instanceof Uint8Array ? data.slice() : new Uint8Array(data).slice();
    return this.binaryType === 'blob' ? new Blob([bytes]) : bytes.buffer;
  }
}

/** Every turn a socket or a blob could need; nothing of the seam is slower. */
async function settled(): Promise<void> {
  for (let i = 0; i < 5; i++)
    await new Promise((resolve) => {
      setTimeout(resolve, 2);
    });
}

test('a pipe with a limit delivers a frame at it and ends both ends with 1009 over it (bitruntime#26)', async () => {
  const [a, b] = pipe(8);
  const closes: Array<[string, number, string]> = [];
  const frames: unknown[] = [];
  a.listen({ close: (code, reason) => closes.push(['a', code, reason]) });
  b.listen({ frame: (frame) => frames.push(frame.data), close: (code, reason) => closes.push(['b', code, reason]) });
  a.send({ kind: 'text', data: '12345678' });
  // Five two-byte scalars: 10 bytes of UTF-8, over the limit of 8.
  a.send({ kind: 'text', data: 'é'.repeat(5) });
  await new Promise((resolve) => setTimeout(resolve, 10));
  assert.deepEqual(frames, ['12345678']);
  assert.deepEqual(closes.map(([side, code]) => [side, code]).sort(), [['a', 1009], ['b', 1009]]);
  assert.equal(a.state, 'closed');
  assert.equal(b.state, 'closed');
  assert.throws(() => pipe(-1), RangeError);
});
