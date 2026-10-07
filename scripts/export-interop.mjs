import assert from 'node:assert/strict';
import {spawn, spawnSync} from 'node:child_process';
import {mkdtempSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createInterface} from 'node:readline';
import {atom, encodeMessage} from '@bitspark/bitwire';
import {addressed} from '../dist/core/ts/src/index.js';
import {connectWebSocket, listenWebSocket} from '../dist/websocket/ts/src/index.js';
import {Table, Importer} from '../dist/export/ts/src/index.js';
const directory = mkdtempSync(join(tmpdir(), 'bitruntime-export-interop-'));
process.on('exit', () => rmSync(directory, {recursive: true, force: true}));
const binary = join(directory, process.platform === 'win32' ? 'export.exe' : 'export');
assert.equal(spawnSync('go', ['build', '-o', binary, './conformance/interop/export'], {stdio: 'inherit'}).status, 0, 'Go export peer build failed');
const text = (s) => atom(new TextEncoder().encode(s));
const ROUTE = text('r'), ROOT = [text('x')], hello = text('hello');
function peer(args) {
  const child = spawn(binary, args, {stdio: ['pipe', 'pipe', 'inherit']});
  const lines = [], readers = [];
  createInterface({input: child.stdout}).on('line', (l) => { const r = readers.shift(); if (r) r(l); else lines.push(l); });
  const timeout = setTimeout(() => child.kill(), 30000);
  const ended = new Promise((resolve) => child.once('exit', (code) => { clearTimeout(timeout); resolve(code); while (readers.length) readers.shift()(undefined); }));
  return {child, ended, line: () => lines.length ? Promise.resolve(lines.shift()) : new Promise((r) => readers.push(r))};
}
const until = async (cond, what) => { for (let i = 0; i < 300; i++) { if (cond()) return; await new Promise((r) => setTimeout(r, 10)); } assert.fail(what); };

// Go exports; TypeScript imports, sends and releases.
{
  const server = peer(['serve']);
  const endpoint = await connectWebSocket(await server.line());
  const ep = addressed(endpoint), importer = new Importer(ep, ROOT);
  const ref = await new Promise((resolve) => ep.receive((p, v) => { if (p.length === 1 && p[0].equals(ROUTE)) resolve(v); }));
  const imported = importer.import(ref);
  const release = imported.hold();
  await imported.wire.send(hello);
  release();
  assert.equal(await server.line(), 'got ' + Buffer.from(encodeMessage(hello)).toString('hex'));
  assert.equal(await server.line(), 'released');
  await endpoint.close(); server.child.stdin.end();
  assert.equal(await server.ended, 0);
}

// TypeScript exports; Go imports, sends and releases.
{
  const got = []; let table;
  const listener = await listenWebSocket({}, (endpoint) => {
    const ep = addressed(endpoint);
    table = new Table(8);
    ep.receive((p, v) => { if (p.length && p[0].equals(ROOT[0])) table.deliver(p.slice(1), v); });
    void ep.send([ROUTE], table.export({send: async (v) => { got.push(v); }}));
  });
  try {
    const client = peer(['client', listener.url]);
    assert.equal(await client.line(), 'ok');
    assert.equal(await client.ended, 0);
    await until(() => got.length === 1 && table.live === 0, 'the Go importer did not send and release');
    assert.ok(got[0].equals(hello));
  } finally { await listener.close(); }
}
console.log('Go and TypeScript export, import, send and release across processes in both roles.');
