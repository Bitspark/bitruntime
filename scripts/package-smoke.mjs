import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {existsSync, mkdtempSync, readFileSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';

// Packs the root TypeScript package, installs the tarball into a fresh
// consumer and exercises its public subpaths. Run `npm run build` first.
const root = fileURLToPath(new URL('../', import.meta.url));
if (!existsSync(join(root, 'dist', 'core', 'ts', 'src', 'index.js'))) throw new Error('Run npm run build before the package smoke.');
const temp = mkdtempSync(join(tmpdir(), 'bitruntime-consumer-'));
const npm = process.platform === 'win32' ? 'npm.cmd' : 'npm';
function run(command, args, cwd, capture = false) {
  const result = spawnSync(command, args, {
    cwd,
    shell: process.platform === 'win32' && command.endsWith('.cmd'),
    encoding: 'utf8',
    stdio: capture ? 'pipe' : 'inherit',
  });
  if (result.status !== 0) throw new Error(`${command} failed: ${result.stderr ?? result.error ?? ''}`);
  return result.stdout;
}

const packed = JSON.parse(run(npm, ['pack', '--json', '--pack-destination', temp], root, true));
const files = packed[0].files.map((file) => file.path);
for (const required of ['LICENSE', 'NOTICE', 'README.md', 'package.json', 'dist/core/ts/src/index.js', 'dist/dispatch/ts/src/index.d.ts'])
  assert.ok(files.includes(required), `the tarball lacks ${required}`);
assert.ok(!files.some((file) => /\/test\//.test(file) || file.endsWith('.go')), 'the tarball carries tests or Go sources');
writeFileSync(join(temp, 'package.json'), '{"type":"module","private":true}\n');
run(npm, ['install', '--ignore-scripts', '--no-audit', '--@bitspark:registry=https://registry.npmjs.org', join(temp, packed[0].filename)], temp);
writeFileSync(join(temp, 'smoke.mjs'), `
import assert from 'node:assert/strict';
import {compose, select, send, asAddressed, pair, PublicError} from '@bitspark/bitruntime/core';
import {call, createDispatcher, handle} from '@bitspark/bitruntime/dispatch';
import {Peer, PROTOCOL} from '@bitspark/bitruntime/engine';
import {pipe, sendable} from '@bitspark/bitruntime/transports';

const key = new TextEncoder().encode('child');
const seen = [];
const primitive = {send(message) { seen.push(message); }};
const leaf = compose(primitive);
const tree = compose({send() { throw new Error('wrong own'); }}, [[key, leaf]]);
const message = {frame: {version: 1, kind: 'event', data: {ok: true}}};
assert.equal(select(tree, [key]), leaf);
assert.equal(tree.decompose().children[0][1], leaf);
send(tree, [key], message);
asAddressed(tree).send(['child'], message);
assert.deepEqual(seen, [message, message]);
assert.throws(() => send(tree, [Uint8Array.of(255)], message));

const [caller, callee] = pair();
const dispatcher = createDispatcher(callee);
handle(dispatcher, ['echo'], (value) => value);
assert.equal(await call(caller, ['echo'], 'through a pair'), 'through a pair');
await assert.rejects(call(caller, ['absent']), (error) => error instanceof PublicError && error.code === 'method_not_found');
caller.close();

const [near, far] = pipe();
const client = new Peer();
const server = new Peer({role: 'server', prepare: (peer) => handle(createDispatcher(peer.wire()), ['echo'], (value) => value)});
await Promise.all([client.attach(near), server.attach(far)]);
assert.equal(await call(client.wire(), ['echo'], PROTOCOL), 'bitwire/1');
client.close();
assert.equal(sendable(1006), false);

// The received-context machinery is module-private: no subpath reaches it.
for (const hidden of ['@bitspark/bitruntime', '@bitspark/bitruntime/core/ts/src/internal/context.js', '@bitspark/bitruntime/dist/core/ts/src/internal/context.js']) {
  await assert.rejects(import(hidden), (error) => ['ERR_PACKAGE_PATH_NOT_EXPORTED', 'ERR_MODULE_NOT_FOUND'].includes(error.code), hidden);
}
console.log('Fresh installed npm tarball consumer passed.');
`);
run(process.execPath, ['smoke.mjs'], temp);
const metadata = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8'));
assert.equal(metadata.version, packed[0].version);
console.log(`Packed ${metadata.name}@${metadata.version}: ${join(temp, packed[0].filename)}`);
