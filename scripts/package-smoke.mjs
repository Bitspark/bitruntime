import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {copyFileSync, mkdtempSync, readFileSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
const pkg = join(root, 'core', 'ts');
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

for (const name of ['LICENSE', 'NOTICE']) copyFileSync(join(root, name), join(pkg, name));
const packed = JSON.parse(run(npm, ['pack', '--json', '--pack-destination', temp], pkg, true));
writeFileSync(join(temp, 'package.json'), '{"type":"module","private":true}\n');
run(npm, ['install', '--ignore-scripts', '--no-audit', '--@bitspark:registry=https://registry.npmjs.org', join(temp, packed[0].filename)], temp);
writeFileSync(join(temp, 'smoke.mjs'), `
import assert from 'node:assert/strict';
import {compose, select, send, asAddressed} from '@bitspark/bitruntime-core';
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
console.log('Fresh installed npm tarball consumer passed.');
`);
run(process.execPath, ['smoke.mjs'], temp);
const metadata = JSON.parse(readFileSync(join(pkg, 'package.json'), 'utf8'));
assert.equal(metadata.version, packed[0].version);
console.log(`Packed ${metadata.name}@${metadata.version}: ${join(temp, packed[0].filename)}`);
