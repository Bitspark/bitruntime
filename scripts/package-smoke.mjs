import assert from 'node:assert/strict';
import {spawnSync, execFileSync} from 'node:child_process';
import {existsSync, mkdtempSync, readFileSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join, dirname} from 'node:path';
import {fileURLToPath} from 'node:url';

// Packs the root TypeScript package, installs the tarball into a fresh
// consumer and exercises its public subpaths. Run `npm run build` first.
const root = fileURLToPath(new URL('../', import.meta.url));
if (!existsSync(join(root, 'dist', 'core', 'ts', 'src', 'index.js'))) throw new Error('Run npm run build before the package smoke.');
const temp = mkdtempSync(join(tmpdir(), 'bitruntime-consumer-'));
const npm = process.platform === 'win32' ? 'npm.cmd' : 'npm';
const npmScript = process.platform === 'win32' ? [process.env.npm_execpath,
  ...execFileSync('where.exe', ['npm.cmd'], {encoding:'utf8'}).trim().split(/\r?\n/)
    .map(command => join(dirname(command), 'node_modules/npm/bin/npm-cli.js'))]
  .find(candidate => candidate && existsSync(candidate)) : undefined;
if (process.platform === 'win32' && !npmScript) throw new Error('Cannot locate the installed npm CLI script');
function run(command, args, cwd, capture = false) {
  if (command === npm && npmScript) { command = process.execPath; args = [npmScript, ...args]; }
  const result = spawnSync(command, args, {
    cwd,
    shell: false,
    encoding: 'utf8',
    stdio: capture ? 'pipe' : 'inherit',
  });
  if (result.status !== 0) throw new Error(`${command} failed: ${result.stderr ?? result.error ?? ''}`);
  return result.stdout;
}

const packOutput = JSON.parse(run(npm, ['pack', '--json', '--pack-destination', temp], root, true));
// npm 12 keys results by package name; Node's bundled npm returns a list.
const packed = Array.isArray(packOutput) ? packOutput : Object.values(packOutput);
const files = packed[0].files.map((file) => file.path);
for (const required of ['LICENSE', 'NOTICE', 'README.md', 'package.json', 'dist/core/ts/src/index.js', 'dist/websocket/ts/src/index.d.ts'])
  assert.ok(files.includes(required), `the tarball lacks ${required}`);
assert.ok(!files.some((file) => /\/test\//.test(file) || file.endsWith('.go')), 'the tarball carries tests or Go sources');
writeFileSync(join(temp, 'package.json'), '{"type":"module","private":true}\n');
run(npm, ['install', '--ignore-scripts', '--no-audit', '--@bitspark:registry=https://registry.npmjs.org', join(temp, packed[0].filename)], temp);
writeFileSync(join(temp, 'smoke.mjs'), `
import assert from 'node:assert/strict';
import {atom,tuple,pathEqual} from '@bitspark/bitwire';
import {pair,compose,addressed,bind,asAddressed} from '@bitspark/bitruntime/core';
import {connectWebSocket,listenWebSocket} from '@bitspark/bitruntime/websocket';
const message=tuple([atom([255,0])]);
const [left,right]=pair();const local=new Promise(r=>right.receive(r));await left.send(message);assert.ok((await local).equals(message));await left.close();
const leaf=compose(7);assert.equal(compose(3,[[atom([]),leaf]]).at([atom([])]),leaf);
const [a,b]=pair();const routed=new Promise(r=>b.receive(r));
await bind(asAddressed(compose(a)),[]).send(message);assert.ok((await routed).equals(message));
await a.close();
const server=await listenWebSocket({},wire=>wire.receive(e=>{void wire.send(e);}));
const client=await connectWebSocket(server.url);const scoped=addressed(client);
const remote=new Promise(r=>scoped.receive((path,value)=>r({path,value})));
await bind(scoped,[atom([]),atom([255])]).send(message);const result=await remote;
assert.ok(pathEqual(result.path,[atom([]),atom([255])]));assert.ok(result.value.equals(message));await client.close();await server.close();
for(const retired of ['engine','dispatch','transports'])await assert.rejects(import('@bitspark/bitruntime/'+retired),{code:'ERR_PACKAGE_PATH_NOT_EXPORTED'});
console.log('Fresh installed package: local pair, tree and WebSocket passed.');
`);
run(process.execPath, ['smoke.mjs'], temp);
const metadata = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8'));
assert.equal(metadata.version, packed[0].version);
console.log(`Packed ${metadata.name}@${metadata.version}: ${join(temp, packed[0].filename)}`);
