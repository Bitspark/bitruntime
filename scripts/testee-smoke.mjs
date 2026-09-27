import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { createInterface } from 'node:readline';
import { fileURLToPath } from 'node:url';

// Drives bitruntime's two driver-1 testees (cmd/bitwire-testee) through the
// exchange of bitwire's conformance contract: hello, a peer pairing in each
// direction over WebSockets with an echo call across it, and bye. This is a
// smoke of the testees themselves; bitwire's runner judges conformance.
//
// Usage:
//   node scripts/testee-smoke.mjs
//     runs the TypeScript testee from source (run `npm run build` first);
//   node scripts/testee-smoke.mjs <runtime.tgz> <testee.tgz>
//     installs the packed testee into a fresh consumer, with the runtime
//     dependency resolved to the packed runtime, and runs its bin.
const root = fileURLToPath(new URL('../', import.meta.url));
const temp = mkdtempSync(join(tmpdir(), 'bitruntime-testee-smoke-'));
const npm = process.platform === 'win32' ? 'npm.cmd' : 'npm';
function run(command, args, cwd) {
  const result = spawnSync(command, args, { cwd, shell: process.platform === 'win32' && command.endsWith('.cmd'), encoding: 'utf8', stdio: 'inherit' });
  if (result.status !== 0) throw new Error(`${command} ${args.join(' ')} failed`);
}

const goTestee = join(temp, process.platform === 'win32' ? 'bitwire-testee.exe' : 'bitwire-testee');
run('go', ['build', '-o', goTestee, './cmd/bitwire-testee/go'], root);

let tsCommand = [process.execPath, join(root, 'cmd', 'bitwire-testee', 'ts', 'src', 'testee.ts')];
if (process.argv.length > 2) {
  const [runtime, testee] = process.argv.slice(2).map((path) => resolve(path));
  const consumer = join(temp, 'consumer');
  run(process.execPath, ['-e', `require('node:fs').mkdirSync(${JSON.stringify(consumer)})`]);
  // The testee depends on the runtime by its release URL, which a release
  // publishes only after this smoke; the override resolves it to the packed
  // runtime, the consumer's own dependency.
  writeFileSync(join(consumer, 'package.json'), `${JSON.stringify({
    private: true,
    type: 'module',
    dependencies: { '@bitspark/bitruntime': `file:${runtime}` },
    overrides: { '@bitspark/bitruntime': '$@bitspark/bitruntime' },
  })}\n`);
  run(npm, ['install', '--ignore-scripts', '--no-audit', '--no-fund', '--@bitspark:registry=https://registry.npmjs.org', testee], consumer);
  tsCommand = [process.execPath, join(consumer, 'node_modules', '@bitspark', 'bitruntime-testee', 'dist', 'testee.js')];
}

class Testee {
  #next = 1;
  #pending = new Map();
  constructor(name, command) {
    this.name = name;
    this.process = spawn(command[0], command.slice(1), { cwd: root, stdio: ['pipe', 'pipe', 'inherit'] });
    this.exited = new Promise((resolve) => this.process.once('exit', (code) => resolve(code)));
    createInterface({ input: this.process.stdout }).on('line', (line) => {
      const answer = JSON.parse(line);
      const settle = this.#pending.get(answer.id);
      assert.ok(settle, `${this.name} answered unasked: ${line}`);
      this.#pending.delete(answer.id);
      settle(answer);
    });
  }
  request(op, args = {}) {
    const id = this.#next++;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`${this.name} did not answer ${op} in time`)), 15_000);
      this.#pending.set(id, (answer) => {
        clearTimeout(timer);
        resolve(answer);
      });
      this.process.stdin.write(`${JSON.stringify({ id, op, ...args })}\n`);
    });
  }
  async ok(op, args) {
    const answer = await this.request(op, args);
    assert.ok(!answer.error, `${this.name} ${op}: ${JSON.stringify(answer.error)}`);
    return answer.ok;
  }
}

const go = new Testee('go', [goTestee]);
const ts = new Testee('ts', tsCommand);
for (const [testee, language] of [[go, 'go'], [ts, 'typescript']]) {
  const hello = await testee.ok('hello');
  assert.equal(hello.driver, 1, `${testee.name} hello`);
  assert.equal(hello.language, language);
  for (const layer of ['seam', 'peer']) assert.ok(hello.layers.includes(layer), `${testee.name} lacks the ${layer} layer`);
  assert.ok(!hello.layers.includes('tunnel'), `${testee.name} claims tunnels`);
  assert.ok(hello.features.includes('listen'), `${testee.name} cannot listen`);
}

// Each direction: the server listens, the client dials, the server answers an
// echo, and a call from the client comes back with its params.
for (const [server, client] of [[go, ts], [ts, go]]) {
  await server.ok('reset');
  await client.ok('reset');
  const listener = await server.ok('peer.listen', {});
  const dialled = await client.ok('peer.dial', { url: listener.url });
  const accepted = await server.ok('peer.accept', { on: listener.handle, within_ms: 10_000 });
  await server.ok('peer.handle', { on: accepted.handle, method: 'echo', behavior: { kind: 'echo' } });
  const params = { n: 1, text: 'é😀', list: [null, true] };
  const call = await client.ok('peer.call', { on: dialled.handle, method: 'echo', params });
  assert.deepEqual(await client.ok('call.await', { on: call.handle, within_ms: 10_000 }), { result: params }, `${client.name} → ${server.name}`);
  const refused = await client.request('tunnel.open', { on: dialled.handle });
  assert.equal(refused.error?.code, 'unsupported', `${client.name} tunnel.open`);
  console.log(`${client.name} called ${server.name} over a WebSocket.`);
}
for (const testee of [go, ts]) {
  await testee.ok('bye');
  testee.process.stdin.end();
  assert.equal(await testee.exited, 0, `${testee.name} exit`);
}
console.log('Both driver-1 testees passed the smoke.');
