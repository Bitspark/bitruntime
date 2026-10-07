import {spawn, spawnSync} from 'node:child_process';
import {mkdirSync, mkdtempSync, writeFileSync, readFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';

const revision = process.argv[2];
if (!revision || !/^[a-zA-Z0-9._-]+$/.test(revision)) {
  throw new Error('Pass the pushed commit or release tag to verify');
}
const temp = mkdtempSync(join(tmpdir(), 'bitruntime-go-consumer-'));
function run(args) {
  const result = spawnSync('go', args, {
    cwd: temp, stdio: 'inherit', env: {...process.env, GOWORK: 'off'},
  });
  if (result.status !== 0) throw new Error(`go ${args.join(' ')} failed`);
}
run(['mod', 'init', 'example.com/bitruntime-consumer']);
writeFileSync(join(temp, 'main.go'), readFileSync(new URL('../conformance/interop/go/main.go', import.meta.url)));
run(['get', `github.com/Bitspark/bitruntime@${revision}`]);
run(['mod', 'tidy']);
run(['run', '.', 'smoke']);

// The hydrated peer from the public module: serve, then call with a reply Wire.
const hydrated = join(temp, 'hydrated');
mkdirSync(hydrated);
writeFileSync(join(hydrated, 'main.go'), readFileSync(new URL('../conformance/interop/hydrated/main.go', import.meta.url)));
// Not `hydrated`: that is the peer's source directory, and go build -o into an
// existing directory writes the binary inside it.
const binary = join(temp, process.platform === 'win32' ? 'hydrated-peer.exe' : 'hydrated-peer');
run(['build', '-o', binary, './hydrated']);
const server = spawn(binary, ['serve'], {stdio: ['pipe', 'pipe', 'inherit']});
const url = await new Promise((resolve, reject) => {
  server.stdout.once('data', (d) => resolve(String(d).trim().split(/\s+/)[0]));
  server.once('error', reject);
});
const client = spawnSync(binary, ['client', url], {encoding: 'utf8', timeout: 30000});
server.stdin.end();
if (client.status !== 0 || client.stdout.trim() !== 'ok') throw new Error('hydrated Go peer failed: ' + client.stderr);
console.log(`Installed public Go module at ${revision} in ${temp}, including the hydrated peer`);
