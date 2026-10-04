import {spawnSync} from 'node:child_process';
import {mkdtempSync, writeFileSync, readFileSync} from 'node:fs';
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
console.log(`Installed public Go module at ${revision} in ${temp}`);
