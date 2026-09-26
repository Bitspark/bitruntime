// Runs the interoperability scenario in conformance/interop/README.md: every
// client program against every server program over real WebSockets, and the
// byte-level recorder against every server. Usage:
//   node scripts/interop.mjs [program ...]
// Programs default to every one that can be built here.
import assert from 'node:assert/strict';
import {spawn, spawnSync} from 'node:child_process';
import {existsSync, mkdtempSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
const interop = join(root, 'conformance', 'interop');
const temp = mkdtempSync(join(tmpdir(), 'bitruntime-interop-'));
const exe = process.platform === 'win32' ? '.exe' : '';
const npm = process.platform === 'win32' ? 'npm.cmd' : 'npm';

function run(command, args, cwd, env = {}) {
  const result = spawnSync(command, args, {
    cwd, encoding: 'utf8', stdio: 'pipe', env: {...process.env, ...env},
    shell: process.platform === 'win32' && command.endsWith('.cmd'),
  });
  if (result.status !== 0) throw new Error(`${command} ${args.join(' ')} failed:\n${result.stdout}\n${result.stderr}`);
  return result.stdout;
}

// Each program is a command line that takes `server <port>` or `client <url>`.
const builders = {
  'bitruntime-go': () => {
    const out = join(temp, `bitruntime-go${exe}`);
    run('go', ['build', '-o', out, './conformance/interop/bitruntime/go'], root);
    return [out];
  },
  'nightseam-go': () => {
    const out = join(temp, `nightseam-go${exe}`);
    // Its own module, pinned to the released v0.6.0 and Bitwire 0.2.0.
    run('go', ['build', '-mod=readonly', '-o', out, '.'], join(interop, 'nightseam', 'go'), {GOWORK: 'off'});
    return [out];
  },
  'bitruntime-ts': () => {
    const dir = join(interop, 'bitruntime', 'ts');
    if (!existsSync(join(dir, 'main.mjs'))) return undefined;
    if (!existsSync(join(root, 'dist'))) run(npm, ['run', 'build'], root);
    return [process.execPath, join(dir, 'main.mjs')];
  },
  'nightseam-ts': () => {
    const dir = join(interop, 'nightseam', 'ts');
    if (!existsSync(join(dir, 'package.json'))) return undefined;
    run(npm, ['ci', '--ignore-scripts', '--no-audit'], dir);
    return [process.execPath, join(dir, 'main.mjs')];
  },
};

const requested = process.argv.slice(2);
const programs = {};
for (const [name, build] of Object.entries(builders)) {
  if (requested.length && !requested.includes(name)) continue;
  const command = build();
  if (command) programs[name] = command;
}
const recorder = join(temp, `recorder${exe}`);
run('go', ['build', '-o', recorder, './conformance/interop/recorder/go'], root);

let port = 17500 + Math.floor(Math.random() * 400);
function startServer(command) {
  const [file, ...args] = command;
  const child = spawn(file, [...args, 'server', String(++port)], {stdio: ['ignore', 'pipe', 'pipe']});
  return new Promise((resolve, reject) => {
    let out = '';
    child.stdout.on('data', (chunk) => {
      out += chunk;
      const match = /LISTEN (\S+)/.exec(out);
      if (match) resolve({child, url: match[1]});
    });
    child.on('exit', (code) => reject(new Error(`server exited with ${code}: ${out}`)));
    setTimeout(() => reject(new Error(`server did not listen: ${out}`)), 20_000);
  });
}
function runClient(command, url) {
  const [file, ...args] = command;
  return JSON.parse(run(file, [...args, 'client', url], root));
}

function expected(client) {
  return {
    echo: {a: [1, 'x', null, true], n: 1000, u: '😀'},
    nested: {space: 'a/b', params: {x: 1}},
    unicodeEmpty: 'unicode-empty',
    missing: 'method_not_found',
    fail: {code: 'bad_request', message: 'refused on purpose', data: {n: 1}},
    pong: {echo: 7},
    withdrawn: true,
    serverSawCancel: true,
    meta: {tenant: 't1'},
    reverse: `${client}-client`,
    bigLength: 3145728,
    concurrent: 20,
  };
}

const transcripts = {};
const failures = [];
for (const [server, serverCommand] of Object.entries(programs)) {
  const {child, url} = await startServer(serverCommand);
  try {
    for (const [client, clientCommand] of Object.entries(programs)) {
      try {
        assert.deepStrictEqual(runClient(clientCommand, url), expected(client));
        console.log(`ok   ${client} -> ${server}`);
      } catch (error) {
        failures.push(`${client} -> ${server}: ${error.message}`);
        console.log(`FAIL ${client} -> ${server}`);
      }
    }
    transcripts[server] = run(recorder, [url], root);
  } finally {
    child.kill();
  }
}
writeFileSync(join(temp, 'transcripts.json'), JSON.stringify(transcripts, null, 2));
for (const language of ['go', 'ts']) {
  const ours = transcripts[`bitruntime-${language}`], theirs = transcripts[`nightseam-${language}`];
  if (ours === undefined || theirs === undefined) continue;
  if (ours === theirs) console.log(`ok   bitruntime-${language} sends the bytes nightseam-${language} sends`);
  else failures.push(`bitruntime-${language} transcript differs from nightseam-${language}:\n--- nightseam\n${theirs}\n--- bitruntime\n${ours}`);
}
console.log(`Transcripts: ${join(temp, 'transcripts.json')}`);
if (failures.length) {
  console.error(failures.join('\n\n'));
  process.exit(1);
}
console.log(`Interoperability passed for ${Object.keys(programs).join(', ')}.`);
