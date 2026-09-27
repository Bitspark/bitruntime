import { spawnSync } from 'node:child_process';
import { cpSync, mkdirSync, mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

// Packs the TypeScript driver-1 testee (cmd/bitwire-testee/ts) as its own
// npm-compatible tarball, bitspark-bitruntime-testee-<version>.tgz, beside the
// package's. It depends on @bitspark/bitruntime by this version's release
// asset, so an installed testee runs exactly the published runtime, and no
// testee code ships inside the runtime package.
//
// Usage: node scripts/testee-package.mjs <destination directory>
const root = fileURLToPath(new URL('../', import.meta.url));
const destination = resolve(process.argv[2] ?? '');
if (!process.argv[2]) throw new Error('Usage: node scripts/testee-package.mjs <destination directory>');
const npm = process.platform === 'win32' ? 'npm.cmd' : 'npm';
function run(command, args, cwd, capture = false) {
  const result = spawnSync(command, args, {
    cwd,
    shell: process.platform === 'win32' && command.endsWith('.cmd'),
    encoding: 'utf8',
    stdio: capture ? 'pipe' : 'inherit',
  });
  if (result.status !== 0) throw new Error(`${command} ${args.join(' ')} failed: ${result.stderr ?? result.error ?? ''}`);
  return result.stdout;
}

const manifest = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8'));
const version = manifest.version;
const testee = join(root, 'cmd', 'bitwire-testee', 'ts');
const staging = join(mkdtempSync(join(tmpdir(), 'bitruntime-testee-')), 'package');
mkdirSync(staging, { recursive: true });
run(process.execPath, [join(root, 'node_modules', 'typescript', 'bin', 'tsc'), '-p', join(testee, 'tsconfig.json'), '--outDir', join(staging, 'dist')], root);
const entry = join(staging, 'dist', 'testee.js');
const compiled = readFileSync(entry, 'utf8');
if (!compiled.startsWith('#!')) writeFileSync(entry, `#!/usr/bin/env node\n${compiled}`);
for (const file of ['LICENSE', 'NOTICE']) cpSync(join(root, file), join(staging, file));
cpSync(join(root, 'cmd', 'bitwire-testee', 'README.md'), join(staging, 'README.md'));
writeFileSync(join(staging, 'package.json'), `${JSON.stringify({
  name: '@bitspark/bitruntime-testee',
  version,
  description: "bitruntime's TypeScript driver-1 testee for bitwire's bitwire/1 conformance contract. Test tooling, never a runtime dependency.",
  license: 'Apache-2.0',
  type: 'module',
  repository: { type: 'git', url: 'git+https://github.com/Bitspark/bitruntime.git', directory: 'cmd/bitwire-testee/ts' },
  bin: { 'bitwire-testee': 'dist/testee.js' },
  files: ['dist', 'README.md', 'LICENSE', 'NOTICE'],
  engines: manifest.engines,
  dependencies: {
    '@bitspark/bitruntime': `https://github.com/Bitspark/bitruntime/releases/download/v${version}/bitspark-bitruntime-${version}.tgz`,
    '@bitspark/bitwire': manifest.dependencies['@bitspark/bitwire'],
    ws: manifest.devDependencies.ws,
  },
}, null, 2)}\n`);
mkdirSync(destination, { recursive: true });
const packed = JSON.parse(run(npm, ['pack', '--json', '--pack-destination', destination], staging, true));
// npm 11 answers an array of packages, npm 12 an object keyed by package.
const [tarball] = Array.isArray(packed) ? packed : Object.values(packed);
console.log(join(destination, tarball.filename));
