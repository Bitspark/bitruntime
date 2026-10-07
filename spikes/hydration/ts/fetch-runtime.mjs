// Deliberately installs the immutable released runtime, not this checkout.
import { createHash } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';

const name = 'bitspark-bitruntime-0.6.0.tgz';
const hash = 'da651aee6f14cc5a1116af55ab1bd575650a562ea6efc08f14dc840cff8d9c19';
const base = 'https://github.com/Bitspark/bitruntime/releases/download/v0.6.0';
const response = await fetch(`${base}/${name}`);
if (!response.ok) throw new Error(`Runtime download failed: ${response.status}`);
const bytes = new Uint8Array(await response.arrayBuffer());
if (createHash('sha256').update(bytes).digest('hex') !== hash) throw new Error('Runtime hash mismatch');
const sums = await fetch(`${base}/SHA256SUMS`);
if (!sums.ok) throw new Error(`Release sums unavailable: ${sums.status}`);
if (!(await sums.text()).split('\n').some(line => line.trim().split(/\s+\*?/).join(' ') === `${hash} ${name}`)) {
  throw new Error('Release does not attest pinned runtime');
}
const directory = new URL('./artifacts/', import.meta.url);
await mkdir(directory, { recursive: true });
await writeFile(new URL(name, directory), bytes);
console.log(`${name} sha256 ${hash}`);
