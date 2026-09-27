#!/usr/bin/env node
// Bitspark naming check. The rule: a project's name is its GitHub repository
// name, character for character (https://github.com/Bitspark/.github/blob/master/NAMING.md).
//
//   node naming.mjs            report prose that spells a repository name differently
//   node naming.mjs --fix      rewrite those spellings in place
//   node naming.mjs --update   refresh bitspark-repositories.txt from GitHub (needs gh)
//
// Copy this file and bitspark-repositories.txt into a repository unchanged.
// Paths listed in .namingignore at the repository root are skipped; each line
// is a path prefix or a glob with `*`, and `#` starts a comment.
import { execFileSync } from 'node:child_process';
import { existsSync, readFileSync, statSync, writeFileSync } from 'node:fs';
import { dirname, join, relative, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const snapshot = join(here, 'bitspark-repositories.txt');

// Repository names that are also ordinary words or external products. The rule
// applies to them, but prose cannot tell "stack" the word from Stack the
// repository, so the check does not flag them.
const ordinaryWords = new Set([
  'arche', 'atlas', 'constellation', 'corpus', 'design', 'dm', 'fieldtest', 'foundry',
  'graph', 'prism', 'radial', 'refile', 'schema', 'slang', 'stack', 'taxis', 'weft',
]);

if (process.argv.includes('--update')) {
  const listed = JSON.parse(execFileSync('gh', ['repo', 'list', 'Bitspark', '--limit', '1000', '--json', 'name,isFork'], { encoding: 'utf8', maxBuffer: 1 << 30 }));
  // A fork carries its upstream project's name, which that project spells.
  const names = listed.filter((r) => !r.isFork).map((r) => r.name).sort((a, b) => a.localeCompare(b, 'en', { sensitivity: 'base' }));
  writeFileSync(snapshot, `# Bitspark repository names, excluding forks. Refresh: node naming.mjs --update\n${names.join('\n')}\n`);
  console.log(`${names.length} names written to ${snapshot}`);
  process.exit(0);
}

const canonical = new Map();
for (const line of readFileSync(snapshot, 'utf8').split('\n')) {
  const name = line.trim();
  if (name && !name.startsWith('#') && !ordinaryWords.has(name.toLowerCase())) canonical.set(name.toLowerCase(), name);
}

const root = execFileSync('git', ['rev-parse', '--show-toplevel'], { encoding: 'utf8' }).trim();
const ignore = existsSync(join(root, '.namingignore'))
  ? readFileSync(join(root, '.namingignore'), 'utf8').split('\n').map((l) => l.replace(/#.*/, '').trim()).filter(Boolean)
  : [];
const ignored = (path) => ignore.some((pattern) => pattern.includes('*')
  ? new RegExp(`^${pattern.split('*').map((p) => p.replace(/[.+?^${}()|[\]\\]/g, '\\$&')).join('.*')}$`).test(path)
  : path === pattern || path.startsWith(pattern.endsWith('/') ? pattern : `${pattern}/`));

// The snapshot is the list of names, not prose about them.
const own = relative(root, snapshot).split(sep).join('/');
const files = execFileSync('git', ['ls-files', '-z'], { cwd: root, encoding: 'utf8', maxBuffer: 1 << 30 }).split('\0')
  .filter((path) => /\.(md|mdx|markdown|txt|rst)$/i.test(path) && path !== own && !ignored(path))
  // Build and dependency files that end in .txt are code, not prose.
  .filter((path) => !/(^|\/)(CMakeLists|requirements[^/]*|constraints)\.txt$/i.test(path));

// Blank out what is not prose: inline code, link targets, URLs and HTML tags.
// Identifiers and addresses follow their own rules; a path that names a
// repository already spells it as the repository does.
function prose(line) {
  const blank = (m) => ' '.repeat(m.length);
  return line
    .replace(/(`+)[^`]*?\1/g, blank)
    .replace(/\]\([^)]*\)/g, blank)
    .replace(/<[^>]*>/g, blank)
    .replace(/\b[a-z][a-z0-9+.-]*:\/\/\S+/gi, blank);
}

// The longest hyphen-joined run of segments that is a repository name wins, so
// logos-db is one name while Bitwire-based still flags Bitwire. An underscore
// belongs to its word: shelm_archive is one word, not SHELM.
function findings(masked) {
  const found = [];
  for (const token of masked.matchAll(/[A-Za-z0-9_]+(?:-[A-Za-z0-9_]+)*/g)) {
    const segments = token[0].split('-');
    let offset = token.index;
    for (let i = 0; i < segments.length;) {
      let matched = 0;
      for (let j = segments.length; j > i; j--) {
        const candidate = segments.slice(i, j).join('-');
        const name = canonical.get(candidate.toLowerCase());
        if (name) {
          if (candidate !== name) found.push({ at: offset, text: candidate, name });
          matched = j - i;
          break;
        }
      }
      const step = Math.max(matched, 1);
      offset += segments.slice(i, i + step).join('-').length + 1;
      i += step;
    }
  }
  return found;
}

const fix = process.argv.includes('--fix');
let count = 0;
for (const path of files) {
  const full = join(root, path);
  if (statSync(full).size > 2_000_000) continue;
  const lines = readFileSync(full, 'utf8').split('\n');
  let fence = null;
  let changed = false;
  lines.forEach((line, index) => {
    const opening = line.match(/^\s*(```+|~~~+)\s*([\w+-]*)/);
    if (fence) {
      if (opening && !opening[2] && opening[1][0] === fence.marker[0] && opening[1].length >= fence.marker.length) {
        fence = null;
        return;
      }
      // A ```text block is prose laid out by hand; any other block is code.
      if (!fence.prose) return;
    } else if (opening) {
      fence = { marker: opening[1], prose: /^(text|txt)$/i.test(opening[2]) };
      return;
    }
    if (/^\s*>/.test(line)) return; // quotations stay verbatim
    const found = findings(prose(line));
    for (const f of found) {
      count++;
      if (!fix) console.log(`${path}:${index + 1}:${f.at + 1}: ${f.text} → ${f.name}`);
    }
    if (fix && found.length) {
      let out = line;
      for (const f of [...found].reverse()) out = out.slice(0, f.at) + f.name + out.slice(f.at + f.text.length);
      lines[index] = out;
      changed = true;
    }
  });
  if (changed) writeFileSync(full, lines.join('\n'));
}
if (fix) {
  console.log(`${count} spellings rewritten.`);
} else if (count) {
  console.log(`\n${count} spellings differ from their repository names. Run node ${join(here, 'naming.mjs')} --fix, then review.`);
  process.exit(1);
}
