// Ported from nightseam v0.6.0 runtime/ts/src/unicode.test.ts. The rows of
// vectors/bitwire-1/unicode.json are judged at the raw frame boundary exactly
// as v0.6.0 judges them. v0.6.0 also held each row to its family validator
// (validate.ts), which is not part of this port; the runtime's own outgoing
// value guard is held to the same rows in its place.
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import { decodeEnvelope } from '../src/internal/envelope.ts';
import { pair } from '../src/index.ts';
import { emit } from '../../../dispatch/ts/src/index.ts';

const table = JSON.parse(
  readFileSync(new URL('../../../vectors/bitwire-1/unicode.json', import.meta.url), 'utf8'),
) as { rows: { name: string; raw: string; valid: boolean }[] };

test('Unicode scalar input domain is shared before decoding', () => {
  for (const row of table.rows) {
    const checkFrame = (): void => {
      decodeEnvelope('{"version":1,"kind":"event","event":"probe","data":' + row.raw + '}', 's:', 'c:');
    };
    if (row.valid) assert.doesNotThrow(checkFrame, row.name);
    else assert.throws(checkFrame, /invalid Unicode: expected Unicode scalar strings/, row.name);
  }
});

test('an outgoing value is held to the same Unicode rows before admission', (t) => {
  const [a, b] = pair();
  t.after(() => a.close());
  b.receive({ message: () => {} });
  for (const row of table.rows) {
    // A duplicate overwritten value is already lost after JSON.parse; it is
    // held at the raw frame boundary above rather than reconstructed here.
    if (row.name === 'overwritten malformed value') continue;
    const value: unknown = JSON.parse(row.raw);
    const check = (): void => emit(a, ['probe'], value);
    if (row.valid) assert.doesNotThrow(check, row.name);
    else assert.throws(check, { code: 'invalid_message' }, row.name);
  }
});
