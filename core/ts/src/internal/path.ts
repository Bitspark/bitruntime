/**
 * The canonical encoding of an addressed path as bitwire/1 carries it in a
 * frame's method or event name. Module-private: no package subpath exports it.
 */
import type { Path } from '@bitspark/bitwire';
import { InvalidPathError } from '../error.ts';

function scalar(value: string): void {
  if (typeof value !== 'string') throw new InvalidPathError();
  for (let i = 0; i < value.length; i++) {
    const unit = value.charCodeAt(i);
    if (unit >= 0xd800 && unit <= 0xdbff) {
      const low = value.charCodeAt(++i);
      if (!(low >= 0xdc00 && low <= 0xdfff)) throw new InvalidPathError();
    } else if (unit >= 0xdc00 && unit <= 0xdfff) throw new InvalidPathError();
  }
}

/** UTF-8 byte-length-prefixed segments; [] is '', whereas [''] is '0:'. */
export function encodePath(path: Path): string {
  const encoder = new TextEncoder();
  return path
    .map((segment) => {
      scalar(segment);
      return `${encoder.encode(segment).length}:${segment}`;
    })
    .join('');
}

/** Accepts only the canonical encoding, retaining dots, empty strings and BOMs. */
export function decodePath(encoded: string): string[] {
  scalar(encoded);
  const bytes = new TextEncoder().encode(encoded);
  const decoder = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true });
  const path: string[] = [];
  for (let offset = 0; offset < bytes.length; ) {
    const start = offset;
    let length = 0;
    while (offset < bytes.length && bytes[offset] !== 58) {
      const digit = bytes[offset++]! - 48;
      if (digit < 0 || digit > 9) throw new InvalidPathError();
      length = length * 10 + digit;
      if (!Number.isSafeInteger(length)) throw new InvalidPathError();
    }
    if (offset === start || offset === bytes.length || (offset - start > 1 && bytes[start] === 48))
      throw new InvalidPathError();
    offset++;
    if (length > bytes.length - offset) throw new InvalidPathError();
    try {
      path.push(decoder.decode(bytes.subarray(offset, offset + length)));
    } catch {
      throw new InvalidPathError();
    }
    offset += length;
  }
  return path;
}
