/** Unicode is checked before parsing or serialization can discard a string. */
const refusal = 'invalid Unicode: expected Unicode scalar strings';

export function scalarString(value: string): void {
  for (let i = 0; i < value.length; i++) {
    const unit = value.charCodeAt(i);
    if (unit >= 0xdc00 && unit <= 0xdfff) throw new Error(refusal);
    if (unit < 0xd800 || unit > 0xdbff) continue;
    const low = value.charCodeAt(++i);
    if (!(low >= 0xdc00 && low <= 0xdfff)) throw new Error(refusal);
  }
}

export function scalarValue(value: unknown, seen = new Set<object>()): void {
  if (typeof value === 'string') {
    scalarString(value);
    return;
  }
  if (value === null || typeof value !== 'object' || seen.has(value)) return;
  seen.add(value);
  for (const key of Object.keys(value)) {
    scalarString(key);
    scalarValue((value as Record<string, unknown>)[key], seen);
  }
  seen.delete(value);
}

/** Checks Unicode in original JSON text, including duplicate members a
 * decoder would discard. The caller still validates syntax and envelope. */
export function scalarJSON(text: string): void {
  scalarString(text);
  let start = -1;
  for (let i = 0; i < text.length; i++) {
    if (start >= 0 && text[i] === '\\') {
      i++;
      continue;
    }
    if (text[i] !== '"') continue;
    if (start < 0) start = i;
    else {
      scalarString(JSON.parse(text.slice(start, i + 1)) as string);
      start = -1;
    }
  }
}
