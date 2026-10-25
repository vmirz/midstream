/**
 * Ranked, highlighted search over names: topics, field paths.
 *
 * Every whitespace-separated term must appear in the name. The exact name ranks first, then names
 * that start with a term, then names where a term starts a word, then plain substrings. Pure and
 * import-free, so `node --test` loads it without the bundler.
 */

/** Characters that start a new word: `orders.cdc`, `orders-cdc`, `email_hash`, `items[].sku`. */
const WORD_BOUNDARY = new Set(['.', '-', '_', '/', ':', '[', ']']);

export interface NameMatch {
  /** Lower is better: 0 is an exact match. */
  score: number;
  /** Ranges of the name that matched, sorted and merged, for emphasis. */
  ranges: Array<[number, number]>;
}

/** The terms of a query, lowercased; none for a blank query. */
export function searchTerms(query: string): string[] {
  return query.trim().toLowerCase().split(/\s+/).filter(Boolean);
}

/** How well `lowerName` matches every one of `terms`, or null when one is missing. */
export function matchName(lowerName: string, terms: string[]): NameMatch | null {
  const ranges: Array<[number, number]> = [];
  let score = 0;
  for (const term of terms) {
    const at = bestOccurrence(lowerName, term);
    if (at < 0) return null;
    ranges.push([at, at + term.length]);
    score += at === 0 ? 1 : WORD_BOUNDARY.has(lowerName[at - 1] as string) ? 2 : 3;
  }
  return { score: lowerName === terms.join(' ') ? 0 : score, ranges: mergeRanges(ranges) };
}

/** The first occurrence at the start or after a word boundary, else the first occurrence. */
function bestOccurrence(name: string, term: string): number {
  const first = name.indexOf(term);
  if (first <= 0) return first;
  for (let at = first; at >= 0; at = name.indexOf(term, at + 1)) {
    if (WORD_BOUNDARY.has(name[at - 1] as string)) return at;
  }
  return first;
}

function mergeRanges(ranges: Array<[number, number]>): Array<[number, number]> {
  const sorted = [...ranges].sort((left, right) => left[0] - right[0]);
  const merged: Array<[number, number]> = [];
  for (const [start, end] of sorted) {
    const last = merged[merged.length - 1];
    if (last && start <= last[1]) last[1] = Math.max(last[1], end);
    else merged.push([start, end]);
  }
  return merged;
}

/** Split a name into plain and matched segments for rendering. */
export function segmentName(name: string, ranges: Array<[number, number]>): Array<{ text: string; match: boolean }> {
  const segments: Array<{ text: string; match: boolean }> = [];
  let cursor = 0;
  for (const [start, end] of ranges) {
    if (start > cursor) segments.push({ text: name.slice(cursor, start), match: false });
    segments.push({ text: name.slice(start, end), match: true });
    cursor = end;
  }
  if (cursor < name.length) segments.push({ text: name.slice(cursor), match: false });
  return segments;
}
