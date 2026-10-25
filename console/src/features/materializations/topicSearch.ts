// Type-only, so `node --test` can load this module without the bundler's path aliases.
import type { TopicSummary } from '@shared/types';

import { matchName, searchTerms } from '../../lib/nameSearch.ts';

export { segmentName } from '../../lib/nameSearch.ts';

/** A catalog prepared for searching: names are lowercased once, not on every keystroke. */
export interface TopicIndex {
  topics: TopicSummary[];
  lower: string[];
  /** Topics with a category, which the picker hides unless asked. */
  hiddenTotal: number;
}

export interface TopicMatch {
  topic: TopicSummary;
  /** Ranges of `topic.name` that matched, sorted and merged, for emphasis. */
  ranges: Array<[number, number]>;
}

export interface TopicSearchResult {
  matches: TopicMatch[];
  /** Hidden topics the query also matches, so the picker can offer to show them. */
  hiddenMatches: number;
}

export function indexTopics(topics: TopicSummary[]): TopicIndex {
  return {
    topics,
    lower: topics.map((topic) => topic.name.toLowerCase()),
    hiddenTotal: topics.filter((topic) => topic.category).length,
  };
}

/**
 * Rank topics against a whitespace-separated query in which every term must appear in the name.
 *
 * The exact name comes first, then names that start with a term, then names where a term starts a
 * word, then plain substrings; ties go to the shorter name, then the alphabetically first. With no
 * query every visible topic is returned in catalog order, which is alphabetical.
 */
export function searchTopics(
  index: TopicIndex,
  query: string,
  { includeHidden }: { includeHidden: boolean },
): TopicSearchResult {
  const terms = searchTerms(query);
  const scored: Array<TopicMatch & { score: number }> = [];
  let hiddenMatches = 0;

  for (let position = 0; position < index.topics.length; position += 1) {
    const topic = index.topics[position]!;
    const match = matchName(index.lower[position]!, terms);
    if (!match) continue;
    if (topic.category && !includeHidden) {
      if (terms.length > 0) hiddenMatches += 1;
      continue;
    }
    scored.push({ topic, ranges: match.ranges, score: match.score });
  }

  if (terms.length > 0) {
    scored.sort(
      (left, right) =>
        left.score - right.score ||
        left.topic.name.length - right.topic.name.length ||
        left.topic.name.localeCompare(right.topic.name),
    );
  }
  return { matches: scored.map(({ topic, ranges }) => ({ topic, ranges })), hiddenMatches };
}
