// Copyright (c) 2026 Kenneth Stott
// Canary: 3bae7aa9-d0f4-4f95-8a70-5cafcfe335d4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The glossary's one search, as the term list (the server's search_items, glossary.py) and the
// Ontology View's graph both read it: a phrase in double quotes is one item, spaces and all; every
// other word is one item; stray quote marks are dropped. Something matches when any item is in it.

const ITEM = /"([^"]*)"|(\S+)/g;

export function searchItems(q: string): string[] {
  const out: string[] = [];
  for (const m of q.matchAll(ITEM)) {
    const item = (m[1] !== undefined ? m[1] : m[2].replaceAll('"', "")).trim().toLowerCase();
    if (item) out.push(item);
  }
  return out;
}

/** Whether any of ``items`` is in any of ``texts`` (case-insensitive). */
export function matchesSearch(texts: (string | undefined)[], items: string[]): boolean {
  const hay = texts.filter((t): t is string => !!t).map((t) => t.toLowerCase());
  return items.some((i) => hay.some((h) => h.includes(i)));
}

/** A page's search over a row's ``texts``: an empty search keeps every row; otherwise a row is kept
 * when any of the search's items is in any of its texts. The one rule every page-level search bar
 * follows (FilterInput's pages, the glossary's list and graph). */
export function matchesQuery(texts: (string | null | undefined)[], query: string): boolean {
  const items = searchItems(query);
  if (items.length === 0) return true;
  return matchesSearch(
    texts.map((t) => t ?? undefined),
    items,
  );
}
