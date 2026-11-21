// Copyright (c) 2026 Kenneth Stott
// Canary: 3bae7aa9-d0f4-4f95-8a70-5cafcfe335d4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2143 (PAGE SEARCH USES THE WEB-SEARCH SYNTAX): every page search reads what people type into
// a web search, as the term list (the server's search_items, glossary.py) and the Ontology View's
// graph also read it. Items separated by spaces are all required; the word or between two items
// means either, and binds tighter than a space; an item with a leading - must not be found; a
// phrase in double quotes is one item, spaces and all. A - counts only at the start of an item; a
// quoted or, or a quoted text beginning with -, is searched as text. A leading, trailing or doubled
// or, a lone -, and stray quote marks are dropped. The cases both sides answer alike are in
// tests/fixtures/page_search_cases.json.

/** One item of a search: its text, lower-cased, and whether it must not be found. */
export interface SearchItem {
  text: string;
  excluded: boolean;
}

/** A search as it is read: every group must hold, and a group holds when any of its items does
 * (the items joined by or). No groups is no search. */
export type SearchGroups = SearchItem[][];

const TOKEN = /(-?)"([^"]*)"|(\S+)/g;

export function searchItems(q: string): SearchGroups {
  const groups: SearchGroups = [];
  let either = false;
  for (const m of q.matchAll(TOKEN)) {
    let text: string;
    let excluded: boolean;
    if (m[3] === undefined) {
      text = m[2];
      excluded = m[1] === "-";
    } else {
      const word = m[3].replaceAll('"', "");
      if (word.toLowerCase() === "or") {
        either = groups.length > 0;
        continue;
      }
      excluded = word.startsWith("-");
      text = excluded ? word.slice(1) : word;
    }
    text = text.trim().toLowerCase();
    if (!text) continue;
    const item = { text, excluded };
    if (either) groups[groups.length - 1].push(item);
    else groups.push([item]);
    either = false;
  }
  return groups;
}

/** Whether ``texts`` hold the search ``groups`` (case-insensitive): an item is found when it is in
 * any of the texts. */
export function matchesSearch(texts: (string | null | undefined)[], groups: SearchGroups): boolean {
  const hay = texts.filter((t): t is string => !!t).map((t) => t.toLowerCase());
  const found = (i: SearchItem) => hay.some((h) => h.includes(i.text)) !== i.excluded;
  return groups.every((g) => g.some(found));
}

/** A page's search over a row's ``texts``: an empty search keeps every row. The one rule every
 * page-level search bar follows (FilterInput's pages, the glossary's list and graph). */
export function matchesQuery(texts: (string | null | undefined)[], query: string): boolean {
  return matchesSearch(texts, searchItems(query));
}
