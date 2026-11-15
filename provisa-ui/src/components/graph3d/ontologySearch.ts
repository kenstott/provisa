// Copyright (c) 2026 Kenneth Stott
// Canary: e0a93593-cb81-40f1-bee3-71fead136bbe
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2143 (PAGE SEARCH USES THE WEB-SEARCH SYNTAX): the page search as the Ontology View's query
// states it at its root, so the server keeps the terms the page's own filter keeps
// (lib/searchItems.ts; tests/fixtures/page_search_cases.json).

import type { Where } from "../../api/metaGraph";
import type { SearchGroups, SearchItem } from "../../lib/searchItems";

/** A search item as an ILIKE pattern: anywhere in the text, its own % _ and \\ taken literally. */
const anywhere = (item: string) => `%${item.replace(/[\\%_]/g, (c) => `\\${c}`)}%`;

/** REQ-2143: the terms the page's search matches, searchItems' rule -- every group, each by any
 * of its items, an item in the name or the definition whatever its case. */
export function searchWhere(groups: SearchGroups): Where | undefined {
  const each = groups.map((g): Where => ({ _or: g.flatMap(itemWhere) }));
  return each.length === 0 ? undefined : each.length === 1 ? each[0] : { _and: each };
}

/** An item found, or for an excluded one not found: a term with no definition is not found in
 * it, which SQL's NOT alone would not say of a null. */
function itemWhere(i: SearchItem): Where[] {
  const name = { name: { ilike: anywhere(i.text) } };
  const definition = { definition: { ilike: anywhere(i.text) } };
  if (!i.excluded) return [name, definition];
  return [{ _not: { _or: [name, { _and: [{ definition: { is_null: false } }, definition] }] } }];
}
