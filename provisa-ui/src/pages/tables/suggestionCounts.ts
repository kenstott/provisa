// Copyright (c) 2026 Kenneth Stott
// Canary: 3b9e1f47-6c2d-4a85-9e0b-7d14c5a8f203
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2180 (amended 2026-11-24): the per-table suggestion counts the Tables page badges. Kept apart
// from TableSuggestions.tsx so that module exports components only (react-refresh).

import { useEffect, useState } from "react";
import { suggestionCounts, type SuggestionCount } from "../../api/productSuggestions";

/** One table of the Tables page, as a suggestion names it. */
export interface SuggestedTable {
  domainId: string;
  tableName: string;
}

export const keyOf = (domainId: string, tableName: string) => `${domainId}.${tableName}`;

export function errorText(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

/** domain.table -> how many suggestions name it, for the tables whose suggestions go to the
 *  caller; a failed read is handed to ``onError``. */
export function useSuggestionCounts(onError: (problem: string) => void): Map<string, number> {
  const [counts, setCounts] = useState<Map<string, number>>(() => new Map());
  useEffect(() => {
    let live = true;
    suggestionCounts().then(
      (rows: SuggestionCount[]) => {
        if (live)
          setCounts(new Map(rows.map((r) => [keyOf(r.domain_id, r.table_name), r.suggestions])));
      },
      (e: unknown) => {
        if (live) onError(errorText(e));
      },
    );
    return () => {
      live = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- read once per page
  }, []);
  return counts;
}

/** How many suggestions name ``table``; undefined when none does (the server lists only the
 *  tables some suggestion names). */
export function countOf(counts: Map<string, number>, table: SuggestedTable): number | undefined {
  return counts.get(keyOf(table.domainId, table.tableName));
}
