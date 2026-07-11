// Copyright (c) 2026 Kenneth Stott
// Canary: e203b774-09b9-4f3a-a172-efc74bdcf20b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/** Add `table_col` aliases to any SELECT column whose bare name conflicts with another. */
export function autoAliasConflicts(sql: string): string {
  // Isolate SELECT item list (between SELECT and the first FROM not inside parens)
  const selectRe = /\bSELECT\s+([\s\S]+?)\s+FROM\b/i;
  const m = sql.match(selectRe);
  if (!m) return sql;
  const colList = m[1];

  // Split items by top-level commas
  const items: string[] = [];
  let depth = 0,
    cur = "";
  for (const ch of colList) {
    if (ch === "(") depth++;
    else if (ch === ")") depth--;
    else if (ch === "," && depth === 0) {
      items.push(cur.trim());
      cur = "";
      continue;
    }
    cur += ch;
  }
  if (cur.trim()) items.push(cur.trim());

  // Extract bare column name from patterns like: tbl."col", tbl.col, "tbl"."col", "tbl".col
  // Ignore items that already have an alias (contain AS or a second word after the ref).
  const colRef = /^"?(\w+)"?\."?(\w+)"?\s*$/i;
  const parsed = items.map((item) => {
    const alreadyAliased = /\bas\s+\w+/i.test(item) || /^"?\w+"?\."?\w+"?\s+\w+\s*$/i.test(item);
    const match = item.trim().match(colRef);
    if (!match || alreadyAliased)
      return { item, colLower: null as string | null, tableAlias: null as string | null };
    return { item, colLower: match[2].toLowerCase(), tableAlias: match[1] };
  });

  // Count each bare column name
  const freq = new Map<string, number>();
  parsed.forEach(({ colLower }) => {
    if (colLower) freq.set(colLower, (freq.get(colLower) ?? 0) + 1);
  });

  // Rebuild, appending alias where needed
  const newItems = parsed.map(({ item, colLower, tableAlias }) => {
    if (!colLower || !tableAlias || (freq.get(colLower) ?? 0) <= 1) return item;
    const origCol = item.trim().match(/"?(\w+)"?\s*$/i)?.[1] ?? colLower;
    return `${item} ${tableAlias}_${origCol}`;
  });

  return sql.replace(selectRe, `SELECT ${newItems.join(", ")} FROM`);
}

export function normalizeDomain(id: string): string {
  return id.replace(/[^a-zA-Z0-9]/g, "_").replace(/^_+|_+$/g, "");
}
