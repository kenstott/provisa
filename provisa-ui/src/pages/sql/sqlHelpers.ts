// Copyright (c) 2026 Kenneth Stott
// Canary: 59b4ba25-2299-4520-b805-8af36512b12c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { Metric, RegisteredTable, Relationship } from "../../types/admin";

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

// ── REQ-1322: semantic metric helpers ────────────────────────────────────────

/** Split registered tables into star-schema modeling groups for the schema browser. */
export function groupModelingTables(tables: RegisteredTable[]): {
  facts: RegisteredTable[];
  dimensions: RegisteredTable[];
} {
  return {
    facts: tables.filter((t) => t.modelingRole === "fact"),
    dimensions: tables.filter((t) => t.modelingRole === "dimension"),
  };
}

/**
 * v1 valid-dimension set for a metric: all columns of tables whose name appears
 * as a `table.column` reference in the metric expression, plus columns of tables
 * one relationship hop away from those tables.
 */
export function metricDimensionTables(
  metric: Metric,
  tables: RegisteredTable[],
  existingRels: Relationship[],
): RegisteredTable[] {
  const byName = new Map(tables.map((t) => [t.tableName, t]));
  const referenced = new Set<string>();
  for (const m of metric.expression.matchAll(/\b(\w+)\.(\w+)\b/g)) {
    if (byName.has(m[1])) referenced.add(m[1]);
  }
  const oneHop = new Set<string>(referenced);
  for (const rel of existingRels) {
    if (referenced.has(rel.sourceTableName)) oneHop.add(rel.targetTableName);
    if (referenced.has(rel.targetTableName)) oneHop.add(rel.sourceTableName);
  }
  return [...oneHop].filter((n) => byName.has(n)).map((n) => byName.get(n)!);
}

/**
 * Semantic-only SQL for a metric (REQ-1321: the server compiler is the single
 * generator — the UI never emits joins or aggregation for metrics).
 * Dimensions are `table.column` refs.
 */
export function buildSemanticMetricSql(metricName: string, dims: string[]): string {
  if (dims.length === 0) return `SELECT value FROM metrics.${metricName}`;
  const dimList = dims.join(", ");
  return `SELECT ${dimList}, value\nFROM metrics.${metricName}\nGROUP BY ${dimList}`;
}

/**
 * REQ-1318: recognize a pure semantic metric query (single SELECT over
 * `metrics.<name>`, no other FROM sources). Returns the metric name and the
 * selected dimensions, or null when the SQL is not a pure metric query.
 */
export function parseSemanticMetricQuery(
  sql: string,
): { metric: string; dimensions: string[] } | null {
  const trimmed = sql.trim().replace(/;+$/, "");
  const m = trimmed.match(
    /^SELECT\s+([\s\S]+?)\s+FROM\s+metrics\.(\w+)\s*(?:GROUP\s+BY\s+[\s\S]+)?$/i,
  );
  if (!m) return null;
  const dimensions = m[1]
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s && s.toLowerCase() !== "value");
  return { metric: m[2], dimensions };
}
