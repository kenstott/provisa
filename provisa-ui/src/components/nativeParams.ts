// Copyright (c) 2026 Kenneth Stott
// Canary: facd947d-a22a-404e-99df-5f13be4a03ec
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Native API filter parameters (REQ-452 family): API-backed tables expose
// path/query params as native-filter columns. A path_param is REQUIRED — a
// governed SELECT * cannot run without a WHERE predicate on it, so Preview
// and Profile must collect these values first.

import type { RegisteredTable, TableColumn } from "../types/admin";
import { normalizeDomain } from "../pages/sql/sqlHelpers";

export const PREVIEW_ROW_LIMIT = 1000;

function tableRef(table: RegisteredTable): string {
  return `"${normalizeDomain(table.domainId)}"."${table.alias || table.tableName}"`;
}

/** Governed row-limited SELECT * for the Profile sample, params as WHERE predicates. */
export function previewSql(
  table: RegisteredTable,
  params: Record<string, string>,
  limit: number = PREVIEW_ROW_LIMIT,
): string {
  return `SELECT * FROM ${tableRef(table)}${buildParamWhere(table, params)} LIMIT ${limit}`;
}

/** One PAGE of a governed SELECT * — the viewer never loads the whole dataset.

    Filters and sorts are pushed into the SQL so they act on the full relation,
    not the fetched slice. Group columns lead the ORDER BY so group blocks stay
    contiguous across page boundaries; primary-key columns are appended as a
    tiebreaker so OFFSET paging is stable under a user-chosen order.

    With no sort and no grouping chosen there is no ORDER BY at all. A
    primary-key tiebreaker on its own orders a relation the user never asked to
    order, and that sort is a full scan: on the ops traces table (2.8M rows)
    `ORDER BY trace_id LIMIT 51` took 5m20s against the same page unordered at
    3.6s, so every telemetry report opened on a spinner that never resolved.

    Fetch pageSize+1 rows: the extra row only signals that a next page exists. */
export function pagedViewerSql(
  table: RegisteredTable,
  params: Record<string, string>,
  filters: Record<string, string>,
  sorts: { col: string; dir: "asc" | "desc" }[],
  groupBy: string[],
  page: number,
  pageSize: number,
): string {
  const predicates: string[] = [];
  const paramWhere = buildParamWhere(table, params);
  if (paramWhere) predicates.push(paramWhere.replace(/^ WHERE /, ""));
  for (const [col, value] of Object.entries(filters)) {
    if (!value.trim()) continue;
    const escaped = value.replace(/'/g, "''");
    predicates.push(`LOWER(CAST("${col}" AS VARCHAR)) LIKE LOWER('%${escaped}%')`);
  }
  const where = predicates.length > 0 ? ` WHERE ${predicates.join(" AND ")}` : "";

  const orderCols: string[] = [];
  for (const col of groupBy) orderCols.push(`"${col}" ASC`);
  for (const s of sorts) if (!groupBy.includes(s.col)) orderCols.push(`"${s.col}" ${s.dir.toUpperCase()}`);
  if (orderCols.length > 0) {
    for (const c of table.columns) {
      if (c.isPrimaryKey) {
        const name = c.alias || c.columnName;
        if (!groupBy.includes(name) && !sorts.some((s) => s.col === name))
          orderCols.push(`"${name}" ASC`);
      }
    }
  }
  const orderBy = orderCols.length > 0 ? ` ORDER BY ${orderCols.join(", ")}` : "";

  return `SELECT * FROM ${tableRef(table)}${where}${orderBy} LIMIT ${pageSize + 1} OFFSET ${page * pageSize}`;
}

export function requiredParamColumns(table: RegisteredTable): TableColumn[] {
  return table.columns.filter((c) => c.nativeFilterType === "path_param");
}

export function optionalParamColumns(table: RegisteredTable): TableColumn[] {
  return table.columns.filter((c) => c.nativeFilterType === "query_param");
}

const NUMERIC_TYPES = /int|numeric|decimal|double|float|real|bigint|smallint/i;

function sqlLiteral(value: string, dataType: string | null): string {
  if (dataType && NUMERIC_TYPES.test(dataType) && value.trim() !== "" && !isNaN(Number(value)))
    return value.trim();
  return `'${value.replace(/'/g, "''")}'`;
}

/** WHERE clause for the provided param values; empty string when none set. */
export function buildParamWhere(
  table: RegisteredTable,
  values: Record<string, string>,
): string {
  const predicates = [...requiredParamColumns(table), ...optionalParamColumns(table)]
    .filter((c) => (values[c.columnName] ?? "").trim() !== "")
    .map(
      (c) =>
        `"${c.alias || c.columnName}" = ${sqlLiteral(values[c.columnName], c.dataType)}`,
    );
  return predicates.length > 0 ? ` WHERE ${predicates.join(" AND ")}` : "";
}

/** True once every required param has a non-blank value. */
export function requiredParamsSatisfied(
  table: RegisteredTable,
  values: Record<string, string>,
): boolean {
  return requiredParamColumns(table).every(
    (c) => (values[c.columnName] ?? "").trim() !== "",
  );
}
