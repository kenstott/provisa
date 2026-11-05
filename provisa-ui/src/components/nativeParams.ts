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

import type { ListedTable, ListedColumn } from "../types/admin";
import { quoteIdent, tableSqlRef } from "../naming";
import type { ActiveFilter } from "../pages/sql/columnFilter";
import {
  binder,
  filterToSql,
  timestampText,
  type Bind,
  type BoundStatement,
} from "./columnFilterSql";
import { dateBounds, isUsable } from "../pages/sql/columnFilter";

export const PREVIEW_ROW_LIMIT = 1000;

/** Governed row-limited SELECT * for the Profile sample, params as WHERE predicates. */
export function previewSql(
  table: ListedTable,
  params: Record<string, string>,
  limit: number = PREVIEW_ROW_LIMIT,
): BoundStatement {
  const { bind, params: bound } = binder();
  return {
    sql: `SELECT * FROM ${tableSqlRef(table)}${buildParamWhere(table, params, bind)} LIMIT ${limit}`,
    params: bound,
  };
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
  table: ListedTable,
  params: Record<string, string>,
  filters: ActiveFilter[],
  sorts: { col: string; dir: "asc" | "desc" }[],
  groupBy: string[],
  page: number,
  pageSize: number,
): BoundStatement {
  // Every value -- a native parameter's, a filter's -- is bound; none is written into the text.
  const { bind, params: bound } = binder();
  const predicates: string[] = [];
  const paramWhere = buildParamWhere(table, params, bind);
  if (paramWhere) predicates.push(paramWhere.replace(/^ WHERE /, ""));
  // REQ-1937: each typed column filter as a predicate that means what the in-browser test means.
  for (const f of filters) {
    const predicate = filterToSql(f, bind);
    if (predicate) predicates.push(predicate);
  }
  const where = predicates.length > 0 ? ` WHERE ${predicates.join(" AND ")}` : "";

  const orderCols: string[] = [];
  // Groups come first, each in the direction its column is sorted (ascending when it is not).
  for (const col of groupBy) {
    const dir = sorts.find((s) => s.col === col)?.dir === "desc" ? "DESC" : "ASC";
    orderCols.push(`${quoteIdent(col)} ${dir}`);
  }
  for (const s of sorts)
    if (!groupBy.includes(s.col))
      orderCols.push(`${quoteIdent(s.col)} ${s.dir === "desc" ? "DESC" : "ASC"}`);
  if (orderCols.length > 0) {
    for (const c of table.columns) {
      if (c.isPrimaryKey) {
        const name = c.alias || c.columnName;
        if (!groupBy.includes(name) && !sorts.some((s) => s.col === name))
          orderCols.push(`${quoteIdent(name)} ASC`);
      }
    }
  }
  const orderBy = orderCols.length > 0 ? ` ORDER BY ${orderCols.join(", ")}` : "";

  return {
    sql: `SELECT * FROM ${tableSqlRef(table)}${where}${orderBy} LIMIT ${pageSize + 1} OFFSET ${page * pageSize}`,
    params: bound,
  };
}

/** One column filter as the server takes it (REQ-1921): the filter as the viewer holds it, with a
 * date filter's bounds resolved here — "today" is this browser's today. */
export interface ViewerFilterRequest {
  col: string;
  kind: ActiveFilter["kind"];
  op: string;
  a?: string;
  b?: string;
  values?: string[];
  from?: string;
  to?: string;
}

/** What the viewer chose, for a table whose statement the SERVER writes: a DRAFT is read by no
 * statement a caller sends, so its viewer sends these choices — the same inputs
 * `pagedViewerSql` writes its statement from — and the server writes the statement. */
export interface ViewerRequest {
  params: { column: string; value: string | number }[];
  filters: ViewerFilterRequest[];
  sorts: { col: string; dir: "asc" | "desc" }[];
  groupBy: string[];
  page: number;
  pageSize: number;
}

export function viewerRequest(
  table: ListedTable,
  params: Record<string, string>,
  filters: ActiveFilter[],
  sorts: { col: string; dir: "asc" | "desc" }[],
  groupBy: string[],
  page: number,
  pageSize: number,
  now: Date = new Date(),
): ViewerRequest {
  const sent: ViewerFilterRequest[] = [];
  for (const f of filters) {
    // A filter that is not usable yet narrows nothing, here as in the statement the viewer writes.
    if (!isUsable(f.kind, f.spec)) continue;
    const { op, a, b, values } = f.spec;
    const filter: ViewerFilterRequest = { col: f.col, kind: f.kind, op };
    if (a !== undefined) filter.a = a;
    if (b !== undefined) filter.b = b;
    if (values !== undefined) filter.values = values;
    if (f.kind === "date" && op !== "isEmpty" && op !== "notEmpty" && op !== "in") {
      const bounds = dateBounds(f.spec, now);
      if (!bounds) continue;
      if (bounds.from !== undefined) filter.from = timestampText(bounds.from);
      if (bounds.to !== undefined) filter.to = timestampText(bounds.to);
    }
    sent.push(filter);
  }
  return {
    params: [...requiredParamColumns(table), ...optionalParamColumns(table)]
      .filter((c) => (params[c.columnName] ?? "").trim() !== "")
      .map((c) => ({
        column: c.alias || c.columnName,
        value: paramValue(params[c.columnName], c.dataType),
      })),
    filters: sent,
    sorts,
    groupBy,
    page,
    pageSize,
  };
}

/** Whether a column is a parameter its source cannot be read without. The registration records
 * it (`nativeFilterRequired`) from what the source states: an OpenAPI path parameter or one
 * marked required, a GraphQL argument that is non-null with no default. A column registered
 * before that was recorded says nothing; then only a path parameter is known to be required. */
export function isRequiredParam(c: {
  nativeFilterType?: string | null;
  nativeFilterRequired?: boolean | null;
  requiredBySteward?: boolean;
}): boolean {
  if (!c.nativeFilterType) return false;
  // REQ-1986: a steward may require a parameter its source does not.
  if (c.requiredBySteward) return true;
  if (c.nativeFilterRequired === true || c.nativeFilterRequired === false) {
    return c.nativeFilterRequired;
  }
  return c.nativeFilterType === "path_param";
}

export function requiredParamColumns(table: ListedTable): ListedColumn[] {
  return table.columns.filter((c) => isRequiredParam(c));
}

/** The parameters a read may be given and does not need. */
export function optionalParamColumns(table: ListedTable): ListedColumn[] {
  return table.columns.filter((c) => !!c.nativeFilterType && !isRequiredParam(c));
}

const NUMERIC_TYPES = /int|numeric|decimal|double|float|real|bigint|smallint/i;

/** A native parameter's value as it is bound: a number for a numeric column, else its text. */
function paramValue(value: string, dataType: string | null): string | number {
  if (dataType && NUMERIC_TYPES.test(dataType) && value.trim() !== "" && !isNaN(Number(value)))
    return Number(value.trim());
  return value;
}

/** WHERE clause for the provided param values, each bound; empty string when none set. */
export function buildParamWhere(
  table: ListedTable,
  values: Record<string, string>,
  bind: Bind,
): string {
  const predicates = [...requiredParamColumns(table), ...optionalParamColumns(table)]
    .filter((c) => (values[c.columnName] ?? "").trim() !== "")
    .map(
      (c) =>
        `${quoteIdent(c.alias || c.columnName)} = ${bind(paramValue(values[c.columnName], c.dataType))}`,
    );
  return predicates.length > 0 ? ` WHERE ${predicates.join(" AND ")}` : "";
}

/** A parameter's name as a caller gives it (REST, JSON:API and gRPC `params`): its column
 * without the `_nf_` prefix. */
export function parameterName(c: { columnName: string }): string {
  return c.columnName.startsWith("_nf_") ? c.columnName.slice(4) : c.columnName;
}

/** The table's parameter columns: required ones first. */
export function paramColumns(table: ListedTable): ListedColumn[] {
  return [...requiredParamColumns(table), ...optionalParamColumns(table)];
}

/** The `params` object a REST or JSON:API read sends: each non-blank value by parameter name,
 * a number for a numeric parameter. */
export function requestParams(
  table: ListedTable,
  values: Record<string, string>,
): Record<string, string | number> {
  const out: Record<string, string | number> = {};
  for (const c of paramColumns(table)) {
    const value = values[c.columnName] ?? "";
    if (value.trim() !== "") out[parameterName(c)] = paramValue(value, c.dataType);
  }
  return out;
}

/** True once every required param has a non-blank value. */
export function requiredParamsSatisfied(
  table: ListedTable,
  values: Record<string, string>,
): boolean {
  return requiredParamColumns(table).every((c) => (values[c.columnName] ?? "").trim() !== "");
}
