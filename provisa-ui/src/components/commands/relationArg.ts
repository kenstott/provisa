// Copyright (c) 2026 Kenneth Stott
// Canary: 2f9c6e30-4a17-4d82-b5e8-0c3d7f1a9b46
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1159 (amended 2026-11-27): a relation argument given in the argument form, as the one value
// the argument takes. A result_set is given by a table the role reads (read as a query, its declared
// columns only), a written query, or pasted rows (CSV with a header, or a JSON array of objects),
// each held to RELATION_ROW_CAP rows. A table_ref names a table its service reads itself, so it is
// given only by a table, as its SQL name (provisa/executor/function_dispatch.py refuses a query or
// rows for one).
// A result_set that declares its columns is given only by a table that has every one of them, by
// name, with a type that reads as the declared type (fittingTables); a table_ref, or a result_set
// that declares none, takes any table the role reads.

import type { ActionArg } from "../../api/actions";
import { quoteIdent, tableSqlRef, type SqlNamedTable } from "../../naming";
import type { ArgValue } from "./commandQueries";

/** The rows a relation argument given in the form carries at most. */
export const RELATION_ROW_CAP = 100;

export type RelationSource = "table" | "query" | "rows";

/** The ways ``arg`` is given: a table_ref by a table only. */
export const relationSources = (arg: ActionArg): RelationSource[] =>
  arg.argKind === "table_ref" ? ["table"] : ["table", "query", "rows"];

/** A table a relation argument may be given by: its SQL name, and its columns as the catalog gives
 * them (the SQL name a statement reads each by, and its IR type, null where none is recorded). */
export interface ArgTable extends SqlNamedTable {
  columns: readonly { computedSqlAlias: string; dataType: string | null }[];
}

const INTEGER = new Set(["integer", "int", "bigint", "smallint", "int2", "int4", "int8"]);
const DECIMAL = new Set(["double", "float", "real", "numeric", "decimal", "float4", "float8"]);
const BOOLEAN = new Set(["boolean", "bool"]);

/** A type's name without its case or its parameters: ``NUMERIC(10,2)`` is ``numeric``. */
const baseType = (type: string) =>
  type
    .toLowerCase()
    .replace(/\(.*\)$/, "")
    .trim();

/** Whether a value of type ``have`` reads as the declared type ``want``: an integer reads as a
 * decimal too; any other type reads only as itself. */
export function readsAs(have: string, want: string): boolean {
  const h = baseType(have);
  const w = baseType(want);
  if (INTEGER.has(w)) return INTEGER.has(h);
  if (DECIMAL.has(w)) return INTEGER.has(h) || DECIMAL.has(h);
  if (BOOLEAN.has(w)) return BOOLEAN.has(h);
  return h === w;
}

/** The tables ``arg`` may be given by: for a result_set that declares its columns, those that have
 * every declared column by name (as the catalog spells it) with a type that reads as the declared
 * type; for any other relation argument, every table. */
export function fittingTables<T extends ArgTable>(
  arg: ActionArg,
  tables: readonly T[],
): readonly T[] {
  const declared = arg.argKind === "result_set" ? arg.columns : undefined;
  if (!declared || declared.length === 0) return tables;
  return tables.filter((tb) =>
    declared.every((want) =>
      tb.columns.some(
        (col) =>
          col.computedSqlAlias === want.name &&
          col.dataType !== null &&
          readsAs(col.dataType, want.type),
      ),
    ),
  );
}

/** A table the role reads, given for ``arg``. */
export function tableValue(arg: ActionArg, table: SqlNamedTable): ArgValue {
  if (arg.argKind === "table_ref")
    return { kind: "relation", text: tableSqlRef(table), rows: null };
  const columns = arg.columns?.map((c) => quoteIdent(c.name)).join(", ") || "*";
  return {
    kind: "relation",
    text: `select ${columns} from ${tableSqlRef(table)} limit ${RELATION_ROW_CAP}`,
    rows: null,
  };
}

/** A written query, held to the row cap. */
export function queryValue(text: string): ArgValue {
  const body = text.trim().replace(/;+\s*$/, "");
  return {
    kind: "relation",
    text: `select * from (${body}) as given limit ${RELATION_ROW_CAP}`,
    rows: null,
  };
}

/** Why pasted rows are refused. */
export type RowsRefusal = "notRows" | "overCap";

/** A cell that does not read as its declared column's type. */
class CellRefused extends Error {}

/** A CSV cell as its declared column's type reads it; a column not declared keeps its text. */
function typedCell(text: string, type: string | undefined): unknown {
  const t = baseType(type ?? "");
  if (INTEGER.has(t) || DECIMAL.has(t)) {
    const n = Number(text);
    if (text === "" || Number.isNaN(n)) throw new CellRefused(text);
    return n;
  }
  if (BOOLEAN.has(t)) {
    if (text !== "true" && text !== "false") throw new CellRefused(text);
    return text === "true";
  }
  return text;
}

/** One CSV line's cells: comma-separated, a quoted cell holding commas and doubled quotes. */
function csvCells(line: string): string[] {
  const cells: string[] = [];
  let cell = "";
  let quoted = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (quoted) {
      if (ch === '"' && line[i + 1] === '"') {
        cell += '"';
        i += 1;
      } else if (ch === '"') quoted = false;
      else cell += ch;
    } else if (ch === '"') quoted = true;
    else if (ch === ",") {
      cells.push(cell);
      cell = "";
    } else cell += ch;
  }
  cells.push(cell);
  return cells.map((c) => c.trim());
}

function csvRows(text: string, arg: ActionArg): Record<string, unknown>[] | null {
  const lines = text.split(/\r?\n/).filter((l) => l.trim() !== "");
  if (lines.length === 0) return null;
  const header = csvCells(lines[0]);
  if (header.some((h) => h === "")) return null;
  const types = new Map((arg.columns ?? []).map((c) => [c.name, c.type]));
  const rows = lines.slice(1).map((line) => csvCells(line));
  if (rows.some((cells) => cells.length !== header.length)) return null;
  try {
    return rows.map((cells) =>
      Object.fromEntries(header.map((h, i) => [h, typedCell(cells[i], types.get(h))])),
    );
  } catch (e) {
    if (e instanceof CellRefused) return null;
    throw e;
  }
}

function jsonRows(text: string): Record<string, unknown>[] | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return null;
  }
  const isRow = (r: unknown) => typeof r === "object" && r !== null && !Array.isArray(r);
  return Array.isArray(parsed) && parsed.every(isRow)
    ? (parsed as Record<string, unknown>[])
    : null;
}

/** Pasted rows for ``arg``: a JSON array of objects, or CSV whose first line names the columns. */
export function rowsValue(text: string, arg: ActionArg): ArgValue | RowsRefusal {
  const trimmed = text.trim();
  const rows = trimmed.startsWith("[") ? jsonRows(trimmed) : csvRows(trimmed, arg);
  if (rows === null) return "notRows";
  if (rows.length > RELATION_ROW_CAP) return "overCap";
  return { kind: "relation", text: JSON.stringify(rows), rows };
}
