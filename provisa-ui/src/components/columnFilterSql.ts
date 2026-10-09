// Copyright (c) 2026 Kenneth Stott
// Canary: 7d1c3b2e-5a64-4f08-9c2d-3e1b8a6f40d7
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1937: the SQL form of a typed column filter, for the server-paged viewer. It means what
// `matchesFilter` (pages/sql/columnFilter.ts) means over held rows: text operators ignore case,
// a date operator is the same absolute bounds the browser resolved in its own time zone, and a
// checklist matches the exact values ticked. Built from STRPOS/SUBSTR/LENGTH rather than LIKE so
// no wildcard in what the reader typed changes the match.

import {
  EMPTY_VALUE,
  dateBounds,
  isNumber,
  isUsable,
  type ActiveFilter,
  type FilterSpec,
} from "../pages/sql/columnFilter";

/** The ONE way an identifier is written: double-quoted, quotes doubled. */
export const quoteIdent = (name: string) => `"${name.replace(/"/g, '""')}"`;

/** The ONE way a text value is written: single-quoted, quotes doubled. Backslash is a plain
    character in standard SQL strings, which is how the governed pipeline reads them (PostgreSQL
    dialect), and the transpilers re-escape it for any target that treats it otherwise. */
export const quoteLiteral = (value: string) => `'${value.replace(/'/g, "''")}'`;

/** A number as typed, written only after it passed `isNumber`. */
export function numberLiteral(value: string): string {
  if (!isNumber(value)) throw new Error("not a number");
  return value.trim();
}

const pad = (n: number) => String(n).padStart(2, "0");
/** A local wall-clock instant as a typed TIMESTAMP literal; built from numbers only. */
export function timestampLiteral(ms: number): string {
  const d = new Date(ms);
  return (
    `TIMESTAMP '${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ` +
    `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}'`
  );
}

/** One column filter as a SQL predicate; null when it is not usable yet (it narrows nothing). */
export function filterToSql(f: ActiveFilter, now: Date = new Date()): string | null {
  const { col, kind, spec } = f;
  if (!isUsable(kind, spec)) return null;
  const c = quoteIdent(col);
  const text = `LOWER(CAST(${c} AS VARCHAR))`;
  const x = (spec.a ?? "").toLowerCase();
  switch (spec.op) {
    case "isEmpty":
      return kind === "text" ? `(${c} IS NULL OR CAST(${c} AS VARCHAR) = '')` : `${c} IS NULL`;
    case "notEmpty":
      return kind === "text"
        ? `(${c} IS NOT NULL AND CAST(${c} AS VARCHAR) <> '')`
        : `${c} IS NOT NULL`;
    case "in":
      return inList(c, spec);
    default:
  }
  if (kind === "number") {
    const sql: Record<string, string> = { eq: "=", ne: "<>", lt: "<", le: "<=", gt: ">", ge: ">=" };
    if (spec.op === "between") {
      const [lo, hi] = Number(spec.a) <= Number(spec.b) ? [spec.a, spec.b] : [spec.b, spec.a];
      return `${c} BETWEEN ${numberLiteral(lo ?? "")} AND ${numberLiteral(hi ?? "")}`;
    }
    return `${c} ${sql[spec.op]} ${numberLiteral(spec.a ?? "")}`;
  }
  if (kind === "date") {
    const b = dateBounds(spec, now);
    if (!b) return null;
    const ts = `CAST(${c} AS TIMESTAMP)`;
    const parts: string[] = [];
    if (b.from !== undefined) parts.push(`${ts} >= ${timestampLiteral(b.from)}`);
    if (b.to !== undefined) parts.push(`${ts} < ${timestampLiteral(b.to)}`);
    return parts.length > 0 ? `(${parts.join(" AND ")})` : null;
  }
  switch (spec.op) {
    case "contains":
      return `STRPOS(${text}, ${quoteLiteral(x)}) > 0`;
    case "notContains":
      return `(${c} IS NULL OR STRPOS(${text}, ${quoteLiteral(x)}) = 0)`;
    case "equals":
      return `${text} = ${quoteLiteral(x)}`;
    case "startsWith":
      return `SUBSTR(${text}, 1, LENGTH(${quoteLiteral(x)})) = ${quoteLiteral(x)}`;
    case "endsWith":
      return `(LENGTH(${text}) >= LENGTH(${quoteLiteral(x)}) AND SUBSTR(${text}, LENGTH(${text}) - LENGTH(${quoteLiteral(x)}) + 1) = ${quoteLiteral(x)})`;
    default:
      return null;
  }
}

function inList(c: string, spec: FilterSpec): string {
  const ci = spec.a === "ci";
  const all = spec.values ?? [];
  const withEmpty = all.includes(EMPTY_VALUE);
  const vals = all.filter((v) => v !== EMPTY_VALUE);
  const parts: string[] = [];
  if (vals.length > 0) {
    parts.push(
      ci
        ? `LOWER(CAST(${c} AS VARCHAR)) IN (${vals.map((v) => quoteLiteral(v.toLowerCase())).join(", ")})`
        : `CAST(${c} AS VARCHAR) IN (${vals.map(quoteLiteral).join(", ")})`,
    );
  }
  if (withEmpty) parts.push(`${c} IS NULL`, `CAST(${c} AS VARCHAR) = ''`);
  return parts.length === 1 ? parts[0] : `(${parts.join(" OR ")})`;
}
