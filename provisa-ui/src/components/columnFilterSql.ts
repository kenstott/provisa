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
//
// Nothing the reader typed is written into the statement. Every value is handed to `bind`, which
// keeps it and answers the placeholder ($1, $2, …) that stands for it; the server binds the
// values when the statement runs (/data/sql `params`). Identifiers go through the UI's one
// quoter (naming.quoteIdent).

import { quoteIdent } from "../naming";
import {
  EMPTY_VALUE,
  dateBounds,
  isNumber,
  isUsable,
  type ActiveFilter,
  type FilterSpec,
} from "../pages/sql/columnFilter";

/** Keeps a value and answers the placeholder that stands for it in the statement. */
export type Bind = (value: string | number) => string;

/** A statement and the values of its placeholders, in order. */
export interface BoundStatement {
  sql: string;
  params: (string | number)[];
}

/** A `Bind` that numbers placeholders $1, $2, … over the list it fills. */
export function binder(): { bind: Bind; params: (string | number)[] } {
  const params: (string | number)[] = [];
  return {
    params,
    bind: (value) => {
      params.push(value);
      return `$${params.length}`;
    },
  };
}

const pad = (n: number) => String(n).padStart(2, "0");
/** A local wall-clock instant as the text of a timestamp; built from numbers only. */
export function timestampText(ms: number): string {
  const d = new Date(ms);
  return (
    `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ` +
    `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
  );
}

/** A number as typed, as a number; only after it passed `isNumber`. */
export function numberValue(value: string | undefined): number {
  if (!isNumber(value)) throw new Error("not a number");
  return Number(value.trim());
}

/** One column filter as a SQL predicate over bound values; null when it is not usable yet (it
    narrows nothing, and binds nothing). */
export function filterToSql(f: ActiveFilter, bind: Bind, now: Date = new Date()): string | null {
  const { col, kind, spec } = f;
  if (!isUsable(kind, spec)) return null;
  const c = quoteIdent(col);
  const text = `LOWER(CAST(${c} AS VARCHAR))`;
  // A text operand, bound once and named wherever the predicate needs it.
  const operand = () => `CAST(${bind((spec.a ?? "").toLowerCase())} AS VARCHAR)`;
  switch (spec.op) {
    case "isEmpty":
      return kind === "text" ? `(${c} IS NULL OR CAST(${c} AS VARCHAR) = '')` : `${c} IS NULL`;
    case "notEmpty":
      return kind === "text"
        ? `(${c} IS NOT NULL AND CAST(${c} AS VARCHAR) <> '')`
        : `${c} IS NOT NULL`;
    case "in":
      return inList(c, spec, bind);
    default:
  }
  if (kind === "number") {
    const sql: Record<string, string> = { eq: "=", ne: "<>", lt: "<", le: "<=", gt: ">", ge: ">=" };
    if (spec.op === "between") {
      const [lo, hi] = [numberValue(spec.a), numberValue(spec.b)].sort((p, q) => p - q);
      return `${c} BETWEEN ${bind(lo)} AND ${bind(hi)}`;
    }
    return `${c} ${sql[spec.op]} ${bind(numberValue(spec.a))}`;
  }
  if (kind === "date") {
    const b = dateBounds(spec, now);
    if (!b) return null;
    const ts = `CAST(${c} AS TIMESTAMP)`;
    const at = (ms: number) => `CAST(${bind(timestampText(ms))} AS TIMESTAMP)`;
    const parts: string[] = [];
    if (b.from !== undefined) parts.push(`${ts} >= ${at(b.from)}`);
    if (b.to !== undefined) parts.push(`${ts} < ${at(b.to)}`);
    return parts.length > 0 ? `(${parts.join(" AND ")})` : null;
  }
  switch (spec.op) {
    case "contains":
      return `STRPOS(${text}, ${operand()}) > 0`;
    case "notContains":
      return `(${c} IS NULL OR STRPOS(${text}, ${operand()}) = 0)`;
    case "equals":
      return `${text} = ${operand()}`;
    case "startsWith": {
      const x = operand();
      return `SUBSTR(${text}, 1, LENGTH(${x})) = ${x}`;
    }
    case "endsWith": {
      const x = operand();
      return `(LENGTH(${text}) >= LENGTH(${x}) AND SUBSTR(${text}, LENGTH(${text}) - LENGTH(${x}) + 1) = ${x})`;
    }
    default:
      return null;
  }
}

function inList(c: string, spec: FilterSpec, bind: Bind): string {
  const ci = spec.a === "ci";
  const all = spec.values ?? [];
  const withEmpty = all.includes(EMPTY_VALUE);
  const vals = all.filter((v) => v !== EMPTY_VALUE);
  const parts: string[] = [];
  if (vals.length > 0) {
    const listed = vals.map((v) => `CAST(${bind(ci ? v.toLowerCase() : v)} AS VARCHAR)`).join(", ");
    parts.push(
      ci ? `LOWER(CAST(${c} AS VARCHAR)) IN (${listed})` : `CAST(${c} AS VARCHAR) IN (${listed})`,
    );
  }
  if (withEmpty) parts.push(`${c} IS NULL`, `CAST(${c} AS VARCHAR) = ''`);
  return parts.length === 1 ? parts[0] : `(${parts.join(" OR ")})`;
}
