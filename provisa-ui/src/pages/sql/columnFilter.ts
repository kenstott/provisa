// Copyright (c) 2026 Kenneth Stott
// Canary: 69c42d56-b2b3-443e-830b-6b9057eab9f9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1937: Excel-style typed column filters. One model, two evaluators that mean the same thing:
// `matchesFilter` here (client-held rows) and `filterToSql` in components/columnFilterSql.ts
// (server-paged viewer). Plain text keeps meaning "contains", so a filter typed before this
// existed means what it always did.

export type ColumnKind = "text" | "number" | "date" | "boolean";

export type FilterOp =
  | "contains"
  | "notContains"
  | "equals"
  | "startsWith"
  | "endsWith"
  | "eq"
  | "ne"
  | "lt"
  | "le"
  | "gt"
  | "ge"
  | "between"
  | "before"
  | "after"
  | "lastDays"
  | "today"
  | "thisWeek"
  | "thisMonth"
  | "lastMonth"
  | "thisYear"
  | "isEmpty"
  | "notEmpty"
  | "in";

/** One column's filter. `a`/`b` are operands as typed (a number, a date or text); `values` is the
    ticked checklist for `in`. `EMPTY_VALUE` inside `values` stands for a null/blank cell. */
export interface FilterSpec {
  op: FilterOp;
  a?: string;
  b?: string;
  values?: string[];
}

export interface ActiveFilter {
  col: string;
  kind: ColumnKind;
  spec: FilterSpec;
}

/** The checklist's stand-in for a null or blank cell. */
export const EMPTY_VALUE = "\u0000empty";

export const OPS_BY_KIND: Record<ColumnKind, FilterOp[]> = {
  text: [
    "contains",
    "equals",
    "startsWith",
    "endsWith",
    "notContains",
    "isEmpty",
    "notEmpty",
    "in",
  ],
  number: ["eq", "ne", "lt", "le", "gt", "ge", "between", "isEmpty", "notEmpty", "in"],
  date: [
    "before",
    "after",
    "between",
    "lastDays",
    "today",
    "thisWeek",
    "thisMonth",
    "lastMonth",
    "thisYear",
    "isEmpty",
    "notEmpty",
    "in",
  ],
  boolean: ["in", "isEmpty", "notEmpty"],
};

/** Operators that take no operand. */
export const NO_OPERAND: ReadonlySet<FilterOp> = new Set([
  "today",
  "thisWeek",
  "thisMonth",
  "lastMonth",
  "thisYear",
  "isEmpty",
  "notEmpty",
]);
export const TWO_OPERANDS: ReadonlySet<FilterOp> = new Set(["between"]);

/** THE one table from an engine's type name to a filter family. Names are the base type, lower
    case, without a length/precision or a "with/without time zone" suffix; the spellings are the
    ones DuckDB, PostgreSQL and Trino report, plus the normalized names the pipeline derives from
    Arrow (BIGINT, DOUBLE, DECIMAL, TIMESTAMP, DATE, TIME, INTERVAL, BLOB, VARCHAR, BOOLEAN). A
    type not in the table is text; so is a column that reports no type. A time of day is text: a
    date operator has nothing to compare it with. */
export const TYPE_FAMILIES: Readonly<Record<string, ColumnKind>> = {
  boolean: "boolean",
  bool: "boolean",
  tinyint: "number",
  smallint: "number",
  int: "number",
  integer: "number",
  bigint: "number",
  hugeint: "number",
  utinyint: "number",
  usmallint: "number",
  uinteger: "number",
  ubigint: "number",
  uhugeint: "number",
  int1: "number",
  int2: "number",
  int4: "number",
  int8: "number",
  smallserial: "number",
  serial: "number",
  bigserial: "number",
  real: "number",
  float: "number",
  float4: "number",
  float8: "number",
  double: "number",
  "double precision": "number",
  decimal: "number",
  numeric: "number",
  date: "date",
  datetime: "date",
  timestamp: "date",
  timestamptz: "date",
  timestamp_s: "date",
  timestamp_ms: "date",
  timestamp_ns: "date",
};

const TYPE_SUFFIX = /\s+(with|without)\s+time\s+zone$/;

/** A column's filter family from its declared engine type. Null or unmapped is text. */
export function kindFromType(dataType: string | null | undefined): ColumnKind {
  if (!dataType) return "text";
  const base = dataType
    .toLowerCase()
    .replace(/\(.*?\)/g, "")
    .trim();
  // "timestamp with time zone" is the entry for "timestamp"; "time with time zone" becomes "time",
  // which has no entry, so a time of day stays text.
  const bare = base.replace(TYPE_SUFFIX, "");
  return TYPE_FAMILIES[bare] ?? "text";
}

// A date or a timestamp as results carry one: ISO-8601, date first.
const ISO_MOMENT = /^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}(:?\d{2})?)?)?$/;

/** A column's filter family read off its values, for a result that reports no column types:
    boolean when every value held is a boolean, number when every one is a number, date when
    every one is an ISO date or timestamp, and text otherwise — including a column with no
    value to read, and one that mixes kinds. Blank cells say nothing either way. A number
    written as text ("42") is text: the result said it was text by sending it that way. */
export function kindFromValues(values: Iterable<unknown>): ColumnKind {
  let seen = false;
  let bool = true;
  let num = true;
  let date = true;
  for (const v of values) {
    if (v == null || v === "") continue;
    seen = true;
    if (typeof v !== "boolean") bool = false;
    if (typeof v !== "number" && typeof v !== "bigint") num = false;
    if (!(typeof v === "string" && ISO_MOMENT.test(v) && !Number.isNaN(Date.parse(v))))
      date = false;
    if (!bool && !num && !date) return "text";
  }
  if (!seen) return "text";
  return bool ? "boolean" : num ? "number" : date ? "date" : "text";
}

const DAY_MS = 86_400_000;

/** A calendar date typed as yyyy-mm-dd, as local midnight; null when it is not one. */
export function parseDay(s: string | undefined): Date | null {
  if (!s) return null;
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(s.trim());
  if (!m) return null;
  const d = new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
  return isNaN(d.getTime()) ? null : d;
}

/** A cell's moment. A bare date is local midnight; a timestamp without an offset is local wall
    clock (what the SQL side compares it as). */
export function parseMoment(v: unknown): number | null {
  if (v == null || v === "") return null;
  if (v instanceof Date) return v.getTime();
  if (typeof v === "number") return v;
  const s = String(v).trim();
  const day = parseDay(s);
  if (day) return day.getTime();
  const t = Date.parse(s.includes(" ") && !s.includes("T") ? s.replace(" ", "T") : s);
  return isNaN(t) ? null : t;
}

export interface Bounds {
  /** Inclusive lower bound, ms; undefined = open. */
  from?: number;
  /** Exclusive upper bound, ms; undefined = open. */
  to?: number;
}

/** A date operator as absolute bounds, resolved against `now` in the BROWSER's time zone. The
    same bounds feed the in-browser test and the SQL literals, so both modes agree. Null when the
    operands are not usable yet. */
export function dateBounds(spec: FilterSpec, now: Date = new Date()): Bounds | null {
  const startOfDay = (d: Date) => new Date(d.getFullYear(), d.getMonth(), d.getDate());
  const today = startOfDay(now);
  const a = parseDay(spec.a);
  const b = parseDay(spec.b);
  const next = (d: Date) => new Date(d.getFullYear(), d.getMonth(), d.getDate() + 1);
  switch (spec.op) {
    case "before":
      return a ? { to: a.getTime() } : null;
    case "after":
      return a ? { from: next(a).getTime() } : null;
    case "between":
      return a && b ? { from: a.getTime(), to: next(b).getTime() } : null;
    case "lastDays": {
      const n = Number(spec.a);
      return Number.isFinite(n) && n > 0
        ? { from: now.getTime() - n * DAY_MS, to: now.getTime() + 1 }
        : null;
    }
    case "today":
      return { from: today.getTime(), to: next(today).getTime() };
    case "thisWeek": {
      // Monday to Sunday.
      const back = (today.getDay() + 6) % 7;
      const start = new Date(today.getFullYear(), today.getMonth(), today.getDate() - back);
      return {
        from: start.getTime(),
        to: new Date(start.getFullYear(), start.getMonth(), start.getDate() + 7).getTime(),
      };
    }
    case "thisMonth":
      return {
        from: new Date(now.getFullYear(), now.getMonth(), 1).getTime(),
        to: new Date(now.getFullYear(), now.getMonth() + 1, 1).getTime(),
      };
    case "lastMonth":
      return {
        from: new Date(now.getFullYear(), now.getMonth() - 1, 1).getTime(),
        to: new Date(now.getFullYear(), now.getMonth(), 1).getTime(),
      };
    case "thisYear":
      return {
        from: new Date(now.getFullYear(), 0, 1).getTime(),
        to: new Date(now.getFullYear() + 1, 0, 1).getTime(),
      };
    default:
      return null;
  }
}

const blank = (v: unknown) => v == null || v === "";

/** The text a checklist shows and matches for a cell. */
export function cellKey(v: unknown): string {
  return blank(v) ? EMPTY_VALUE : String(v);
}

/** A plain decimal number as typed: no exponent, no sign other than a leading minus. The same test
    gates the in-browser filter and the SQL written for it, so a value is never a number in one and
    text in the other. */
export const isNumber = (s: string | undefined): s is string =>
  s !== undefined && /^-?\d+(\.\d+)?$/.test(s.trim());

/** A text operand SQL cannot carry (NUL). It is not a filter, in either mode. */
const carriable = (s: string | undefined) => s === undefined || !s.includes("\u0000");

/** True when the spec has what it needs to filter; an incomplete one narrows nothing. */
export function isUsable(kind: ColumnKind, spec: FilterSpec): boolean {
  switch (spec.op) {
    case "isEmpty":
    case "notEmpty":
      return true;
    case "in":
      return (
        (spec.values?.length ?? 0) > 0 &&
        (spec.values ?? []).every((v) => v === EMPTY_VALUE || carriable(v))
      );
    default:
  }
  if (kind === "date") return dateBounds(spec) !== null;
  if (kind === "number") {
    return spec.op === "between" ? isNumber(spec.a) && isNumber(spec.b) : isNumber(spec.a);
  }
  return (spec.a ?? "") !== "" && carriable(spec.a);
}

/** Whether a cell passes. Text operators are case-insensitive. */
export function matchesFilter(kind: ColumnKind, spec: FilterSpec, v: unknown): boolean {
  if (!isUsable(kind, spec)) return true;
  switch (spec.op) {
    case "isEmpty":
      return blank(v);
    case "notEmpty":
      return !blank(v);
    case "in":
      // Typed in the box (a: "ci") the list matches whole values ignoring case; ticked from the
      // checklist it matches the exact values listed.
      return spec.a === "ci"
        ? !blank(v) && (spec.values ?? []).some((x) => x.toLowerCase() === String(v).toLowerCase())
        : (spec.values ?? []).includes(cellKey(v));
    default:
  }
  if (kind === "number") {
    if (blank(v)) return false;
    const n = Number(v);
    if (isNaN(n)) return false;
    const a = Number(spec.a);
    switch (spec.op) {
      case "eq":
        return n === a;
      case "ne":
        return n !== a;
      case "lt":
        return n < a;
      case "le":
        return n <= a;
      case "gt":
        return n > a;
      case "ge":
        return n >= a;
      case "between":
        return n >= Math.min(a, Number(spec.b)) && n <= Math.max(a, Number(spec.b));
      default:
        return true;
    }
  }
  if (kind === "date") {
    const m = parseMoment(v);
    if (m === null) return false;
    const bounds = dateBounds(spec);
    if (!bounds) return true;
    return (
      (bounds.from === undefined || m >= bounds.from) && (bounds.to === undefined || m < bounds.to)
    );
  }
  // text, and boolean's string form
  if (v == null) return spec.op === "notContains";
  const s = String(v).toLowerCase();
  const x = (spec.a ?? "").toLowerCase();
  switch (spec.op) {
    case "contains":
      return s.includes(x);
    case "notContains":
      return !s.includes(x);
    case "equals":
      return s === x;
    case "startsWith":
      return s.startsWith(x);
    case "endsWith":
      return s.endsWith(x);
    default:
      return true;
  }
}

/** The quick syntax in a column's filter box: `>100`, `10..20`, `!text`, `a, b, c`, `=exact`.
    Anything else is plain text, which means contains. Null for an empty box. */
export function parseQuick(text: string, kind: ColumnKind): FilterSpec | null {
  const t = text.trim();
  if (t === "") return null;
  if (t.startsWith("=") && t.length > 1) {
    const rest = t.slice(1).trim();
    return kind === "number" ? { op: "eq", a: rest } : { op: "equals", a: rest };
  }
  if (t.startsWith("!") && t.length > 1) {
    const rest = t.slice(1).trim();
    return kind === "number" ? { op: "ne", a: rest } : { op: "notContains", a: rest };
  }
  if (kind === "number" || kind === "date") {
    const cmp = /^(>=|<=|>|<)\s*(.+)$/.exec(t);
    if (cmp) {
      const [, sym, rest] = cmp;
      const ops: Record<string, FilterOp> =
        kind === "number"
          ? { ">": "gt", ">=": "ge", "<": "lt", "<=": "le" }
          : { ">": "after", ">=": "after", "<": "before", "<=": "before" };
      return { op: ops[sym], a: rest.trim() };
    }
    const range = /^(.+?)\s*\.\.\s*(.+)$/.exec(t);
    if (range) return { op: "between", a: range[1].trim(), b: range[2].trim() };
  }
  if (t.includes(",")) {
    const parts = t
      .split(",")
      .map((p) => p.trim())
      .filter((p) => p !== "");
    if (parts.length >= 2) return { op: "in", a: "ci", values: parts };
  }
  return { op: "contains", a: t };
}

/** The filter that is in force on a column: the menu's choice, else the box's quick syntax. */
export function effectiveSpec(
  kind: ColumnKind,
  menu: FilterSpec | undefined,
  typed: string | undefined,
): FilterSpec | null {
  if (menu) return menu;
  return typed ? parseQuick(typed, kind) : null;
}

/** Distinct values of a column in the rows held, with counts, most frequent first. */
export function distinctValues(
  rows: Record<string, unknown>[],
  col: string,
): { value: string; count: number }[] {
  const counts = new Map<string, number>();
  for (const r of rows) {
    const k = cellKey(r[col]);
    counts.set(k, (counts.get(k) ?? 0) + 1);
  }
  return [...counts.entries()]
    .map(([value, count]) => ({ value, count }))
    .sort(
      (p, q) =>
        q.count - p.count ||
        Number(p.value === EMPTY_VALUE) - Number(q.value === EMPTY_VALUE) ||
        p.value.localeCompare(q.value),
    );
}
