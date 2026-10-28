// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A files source's hints for how the Excel reader finds the tables of its workbooks: its
// mapping's `excel_hints` (provisa/file_source/excel_hints.py). Each hint left unset leaves the
// reader's own rule, shown in the form as Auto-detect. They are given for every workbook of the
// source, for one workbook by its file name, and for one table by the name the reader gives it.

/** Each count hint, with the value the reader uses when it is unset (null: it detects). */
export const COUNT_HINTS = [
  { key: "header_rows", auto: null },
  { key: "max_header_rows", auto: 3 },
  { key: "table_gap_rows", auto: 2 },
  { key: "column_gap", auto: 2 },
  { key: "min_columns", auto: 2 },
  { key: "min_data_rows", auto: 1 },
] as const;
export type CountHint = (typeof COUNT_HINTS)[number]["key"];

/** The hints a workbook (or every workbook) takes, and those a table takes. */
export const WORKBOOK_HINTS: readonly CountHint[] = COUNT_HINTS.map((h) => h.key);
export const TABLE_HINTS: readonly CountHint[] = [
  "header_rows",
  "max_header_rows",
  "min_columns",
  "min_data_rows",
];

export type HintScope = "workbook" | "table";

/** Hints for one workbook or one table, as the form holds them. */
export interface HintEntry {
  scope: HintScope;
  name: string;
  counts: Partial<Record<CountHint, number>>;
  range?: string;
}

/** A source's Excel hints, as the form holds them. */
export interface ExcelHints {
  counts: Partial<Record<CountHint, number>>;
  entries: HintEntry[];
}

export const NO_EXCEL_HINTS: ExcelHints = { counts: {}, entries: [] };

/** The form field the hints ride in (the source form's per-type fields are text). */
export const EXCEL_HINTS_FIELD = "excel_hints";

function counts(raw: unknown, allowed: readonly CountHint[]): Partial<Record<CountHint, number>> {
  const out: Partial<Record<CountHint, number>> = {};
  if (raw && typeof raw === "object") {
    for (const key of allowed) {
      const value = (raw as Record<string, unknown>)[key];
      if (typeof value === "number") out[key] = value;
    }
  }
  return out;
}

/** The hints a source's mapping holds. */
export function excelHintsFromMapping(mappingJson: string | null | undefined): ExcelHints {
  const mapping = mappingJson ? (JSON.parse(mappingJson) as Record<string, unknown>) : {};
  const raw = mapping.excel_hints as Record<string, unknown> | undefined;
  if (!raw) return NO_EXCEL_HINTS;
  const entries: HintEntry[] = [];
  for (const [scope, group, allowed] of [
    ["workbook", "workbooks", WORKBOOK_HINTS],
    ["table", "tables", TABLE_HINTS],
  ] as const) {
    const named = (raw[group] ?? {}) as Record<string, Record<string, unknown>>;
    for (const [name, hints] of Object.entries(named)) {
      entries.push({
        scope,
        name,
        counts: counts(hints, allowed),
        ...(scope === "table" && typeof hints.range === "string" ? { range: hints.range } : {}),
      });
    }
  }
  return { counts: counts(raw, WORKBOOK_HINTS), entries };
}

/** The hints held in the form's fields. */
export function excelHintsOf(fields: Record<string, string>): ExcelHints {
  const held = fields[EXCEL_HINTS_FIELD];
  return held ? (JSON.parse(held) as ExcelHints) : NO_EXCEL_HINTS;
}

/** The form's fields holding ``hints``. */
export function withExcelHints(
  fields: Record<string, string>,
  hints: ExcelHints,
): Record<string, string> {
  return { ...fields, [EXCEL_HINTS_FIELD]: JSON.stringify(hints) };
}

/** The names two entries of one scope share: each would hide the other. */
export function repeatedNames(hints: ExcelHints): Set<string> {
  const seen = new Set<string>();
  const repeated = new Set<string>();
  for (const entry of hints.entries) {
    const key = `${entry.scope}:${entry.name.trim()}`;
    if (entry.name.trim() && seen.has(key)) repeated.add(key);
    seen.add(key);
  }
  return repeated;
}

/** The mapping's ``excel_hints`` for the hints, or null when none is set. */
export function excelHintsMapping(hints: ExcelHints): Record<string, unknown> | null {
  const out: Record<string, unknown> = { ...hints.counts };
  for (const [scope, group] of [
    ["workbook", "workbooks"],
    ["table", "tables"],
  ] as const) {
    const named: Record<string, Record<string, unknown>> = {};
    for (const entry of hints.entries) {
      const name = entry.name.trim();
      if (entry.scope !== scope || !name) continue;
      const level: Record<string, unknown> = { ...entry.counts };
      if (scope === "table" && entry.range?.trim()) level.range = entry.range.trim();
      if (Object.keys(level).length > 0) named[name] = level;
    }
    if (Object.keys(named).length > 0) out[group] = named;
  }
  return Object.keys(out).length > 0 ? out : null;
}

/** A files source's mapping as saved: what it held before, with the hints the form holds. */
export function filesMappingJson(
  previousMappingJson: string | null,
  fields: Record<string, string>,
): string {
  const previous = previousMappingJson
    ? (JSON.parse(previousMappingJson) as Record<string, unknown>)
    : {};
  const { excel_hints: _dropped, ...rest } = previous;
  void _dropped;
  const hints = excelHintsMapping(excelHintsOf(fields));
  return JSON.stringify(hints ? { ...rest, excel_hints: hints } : rest);
}
