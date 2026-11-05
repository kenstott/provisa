// Copyright (c) 2026 Kenneth Stott
// Canary: 3b7e92d4-5a18-4c60-8f2d-61c0a94e7d35
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2158: a promise's checks as a reader reads them -- what each applies to (the table, its rows,
// or one column), the check in plain words, and its thresholds in plain words. A checker's own
// names (expect_column_values_to_not_be_null, missing) and its argument records never reach a
// reader. A check type this module does not name throws: a new check type is named here, with its
// words in the catalogs, before a contract can show it.

import type { TFunction } from "i18next";
import type { ProductDqRule } from "../../api/lineage";

export type Level = "table" | "row" | "column";

/** Every check type the DQ catalog offers, the words that name it and what it applies to. */
const CHECKS: Record<string, { key: string; level: Level }> = {
  expect_column_values_to_not_be_null: { key: "notNull", level: "column" },
  expect_column_values_to_be_unique: { key: "unique", level: "column" },
  expect_column_values_to_be_in_set: { key: "allowedValues", level: "column" },
  expect_column_values_to_match_regex: { key: "pattern", level: "column" },
  expect_column_values_to_be_between: { key: "valueRange", level: "column" },
  expect_column_value_lengths_to_be_between: { key: "textLength", level: "column" },
  expect_column_mean_to_be_between: { key: "average", level: "column" },
  expect_column_unique_value_count_to_be_between: { key: "distinctCount", level: "column" },
  expect_table_row_count_to_be_between: { key: "rowCount", level: "table" },
  expect_table_row_count_to_equal: { key: "rowCount", level: "table" },
  expect_column_pair_values_a_to_be_greater_than_b: { key: "columnOrder", level: "row" },
  unexpected_rows_expectation: { key: "unexpectedRows", level: "row" },
  missing: { key: "missing", level: "column" },
  invalid: { key: "valid", level: "column" },
  duplicate: { key: "duplicate", level: "column" },
  aggregate: { key: "aggregate", level: "column" },
  row_count: { key: "rowCount", level: "table" },
  freshness: { key: "freshness", level: "table" },
  failed_rows: { key: "failedRows", level: "row" },
};

const known = (rule: ProductDqRule) => {
  const check = CHECKS[rule.check_type];
  if (!check) throw new Error(`promise check type "${rule.check_type}" has no plain name`);
  return check;
};

/** What a rule applies to: its column's name, or the table or its rows. */
export type AppliesTo = { level: "table" | "row" } | { level: "column"; column: string };

export function appliesTo(rule: ProductDqRule): AppliesTo {
  const { level } = known(rule);
  if (level === "column") {
    if (!rule.column_name) throw new Error(`promise check "${rule.check_type}" names no column`);
    return { level, column: rule.column_name };
  }
  return { level };
}

/** The order a promise table reads in: the table's checks, then its rows', then each column's. */
const ORDER: Record<Level, number> = { table: 0, row: 1, column: 2 };
export const appliesOrder = (a: AppliesTo) => ORDER[a.level];

export const checkName = (rule: ProductDqRule, t: TFunction) =>
  t(`contractPromises.check.${known(rule).key}`);

const THRESHOLDS: Record<string, string> = {
  must_be: "eq",
  must_not_be: "ne",
  must_be_greater_than: "gt",
  must_be_greater_than_or_equal: "gte",
  must_be_less_than: "lt",
  must_be_less_than_or_equal: "lte",
};
const AGGREGATES = ["avg", "sum", "min", "max", "minLength", "maxLength", "avgLength"];

const list = (v: unknown) => (Array.isArray(v) ? v.join(", ") : String(v));
const has = (v: unknown) => v !== undefined && v !== null;

/** A Soda threshold ({ must_be: 0, metric: percent }) as words. */
function threshold(raw: unknown, t: TFunction): string[] {
  if (!raw || typeof raw !== "object") return [];
  const { metric } = raw as Record<string, unknown>;
  const rest = Object.entries(raw).filter(([op]) => op !== "metric" && op !== "level");
  const value = (v: unknown) =>
    metric === "percent" ? t("contractPromises.detail.percent", { value: v }) : String(v);
  return rest.map(([op, v]) => {
    if (op === "must_be_between" && v && typeof v === "object") {
      const range = v as Record<string, unknown>;
      return t("contractPromises.detail.between", {
        min: value(range.greater_than_or_equal),
        max: value(range.less_than_or_equal),
      });
    }
    const key = THRESHOLDS[op];
    if (!key) throw new Error(`promise threshold "${op}" has no plain name`);
    return t(`contractPromises.threshold.${key}`, { value: value(v) });
  });
}

/** A rule's thresholds and accepted values in plain words; empty when it states none. */
export function checkDetails(rule: ProductDqRule, t: TFunction): string {
  known(rule);
  const a = rule.args;
  const d = (key: string, values: Record<string, unknown>) =>
    t(`contractPromises.detail.${key}`, values);
  const parts: string[] = [];
  if (has(a.value)) parts.push(t("contractPromises.threshold.eq", { value: String(a.value) }));
  if (has(a.min_value) && has(a.max_value)) {
    parts.push(d("between", { min: a.min_value, max: a.max_value }));
  } else if (has(a.min_value)) {
    parts.push(d("atLeast", { value: a.min_value }));
  } else if (has(a.max_value)) {
    parts.push(d("atMost", { value: a.max_value }));
  }
  if (has(a.value_set)) parts.push(d("oneOf", { values: list(a.value_set) }));
  if (has(a.regex)) parts.push(d("matches", { pattern: a.regex }));
  if (has(a.column_A) && has(a.column_B)) {
    parts.push(d(a.or_equal === true ? "orEqual" : "greater", { a: a.column_A, b: a.column_B }));
  }
  if (has(a.unexpected_rows_query)) parts.push(d("query", { query: a.unexpected_rows_query }));
  if (typeof a.mostly === "number" && a.mostly < 1) {
    parts.push(d("mostly", { percent: Math.round(a.mostly * 100) }));
  }
  if (has(a.function)) {
    const fn = String(a.function);
    if (!AGGREGATES.includes(fn)) throw new Error(`promise aggregate "${fn}" has no plain name`);
    parts.push(t(`contractPromises.aggregate.${fn}`));
  }
  if (has(a.missing_values)) parts.push(d("alsoMissing", { values: list(a.missing_values) }));
  if (has(a.valid_values)) parts.push(d("oneOf", { values: list(a.valid_values) }));
  if (has(a.valid_min)) parts.push(d("atLeast", { value: a.valid_min }));
  if (has(a.valid_max)) parts.push(d("atMost", { value: a.valid_max }));
  if (has(a.valid_min_length)) parts.push(d("minLength", { value: a.valid_min_length }));
  if (has(a.valid_max_length)) parts.push(d("maxLength", { value: a.valid_max_length }));
  if (has(a.expression)) parts.push(d("expression", { expression: a.expression }));
  if (has(a.query)) parts.push(d("query", { query: a.query }));
  parts.push(...threshold(a.threshold, t));
  return parts.join("; ");
}
