// Copyright (c) 2026 Kenneth Stott
// Canary: 84957aa3-2456-40f5-8384-e98ce786e83c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A batch run on the SQL page answers one result per statement, in order: each statement's rows,
// the results store it landed in, or why it failed -- the others still shown.

import type { StatementAnswer } from "../../api/admin";
import type { ResultRedirect } from "../../types/resultRedirect";
import { resultIsPartial } from "./sqlHelpers";

export interface StatementResult {
  /** The statement as sent (the sampler's LIMIT included). */
  statement: string;
  columns: string[];
  /** REQ-1937: each column's SQL type; null when the engine reported none. */
  columnTypes: Record<string, string | null>;
  rows: Record<string, unknown>[];
  error: string;
  redirect: ResultRedirect | null;
  /** REQ-1937: the rows held are part of a longer answer. */
  partial: boolean;
}

export function toStatementResult(a: StatementAnswer): StatementResult {
  return {
    statement: a.statement,
    columns: a.columns,
    columnTypes: Object.fromEntries(a.columns.map((c, i) => [c, a.column_types?.[i] ?? null])),
    rows: a.rows,
    error: a.error ?? "",
    redirect: a.redirect ?? null,
    partial: a.error ? false : resultIsPartial(a.statement, a.rows.length, a.warnings),
  };
}

const NAME = String.raw`(?:"[^"]+"|[\w$]+)`;
const FROM = new RegExp(String.raw`\bfrom\s+(${NAME}(?:\s*\.\s*${NAME})*)`, "i");

/** The table a statement reads first, by its own name (`glossary_terms` of `meta.glossary_terms`),
 * to tell its result apart; null when it reads none (`SELECT 1`). */
export function statementLabel(statement: string): string | null {
  const m = statement.match(FROM);
  if (!m) return null;
  const last = m[1].split(".").pop() as string;
  return last.trim().replace(/^"|"$/g, "");
}

/** The statements, sampled each on its own, as one batch: a statement's trailing line comment
 * cannot swallow the separator. */
export const joinStatements = (statements: string[]) => statements.join("\n;\n");
