// Copyright (c) 2026 Kenneth Stott
// Canary: 3b7e1f52-6c0a-4d58-9a43-2f1c8e5d7a90
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { format as formatSql } from "sql-formatter";

/** The first-line comment that carries a statement's bound parameters; the SQL editor reads it. */
export const PARAMS_COMMENT_PREFIX = "-- provisa-params:";

/**
 * SQL handed to the SQL editor from a link or another page, formatted as the UI formats machine
 * SQL everywhere else (keywords upper case, two-space indent, the editor's PostgreSQL dialect). A
 * leading "-- provisa-params:" line stays as it was, above the formatted statement.
 */
export function prettifySql(sql: string): string {
  const newline = sql.indexOf("\n");
  const first = newline === -1 ? sql : sql.slice(0, newline);
  if (!first.trim().startsWith(PARAMS_COMMENT_PREFIX)) return format(sql);
  const body = newline === -1 ? "" : sql.slice(newline + 1);
  return body.trim() === "" ? sql : `${first}\n${format(body)}`;
}

function format(sql: string): string {
  return formatSql(sql, { language: "postgresql", tabWidth: 2, keywordCase: "upper" });
}
