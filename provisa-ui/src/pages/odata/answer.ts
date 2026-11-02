// Copyright (c) 2026 Kenneth Stott
// Canary: 1ba81ba0-4947-420e-93da-84fb97f552f2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Requirements: REQ-1949, REQ-2193

// An OData read's answer as the explorer shows it: whether a row limit cut it, and each row as
// the results grid draws it.

import type { StatementWarning } from "../../api/admin";

/** The server's codes for "this answer was cut at a row limit" (compiler/row_limit.py). */
const ROWS_CUT_CODES: ReadonlySet<string> = new Set([
  "statement.rows_cut",
  "statement.rows_cut_unchecked",
]);

export function rowsCut(warnings: readonly StatementWarning[]): boolean {
  return warnings.some((w) => ROWS_CUT_CODES.has(w.code));
}

/** A row as the grid shows it: an expanded navigation property (an entity, or a list of them) as
 * its JSON, the rest as given. */
export function gridRow(row: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(row).map(([k, v]) => [
      k,
      v !== null && typeof v === "object" ? JSON.stringify(v) : v,
    ]),
  );
}
