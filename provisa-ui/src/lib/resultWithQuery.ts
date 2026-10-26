// Copyright (c) 2026 Kenneth Stott
// Canary: 961fd945-94a2-451d-b607-98fb6cbbe44d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * An explorer tab's last result, kept as one unit with the exact query that produced it. The
 * query in the editor is saved on every edit and the result only when a run answers, so a result
 * kept apart from its query is restored beside whatever the editor was last edited to.
 */
export interface KeptResult<T> {
  query: string;
  result: T;
}

export function kept<T>(query: string, result: T): KeptResult<T> {
  return { query: query.trim(), result };
}

/** The kept result, when the editor holds the query that produced it; otherwise none. */
export function restored<T>(k: KeptResult<T> | null | undefined, query: string): T | null {
  if (!k || typeof k.query !== "string") return null;
  return k.query === query.trim() ? k.result : null;
}
