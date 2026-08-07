// Copyright (c) 2026 Kenneth Stott
// Canary: 9a1c5e73-4f28-4b6d-8e05-2d7b3f9c1a64
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1387: module-level cache for column-name glossary hover lookups, keyed
// `${tableId}:${columnName}`. Stores both hits and misses (null = the server's
// 404 "no term for this column" contract) so a column is fetched at most once
// per page load.

import type { GlossaryTermDetail } from "../../api/glossary";

export const columnGlossaryCache = new Map<string, GlossaryTermDetail | null>();

export function columnGlossaryKey(tableId: number, columnName: string): string {
  return `${tableId}:${columnName}`;
}

// Test seam: tests clear the cache so cases don't leak into each other.
export function clearColumnGlossaryCache(): void {
  columnGlossaryCache.clear();
}
