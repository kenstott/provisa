// Copyright (c) 2026 Kenneth Stott
// Canary: 81a39e43-f617-43e8-8791-1b82da261e7c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * Display name for a materialized view. The registry id is the internal
 * `view-<alias>` (built in schema_common._sync_view_mv); users named it `<alias>`,
 * so strip the `view-` prefix for the Materialized Store table. The raw id is still
 * used for the Refresh/Disable action handlers.
 */
export function displayMvName(mvId: string): string {
  return mvId.replace(/^view-/, "");
}
