// Copyright (c) 2026 Kenneth Stott
// Canary: 9a4e1c62-7d3b-4f58-b2a6-0e8c5d1f7b39
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1907: per-role staleness tolerance on a table. The operator lists role → TTL (seconds); a
// reader's effective TTL is always max(cache_ttl, role_ttl(role)), where cache_ttl is the table's
// resolved TTL (table, else source, else global) and is the operator's floor.

import type { RegisteredTable, RoleTtl, Source } from "../../types/admin";
import { ttlSignalMissingCacheTtl } from "../sources/loadManagement";
import {
  REPLICATE_ALWAYS,
  resolvedReplicate,
  saysReplicated,
} from "../../components/admin/replicate";

/** The per-row validation error key for a role TTL list, or null when the row is valid. */
export function roleTtlRowError(rows: RoleTtl[], index: number): string | null {
  const row = rows[index];
  if (!Number.isInteger(row.ttl) || row.ttl < 0) return "tableEditForm.roleTtlInvalidTtl";
  if (rows.findIndex((r) => r.role === row.role) !== index) {
    return "tableEditForm.roleTtlDuplicateRole";
  }
  return null;
}

/** True when every row of a role TTL list is valid (checked before save). */
export function roleTtlValid(rows: RoleTtl[]): boolean {
  return rows.every((_, i) => roleTtlRowError(rows, i) === null);
}

/**
 * True when a table's effective change signal (its own, else its source's) is ttl/ttl_probe, no
 * landing cache_ttl resolves (table, else source — REQ-930), and the settings say the table is
 * replicated: materialize, a source floored as a whole (its Replicate = Always or its Load
 * Protected), or the table's resolved Replicate (Always or a Hot threshold) / Load Protected.
 * Such a save is refused, as the server's lands_from_config judges it.
 * row_materialize is not exposed to the admin UI, so that case is enforced by the server alone.
 */
export function tableTtlSignalError(
  table: Pick<RegisteredTable, "changeSignal" | "materialize" | "replicate" | "loadProtected">,
  source: Pick<Source, "changeSignal" | "replicate" | "loadProtected"> | undefined,
  resolvedCacheTtl: number | null,
): boolean {
  const signal = table.changeSignal ?? source?.changeSignal ?? null;
  const sourceFloored = (source?.loadProtected ?? false) || source?.replicate === REPLICATE_ALWAYS;
  const landed =
    table.materialize ||
    sourceFloored ||
    saysReplicated(
      resolvedReplicate(table.replicate, source?.replicate),
      table.loadProtected ?? source?.loadProtected ?? false,
    );
  return ttlSignalMissingCacheTtl(signal, resolvedCacheTtl, landed);
}
