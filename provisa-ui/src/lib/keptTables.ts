// Copyright (c) 2026 Kenneth Stott
// Canary: 4e517217-6b50-496d-b2f1-b820679f43cf
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { Dependent } from "./dependents";

/** REQ-1918: a table a remote re-registration KEPT. Either the remote no longer has it and
 *  something still refers to it (`dependents`), or it would have lost columns something refers
 *  to and was left as it was (`columns`: each such column with what refers to it). */
export interface KeptTable {
  id: number | null;
  name: string;
  dependents?: Dependent[];
  columns?: Record<string, Dependent[]>;
}

/** The kept tables a registration or refresh answer reports; empty when it reports none. */
export function keptTablesOf(answer: unknown): KeptTable[] {
  const kept = (answer as { kept_tables?: unknown } | null)?.kept_tables;
  return Array.isArray(kept) ? (kept as KeptTable[]) : [];
}

/** What refers to a kept table, by name, for the line that says why it was kept. */
export function referrersOf(table: KeptTable): string[] {
  const direct = (table.dependents ?? []).map((d) => d.name || String(d.id));
  const viaColumns = Object.entries(table.columns ?? {}).flatMap(([column, dependents]) =>
    dependents.map((d) => `${d.name || String(d.id)} (${column})`),
  );
  return [...direct, ...viaColumns];
}
