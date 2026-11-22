// Copyright (c) 2026 Kenneth Stott
// Canary: 1b7e4c90-5a28-4d63-8f1e-0c9d2a6b3e85
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2262: the run the review after registration starts from.

import type { DiscoveryScope } from "../../api/admin";
import type { ListedTable } from "../../types/admin";
import type { LastRun } from "./restoreScope";

/** The run the review starts from: the tables just registered as the source set, every table as
 *  the target set, a relationship counted either way round. Settings the form holds for the person
 *  (threshold, samples, key value overlap) are its own; the model is the one assigned. */
export function reviewStart(tableIds: number[], tables: ListedTable[], at: Date): LastRun {
  const chosen = new Set(tableIds);
  const scope: DiscoveryScope = {
    kinds: ["foreign_keys", "key_names", "semantics"],
    source_domains: [...new Set(tables.filter((t) => chosen.has(t.id)).map((t) => t.domainId))],
    source_tables: [...tableIds],
    target_same_as_source: false,
    target_domains: [],
    target_tables: null,
    vendor: null,
    model: null,
    // Not read: the form starts Tick from and samples from the person's own settings
    // (DiscoverScopeForm.startingSettings), never from the run it starts from.
    threshold: 0,
    samples_per_column: 1,
    measure_cutoff: null,
    sample_size: null,
    contained_at: null,
    shared_ceiling: null,
    either_way: true,
  };
  return { scope, startedAt: at.toISOString() };
}
