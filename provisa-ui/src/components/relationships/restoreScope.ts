// Copyright (c) 2026 Kenneth Stott
// Canary: 3d9c7a15-6e2b-4f80-a4c1-8b5f0e2d7a96
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-413: the fields of the discovery run before, as the form of the next can hold them now. A
// domain no longer offered and a table no longer registered within the domains kept are left out,
// each said in a note; the rest is kept.

import type { DiscoveryScope } from "../../api/admin";
import type { ScopeTable } from "./DiscoverScopeForm";

/** One value of the previous run left out of the form, as its catalogued message names it. */
export type RestoreNote =
  | { kind: "domainsGone"; names: string[] }
  | { kind: "tablesGone"; count: number }
  | { kind: "vendorNoKey"; vendor: string }
  | { kind: "modelGone"; vendor: string; model: string };

/** The sets of a previous run as the form can hold them now: its domains still offered, its tables
 *  still registered within them, and a note for what was left out. */
export function restoreSets(
  initial: DiscoveryScope,
  domains: string[],
  tables: ScopeTable[],
): {
  sourceDomains: string[];
  sourceTables: string[];
  sameAsSource: boolean;
  targetDomains: string[];
  targetTables: string[];
  notes: RestoreNote[];
} {
  const gone = new Set<string>();
  let tablesGone = 0;
  const held = (asked: string[]) => {
    asked.filter((d) => !domains.includes(d)).forEach((d) => gone.add(d));
    const kept = asked.filter((d) => domains.includes(d));
    // Every domain offered is the form's empty selection: a domain added later is then included.
    return kept.length === domains.length ? [] : kept;
  };
  const heldTables = (asked: number[] | null, within: string[]) => {
    const reach = new Set(within.length > 0 ? within : domains);
    const kept = (asked ?? []).filter((id) =>
      tables.some((tb) => tb.id === id && reach.has(tb.domainId)),
    );
    tablesGone += (asked ?? []).length - kept.length;
    return kept.map(String);
  };
  const sourceDomains = held(initial.source_domains);
  const sourceTables = heldTables(initial.source_tables, sourceDomains);
  const same = initial.target_same_as_source;
  const targetDomains = same ? [] : held(initial.target_domains);
  const targetTables = same ? [] : heldTables(initial.target_tables, targetDomains);
  return {
    sourceDomains,
    sourceTables,
    sameAsSource: same,
    targetDomains,
    targetTables,
    notes: [
      ...(gone.size > 0 ? [{ kind: "domainsGone" as const, names: [...gone].sort() }] : []),
      ...(tablesGone > 0 ? [{ kind: "tablesGone" as const, count: tablesGone }] : []),
    ],
  };
}
