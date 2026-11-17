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
// each said in a note; the rest is kept. A set that held everything registered when the run started
// (every domain, or every table of a domain) is restored as everything, so a table registered since
// is in the next run: the run stored the list it resolved to, not that it asked for all.

import type { DiscoveryScope } from "../../api/admin";
import type { ScopeTable } from "./DiscoverScopeForm";

/** The run a new one starts from: its fields and when it started. */
export interface LastRun {
  scope: DiscoveryScope;
  startedAt: string;
}

/** One value of the previous run left out of the form, as its catalogued message names it. */
export type RestoreNote =
  | { kind: "domainsGone"; names: string[] }
  | { kind: "tablesGone"; count: number }
  | { kind: "vendorNoKey"; vendor: string }
  | { kind: "modelGone"; vendor: string; model: string };

/** Whether the table was registered after ``since`` (an ISO time). A draft not yet registered
 *  (registeredAt null) is in no listing the form is given. */
export function registeredSince(table: ScopeTable, since: string): boolean {
  return table.registeredAt !== null && Date.parse(table.registeredAt) > Date.parse(since);
}

/** The sets of a previous run as the form can hold them now: its domains still offered, its tables
 *  still registered within them, and a note for what was left out. */
export function restoreSets(
  last: LastRun,
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
  const initial = last.scope;
  const offered = new Set(domains);
  const listed = tables.filter((tb) => offered.has(tb.domainId));
  const then = listed.filter((tb) => !registeredSince(tb, last.startedAt));
  const domainsThen = new Set(then.map((tb) => tb.domainId));
  const gone = new Set<string>();
  let tablesGone = 0;
  const held = (asked: string[]) => {
    asked.filter((d) => !offered.has(d)).forEach((d) => gone.add(d));
    const kept = asked.filter((d) => offered.has(d));
    // Every domain offered when the run started is the form's empty selection: a domain added
    // since is then included.
    return [...domainsThen].every((d) => kept.includes(d)) || kept.length === domains.length
      ? []
      : kept;
  };
  const heldTables = (asked: number[] | null, within: string[]) => {
    const reach = new Set(within.length > 0 ? within : domains);
    const asking = new Set(asked ?? []);
    const kept = listed.filter((tb) => asking.has(tb.id) && reach.has(tb.domainId));
    tablesGone += asking.size - kept.length;
    // A domain whose every table of then was asked for is restored whole, with its tables since.
    const whole = new Set(
      [...reach].filter((d) => {
        const thenIn = then.filter((tb) => tb.domainId === d);
        return thenIn.length > 0 && thenIn.every((tb) => asking.has(tb.id));
      }),
    );
    const chosen = listed.filter(
      (tb) => reach.has(tb.domainId) && (asking.has(tb.id) || whole.has(tb.domainId)),
    );
    const all = listed.filter((tb) => reach.has(tb.domainId));
    return chosen.length === all.length ? [] : chosen.map((tb) => String(tb.id));
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
