// Copyright (c) 2026 Kenneth Stott
// Canary: a3c023dd-ed53-4f19-831f-8e0c4082f05c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-2013: what a tour's steps need of the org beyond rights (rights are the itinerary's): the
 * sources, the registered tables (`<source>.<table>`) and whether a data product exists. A tour
 * whose prerequisites the org lacks -- a deep dive built on the standard demo's objects, opened in
 * a named demo or an org that changed them -- is not offered: the check runs when the Deep Dives
 * menu opens and when the tour button picks a tour (useOrgHas reads what the org has), and an unmet
 * tour is left out.
 */
export interface TourPrerequisites {
  sources?: readonly string[];
  /** Registered tables, as `<source id>.<table name>` (the Tables page's `data-table-row`). */
  tables?: readonly string[];
  /** At least one data product. */
  dataProduct?: boolean;
}

export interface OrgHas {
  sources: ReadonlySet<string>;
  tables: ReadonlySet<string>;
  dataProducts: number;
}

export function prerequisitesMet(needs: TourPrerequisites | undefined, has: OrgHas): boolean {
  if (needs === undefined) return true;
  return (
    (needs.sources ?? []).every((s) => has.sources.has(s)) &&
    (needs.tables ?? []).every((t) => has.tables.has(t)) &&
    (!needs.dataProduct || has.dataProducts > 0)
  );
}
