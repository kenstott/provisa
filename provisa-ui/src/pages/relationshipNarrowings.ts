// Copyright (c) 2026 Kenneth Stott
// Canary: 1105f7c2-114a-4d4f-9e22-25fdeeea89a7
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277 rule 9: what narrows the Relationships list -- the domains in view, the search (over
// every text a relationship's row shows, and its alias: a tour step names it so, demoTour.ts) and
// the Rel type column filter -- each named so the page can say why a linked relationship is not listed.

import type { TFunction } from "i18next";
import { filterNarrowing, searchNarrowing, type Narrowing } from "../components/list/leftOut";
import { domainToSqlName } from "../naming";
import type { Relationship } from "../types/admin";
import { SHAPE_CODE } from "../components/relationships/relationship-types";
import { relationshipInCheckedDomains } from "./relationshipDomainFilter";
import { matchesFilter as matchesColumnFilter, type FilterSpec } from "./sql/columnFilter";

/** A relationship as the notice names it: its alias, or the tables it joins. */
export const relName = (r: Relationship) =>
  r.alias || `${r.sourceTableName} → ${r.targetTableName}`;

/** The relationships the list holds: not a remote source's, nor the admin source's. */
export function listableRelationships(
  rels: readonly Relationship[],
  tables: readonly { id: number; sourceId: string; schemaName: string }[],
): Relationship[] {
  const sourceOf = new Map(tables.map((tb) => [tb.id, tb.sourceId]));
  const remote = new Set(
    tables
      .filter((tb) => tb.schemaName === "graphql_remote" || tb.schemaName === "grpc_remote")
      .map((tb) => tb.id),
  );
  return rels.filter(
    (r) => !remote.has(r.sourceTableId) && sourceOf.get(r.sourceTableId) !== "provisa-admin",
  );
}

export function relationshipNarrowings(
  t: TFunction,
  {
    checkedDomains,
    tables,
    addDomain,
    search,
    clearSearch,
    rowTexts,
    relTypeFilter,
    clearRelTypeFilter,
  }: {
    /** The navbar's narrowing (none or every domain checked is none). */
    checkedDomains: ReadonlySet<string> | null;
    tables: readonly { id: number; domainId: string }[];
    addDomain: (domain: string) => void;
    search: string;
    clearSearch: () => void;
    /** Every text a relationship's row shows (relationshipRowTexts). */
    rowTexts: (r: Relationship) => string[];
    relTypeFilter: FilterSpec | null;
    clearRelTypeFilter: () => void;
  },
): {
  listNarrowings: (Narrowing<Relationship> | null)[];
  relTypeNarrowing: Narrowing<Relationship> | null;
} {
  const narrowTo = checkedDomains ? new Set([...checkedDomains].map(domainToSqlName)) : null;
  const targetOf = (r: Relationship) => tables.find((tb) => tb.id === r.targetTableId);
  // The domains a relationship's row shows: its source table's and its target table's.
  const domainIds = (r: Relationship) => [
    ...new Set([r.sourceDomainId, targetOf(r)?.domainId].filter((d): d is string => !!d)),
  ];
  const listNarrowings: (Narrowing<Relationship> | null)[] = [
    narrowTo === null
      ? null
      : {
          hides: (r) => {
            const target = targetOf(r);
            return !relationshipInCheckedDomains(
              narrowTo,
              r.sourceDomainId ? domainToSqlName(r.sourceDomainId) : undefined,
              target === undefined ? null : domainToSqlName(target.domainId),
            );
          },
          reason: (r) => t("detailInList.byDomain", { domains: domainIds(r).join(", ") }),
          action: (r) => t("detailInList.addDomain", { domains: domainIds(r).join(", ") }),
          bringIn: (r) => {
            for (const d of domainIds(r)) addDomain(d);
          },
        },
    searchNarrowing<Relationship>(t, search, clearSearch, (r) => [...rowTexts(r), r.alias]),
  ];
  // REQ-1586 (amended 2026-11-22): the Rel type column carries the list's column filter.
  const relTypeNarrowing = filterNarrowing<Relationship>(
    t,
    relTypeFilter !== null,
    t("relShape.label"),
    (r) =>
      relTypeFilter === null || matchesColumnFilter("text", relTypeFilter, SHAPE_CODE[r.shape]),
    clearRelTypeFilter,
  );
  return { listNarrowings, relTypeNarrowing };
}
