// Copyright (c) 2026 Kenneth Stott
// Canary: 6a2052b1-8748-43f2-92cb-6c2611c02e3e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A product's facts on the graph: its status and its owners (its members are counted from the
// graph). REQ-2219 (amended 2026-11-24, per REQ-2125): its owners are rows of
// data_product_owners_meta, reached by its owners relationship, and its owner fact names them as
// its Owner field does, people and roles as entered.

import { rowsOf, type Selection } from "../../api/metaGraph";
import type { NamedPrincipal } from "../../types/admin";

/** An owners view's columns: each principal, its kind, whether its field names it as entered
 * (named) and whether it holds the capability (holds). REQ-2219 (amended 2026-11-24). */
export const OWNER_ROWS: Selection = { fields: ["principal", "principal_kind", "named", "holds"] };

/** A product's facts as the meta domain reads them. */
export const PRODUCT_FACTS: Selection = {
  fields: ["status"],
  rels: { owners: { sel: OWNER_ROWS } },
};

/** A product named on the graph: its id, name and domain, with its facts. */
export const PRODUCT_NAMED: Selection = {
  fields: ["id", "name", "domain_id", ...PRODUCT_FACTS.fields],
  rels: PRODUCT_FACTS.rels,
};

/** One owner of a product, as its owners relationship answers it. */
export type OwnerRow = {
  principal: string;
  principalKind: "person" | "role";
  named: boolean;
  holds: boolean;
};

/** The principals the field names, as entered (a role by its name, not its holders). */
export function namedOf(rows: readonly OwnerRow[]): NamedPrincipal[] {
  return rows.filter((o) => o.named).map((o) => ({ principal: o.principal, kind: o.principalKind }));
}

/** The people holding it: a named person and every holder of a named role. Rights are theirs. */
export function holdersOf(rows: readonly OwnerRow[]): string[] {
  return rows.filter((o) => o.holds).map((o) => o.principal);
}

/** A product's owners as its owner fact: the principals its Owner field names, as entered. None
 * read, or none named, is no fact. */
export function ownersFact(owners: OwnerRow[] | null | undefined): string | undefined {
  const rows = rowsOf(owners);
  const named = rows && namedOf(rows).map((o) => o.principal);
  return named && named.length > 0 ? named.join(", ") : undefined;
}
