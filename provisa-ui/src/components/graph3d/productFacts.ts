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
// data_product_owners_meta, reached by its owners relationship.

import { rowsOf, type Selection } from "../../api/metaGraph";

/** A product's facts as the meta domain reads them. */
export const PRODUCT_FACTS: Selection = {
  fields: ["status"],
  rels: { owners: { sel: { fields: ["principal"] } } },
};

/** A product named on the graph: its id, name and domain, with its facts. */
export const PRODUCT_NAMED: Selection = {
  fields: ["id", "name", "domain_id", ...PRODUCT_FACTS.fields],
  rels: PRODUCT_FACTS.rels,
};

/** One owner of a product, as its owners relationship answers it. */
export type OwnerRow = { principal: string };

/** A product's owners as its owner fact: the principals its owners relationship reaches. None
 * read, or none named, is no fact. */
export function ownersFact(owners: OwnerRow[] | null | undefined): string | undefined {
  const people = rowsOf(owners)?.map((o) => o.principal);
  return people && people.length > 0 ? people.join(", ") : undefined;
}
