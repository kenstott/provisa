// Copyright (c) 2026 Kenneth Stott
// Canary: c8f49fc7-2599-4200-81b8-2f9d6e5827fd
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174 (amended 2026-11-27, THE ONE QUERY IS A REACH FILTER): what a snippet of the ontology
// starts from -- a search's matches, a term, or a product's own terms -- and the terms within its
// hops of them, as one where on the terms.

import type { Where } from "../../api/metaGraph";
import type { QueryScope } from "./graphModel";
import { searchWhere } from "./ontologySearch";

const all = (conds: Where[]): Where | undefined =>
  conds.length === 0 ? undefined : conds.length === 1 ? conds[0] : { _and: conds };

/** Every node ``s`` is focused on: none, one, or several (QueryScope.focus). */
export function foci(s: Pick<QueryScope, "focus">): readonly string[] {
  if (s.focus === null) return [];
  if (typeof s.focus === "string") return [s.focus];
  if (s.focus.length === 0) throw new Error("a scope focused on nothing has a null focus");
  return s.focus;
}

/** The products a focus names (their ids); none for a term's focus or none. */
export function focusProducts(s: Pick<QueryScope, "focus">): string[] {
  return foci(s)
    .filter((f) => f.startsWith("p:"))
    .map((f) => f.slice(2));
}

/** The terms a focus starts from: the term itself, or a product's -- those its tables' columns
 * are, and those its tables are (a class). */
function focusWhere(focus: string): Where {
  if (focus.startsWith("term:")) return { id: { eq: Number(focus.slice("term:".length)) } };
  if (focus.startsWith("p:")) {
    const member = { dataProductTables: { product_id: { eq: focus.slice(2) } } };
    return { _or: [{ refs: { registeredTable: member } }, { tables: member }] };
  }
  throw new Error(`a graph's focus is a term or a product, not ${focus}`);
}

/** The terms a snippet starts from, at no hop: a search's matches, and the focus (any of
 * several); undefined with neither. */
export function seedWhere(s: Pick<QueryScope, "search" | "focus">): Where | undefined {
  const searched = searchWhere(s.search);
  const focused = foci(s);
  return all([
    ...(searched ? [searched] : []),
    ...(focused.length === 1 ? [focusWhere(focused[0])] : []),
    ...(focused.length > 1 ? [{ _or: focused.map(focusWhere) }] : []),
  ]);
}

/** The terms ``hops`` steps or fewer from ``seed``: the seed, or a term with an edge of a type
 * in ``edgeTypes`` (any, when none) from or to a term ``kept`` within ``hops - 1`` steps. */
export function within(
  seed: Where,
  hops: number,
  kept: Where | undefined,
  edgeTypes: readonly string[],
): Where {
  if (!Number.isInteger(hops) || hops < 0)
    throw new Error(`a reach is a whole number of hops, not ${hops}`);
  if (hops === 0) return seed;
  const near = all([...(kept ? [kept] : []), within(seed, hops - 1, kept, edgeTypes)]) as Where;
  const typed: Where[] = edgeTypes.length === 0 ? [] : [{ rel_type: { in: [...edgeTypes] } }];
  return {
    _or: [
      seed,
      { edgesIn: all([...typed, { source_term: near }]) as Where },
      { edgesOut: all([...typed, { target_term: near }]) as Where },
    ],
  };
}
