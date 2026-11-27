// Copyright (c) 2026 Kenneth Stott
// Canary: 3e8a1f64-9b27-4c5d-8e03-d6f2a7b9c418
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174 (amended 2026-11-26, A CONNECTED ONTOLOGY GRAPH IS ONE QUERY): a focus's or a search's
// hops are nested under one root field (ontologyData.termSelection), so a term reached by two paths
// is in the answer once for each. Here the answer's terms are each held once.

import type { TermEnd, TermRow } from "./ontologyData";

/** Whether an end was read whole: a term is read with its name. */
const readWhole = (e: TermEnd): e is TermRow => "name" in e;

/** One-to-many rows read on two paths to a term, each once, by id; null (none) when neither has
 * any (rowsOf). */
function unionById<T extends { id: number }>(
  a: T[] | null | undefined,
  b: T[] | null | undefined,
): T[] | null | undefined {
  // Every path reads a term with the same selection: both have the rows, or neither.
  if (a === undefined || b === undefined) {
    if (a !== b) throw new Error("a term was read with its edges on one path and not another");
    return undefined;
  }
  // Null is none (rowsOf).
  const rows = [a, b].flatMap((r) => r ?? []);
  return rows.length === 0 ? null : [...new Map(rows.map((r) => [r.id, r])).values()];
}

/** REQ-2174 (amended 2026-11-26, A CONNECTED ONTOLOGY GRAPH IS ONE QUERY): the terms a nested
 * read answered, each once. A term reached by two paths is in the answer once for each; they are
 * merged by id, with every edge either read. */
export function readTerms(roots: TermRow[]): TermRow[] {
  const byId = new Map<number, TermRow>();
  const visit = (t: TermRow) => {
    const had = byId.get(t.id);
    byId.set(
      t.id,
      had === undefined
        ? t
        : {
            ...had,
            edgesOut: unionById(had.edgesOut, t.edgesOut),
            edgesIn: unionById(had.edgesIn, t.edgesIn),
          },
    );
    for (const e of t.edgesOut ?? []) if (e.targetTerm && readWhole(e.targetTerm)) visit(e.targetTerm);
    for (const e of t.edgesIn ?? []) if (e.sourceTerm && readWhole(e.sourceTerm)) visit(e.sourceTerm);
  };
  roots.forEach(visit);
  return [...byId.values()];
}
