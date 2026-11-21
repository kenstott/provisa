// Copyright (c) 2026 Kenneth Stott
// Canary: 4b9f0c63-5e1d-4a27-8c3b-d6e2a9f17b05
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174 (amended 2026-11-07, WHAT THE GRAPH DRAWS): a product page's Lineage panel shows where
// the product comes from, never who consumes it: its tables' columns and what they derive from.

import type { ProductLineage } from "../../api/lineage";

/** ``g`` cut to the product's member columns and everything upstream of them; whatever reads the
 * product (one hop downstream, its consumers) is left out. */
export function upstreamLineage(g: ProductLineage): ProductLineage {
  const members = new Set(g.members);
  const up = new Map<string, string[]>();
  for (const e of g.edges) up.set(e.target, [...(up.get(e.target) ?? []), e.source]);
  const kept = new Set<string>();
  const stack = g.nodes
    .filter((n) => n.relation !== null && members.has(n.relation))
    .map((n) => n.id);
  for (const id of stack) kept.add(id);
  while (stack.length > 0)
    for (const p of up.get(stack.pop() as string) ?? [])
      if (!kept.has(p)) {
        kept.add(p);
        stack.push(p);
      }
  return {
    ...g,
    nodes: g.nodes.filter((n) => kept.has(n.id)),
    edges: g.edges.filter((e) => kept.has(e.source) && kept.has(e.target)),
    outputs: g.outputs.filter((o) => kept.has(o)),
    ...(g.cycles ? { cycles: g.cycles.filter((c) => c.nodes.every((n) => kept.has(n))) } : {}),
  };
}
