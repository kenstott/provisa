// Copyright (c) 2026 Kenneth Stott
// Canary: 5e0c8b27-3f1a-4d96-b7e2-a4d1c6f09b35
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239: a data product's upstream lineage as the one graph component draws it -- one node per
// relation, its columns as its lines, one edge per relation hop. Lanes left to right: what feeds
// the inputs, the inputs, then the product's own tables. The view says what each thing is; the
// graph says how it is drawn.

import type { ProductLineage } from "../../../api/lineage";
import type { GraphEdgeSpec, GraphNodeSpec, GraphSpec } from "../../../components/provisa-graph/model";

export const LANE_UPSTREAM = "upstream";
export const LANE_INPUTS = "inputs";
export const LANE_TABLES = "tables";

export function productLineageSpec(
  g: ProductLineage,
  label: (relation: string) => string,
): GraphSpec {
  const members = new Set(g.members);
  const inputs = new Set(g.inputs.map((i) => i.relation));
  const columns = new Map<string, Set<string>>();
  const relationOf = new Map<string, string>();
  for (const n of g.nodes) {
    // A column with no relation (a constant, a job) is not a relation's line.
    if (n.relation == null) continue;
    relationOf.set(n.id, n.relation);
    const cols = columns.get(n.relation) ?? new Set<string>();
    cols.add(n.column);
    columns.set(n.relation, cols);
  }
  const nodes: GraphNodeSpec[] = [...columns.entries()]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([relation, cols]) => ({
      id: relation,
      type: members.has(relation) ? "table" : inputs.has(relation) ? "source" : "view",
      name: label(relation),
      lane: members.has(relation) ? LANE_TABLES : inputs.has(relation) ? LANE_INPUTS : LANE_UPSTREAM,
      lines: [...cols].sort().map((text) => ({ text })),
      data: { relation },
    }));
  const seen = new Set<string>();
  const edges: GraphEdgeSpec[] = [];
  for (const e of g.edges) {
    const source = relationOf.get(e.source);
    const target = relationOf.get(e.target);
    if (!source || !target || source === target) continue;
    const id = `${source}->${target}`;
    if (seen.has(id)) continue;
    seen.add(id);
    edges.push({ id, source, target, arrow: "target" });
  }
  return { nodes, edges };
}
