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
// graph says how it is drawn. REQ-2174 (amended 2026-11-07): nothing downstream is drawn.

import type { ProductLineage } from "../../../api/lineage";
import type {
  GraphEdgeSpec,
  GraphNodeSpec,
  GraphSpec,
} from "../../../components/provisa-graph/model";

export const LANE_INPUTS = "inputs";
export const LANE_TABLES = "tables";
/** The lane ``hops`` beyond the inputs, upstream. */
export const upstreamLane = (hops: number) => `upstream-${hops}`;

/** The graph, and its lanes left to right: the furthest upstream first, the product's tables last.
 *  ``hops`` is 0 for the tables, -1 for the inputs, -n-1 for n hops beyond the inputs. */
export interface ProductLineageView {
  spec: GraphSpec;
  lanes: { id: string; hops: number }[];
}

export function productLineageSpec(
  g: ProductLineage,
  label: (relation: string) => string,
): ProductLineageView {
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
  // Relation hops, each once.
  const feeds = new Map<string, Set<string>>(); // relation -> the relations it reads
  for (const e of g.edges) {
    const source = relationOf.get(e.source);
    const target = relationOf.get(e.target);
    if (!source || !target || source === target) continue;
    const read = feeds.get(target) ?? new Set<string>();
    read.add(source);
    feeds.set(target, read);
  }
  const readers = new Map<string, Set<string>>(); // relation -> the relations reading it
  for (const [target, sources] of feeds)
    for (const source of sources) {
      const read = readers.get(source) ?? new Set<string>();
      read.add(target);
      readers.set(source, read);
    }
  /** Every relation reached from ``start`` along ``next``, by its fewest hops. */
  const reach = (
    start: Iterable<string>,
    next: Map<string, Set<string>>,
    stop: (r: string) => boolean,
  ) => {
    const hops = new Map<string, number>();
    let frontier = [...start];
    for (let hop = 1; frontier.length > 0; hop++) {
      const found: string[] = [];
      for (const rel of frontier)
        for (const n of next.get(rel) ?? [])
          if (!stop(n) && !hops.has(n)) {
            hops.set(n, hop);
            found.push(n);
          }
      frontier = found;
    }
    return hops;
  };
  const named = (r: string) => members.has(r) || inputs.has(r);
  // REQ-1667: a relation between an input and a table (read from an input, read by a table) is no
  // lane: it is folded onto an indirect edge from what it reads to what reads it.
  const fromInputs = reach(inputs, readers, named);
  const toMembers = reach(members, feeds, named);
  const between = (rel: string) => fromInputs.has(rel) && toMembers.has(rel);
  // The rest upstream: by their hops beyond the inputs, else by their hops from the tables (one
  // hop: the product's nearest input, as far as its reader can see).
  const beyondInputs = reach(inputs, feeds, (r) => named(r) || between(r));
  const laneHops = (relation: string): number =>
    members.has(relation)
      ? 0
      : inputs.has(relation)
        ? -1
        : beyondInputs.has(relation)
          ? -(beyondInputs.get(relation) as number) - 1
          : -(toMembers.get(relation) ?? 1);
  const laneId = (hops: number) =>
    hops === 0 ? LANE_TABLES : hops === -1 ? LANE_INPUTS : upstreamLane(-hops - 1);
  const drawnSources = (rel: string, via: string[], seenRels: Set<string>): [string, string[]][] =>
    [...(feeds.get(rel) ?? [])].flatMap((up): [string, string[]][] => {
      if (seenRels.has(up)) return [];
      if (!between(up)) return [[up, via]];
      return drawnSources(up, [up, ...via], new Set([...seenRels, up]));
    });
  const edges: GraphEdgeSpec[] = [];
  const seen = new Set<string>();
  for (const target of feeds.keys()) {
    if (between(target)) continue;
    for (const [source, via] of drawnSources(target, [], new Set([target]))) {
      const id = `${source}->${target}`;
      if (seen.has(id)) continue;
      seen.add(id);
      edges.push({
        id,
        source,
        target,
        arrow: "target",
        ...(via.length > 0 ? { dashed: true, label: via.map(label).join(" → ") } : {}),
      });
    }
  }
  const nodes: GraphNodeSpec[] = [...columns.entries()]
    .filter(([relation]) => !between(relation))
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([relation, cols]) => ({
      id: relation,
      type: members.has(relation) ? "table" : inputs.has(relation) ? "source" : "view",
      name: label(relation),
      lane: laneId(laneHops(relation)),
      lines: [...cols].sort().map((text) => ({ text })),
      data: { relation },
    }));
  const lanes = new Map<string, number>();
  for (const n of nodes) lanes.set(n.lane as string, laneHops(n.id));
  return {
    spec: { nodes, edges },
    lanes: [...lanes.entries()].map(([id, hops]) => ({ id, hops })).sort((a, b) => a.hops - b.hops),
  };
}
