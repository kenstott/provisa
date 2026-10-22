// Copyright (c) 2026 Kenneth Stott
// Canary: e54de4eb-5d25-469b-9dc4-2aea1bd491e0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2124: the relationships map's model — a run's columns as nodes, its correlations and its
// dependency network as edges, laid out in 3D by the shared force layout.

import type { ProfileRunResults } from "../../../../api/profiler";
import type { TerrainFamily } from "../terrain/terrainModel";
import { forceLayout } from "../forceLayout";

export type NodeFamily = TerrainFamily | "foreign";
export type EdgeKind = "spearman" | "correlation_ratio" | "dependence";

export interface RelNode {
  id: string;
  family: NodeFamily;
  x: number;
  y: number;
  z: number;
}

export interface RelEdge {
  id: string;
  /** For a dependence, ``a`` is the parent and ``b`` the column that depends on it. */
  a: string;
  b: string;
  kind: EdgeKind;
  /** Spearman is signed; the other measures run 0 to 1. */
  value: number;
  strength: number;
  rows: number | null;
  mutualInformation: number | null;
}

export interface RelationsModel {
  nodes: RelNode[];
  edges: RelEdge[];
}

const num = (v: unknown): number | null => (typeof v === "number" && Number.isFinite(v) ? v : null);

export function buildRelations(results: ProfileRunResults): RelationsModel {
  const family = new Map<string, NodeFamily>();
  for (const c of results.columns ?? [])
    family.set(String(c.column_name), String(c.family) as TerrainFamily);
  const edges: RelEdge[] = [];
  const seen = new Set<string>();
  for (const r of results.correlations ?? []) {
    const value = num(r.value);
    if (value == null) continue;
    const a = String(r.column_name);
    const b = String(r.other_column);
    const kind = String(r.measure) as EdgeKind;
    const id = `${kind}:${[a, b].sort().join("|")}`;
    if (seen.has(id)) continue;
    seen.add(id);
    edges.push({
      id,
      a,
      b,
      kind,
      value,
      strength: Math.min(1, Math.abs(value)),
      rows: num(r.rows),
      mutualInformation: null,
    });
  }
  for (const r of results.dependencies ?? []) {
    if (r.in_network !== true) continue;
    const child = String(r.column_name);
    const uncertainty = num(r.uncertainty);
    if (uncertainty == null) continue;
    for (const parent of [r.parent_1, r.parent_2]) {
      if (parent == null) continue;
      const a = String(parent);
      const id = `dependence:${a}>${child}`;
      if (seen.has(id)) continue;
      seen.add(id);
      edges.push({
        id,
        a,
        b: child,
        kind: "dependence",
        value: uncertainty,
        strength: Math.min(1, uncertainty),
        rows: null,
        mutualInformation: num(r.mutual_information),
      });
    }
  }
  const ids = [...family.keys()];
  for (const e of edges) {
    for (const id of [e.a, e.b]) {
      if (!family.has(id)) {
        // A parent table's column, named <relationship>.<column>.
        family.set(id, "foreign");
        ids.push(id);
      }
    }
  }
  const pos = forceLayout(ids, edges);
  const nodes = ids.map((id) => {
    const [x, y, z] = pos.get(id) as [number, number, number];
    return { id, family: family.get(id) as NodeFamily, x, y, z };
  });
  return { nodes, edges };
}

/** The edges a view shows: at least ``min`` strong, of the kinds switched on. */
export function visibleEdges(m: RelationsModel, min: number, kinds: Set<EdgeKind>): RelEdge[] {
  return m.edges.filter((e) => e.strength >= min && kinds.has(e.kind));
}

/** A node's edges, strongest first. */
export function edgesOf(edges: RelEdge[], node: string): RelEdge[] {
  return edges.filter((e) => e.a === node || e.b === node).sort((x, y) => y.strength - x.strength);
}
