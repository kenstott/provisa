// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2124: the relationships map's model — a run's columns as nodes, its correlations and its
// dependency network as edges, laid out in 3D by a seeded force simulation so the same run always
// lands the same way.

import type { ProfileRunResults } from "../../../../api/profiler";
import type { TerrainFamily } from "../terrain/terrainModel";

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

const LAYOUT_RADIUS = 6;
const ITERATIONS = 360;

const num = (v: unknown): number | null => (typeof v === "number" && Number.isFinite(v) ? v : null);

/** Fruchterman–Reingold in three dimensions from a golden-spiral start; deterministic. */
function layout(ids: string[], edges: RelEdge[]): Map<string, [number, number, number]> {
  const n = ids.length;
  const index = new Map(ids.map((id, i) => [id, i]));
  const p = ids.map((_, i) => {
    const y = n === 1 ? 0 : 1 - (2 * i) / (n - 1);
    const r = Math.sqrt(1 - y * y);
    const phi = i * Math.PI * (3 - Math.sqrt(5));
    return [
      Math.cos(phi) * r * LAYOUT_RADIUS,
      y * LAYOUT_RADIUS,
      Math.sin(phi) * r * LAYOUT_RADIUS,
    ];
  });
  const k = Math.cbrt((4 * LAYOUT_RADIUS ** 3) / Math.max(1, n));
  const springs = edges.map(
    (e) => [index.get(e.a) as number, index.get(e.b) as number, e.strength] as const,
  );
  let temp = LAYOUT_RADIUS / 2;
  for (let it = 0; it < ITERATIONS; it++) {
    const d = p.map(() => [0, 0, 0]);
    for (let i = 0; i < n; i++) {
      for (let j = i + 1; j < n; j++) {
        const dx = p[i][0] - p[j][0];
        const dy = p[i][1] - p[j][1];
        const dz = p[i][2] - p[j][2];
        const dist = Math.max(0.01, Math.hypot(dx, dy, dz));
        const f = (k * k) / dist / dist;
        d[i][0] += dx * f;
        d[i][1] += dy * f;
        d[i][2] += dz * f;
        d[j][0] -= dx * f;
        d[j][1] -= dy * f;
        d[j][2] -= dz * f;
      }
    }
    for (const [i, j, w] of springs) {
      const dx = p[i][0] - p[j][0];
      const dy = p[i][1] - p[j][1];
      const dz = p[i][2] - p[j][2];
      const dist = Math.max(0.01, Math.hypot(dx, dy, dz));
      const f = ((dist / k) * (0.3 + w)) / 1.3;
      d[i][0] -= dx * f;
      d[i][1] -= dy * f;
      d[i][2] -= dz * f;
      d[j][0] += dx * f;
      d[j][1] += dy * f;
      d[j][2] += dz * f;
    }
    for (let i = 0; i < n; i++) {
      // A pull toward the centre keeps unconnected columns from drifting off.
      d[i][0] -= p[i][0] * 0.05;
      d[i][1] -= p[i][1] * 0.05;
      d[i][2] -= p[i][2] * 0.05;
      const len = Math.max(0.0001, Math.hypot(d[i][0], d[i][1], d[i][2]));
      const step = Math.min(len, temp);
      p[i][0] += (d[i][0] / len) * step;
      p[i][1] += (d[i][1] / len) * step;
      p[i][2] += (d[i][2] / len) * step;
    }
    temp *= 0.985;
  }
  let maxR = 0;
  for (const q of p) maxR = Math.max(maxR, Math.hypot(q[0], q[1], q[2]));
  const scale = maxR > 0 ? LAYOUT_RADIUS / maxR : 1;
  return new Map(ids.map((id, i) => [id, [p[i][0] * scale, p[i][1] * scale, p[i][2] * scale]]));
}

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
  const pos = layout(ids, edges);
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
