// Copyright (c) 2026 Kenneth Stott
// Canary: 8e2b5d1c-47a9-4f36-b0c8-1d93a6e7f524
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174: the layered layout. Each edge kind RANKS (one end above the other), FLOWS (one end
// before the other) or is LATERAL (neither). A layered layout puts each node one level below the
// deepest node ranking or flowing into it, keeps every parent, orders each level by barycenter
// sweeps, and ignores an edge that closes a cycle. Ranks run top-down, flows left-to-right.
// Lateral edges place nothing: the scene draws them as arcs over the rest.

import type { EdgeKind, GraphData, GraphEdge } from "./graphModel";

export type EdgeClass = "rank" | "flow" | "lateral";

/** A term edge type's class, and whether its ``a`` end is the earlier / upper one. */
const TERM_EDGE: Record<string, { cls: EdgeClass; aFirst: boolean }> = {
  // a is a kind / part / value of b: b above.
  KIND_OF: { cls: "rank", aFirst: false },
  PART_OF: { cls: "rank", aFirst: false },
  VALID_VALUE_OF: { cls: "rank", aFirst: false },
  // a is derived from b, a replaces b: b before.
  DERIVED_FROM: { cls: "flow", aFirst: false },
  REPLACES: { cls: "flow", aFirst: false },
  RELATED_TO: { cls: "lateral", aFirst: true },
  SYNONYM_OF: { cls: "lateral", aFirst: true },
  PREFERRED_TERM_FOR: { cls: "lateral", aFirst: true },
  TRANSLATION_OF: { cls: "lateral", aFirst: true },
  ANTONYM_OF: { cls: "lateral", aFirst: true },
};

/** Every other kind: its class, and whether ``a`` is the upper / earlier end. */
const KIND: Record<Exclude<EdgeKind, "term_edge">, { cls: EdgeClass; aFirst: boolean }> = {
  // The edge runs from the member to its domain: the domain is above.
  in_domain: { cls: "rank", aFirst: false },
  member: { cls: "rank", aFirst: true },
  has_column: { cls: "rank", aFirst: true },
  term_binding: { cls: "rank", aFirst: true },
  command: { cls: "rank", aFirst: true },
  checks: { cls: "rank", aFirst: true },
  profiled: { cls: "rank", aFirst: true },
  owns: { cls: "rank", aFirst: true },
  lineage: { cls: "flow", aFirst: true },
  column_lineage: { cls: "flow", aFirst: true },
  product_link: { cls: "lateral", aFirst: true },
  relationship: { cls: "lateral", aFirst: true },
  relationship_outside: { cls: "lateral", aFirst: true },
  tagged: { cls: "lateral", aFirst: true },
};

function edgeInfo(e: GraphEdge): { cls: EdgeClass; aFirst: boolean } {
  if (e.kind !== "term_edge") return KIND[e.kind];
  const info = TERM_EDGE[e.label ?? ""];
  if (!info) throw new Error(`term edge ${e.id} has no known type: ${e.label}`);
  return info;
}

export function edgeClass(e: GraphEdge): EdgeClass {
  return edgeInfo(e).cls;
}

/** The edges that set levels, each as [from, to]: ``from`` above or before ``to``. */
function directedEdges(g: GraphData): { cls: EdgeClass; from: string; to: string; id: string }[] {
  const out = [];
  for (const e of g.edges) {
    const { cls, aFirst } = edgeInfo(e);
    if (cls === "lateral" || e.a === e.b) continue;
    out.push(aFirst ? { cls, from: e.a, to: e.b, id: e.id } : { cls, from: e.b, to: e.a, id: e.id });
  }
  return out.sort((x, y) => x.id.localeCompare(y.id));
}

/** The ids of the directed edges that close a cycle (a depth-first walk in id order). */
function backEdges(ids: string[], edges: { from: string; to: string; id: string }[]): Set<string> {
  const out = new Map<string, { to: string; id: string }[]>();
  for (const e of edges) out.set(e.from, [...(out.get(e.from) ?? []), e]);
  const state = new Map<string, 1 | 2>();
  const back = new Set<string>();
  const visit = (id: string) => {
    state.set(id, 1);
    for (const e of out.get(id) ?? []) {
      const s = state.get(e.to);
      if (s === 1) back.add(e.id);
      else if (s === undefined) visit(e.to);
    }
    state.set(id, 2);
  };
  for (const id of ids) if (!state.has(id)) visit(id);
  return back;
}

/** How a graph is laid out when it opens (REQ-2174), and whether Layered is possible. */
export function layeredFit(g: GraphData): { layered: boolean; possible: boolean } {
  const directed = directedEdges(g);
  const ids = g.nodes.map((n) => n.id).sort();
  const possible = backEdges(ids, directed).size === 0;
  const classes = new Set(directed.map((e) => e.cls));
  return { layered: possible && classes.size === 1, possible };
}

export interface Levels {
  /** Each level's node ids in order. */
  levels: string[][];
  /** Levels run left-to-right (the drawn edges all flow), else top-down. */
  across: boolean;
  /** The directed edges kept (cycle-closing ones dropped), as [from, to]. */
  edges: [string, string][];
}

const SWEEPS = 24;

/** Crossings between the edges of adjacent levels. */
export function crossings(levels: string[][], edges: [string, string][]): number {
  const level = new Map<string, number>();
  const pos = new Map<string, number>();
  levels.forEach((l, i) =>
    l.forEach((id, j) => {
      level.set(id, i);
      pos.set(id, j);
    }),
  );
  const between = new Map<number, [number, number][]>();
  for (const [f, t] of edges) {
    const lf = level.get(f) as number;
    if (level.get(t) !== lf + 1) continue;
    between.set(lf, [...(between.get(lf) ?? []), [pos.get(f) as number, pos.get(t) as number]]);
  }
  let n = 0;
  for (const list of between.values())
    for (let i = 0; i < list.length; i++)
      for (let j = i + 1; j < list.length; j++)
        if ((list[i][0] - list[j][0]) * (list[i][1] - list[j][1]) < 0) n++;
  return n;
}

/** The levels of ``g`` and the order within each, by barycenter sweeps. */
export function layerLevels(g: GraphData): Levels {
  const directed = directedEdges(g);
  const ids = g.nodes.map((n) => n.id).sort();
  const back = backEdges(ids, directed);
  const kept = directed.filter((e) => !back.has(e.id));
  const parents = new Map<string, string[]>();
  const children = new Map<string, string[]>();
  for (const e of kept) {
    parents.set(e.to, [...(parents.get(e.to) ?? []), e.from]);
    children.set(e.from, [...(children.get(e.from) ?? []), e.to]);
  }
  // One level below the deepest parent.
  const depth = new Map<string, number>();
  const depthOf = (id: string): number => {
    const known = depth.get(id);
    if (known !== undefined) return known;
    const d = Math.max(-1, ...(parents.get(id) ?? []).map(depthOf)) + 1;
    depth.set(id, d);
    return d;
  };
  for (const id of ids) depthOf(id);
  const deepest = Math.max(0, ...depth.values());
  const levels: string[][] = Array.from({ length: deepest + 1 }, () => []);
  for (const id of ids) levels[depth.get(id) as number].push(id);

  const edges = kept.map((e) => [e.from, e.to] as [string, string]);
  let best = levels.map((l) => [...l]);
  let bestCross = crossings(best, edges);
  const order = levels.map((l) => [...l]);
  // A node's key is the mean place, as a fraction of its level, of its neighbours on one side.
  const sweep = (l: number, side: Map<string, string[]>) => {
    const place = new Map<string, number>();
    for (const row of order) row.forEach((id, i) => place.set(id, (i + 0.5) / row.length));
    const keyed = order[l].map((id, i) => {
      const nb = side.get(id) ?? [];
      const key = nb.length === 0 ? place.get(id) as number : nb.reduce((s, n) => s + (place.get(n) as number), 0) / nb.length;
      return { id, key, i };
    });
    keyed.sort((x, y) => x.key - y.key || x.i - y.i);
    order[l] = keyed.map((k) => k.id);
  };
  for (let s = 0; s < SWEEPS && bestCross > 0; s++) {
    if (s % 2 === 0) for (let l = 1; l <= deepest; l++) sweep(l, parents);
    else for (let l = deepest - 1; l >= 0; l--) sweep(l, children);
    const c = crossings(order, edges);
    if (c < bestCross) {
      bestCross = c;
      best = order.map((r) => [...r]);
    }
  }
  return { levels: best, across: kept.length > 0 && kept.every((e) => e.cls === "flow"), edges };
}

/** Non-decreasing fit of ``want`` with at least ``gap`` between neighbours (pool adjacent violators
 * on the values less their index): each node as near its wish as the spacing allows. */
function spaced(want: number[], gap: number): number[] {
  const blocks: { sum: number; n: number }[] = [];
  want.forEach((w, i) => {
    blocks.push({ sum: w - i * gap, n: 1 });
    while (blocks.length > 1) {
      const b = blocks[blocks.length - 1];
      const a = blocks[blocks.length - 2];
      if (a.sum / a.n <= b.sum / b.n) break;
      blocks.splice(blocks.length - 2, 2, { sum: a.sum + b.sum, n: a.n + b.n });
    }
  });
  const out: number[] = [];
  for (const b of blocks) for (let k = 0; k < b.n; k++) out.push(b.sum / b.n + out.length * gap);
  return out;
}

const LEVEL_GAP = 3;
/** Wider across than down: a level's names sit side by side. */
const SPREAD = 5;

/** Each node's place: levels top-down, or left-to-right where only flows set them. */
export function layeredLayout(g: GraphData): Map<string, [number, number, number]> {
  const { levels, across, edges } = layerLevels(g);
  const parents = new Map<string, string[]>();
  const children = new Map<string, string[]>();
  for (const [f, t] of edges) {
    parents.set(t, [...(parents.get(t) ?? []), f]);
    children.set(f, [...(children.get(f) ?? []), t]);
  }
  const x = new Map<string, number>();
  levels.forEach((row) => row.forEach((id, i) => x.set(id, i)));
  const settle = (row: string[], wish: (id: string) => number) => {
    const placed = spaced(row.map(wish), 1);
    row.forEach((id, i) => x.set(id, placed[i]));
  };
  // Down, a node under the mean of its parents; up, over the middle of its children; ends up.
  for (let pass = 0; pass < 4; pass++) {
    for (let l = 1; l < levels.length; l++)
      settle(levels[l], (id) => {
        const p = parents.get(id);
        return p ? p.reduce((s, n) => s + (x.get(n) as number), 0) / p.length : (x.get(id) as number);
      });
    for (let l = levels.length - 2; l >= 0; l--)
      settle(levels[l], (id) => {
        const c = children.get(id);
        if (!c) return x.get(id) as number;
        const at = c.map((n) => x.get(n) as number);
        return (Math.min(...at) + Math.max(...at)) / 2;
      });
  }
  const all = [...x.values()];
  const mid = all.length === 0 ? 0 : (Math.min(...all) + Math.max(...all)) / 2;
  const deepest = levels.length - 1;
  const out = new Map<string, [number, number, number]>();
  levels.forEach((row, l) =>
    row.forEach((id) => {
      const cross = ((x.get(id) as number) - mid) * SPREAD;
      out.set(
        id,
        across
          ? [(l - deepest / 2) * SPREAD * 1.6, -cross, 0]
          : [cross, (deepest / 2 - l) * LEVEL_GAP, 0],
      );
    }),
  );
  return out;
}
