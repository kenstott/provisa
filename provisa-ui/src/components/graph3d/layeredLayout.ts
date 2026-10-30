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
// deepest node ranking or flowing into it, keeps every parent, orders each level to cut crossings
// (orderLevels: median sweeps, transposes, the best order kept), and ignores an edge that closes a
// cycle. Ranks run top-down, flows left-to-right. Lateral edges place nothing and count no
// crossing: the scene draws them as arcs over the rest.

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

// Crossing minimisation: alternating down / up sweeps ordering each level by the median (or the
// mean) place of its neighbours on the side just settled, then an adjacent-swap (transpose) pass
// and a sifting pass (each node moved to the place in its level where its edges cross least), the
// best order seen kept. It starts from the order given and from two that put the nodes sharing a
// neighbour together (one from above, one from below), with each key, and keeps the best of the
// six. A search stops at no crossing, after SWEEPS sweeps, or when STALE sweeps in a row found
// nothing better; the best order is then polished by transposes and sifting until they gain
// nothing. An edge spanning more than one level passes each level between through a stand-in, so
// its crossings count like any other's.
const SWEEPS = 24;
const STALE = 6;
/** Adjacent-swap passes over one level at most, per transpose. */
const TRANSPOSE_PASSES = 8;
/** Polishing rounds at most, after the sweeps. */
const POLISH = 8;
/** Sifting tries every place in a level: a level longer than this is left to the sweeps and
 * transposes (its cost grows with the square of the level). */
const SIFT_MAX = 400;

/** A long edge's stand-in on a level it passes. Real ids never start with NUL. */
const standIn = (edge: number, level: number) => `\u0000${edge}:${level}`;
const isStandIn = (id: string) => id.startsWith("\u0000");

/** Levels and one-level edges: each long edge cut at the levels it passes. */
interface Proper {
  levels: string[][];
  /** Each node's neighbours one level up and one level down. */
  up: Map<string, string[]>;
  down: Map<string, string[]>;
}

function properLevels(levels: string[][], edges: [string, string][]): Proper {
  const level = new Map<string, number>();
  levels.forEach((row, i) => row.forEach((id) => level.set(id, i)));
  const out = levels.map((row) => [...row]);
  const up = new Map<string, string[]>();
  const down = new Map<string, string[]>();
  const link = (f: string, t: string) => {
    down.set(f, [...(down.get(f) ?? []), t]);
    up.set(t, [...(up.get(t) ?? []), f]);
  };
  edges.forEach(([f, t], k) => {
    const lf = level.get(f);
    const lt = level.get(t);
    if (lf === undefined || lt === undefined) throw new Error(`edge ${f}>${t} names a node on no level`);
    if (lt <= lf) throw new Error(`edge ${f}>${t} does not run to a lower level`);
    let prev = f;
    for (let l = lf + 1; l < lt; l++) {
      const s = standIn(k, l);
      out[l].push(s);
      link(prev, s);
      prev = s;
    }
    link(prev, t);
  });
  return { levels: out, up, down };
}

/** Pairs of one-level edges that cross, between every two adjacent levels. */
function countProper(p: Proper): number {
  let n = 0;
  for (let l = 0; l + 1 < p.levels.length; l++) {
    const below = new Map(p.levels[l + 1].map((id, i) => [id, i]));
    // Each edge as (upper place, lower place), sorted; crossings are the inversions of the lower
    // places, counted with a Fenwick tree.
    const pairs: [number, number][] = [];
    p.levels[l].forEach((id, i) => {
      for (const t of p.down.get(id) ?? []) pairs.push([i, below.get(t) as number]);
    });
    pairs.sort((a, b) => a[0] - b[0] || a[1] - b[1]);
    const size = p.levels[l + 1].length;
    const tree = new Array<number>(size + 1).fill(0);
    let seen = 0;
    for (const [, lo] of pairs) {
      let greater = seen;
      for (let i = lo + 1; i > 0; i -= i & -i) greater -= tree[i];
      n += greater;
      for (let i = lo + 1; i <= size; i += i & -i) tree[i] += 1;
      seen += 1;
    }
  }
  return n;
}

/** Crossings between the edges of adjacent levels, an edge spanning levels counted at each one it
 * passes. */
export function crossings(levels: string[][], edges: [string, string][]): number {
  return countProper(properLevels(levels, edges));
}

const median = (xs: number[]) => {
  const s = [...xs].sort((a, b) => a - b);
  const m = s.length >> 1;
  return s.length % 2 === 1 ? s[m] : (s[m - 1] + s[m]) / 2;
};

/** The order ``p`` starts in, each level after the first in the order its upper neighbours reach
 * it: nodes sharing a neighbour side by side. Nodes nothing above reaches keep their order, last. */
function grouped(p: Proper, fixed: ReadonlySet<number>): string[][] {
  const out = [p.levels[0]];
  for (let l = 1; l < p.levels.length; l++) {
    if (fixed.has(l)) {
      out.push(p.levels[l]);
      continue;
    }
    const here = new Set(p.levels[l]);
    const seen = new Set<string>();
    const row: string[] = [];
    for (const id of out[l - 1])
      for (const t of p.down.get(id) ?? [])
        if (here.has(t) && !seen.has(t)) {
          seen.add(t);
          row.push(t);
        }
    for (const id of p.levels[l]) if (!seen.has(id)) row.push(id);
    out.push(row);
  }
  return out;
}

/** The order ``p`` starts in, each free level after the first sorted by the mean key of its
 * upper neighbours (a node's key: its place, on the first level or a fixed one, else that mean),
 * then each free level above the last regrouped in the order its lower neighbours sit: nodes
 * sharing a lower neighbour (a table's columns) side by side. Nodes with no lower neighbour keep
 * their order, last. */
function groupedBelow(p: Proper, fixed: ReadonlySet<number>): string[][] {
  const out = p.levels.map((row) => [...row]);
  const key = new Map<string, number>();
  const own = (l: number) => out[l].forEach((id, i) => key.set(id, (i + 0.5) / out[l].length));
  own(0);
  for (let l = 1; l < out.length; l++) {
    if (fixed.has(l)) {
      own(l);
      continue;
    }
    const was = new Map(out[l].map((id, i) => [id, i]));
    out[l].forEach((id, i) => {
      const ns = p.up.get(id) ?? [];
      key.set(
        id,
        ns.length === 0
          ? (i + 0.5) / out[l].length
          : ns.reduce((sum, n) => sum + (key.get(n) as number), 0) / ns.length,
      );
    });
    out[l].sort(
      (x, y) =>
        (key.get(x) as number) - (key.get(y) as number) ||
        (was.get(x) as number) - (was.get(y) as number),
    );
  }
  for (let l = out.length - 2; l >= 0; l--) {
    if (fixed.has(l)) continue;
    // Each lower node's upper neighbours on this level, in this level's order.
    const ups = new Map<string, string[]>();
    for (const id of out[l])
      for (const t of p.down.get(id) ?? []) ups.set(t, [...(ups.get(t) ?? []), id]);
    const seen = new Set<string>();
    const row: string[] = [];
    for (const t of out[l + 1])
      for (const id of ups.get(t) ?? [])
        if (!seen.has(id)) {
          seen.add(id);
          row.push(id);
        }
    for (const id of out[l]) if (!seen.has(id)) row.push(id);
    out[l] = row;
  }
  return out;
}

/** Sweeps, transposes and sifting from ``start``, levels keyed by ``key``: the best order seen
 * and its crossings. */
function search(
  p: Proper,
  start: string[][],
  fixed: ReadonlySet<number>,
  key: "median" | "mean",
): { order: string[][]; crossings: number } {
  let order = start.map((row) => [...row]);
  const place = new Map<string, number>();
  const setPlaces = (l: number) => order[l].forEach((id, i) => place.set(id, i));
  order.forEach((_, l) => setPlaces(l));
  const frac = (id: string, l: number) => ((place.get(id) as number) + 0.5) / order[l].length;
  const count = () => countProper({ ...p, levels: order });
  const last = order.length - 1;
  const free = (l: number) => !fixed.has(l);

  // A level sorted by its nodes' key (then the other one) among their neighbours on ``side``,
  // ``nb`` levels away; a node with none there keeps its own place. Ties keep the current order.
  const sweep = (l: number, side: Map<string, string[]>, nb: number) => {
    const keyed = order[l].map((id, i) => {
      const ns = side.get(id) ?? [];
      if (ns.length === 0) {
        const own = frac(id, l);
        return { id, a: own, b: own, i };
      }
      const ps = ns.map((n) => frac(n, nb));
      const med = median(ps);
      const mean = ps.reduce((sum, v) => sum + v, 0) / ps.length;
      return key === "median" ? { id, a: med, b: mean, i } : { id, a: mean, b: med, i };
    });
    keyed.sort((x, y) => x.a - y.a || x.b - y.b || x.i - y.i);
    order[l] = keyed.map((k) => k.id);
    setPlaces(l);
  };

  // Crossings between v's and w's edges (both sides) with v placed before w.
  const pairCross = (v: string, w: string) => {
    let n = 0;
    for (const side of [p.up, p.down]) {
      const vs = side.get(v) ?? [];
      const ws = side.get(w) ?? [];
      for (const a of vs)
        for (const b of ws) if ((place.get(a) as number) > (place.get(b) as number)) n++;
    }
    return n;
  };
  const transpose = (l: number) => {
    for (let pass = 0; pass < TRANSPOSE_PASSES; pass++) {
      let swapped = false;
      const row = order[l];
      for (let i = 0; i + 1 < row.length; i++) {
        const v = row[i];
        const w = row[i + 1];
        if (pairCross(w, v) < pairCross(v, w)) {
          row[i] = w;
          row[i + 1] = v;
          place.set(w, i);
          place.set(v, i + 1);
          swapped = true;
        }
      }
      if (!swapped) return;
    }
  };
  // Each node in turn moved to the first place in its level where its edges cross least.
  const sift = (l: number) => {
    if (order[l].length > SIFT_MAX) return;
    for (const v of [...order[l]]) {
      const row = order[l].filter((id) => id !== v);
      let cost = row.reduce((sum, u) => sum + pairCross(v, u), 0);
      let bestAt = 0;
      let bestCost = cost;
      row.forEach((u, j) => {
        cost += pairCross(u, v) - pairCross(v, u);
        if (cost < bestCost) {
          bestCost = cost;
          bestAt = j + 1;
        }
      });
      row.splice(bestAt, 0, v);
      order[l] = row;
      setPlaces(l);
    }
  };
  const refine = () => {
    for (let l = 0; l <= last; l++)
      if (free(l)) {
        transpose(l);
        sift(l);
      }
  };

  let best = order.map((row) => [...row]);
  let bestCross = count();
  const keep = () => {
    const c = count();
    if (c >= bestCross) return false;
    bestCross = c;
    best = order.map((row) => [...row]);
    return true;
  };
  let stale = 0;
  for (let s = 0; s < SWEEPS && bestCross > 0 && stale < STALE; s++) {
    for (let l = 1; l <= last; l++) if (free(l)) sweep(l, p.up, l - 1);
    for (let l = last - 1; l >= 0; l--) if (free(l)) sweep(l, p.down, l + 1);
    refine();
    stale = keep() ? 0 : stale + 1;
  }
  order = best.map((row) => [...row]);
  order.forEach((_, l) => setPlaces(l));
  for (let r = 0; r < POLISH && bestCross > 0; r++) {
    refine();
    if (!keep()) break;
  }
  return { order: best, crossings: bestCross };
}

/** ``levels`` reordered to cut the crossings of ``edges`` (each [upper, lower], spanning one level
 * or more); the levels in ``fixed`` keep their order. Same input, same order. */
export function orderLevels(
  levels: string[][],
  edges: [string, string][],
  fixed: ReadonlySet<number>,
): string[][] {
  const p = properLevels(levels, edges);
  let won: { order: string[][]; crossings: number } | null = null;
  for (const start of [p.levels, grouped(p, fixed), groupedBelow(p, fixed)])
    for (const key of ["median", "mean"] as const) {
      const found = search(p, start, fixed, key);
      if (won === null || found.crossings < won.crossings) won = found;
    }
  return (won as { order: string[][] }).order.map((row) => row.filter((id) => !isStandIn(id)));
}

/** The levels of ``g`` and the order within each, crossings cut (orderLevels). */
export function layerLevels(g: GraphData): Levels {
  const directed = directedEdges(g);
  const ids = g.nodes.map((n) => n.id).sort();
  const back = backEdges(ids, directed);
  const kept = directed.filter((e) => !back.has(e.id));
  const parents = new Map<string, string[]>();
  for (const e of kept) parents.set(e.to, [...(parents.get(e.to) ?? []), e.from]);
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
  return {
    levels: orderLevels(levels, edges, new Set()),
    across: kept.length > 0 && kept.every((e) => e.cls === "flow"),
    edges,
  };
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
