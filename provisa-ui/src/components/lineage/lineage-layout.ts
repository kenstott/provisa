// Copyright (c) 2026 Kenneth Stott
// Canary: 3e7d9a52-1b64-4c08-8f2a-6d5c0e9b7a41
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1665: the lineage DAG as relation boxes with column rows, the shape the SQL canvas draws.
//
// A column-per-node layout has no notion of a relation as a box, so a federation graph sprawls: a
// layout engine spreads the columns of one table across the canvas and the reader loses which
// dataset a column belongs to. Here a relation is ONE box whose rows are its columns, edges run row
// to row, and only the boxes are laid out — rows are pinned inside their box in a stable order.
//
// Intermediate computations get boxes too: a CTE or subquery is a relation the lineage already
// names, and the statement's final projection becomes the `result` box (or the view it defines).
// Rows that no edge touches are not drawn; the box header counts them, so a wide table referenced
// by two columns stays two rows tall.
//
// Pure: no cytoscape here. The component turns these into elements and positions.

import type { LineageGraphData, LineageNode } from "../../api/lineage";

export type Role = "source" | "intermediate" | "output" | "command" | "dataset";

/** How a field of each role is typeset — one definition for the DAG and its legend. */
export interface RoleStyle {
  color: string;
  fill: string;
  fontStyle: "normal" | "italic";
  fontWeight: "normal" | "bold";
}
export const ROLE_STYLE: Record<Role, RoleStyle> = {
  // a base column leaf
  source: { color: "#2f9e44", fill: "#ffffff", fontStyle: "normal", fontWeight: "normal" },
  // produced AND consumed: a hand-off between datasets
  intermediate: { color: "#0c8599", fill: "#ffffff", fontStyle: "normal", fontWeight: "normal" },
  // a terminal result column
  output: { color: "#1c7ed6", fill: "#ffffff", fontStyle: "italic", fontWeight: "normal" },
  // an opaque command boundary
  command: { color: "#9c36b5", fill: "#ffffff", fontStyle: "normal", fontWeight: "normal" },
  // a collapsed relation
  dataset: { color: "#5c7cfa", fill: "#ffffff", fontStyle: "normal", fontWeight: "bold" },
};

export interface LineageRow {
  id: string;
  label: string;
  role: Role;
  kind: LineageNode["kind"];
  output: boolean;
  materialized: boolean;
  cycle: string; // "no" | "feedback" | "error"
}

export interface LineageBox {
  id: string;
  /** The relation this box stands for; null for a synthetic box (result, a lone unresolved column). */
  relation: string | null;
  label: string;
  rows: LineageRow[];
  collapsed: boolean;
  /** Columns of the relation no edge touches — counted in the header, not drawn. */
  unused: number;
  /** REQ-1667: the swimlane, by distance from the lane-0 relations (negative = upstream). */
  lane: number | null;
  width: number;
  height: number;
}

export interface LineageDrawnEdge {
  id: string;
  source: string; // row id, or box id when that end is collapsed
  target: string;
  label: string;
  command: boolean;
  count: number;
}

export interface LineageLayoutModel {
  boxes: LineageBox[];
  /** Row-to-row (or box) edges, the ones drawn. */
  edges: LineageDrawnEdge[];
}

export const RESULT_BOX = "result";
export const boxId = (relation: string) => `rel:${relation}`;

// Geometry in canvas units. Monospace at 11px runs about 6.6px per glyph.
export const HEADER_H = 26;
export const ROW_H = 20;
export const FOOTER_H = 16;
export const BOX_MIN_W = 140;
export const BOX_MAX_W = 320;
const CHAR_W = 6.6;
const ROW_PAD_X = 12;

const textWidth = (s: string) => s.length * CHAR_W;

export interface LineageModelOptions {
  /** The box the statement's final projection lands in — the view or MV it defines, else "result". */
  resultLabel?: string;
  /** Draw every column as a row, not only the ones an edge touches (Complete Lineage: every column
   *  the role can see is in use, whether or not a registered view derives from it). */
  allColumns?: boolean;
  /** REQ-1667: relations that define lane 0. Every other box takes the lane of its distance from
   *  them — contributors -1, -2… by shortest upstream path, consumers +1. */
  lanesAround?: ReadonlySet<string>;
}

export function buildLineageModel(
  graph: LineageGraphData,
  collapsedRelations: ReadonlySet<string> | undefined,
  options: LineageModelOptions = {},
): LineageLayoutModel {
  const resultLabel = options.resultLabel ?? RESULT_BOX;
  const cycleClass: Record<string, string> = {};
  for (const c of graph.cycles ?? []) for (const n of c.nodes) cycleClass[n] = c.classification;
  const outputs = new Set(graph.outputs);
  const hasIn = new Set(graph.edges.map((e) => e.target));
  const hasOut = new Set(graph.edges.map((e) => e.source));
  // An output is always a row, edge or not: it is what the graph is FOR.
  const touched = new Set([...hasIn, ...hasOut, ...outputs]);

  const roleOf = (n: LineageNode): Role => {
    if (n.kind === "command") return "command";
    if (n.kind === "source") return "source";
    return hasIn.has(n.id) && hasOut.has(n.id) ? "intermediate" : "output";
  };

  // Which box each node lives in. A relation names its own box; the final projection's columns
  // share the result box; anything else unresolved stands alone as a one-row box.
  const boxOf = (n: LineageNode): { id: string; relation: string | null; label: string } => {
    if (n.relation) return { id: boxId(n.relation), relation: n.relation, label: n.relation };
    if (outputs.has(n.id) || n.kind === "derived")
      return { id: boxId(resultLabel), relation: null, label: resultLabel };
    return { id: `lone:${n.id}`, relation: null, label: n.column };
  };

  const boxes = new Map<string, LineageBox>();
  const nodeBox = new Map<string, string>();
  for (const n of graph.nodes) {
    const b = boxOf(n);
    nodeBox.set(n.id, b.id);
    let box = boxes.get(b.id);
    if (!box) {
      box = {
        id: b.id,
        relation: b.relation,
        label: b.label,
        rows: [],
        collapsed: !!b.relation && !!collapsedRelations?.has(b.relation),
        unused: 0,
        lane: null,
        width: 0,
        height: 0,
      };
      boxes.set(b.id, box);
    }
    // A lone unresolved column is its own box, so it is drawn even with no edge.
    if (!options.allColumns && !touched.has(n.id) && b.relation !== null) {
      box.unused += 1;
      continue;
    }
    box.rows.push({
      id: n.id,
      label: n.column,
      role: roleOf(n),
      kind: n.kind,
      output: outputs.has(n.id),
      materialized: n.materialized,
      cycle: cycleClass[n.id] ?? "no",
    });
  }

  for (const box of boxes.values()) {
    // Stable row order: the order the graph lists them, sources first inside a mixed box.
    const rank: Record<Role, number> = {
      source: 0,
      intermediate: 1,
      output: 2,
      command: 3,
      dataset: 4,
    };
    box.rows.sort((a, b) => rank[a.role] - rank[b.role]);
    const header = box.collapsed
      ? `${box.label}  (${box.rows.length + box.unused} cols)`
      : box.label;
    const widest = Math.max(
      textWidth(header) + 16,
      ...box.rows.map((r) => textWidth(r.label) + ROW_PAD_X * 2),
    );
    box.width = Math.min(BOX_MAX_W, Math.max(BOX_MIN_W, Math.ceil(widest)));
    box.height = box.collapsed
      ? HEADER_H + 6
      : HEADER_H + box.rows.length * ROW_H + (box.unused > 0 ? FOOTER_H : 0) + 4;
  }

  // The end an edge is drawn to: the row, or the whole box when that relation is collapsed.
  const drawnEnd = (nodeId: string): string => {
    const id = nodeBox.get(nodeId) ?? nodeId;
    const box = boxes.get(id);
    return box && box.collapsed ? box.id : nodeId;
  };

  const rolled = new Map<string, LineageDrawnEdge>();
  for (const e of graph.edges) {
    const source = drawnEnd(e.source);
    const target = drawnEnd(e.target);
    if (source === target) continue; // internal to one collapsed relation
    const label = (e.ops ?? [])
      .filter((o) => o.kind !== "identity" && o.kind !== "constant")
      .map((o) => (o.args?.length ? `${o.name}(${o.args.join(", ")})` : o.name))
      .join(" ");
    const command = (e.ops ?? []).some((o) => o.kind === "command");
    const key = `${source} ${target}`;
    const prior = rolled.get(key);
    if (prior) {
      prior.count += 1;
      prior.command = prior.command || command;
      prior.label = ""; // several transforms behind one drawn edge: report the count, not one formula
    } else {
      rolled.set(key, { id: `e${rolled.size}`, source, target, label, command, count: 1 });
    }
  }

  orderRowsByUpstream(boxes, nodeBox, graph);
  if (options.lanesAround) assignLanes(boxes, nodeBox, graph, options.lanesAround);

  return {
    boxes: Array.from(boxes.values()),
    edges: Array.from(rolled.values()),
  };
}

// Rows in a downstream box take the order of the rows that feed them (their mean upstream row
// index), so a column-for-column copy draws as parallel wires instead of a braid. Boxes are
// visited upstream-first; a row nothing feeds keeps its role rank, after the fed ones.
function orderRowsByUpstream(
  boxes: Map<string, LineageBox>,
  nodeBox: Map<string, string>,
  graph: LineageGraphData,
): void {
  const upstreamOf = new Map<string, string[]>();
  const feeders = new Map<string, Set<string>>(); // box → boxes feeding it
  for (const e of graph.edges) {
    const sb = nodeBox.get(e.source);
    const tb = nodeBox.get(e.target);
    if (!sb || !tb || sb === tb) continue;
    (upstreamOf.get(e.target) ?? upstreamOf.set(e.target, []).get(e.target)!).push(e.source);
    (feeders.get(tb) ?? feeders.set(tb, new Set()).get(tb)!).add(sb);
  }
  const rowIndex = new Map<string, number>();
  const done = new Set<string>();
  const visit = (id: string, trail: Set<string>) => {
    if (done.has(id) || trail.has(id)) return; // a cycle keeps whichever order it already has
    trail.add(id);
    for (const f of feeders.get(id) ?? []) visit(f, trail);
    trail.delete(id);
    const box = boxes.get(id);
    if (!box) return;
    const key = (r: LineageRow): number | null => {
      const ups = (upstreamOf.get(r.id) ?? [])
        .map((u) => rowIndex.get(u))
        .filter((i): i is number => i !== undefined);
      return ups.length ? ups.reduce((a, b) => a + b, 0) / ups.length : null;
    };
    const ranked = box.rows.map((r, i) => ({ r, i, k: key(r) }));
    ranked.sort((a, b) => {
      if (a.k === null && b.k === null) return a.i - b.i;
      if (a.k === null) return 1;
      if (b.k === null) return -1;
      return a.k - b.k || a.i - b.i;
    });
    box.rows = ranked.map((x) => x.r);
    box.rows.forEach((r, i) => rowIndex.set(r.id, i));
    done.add(id);
  };
  for (const id of boxes.keys()) visit(id, new Set());
}

/** Where row `index` of a box centred at (cx, cy) sits — the top-left origin is the box's own. */
export function rowPosition(
  box: { width: number; height: number },
  center: { x: number; y: number },
  index: number,
): { x: number; y: number } {
  const top = center.y - box.height / 2;
  return { x: center.x, y: top + HEADER_H + index * ROW_H + ROW_H / 2 };
}

export function rowWidth(box: { width: number }): number {
  return box.width - 8;
}

// REQ-1667: lanes by distance from the lane-0 relations. A contributor's lane is its LONGEST
// upstream path to lane 0 (a table that feeds a member both directly and through a view sits at
// -2, so every edge runs rightward across lanes and none doubles back inside one); anything not
// upstream that a lane-0 relation feeds directly is +1. A box the graph does not connect to lane 0
// at all has no lane and is left to the layout.
function assignLanes(
  boxes: Map<string, LineageBox>,
  nodeBox: Map<string, string>,
  graph: LineageGraphData,
  around: ReadonlySet<string>,
): void {
  const feeders = new Map<string, Set<string>>();
  const consumers = new Map<string, Set<string>>();
  for (const e of graph.edges) {
    const sb = nodeBox.get(e.source);
    const tb = nodeBox.get(e.target);
    if (!sb || !tb || sb === tb) continue;
    (feeders.get(tb) ?? feeders.set(tb, new Set()).get(tb)!).add(sb);
    (consumers.get(sb) ?? consumers.set(sb, new Set()).get(sb)!).add(tb);
  }
  const zero = [...boxes.values()]
    .filter((b) => b.relation !== null && around.has(b.relation))
    .map((b) => b.id);
  const zeroSet = new Set(zero);
  // Longest path DOWN to lane 0 from each upstream box, memoized; a cycle contributes nothing.
  const depth = new Map<string, number>();
  const visiting = new Set<string>();
  const depthOf = (id: string): number => {
    if (zeroSet.has(id)) return 0;
    const known = depth.get(id);
    if (known !== undefined) return known;
    if (visiting.has(id)) return -Infinity;
    visiting.add(id);
    let best = -Infinity;
    for (const c of consumers.get(id) ?? []) {
      const d = depthOf(c);
      if (d !== -Infinity) best = Math.max(best, d + 1);
    }
    visiting.delete(id);
    depth.set(id, best);
    return best;
  };
  const upstream = new Set<string>();
  const stack = [...zero];
  while (stack.length > 0) {
    const id = stack.pop()!;
    for (const f of feeders.get(id) ?? []) {
      if (upstream.has(f) || zeroSet.has(f)) continue;
      upstream.add(f);
      stack.push(f);
    }
  }
  const lane = new Map<string, number>();
  for (const id of zero) lane.set(id, 0);
  for (const id of upstream) {
    const d = depthOf(id);
    if (d !== -Infinity) lane.set(id, -d);
  }
  for (const id of zero) {
    for (const c of consumers.get(id) ?? []) if (!lane.has(c)) lane.set(c, 1);
  }
  for (const box of boxes.values()) box.lane = lane.get(box.id) ?? null;
}
