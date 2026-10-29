// Copyright (c) 2026 Kenneth Stott
// Canary: 5b8e2d71-3f4a-4c96-b0e7-9a1c6d4f2e38
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174 (amended 2026-10-28): the graph's 2D view is a lane diagram read left to right -- the
// term hierarchy, the columns its terms bind, their tables, the products those tables belong to.
// Inside the term lane only rank edges are drawn, top-down; a chain ends at the columns binding
// it; columns link to their table and tables to their products. A table's unbound columns are one
// "+N columns" node until the table is expanded. A table in no product ends at the tables lane.
// Lateral and flow edges and lineage are not drawn. Pure data: LaneStage draws what this decides.
// The term lane is the hierarchy: each term drawn with the broader terms its rank edges reach,
// up the chain, whatever a focus keeps (a product's neighbourhood ends at its bound terms).

import type { GraphData, GraphEdge, GraphNode } from "./graphModel";
import { edgeClass, orderLevels } from "./layeredLayout";

export type Lane = "term" | "column" | "table" | "product";

/** The lanes, left to right. */
export const LANES: Lane[] = ["term", "column", "table", "product"];

/** A table's unbound columns, drawn as one node until the table is expanded. */
export interface MoreNode {
  id: string;
  table: string;
  count: number;
  /** The columns it holds, in the table's order: its hover card names them. */
  columns: GraphNode[];
}

/** A drawn line, ``from`` the upper or left end ``to`` the lower or right one. */
export interface LaneLink {
  id: string;
  from: string;
  to: string;
}

export interface LaneDiagram {
  /** The things drawn and the edges between them, as the detail panel lists them. */
  graph: GraphData;
  lane: Map<string, Lane>;
  more: MoreNode[];
  /** Every line drawn: the graph's edges and each "+N columns" node's line to its table. */
  links: LaneLink[];
  /** The whole graph read, where each node's facts are found (nodeFacts). */
  data: GraphData;
}

export const moreId = (table: string) => `more:${table}`;

/** The lane diagram of ``shown`` (the graph the view's layers and filters keep). Columns are
 * read from ``data``, since a table's columns fold into it there; ``expanded`` holds the tables
 * whose unbound columns are drawn one by one. */
export function laneDiagram(
  shown: GraphData,
  data: GraphData,
  expanded: ReadonlySet<string>,
): LaneDiagram {
  const byId = new Map(data.nodes.map((n) => [n.id, n]));
  const node = (id: string): GraphNode => {
    const n = byId.get(id);
    if (!n) throw new Error(`graph edge names ${id}, a node the graph lacks`);
    return n;
  };
  const drawn = new Set(shown.nodes.map((n) => n.id));
  const lane = new Map<string, Lane>();
  const terms = shown.nodes.filter((n) => n.type === "term");
  for (const n of terms) lane.set(n.id, "term");
  // Up each chain: a rank edge's broader term (its b end) joins the lane.
  const ranks = data.edges.filter((e) => e.kind === "term_edge" && edgeClass(e) === "rank");
  for (let i = 0; i < terms.length; i++)
    for (const e of ranks)
      if (e.a === terms[i].id && !lane.has(e.b)) {
        lane.set(e.b, "term");
        terms.push(node(e.b));
      }

  // The tables drawn, and those of the columns a drawn term binds (a preset without tables draws
  // the columns alone); a column drawn only as lineage's end brings no table.
  const tables: string[] = [];
  const addTable = (id: string) => {
    if (lane.has(id)) return;
    lane.set(id, "table");
    tables.push(id);
  };
  for (const n of shown.nodes) if (n.type === "table") addTable(n.id);
  for (const e of shown.edges) {
    if (e.kind !== "term_binding" || lane.get(e.a) !== "term") continue;
    const parent = node(e.b).parent;
    if (node(e.b).type === "column" && parent) addTable(parent);
  }

  const edges: GraphEdge[] = [];
  const links: LaneLink[] = [];
  const add = (e: Omit<GraphEdge, "id">, from: string, to: string) => {
    // Rank edges of two types between two terms are two lines (graphModel's rule).
    const id = `${e.kind}:${e.a}>${e.b}${e.kind === "term_edge" && e.label ? `:${e.label}` : ""}`;
    edges.push({ ...e, id });
    links.push({ id, from, to });
  };

  // Inside the term lane, rank edges only: the broader term above. The graph read holds them
  // all (the view's edge-type filter is in its query), each type its own edge.
  const seenRank = new Set<string>();
  for (const e of ranks) {
    if (lane.get(e.a) !== "term" || lane.get(e.b) !== "term") continue;
    const key = `${e.a}>${e.b}:${e.label}`;
    if (seenRank.has(key)) continue;
    seenRank.add(key);
    add({ a: e.a, b: e.b, kind: e.kind, label: e.label }, e.b, e.a);
  }

  // A chain ends at the columns binding it: those drawn, or of a table drawn. A term that IS a
  // table (a class) ends at the table.
  const bound = new Set<string>();
  const bindings: Omit<GraphEdge, "id">[] = [];
  for (const e of data.edges) {
    if (e.kind !== "term_binding" || lane.get(e.a) !== "term") continue;
    const end = node(e.b);
    if (end.type === "column") {
      if (!drawn.has(end.id) && !(end.parent && lane.get(end.parent) === "table")) continue;
      bound.add(end.id);
    } else if (lane.get(end.id) !== "table") continue;
    bindings.push({ a: e.a, b: e.b, kind: "term_binding", label: e.label });
  }

  // Each table's columns: the bound ones, and the rest as one "+N columns" node until expanded.
  const columns: GraphNode[] = [];
  const more: MoreNode[] = [];
  const ofTable = new Map<string, GraphNode[]>();
  for (const n of data.nodes)
    if (n.type === "column" && n.parent && lane.get(n.parent) === "table")
      ofTable.set(n.parent, [...(ofTable.get(n.parent) ?? []), n]);
  for (const t of tables) {
    const cols = ofTable.get(t) ?? [];
    const unbound = cols.filter((c) => !bound.has(c.id));
    const shownCols = expanded.has(t) ? cols : cols.filter((c) => bound.has(c.id));
    for (const c of shownCols) columns.push(c);
    if (!expanded.has(t) && unbound.length > 0)
      more.push({ id: moreId(t), table: t, count: unbound.length, columns: unbound });
  }
  // A bound column with no table (read from lineage alone) stands in the lane by itself.
  for (const id of bound) if (!node(id).parent) columns.push(node(id));
  for (const c of columns) lane.set(c.id, "column");
  for (const b of bindings) if (lane.has(b.b)) add(b, b.a, b.b);
  for (const c of columns)
    if (c.parent) add({ a: c.parent, b: c.id, kind: "has_column" }, c.id, c.parent);
  for (const m of more) links.push({ id: `${m.id}>${m.table}`, from: m.id, to: m.table });

  // Every product the tables belong to, read from the graph whatever its layers draw (the
  // glossary's Data layer off); a graph of products alone draws those drawn.
  const members = data.edges.filter(
    (e) => e.kind === "member" && node(e.a).type === "product" && lane.get(e.b) === "table",
  );
  const alone = terms.length === 0 && tables.length === 0;
  const products = alone
    ? shown.nodes.filter((n) => n.type === "product")
    : [...new Set(members.map((e) => e.a))].map(node);
  for (const p of products) lane.set(p.id, "product");
  for (const e of members) add({ a: e.a, b: e.b, kind: "member" }, e.b, e.a);

  // In lane order, left to right.
  const nodes = [
    ...terms,
    ...columns,
    ...tables.map(node),
    ...products,
  ];
  return { graph: { nodes, edges }, lane, more, links, data };
}

/** Each lane's nodes top to bottom, crossings cut (orderLevels): the term lane in ``termOrder``
 * as ELK laid it out on its own, the column lane (its "+N columns" nodes too), the tables and the
 * products ordered to suit it. A term bound to a table passes the column lane through a stand-in;
 * lines inside the term lane cross nothing outside it. */
export function laneOrder(d: LaneDiagram, termOrder: string[]): string[][] {
  const levels: string[][] = LANES.map(() => []);
  levels[0] = [...termOrder];
  for (const n of d.graph.nodes) {
    const l = LANES.indexOf(d.lane.get(n.id) as Lane);
    if (l > 0) levels[l].push(n.id);
  }
  for (const m of d.more) levels[LANES.indexOf("column")].push(m.id);
  const laneOf = new Map<string, number>();
  levels.forEach((row, l) => row.forEach((id) => laneOf.set(id, l)));
  const edges = d.links
    .filter((k) => laneOf.get(k.from) !== laneOf.get(k.to))
    .map((k) => [k.from, k.to] as [string, string]);
  return orderLevels(levels, edges, new Set([0]));
}

/** The diagram as Mermaid ``flowchart LR`` text, one subgraph per lane drawn. */
export function laneMermaid(
  d: LaneDiagram,
  laneTitle: (lane: Lane) => string,
  moreLabel: (count: number) => string,
): string {
  const ids = new Map<string, string>();
  const idOf = (id: string) => {
    let m = ids.get(id);
    if (m === undefined) {
      m = `n${ids.size}`;
      ids.set(id, m);
    }
    return m;
  };
  const quote = (s: string) => `"${s.replace(/"/g, "#quot;")}"`;
  const out = ["flowchart LR"];
  for (const l of LANES) {
    const members: [string, string][] = d.graph.nodes
      .filter((n) => d.lane.get(n.id) === l)
      .map((n) => [n.id, n.label]);
    if (l === "column") for (const m of d.more) members.push([m.id, moreLabel(m.count)]);
    if (members.length === 0) continue;
    out.push(`  subgraph lane_${l}[${quote(laneTitle(l))}]`);
    if (l === "term") out.push("    direction TB");
    for (const [id, label] of members) out.push(`    ${idOf(id)}[${quote(label)}]`);
    out.push("  end");
  }
  for (const k of d.links) out.push(`  ${idOf(k.from)} --> ${idOf(k.to)}`);
  return out.join("\n");
}
