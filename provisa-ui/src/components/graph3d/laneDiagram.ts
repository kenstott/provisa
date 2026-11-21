// Copyright (c) 2026 Kenneth Stott
// Canary: 5b8e2d71-3f4a-4c96-b0e7-9a1c6d4f2e38
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174 (amended 2026-10-28; 2026-11-07, WHAT THE GRAPH DRAWS): the graph's 2D view is a lane
// diagram read left to right -- the products, their tables, those tables' columns and the terms
// those columns are tied to, with the links between the terms drawn (the Term relationships filter
// already applied) and nothing else: no related table, no lineage. Inside the term lane the rank
// edges set the levels, top-down; every other term link is an arc laid over them that moves no
// node. A table's columns not tied to a term drawn are one node until the table is expanded. A
// focus's hops are counted before the lanes (graphModel's reach: each hop a step between terms,
// amended 2026-11-12), in 2D as in 3D. Pure data: LaneStage draws what this decides.

import type { GraphData, GraphEdge, GraphNode } from "./graphModel";
import { edgeClass } from "./layeredLayout";

export type Lane = "term" | "column" | "table" | "product";

/** The lanes as drawn, left to right. */
export const LANES: Lane[] = ["product", "table", "column", "term"];

/** A table's columns tied to no term drawn, drawn as one node until the table is expanded. */
export interface MoreNode {
  id: string;
  table: string;
  count: number;
  /** The columns it holds, in the table's order: its hover card names them. */
  columns: GraphNode[];
}

/** A line, ``from`` its end on the term side (inside the term lane, the broader term) ``to`` the
 * other: the lanes are ordered by these. LaneStage draws a line between lanes left to right. */
export interface LaneLink {
  id: string;
  from: string;
  to: string;
  /** A term link's type, named on the line. */
  label?: string;
}

export interface LaneDiagram {
  /** The things drawn and the edges between them, as the detail panel lists them. */
  graph: GraphData;
  lane: Map<string, Lane>;
  more: MoreNode[];
  /** Every line laid out: the graph's edges and each fold node's line to its table, the term
   * links that are not rank edges aside. */
  links: LaneLink[];
  /** The term links that are not rank edges (lateral, flow): drawn over the laid-out term lane,
   * never placing a node. */
  arcs: LaneLink[];
  /** The whole graph read, where each node's facts are found (nodeFacts). */
  data: GraphData;
}

export const moreId = (table: string) => `more:${table}`;

/** The lane diagram of ``shown`` (the graph the view's layers, filters and focus keep). Columns
 * are read from ``data``, since a table's columns fold into it there; ``expanded`` holds the
 * tables whose columns are drawn one by one. */
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
  const termLinks = shown.edges.filter((e) => e.kind === "term_edge");
  const lane = new Map<string, Lane>();
  const tables: string[] = [];
  const addTable = (id: string) => {
    if (lane.has(id)) return;
    lane.set(id, "table");
    tables.push(id);
  };
  const terms: GraphNode[] = [];
  const addTerm = (id: string) => {
    if (lane.has(id)) return;
    lane.set(id, "term");
    terms.push(node(id));
  };
  for (const n of shown.nodes) if (n.type === "term") addTerm(n.id);
  // The tables drawn, and those of the columns a drawn term binds (a preset without tables draws
  // the columns alone); a column drawn only as lineage's end brings no table.
  for (const n of shown.nodes) if (n.type === "table") addTable(n.id);
  for (const e of shown.edges) {
    if (e.kind !== "term_binding" || lane.get(e.a) !== "term") continue;
    const parent = node(e.b).parent;
    if (node(e.b).type === "column" && parent) addTable(parent);
  }

  const edges: GraphEdge[] = [];
  const links: LaneLink[] = [];
  const arcs: LaneLink[] = [];
  const edgeId = (e: Omit<GraphEdge, "id">) =>
    `${e.kind}:${e.a}>${e.b}${e.kind === "term_edge" && e.label ? `:${e.label}` : ""}`;
  const add = (e: Omit<GraphEdge, "id">, from: string, to: string) => {
    // Term links of two types between two terms are two lines (graphModel's rule).
    const id = edgeId(e);
    edges.push({ ...e, id });
    links.push({ id, from, to, ...(e.kind === "term_edge" && e.label ? { label: e.label } : {}) });
  };

  // Between the terms drawn, every link the filters keep, each type its own: a rank edge sets the
  // levels, the broader term above; any other is an arc over them.
  const seenLink = new Set<string>();
  for (const e of termLinks) {
    if (lane.get(e.a) !== "term" || lane.get(e.b) !== "term") continue;
    const id = edgeId(e);
    if (seenLink.has(id)) continue;
    seenLink.add(id);
    if (edgeClass(e) === "rank") add({ a: e.a, b: e.b, kind: e.kind, label: e.label }, e.b, e.a);
    else {
      edges.push({ a: e.a, b: e.b, kind: e.kind, label: e.label, id });
      arcs.push({ id, from: e.a, to: e.b, ...(e.label ? { label: e.label } : {}) });
    }
  }

  // A term's line ends at the columns tied to it: those drawn, or of a table drawn. A term that IS
  // a table (a class) ends at the table.
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

  // Each table's columns: the bound ones, and the rest as one node until expanded.
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
  const nodes = [...products, ...tables.map(node), ...columns, ...terms];
  return { graph: { nodes, edges }, lane, more, links, arcs, data };
}

/** A point on the drawing. */
export interface At {
  x: number;
  y: number;
}

/** Pairs of drawn lines that cross, sharing no end: each line centre to centre, or along its
 * ``route`` (a line drawn beside its lane) where it has one; a pair crossing twice counts twice. */
export function drawnCrossings(
  links: LaneLink[],
  at: (id: string) => At,
  route: (id: string) => At[] | undefined = () => undefined,
): number {
  const side = (a: At, b: At, c: At) =>
    Math.sign((b.x - a.x) * (c.y - a.y) - (b.y - a.y) * (c.x - a.x));
  const cross = (p: At, q: At, r: At, s: At) =>
    side(p, q, r) * side(p, q, s) < 0 && side(r, s, p) * side(r, s, q) < 0;
  const paths = links.map((k) => ({ k, path: route(k.id) ?? [at(k.from), at(k.to)] }));
  let n = 0;
  for (let i = 0; i < paths.length; i++)
    for (let j = i + 1; j < paths.length; j++) {
      const a = paths[i];
      const b = paths[j];
      if ([a.k.from, a.k.to].some((id) => id === b.k.from || id === b.k.to)) continue;
      for (let u = 1; u < a.path.length; u++)
        for (let v = 1; v < b.path.length; v++)
          if (cross(a.path[u - 1], a.path[u], b.path[v - 1], b.path[v])) n++;
    }
  return n;
}

/** What a focused node brings forward: the nodes and the lines between them. */
export interface Connected {
  nodes: Set<string>;
  links: Set<string>;
}

/** Everything connected to ``id`` along the lanes: its own path out in each direction, never
 * doubling back, so a table reached from a term does not bring that table's other columns.
 * REQ-2174 (amended 2026-11-12, A HOP IS A STEP BETWEEN TERMS): a term brings every term it reaches
 * by the term links drawn (the drawn graph already holds only what the hops shown reach) -- up its
 * broader terms, down its narrower ones (never a broader term's other narrower terms), and across
 * every other link (an association) either way, the term so reached bringing its own broader and
 * narrower terms in turn -- and each term reached its columns, their tables and the products
 * holding them, as the 3D view lights every edge of the picked node whatever its type. A column,
 * table or product brings its own path to the left, the terms it reaches to the right, and their
 * broader and narrower terms. */
export function connectedTo(d: LaneDiagram, id: string): Connected {
  const moreIds = new Set(d.more.map((m) => m.id));
  const at = (x: string) => LANES.indexOf(moreIds.has(x) ? "column" : (d.lane.get(x) as Lane));
  const term = LANES.indexOf("term");
  const nodes = new Set([id]);
  const links = new Set<string>();
  /** Follows the lines ``next`` picks from each node of ``from``; returns the nodes reached. */
  const walk = (from: string[], next: (x: string, k: LaneLink) => string | null) => {
    const reached = [...from];
    for (let i = 0; i < reached.length; i++)
      for (const k of d.links) {
        const to = next(reached[i], k);
        if (to === null) continue;
        links.add(k.id);
        nodes.add(to);
        if (!reached.includes(to)) reached.push(to);
      }
    return reached;
  };
  const other = (x: string, k: LaneLink) => (k.from === x ? k.to : k.to === x ? k.from : null);
  const inLane = (x: string, k: LaneLink) =>
    at(k.from) === term && at(k.to) === term && other(x, k) !== null;
  const chain = (from: string[]) => [
    ...walk(from, (x, k) => (inLane(x, k) && k.to === x ? k.from : null)),
    ...walk(from, (x, k) => (inLane(x, k) && k.from === x ? k.to : null)),
  ];
  /** Every term a term reaches along the term lane: a rank line up or down, never turning back
   * (so no sibling), and an arc either way, its far term starting afresh. */
  const terms = (start: string) => {
    type Way = "up" | "down" | "any";
    const reached = new Map<string, Set<Way>>([[start, new Set<Way>(["any"])]]);
    const queue: [string, Way][] = [[start, "any"]];
    const step = (to: string, way: Way, k: LaneLink) => {
      links.add(k.id);
      nodes.add(to);
      const ways = reached.get(to) ?? new Set<Way>();
      if (ways.has(way)) return;
      ways.add(way);
      reached.set(to, ways);
      queue.push([to, way]);
    };
    while (queue.length > 0) {
      const [x, way] = queue.shift() as [string, Way];
      for (const k of d.links) {
        if (!inLane(x, k)) continue;
        // A rank line runs from the broader term to the narrower one.
        if (k.to === x && way !== "down") step(k.from, "up", k);
        if (k.from === x && way !== "up") step(k.to, "down", k);
      }
      for (const k of d.arcs) {
        const to = other(x, k);
        if (to !== null) step(to, "any", k);
      }
    }
    return [...reached.keys()];
  };
  const across = (from: string[], dir: 1 | -1) =>
    walk(from, (x, k) => {
      const to = other(x, k);
      return to !== null && Math.sign(at(to) - at(x)) === dir ? to : null;
    });
  if (at(id) === term) across(terms(id), -1);
  else {
    across([id], -1);
    chain(across([id], 1).filter((x) => at(x) === term));
  }
  return { nodes, links };
}

/** The diagram as Mermaid ``flowchart LR`` text, one subgraph per lane drawn, left to right; a
 * term link is dotted and named by its type. */
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
  const inTerms = (k: LaneLink) => d.lane.get(k.from) === "term" && d.lane.get(k.to) === "term";
  // A term link runs in its own direction: the narrower term KIND_OF the broader one.
  const dotted = (from: string, to: string, label?: string) =>
    `  ${idOf(from)} -.->${label ? `|${quote(label)}|` : ""} ${idOf(to)}`;
  for (const k of d.links)
    out.push(inTerms(k) ? dotted(k.to, k.from, k.label) : `  ${idOf(k.to)} --> ${idOf(k.from)}`);
  for (const k of d.arcs) out.push(dotted(k.from, k.to, k.label));
  return out.join("\n");
}
