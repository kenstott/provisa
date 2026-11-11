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
// product's focus counts hops along that path (lanePath): 1 its tables, 2 their columns, 3 the
// terms tied to them, each hop past 3 one step across the term links. Pure data: LaneStage draws
// what this decides.

import type { GraphData, GraphEdge, GraphNode } from "./graphModel";
import { edgeClass } from "./layeredLayout";
import { SIZE } from "../provisa-graph/tokens";

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

/** The hops of a product's focus, counted along the lane path. */
export interface LanePath {
  /** The product's node id. */
  focus: string;
  depth: number;
}

/** The lane diagram of ``shown`` (the graph the view's layers and filters keep). Columns are
 * read from ``data``, since a table's columns fold into it there; ``expanded`` holds the tables
 * whose columns are drawn one by one. With ``path`` (a product's focus) ``shown`` is the graph
 * unpruned by the focus and the hops are counted here, along the lanes. */
export function laneDiagram(
  shown: GraphData,
  data: GraphData,
  expanded: ReadonlySet<string>,
  path: LanePath | null = null,
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
  // Whether columns are drawn at all: a product's first hop is its tables alone.
  const columnsDrawn = path === null || path.depth >= 2;

  if (path !== null) {
    // 1: the product's tables.
    if (path.depth >= 1)
      for (const e of shown.edges)
        if (e.kind === "member" && e.a === path.focus && node(e.b).type === "table") addTable(e.b);
    // 3: the terms drawn tied to their columns (read from ``data``, where a column is itself);
    // past 3, a step across the term links each hop.
    if (path.depth >= 3) {
      for (const e of data.edges) {
        if (e.kind !== "term_binding" || !drawn.has(e.a) || node(e.a).type !== "term") continue;
        const parent = node(e.b).parent;
        if (node(e.b).type === "column" && parent && lane.get(parent) === "table") addTerm(e.a);
      }
      let frontier = terms.map((n) => n.id);
      for (let hop = 4; hop <= path.depth && frontier.length > 0; hop++) {
        const at = new Set(frontier);
        frontier = [];
        for (const e of termLinks)
          for (const [from, to] of [
            [e.a, e.b],
            [e.b, e.a],
          ])
            if (at.has(from) && !lane.has(to)) {
              addTerm(to);
              frontier.push(to);
            }
      }
    }
  } else {
    for (const n of shown.nodes) if (n.type === "term") addTerm(n.id);
    // The tables drawn, and those of the columns a drawn term binds (a preset without tables draws
    // the columns alone); a column drawn only as lineage's end brings no table.
    for (const n of shown.nodes) if (n.type === "table") addTable(n.id);
    for (const e of shown.edges) {
      if (e.kind !== "term_binding" || lane.get(e.a) !== "term") continue;
      const parent = node(e.b).parent;
      if (node(e.b).type === "column" && parent) addTable(parent);
    }
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
      if (!columnsDrawn) continue;
      if (!drawn.has(end.id) && !(end.parent && lane.get(end.parent) === "table")) continue;
      bound.add(end.id);
    } else if (lane.get(end.id) !== "table") continue;
    bindings.push({ a: e.a, b: e.b, kind: "term_binding", label: e.label });
  }

  // Each table's columns: the bound ones, and the rest as one node until expanded.
  const columns: GraphNode[] = [];
  const more: MoreNode[] = [];
  const ofTable = new Map<string, GraphNode[]>();
  if (columnsDrawn)
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

  // A product's focus draws that product alone; otherwise every product the tables belong to,
  // read from the graph whatever its layers draw (the glossary's Data layer off), and a graph of
  // products alone draws those drawn.
  const members = data.edges.filter(
    (e) =>
      e.kind === "member" &&
      node(e.a).type === "product" &&
      lane.get(e.b) === "table" &&
      (path === null || e.a === path.focus),
  );
  const alone = path === null && terms.length === 0 && tables.length === 0;
  const products =
    path !== null
      ? [node(path.focus)]
      : alone
        ? shown.nodes.filter((n) => n.type === "product")
        : [...new Set(members.map((e) => e.a))].map(node);
  for (const p of products) lane.set(p.id, "product");
  for (const e of members) add({ a: e.a, b: e.b, kind: "member" }, e.b, e.a);

  // In lane order, left to right.
  const nodes = [...products, ...tables.map(node), ...columns, ...terms];
  return { graph: { nodes, edges }, lane, more, links, arcs, data };
}

/** Lines crossing, by place: pairs (a from ``xs``, b from ``ys``) with a below b. */
const inversions = (xs: number[], ys: number[]) => {
  let n = 0;
  for (const a of xs) for (const b of ys) if (a > b) n++;
  return n;
};

const BLOCK_PASSES = 8;
const ROUNDS = 4;

/** ``items`` reordered so that ``cost(a, b)`` (crossings with a placed before b) summed over every
 * pair is cut: each item in turn moved to the first place where it costs least, until a pass
 * moves nothing or BLOCK_PASSES passes. */
function sifted<T>(items: T[], cost: (a: T, b: T) => number): T[] {
  let row = [...items];
  for (let pass = 0; pass < BLOCK_PASSES; pass++) {
    let moved = false;
    for (const v of [...row]) {
      const from = row.indexOf(v);
      const rest = row.filter((x) => x !== v);
      let c = rest.reduce((sum, u) => sum + cost(v, u), 0);
      let bestAt = 0;
      let best = c;
      rest.forEach((u, j) => {
        c += cost(u, v) - cost(v, u);
        if (c < best) {
          best = c;
          bestAt = j + 1;
        }
      });
      rest.splice(bestAt, 0, v);
      row = rest;
      if (bestAt !== from) moved = true;
    }
    if (!moved) break;
  }
  return row;
}

const median = (xs: number[]) => {
  const s = [...xs].sort((a, b) => a - b);
  const m = s.length >> 1;
  return s.length % 2 === 1 ? s[m] : (s[m - 1] + s[m]) / 2;
};

/** Each lane's nodes top to bottom, crossings cut: the term lane in ``termOrder`` as ELK laid it
 * out on its own; each table's columns side by side as one block (its "+N columns" last), so no
 * line from a column to its table crosses another; the blocks, the columns inside each and the
 * products ordered to cut the crossings of the lines from the terms and to the products. A term
 * bound to a table counts in its table's block, as the line it draws passes the column lane there;
 * lines inside the term lane cross nothing outside it. Same diagram, same order. The lanes are
 * returned from the term side: terms, columns, tables, products. */
export function laneOrder(d: LaneDiagram, termOrder: string[]): string[][] {
  const termAt = new Map(termOrder.map((id, i) => [id, i]));
  const moreIds = new Set(d.more.map((m) => m.id));
  const laneOf = (id: string): Lane => (moreIds.has(id) ? "column" : (d.lane.get(id) as Lane));
  const inLane = (l: Lane) => d.graph.nodes.filter((n) => d.lane.get(n.id) === l).map((n) => n.id);
  // The terms above each column (or table), and each column's table, each table's products.
  const termsOf = new Map<string, number[]>();
  const tableOf = new Map<string, string>();
  const productsOf = new Map<string, string[]>();
  for (const k of d.links) {
    const lf = laneOf(k.from);
    const lt = laneOf(k.to);
    if (lf === "term" && lt !== "term") {
      const at = termAt.get(k.from);
      if (at === undefined) throw new Error(`term ${k.from} is not in the term order`);
      termsOf.set(k.to, [...(termsOf.get(k.to) ?? []), at]);
    }
    if (lf === "column" && lt === "table") tableOf.set(k.from, k.to);
    if (lf === "table" && lt === "product") productsOf.set(k.from, [...(productsOf.get(k.from) ?? []), k.to]);
  }
  const columns = [...inLane("column"), ...d.more.map((m) => m.id)];
  const tables = inLane("table");
  // A block: a table and its columns, or a column with no table alone.
  interface Block {
    table: string | null;
    members: string[];
  }
  const blocks: Block[] = [
    ...tables.map((t) => ({ table: t, members: columns.filter((c) => tableOf.get(c) === t) })),
    ...columns.filter((c) => !tableOf.has(c)).map((c) => ({ table: null, members: [c] })),
  ];
  const ups = (id: string) => termsOf.get(id) ?? [];

  // Inside a block: by the median (then mean) place of each column's terms, those with none
  // ("+N columns") last, then adjacent swaps that cut crossings.
  for (const b of blocks) {
    const keyed = b.members.map((id, i) => {
      const ts = ups(id);
      return ts.length === 0
        ? { id, a: Infinity, m: Infinity, i }
        : { id, a: median(ts), m: ts.reduce((s, v) => s + v, 0) / ts.length, i };
    });
    keyed.sort((x, y) => (x.a === y.a ? 0 : x.a - y.a) || (x.m === y.m ? 0 : x.m - y.m) || x.i - y.i);
    b.members = sifted(
      keyed.map((k) => k.id),
      (u, v) => inversions(ups(u), ups(v)),
    );
  }

  // A block's terms: its columns' and those bound to its table.
  const blockTerms = new Map(
    blocks.map((b) => [b, [...b.members.flatMap(ups), ...(b.table ? ups(b.table) : [])]]),
  );
  const mean = (xs: number[]) => xs.reduce((s, v) => s + v, 0) / xs.length;
  const byMean = blocks
    .map((b, i) => ({ b, k: blockTerms.get(b) as number[], i }))
    .sort((x, y) => {
      const kx = x.k.length === 0 ? Infinity : mean(x.k);
      const ky = y.k.length === 0 ? Infinity : mean(y.k);
      return (kx === ky ? 0 : kx - ky) || x.i - y.i;
    });
  let order = byMean.map((x) => x.b);
  let products = inLane("product");
  const productsAt = () => new Map(products.map((p, i) => [p, i]));
  for (let round = 0; round < ROUNDS; round++) {
    const pAt = productsAt();
    const prods = (b: Block) => (b.table ? (productsOf.get(b.table) ?? []) : []).map((p) => pAt.get(p) as number);
    order = sifted(
      order,
      (x, y) =>
        inversions(blockTerms.get(x) as number[], blockTerms.get(y) as number[]) +
        inversions(prods(x), prods(y)),
    );
    // The products below their tables.
    const tAt = new Map<string, number>();
    order.forEach((b, i) => b.table && tAt.set(b.table, i));
    const tablesOf = new Map<string, number[]>();
    for (const [t, ps] of productsOf)
      for (const p of ps) tablesOf.set(p, [...(tablesOf.get(p) ?? []), tAt.get(t) as number]);
    products = sifted(
      products
        .map((p, i) => ({ p, k: tablesOf.get(p) ?? [], i }))
        .sort((x, y) => {
          const kx = x.k.length === 0 ? Infinity : mean(x.k);
          const ky = y.k.length === 0 ? Infinity : mean(y.k);
          return (kx === ky ? 0 : kx - ky) || x.i - y.i;
        })
        .map((x) => x.p),
      (p, q) => inversions(tablesOf.get(p) ?? [], tablesOf.get(q) ?? []),
    );
  }
  return [
    [...termOrder],
    order.flatMap((b) => b.members),
    order.flatMap((b) => (b.table ? [b.table] : [])),
    products,
  ];
}

/** A point on the drawing. */
export interface At {
  x: number;
  y: number;
}

/** Pairs of drawn lines (centre to centre) that cross, sharing no end. */
export function drawnCrossings(links: LaneLink[], at: (id: string) => At): number {
  const side = (a: At, b: At, c: At) => Math.sign((b.x - a.x) * (c.y - a.y) - (b.y - a.y) * (c.x - a.x));
  const segs = links.map((k) => ({ k, p: at(k.from), q: at(k.to) }));
  let n = 0;
  for (let i = 0; i < segs.length; i++)
    for (let j = i + 1; j < segs.length; j++) {
      const a = segs[i];
      const b = segs[j];
      if ([a.k.from, a.k.to].some((id) => id === b.k.from || id === b.k.to)) continue;
      if (side(a.p, a.q, b.p) * side(a.p, a.q, b.q) < 0 && side(b.p, b.q, a.p) * side(b.p, b.q, a.q) < 0)
        n++;
    }
  return n;
}

/** The term lane's vertical gap, as ELK spaces the term lane's nodes. */
const TERM_GAP = 14;
/** A term's height as ProvisaGraph draws it (REQ-2239): blocks stacked closer would overlap. */
export const TERM_HEIGHT = SIZE.nodeHeight;
/** How wide a row of terms with no line out of the lane runs before it wraps. */
const UNLINKED_ROW = 480;
const TERM_ROUNDS = 4;

/** The term lane's blocks in ``order``: terms joined by the rank lines inside the lane keep the
 * shape ELK gave them and move as one; a term alone is a block by itself. */
function termBlocks(d: LaneDiagram, order: string[]): string[][] {
  const joined = new Map(order.map((id) => [id, id]));
  const root = (id: string): string => {
    const up = joined.get(id) as string;
    return up === id ? id : root(up);
  };
  for (const k of d.links)
    if (joined.has(k.from) && joined.has(k.to)) joined.set(root(k.from), root(k.to));
  const blocks = new Map<string, string[]>();
  for (const id of order) {
    const r = root(id);
    blocks.set(r, [...(blocks.get(r) ?? []), id]);
  }
  return [...blocks.values()];
}

/** Whether a block has a line leaving the term lane. */
const reachesOut = (d: LaneDiagram, block: string[]) =>
  d.links.some((k) => block.includes(k.from) && d.lane.get(k.to) !== "term");

/** Where the terms are drawn, given their ``order`` (top to bottom, by block) and where ELK put
 * them (``at``): the blocks stacked in one column at the lane's left edge, centred on the other
 * lanes, so every order the lanes can take is open to them. ELK packs the term lane's blocks by
 * size, a lone term wherever one fits, which is a line across the diagram for no reason. Blocks
 * with no line out of the lane wrap in rows below the column: they cross nothing, and a stack of
 * them would only make the diagram taller. ``widthOf`` is a node's drawn width, for those rows. */
export function stackTerms(
  d: LaneDiagram,
  order: string[],
  at: (id: string) => At,
  widthOf: (id: string) => number = () => 0,
): Map<string, At> {
  const out = new Map<string, At>();
  if (order.length === 0) return out;
  const blocks = termBlocks(d, order);
  const left = Math.min(...order.map((id) => at(id).x));
  const others = d.graph.nodes.filter((n) => d.lane.get(n.id) !== "term").map((n) => at(n.id).y);
  const linked = blocks.filter((b) => reachesOut(d, b));
  const lone = blocks.filter((b) => !reachesOut(d, b));
  const heightOf = (b: string[]) => {
    const ys = b.map((id) => at(id).y);
    return Math.max(...ys) - Math.min(...ys) + TERM_HEIGHT;
  };
  const place = (b: string[], x: number, y: number) => {
    const xs = b.map((id) => at(id).x);
    const ys = b.map((id) => at(id).y);
    const x0 = Math.min(...xs);
    const y0 = Math.min(...ys);
    for (const id of b) out.set(id, { x: x + at(id).x - x0, y: y + at(id).y - y0 });
  };
  const total = linked.reduce((sum, b) => sum + heightOf(b) + TERM_GAP, -TERM_GAP);
  const middle =
    others.length > 0 ? (Math.min(...others) + Math.max(...others)) / 2 : at(order[0]).y;
  let y = linked.length > 0 ? middle - total / 2 : at(order[0]).y;
  for (const b of linked) {
    place(b, left, y);
    y += heightOf(b) + TERM_GAP;
  }
  let x = left;
  let row = 0;
  for (const b of lone) {
    const width = Math.max(...b.map((id) => at(id).x + widthOf(id))) - Math.min(...b.map((id) => at(id).x));
    if (x > left && x + width > left + UNLINKED_ROW) {
      x = left;
      y += row + TERM_GAP;
      row = 0;
    }
    place(b, x, y);
    x += width + TERM_GAP;
    row = Math.max(row, heightOf(b));
  }
  return out;
}

/** The lanes' order for a drawing whose first pass put each node at ``first``. The terms are
 * ordered with the other lanes rather than taken as ELK packed them: from top to bottom as placed,
 * each block of terms then moved to the height its lines reach, for as long as that cuts the
 * crossings drawn (terms stacked by stackTerms, the other lanes spread evenly over the height they
 * had), and finished by moves judged on the drawing itself (refineDrawn). */
export function placedLaneOrder(d: LaneDiagram, first: (id: string) => At): string[][] {
  const terms = d.graph.nodes.filter((n) => d.lane.get(n.id) === "term").map((n) => n.id);
  const start = [...terms].sort((p, q) => first(p).y - first(q).y || first(p).x - first(q).x);
  // Where each node would be drawn, the other lanes at their lane's x, spread over its first height.
  const placed = (lanes: string[][]) => {
    const at = stackTerms(d, lanes[0], first);
    for (const row of lanes.slice(1)) {
      if (row.length === 0) continue;
      const ys = row.map((id) => first(id).y);
      const lo = Math.min(...ys);
      const hi = Math.max(...ys);
      row.forEach((id, i) =>
        at.set(id, { x: first(id).x, y: lo + ((i + 0.5) * (hi - lo)) / row.length }),
      );
    }
    return (id: string) => at.get(id) as At;
  };
  const score = (order: string[]) => {
    const lanes = laneOrder(d, order);
    return { lanes, n: drawnCrossings(d.links, placed(lanes)) };
  };
  let best = score(start);
  for (let round = 0; round < TERM_ROUNDS && best.n > 0; round++) {
    const at = placed(best.lanes);
    const reach = (block: string[]) => {
      const ends = d.links
        .filter((k) => block.includes(k.from) && d.lane.get(k.to) !== "term")
        .map((k) => at(k.to).y);
      return ends.length === 0 ? Infinity : ends.reduce((s, y) => s + y, 0) / ends.length;
    };
    const blocks = termBlocks(d, best.lanes[0]).map((b, i) => ({ b, r: reach(b), i }));
    blocks.sort((x, y) => (x.r === y.r ? x.i - y.i : x.r - y.r));
    const next = score(blocks.flatMap((x) => x.b));
    if (next.n >= best.n) break;
    best = next;
  }
  // Each block in turn moved to where, the other lanes reordered around it, the drawing crosses
  // least: a cost two moves apart (a term and a column each needing the other moved first) is
  // found only this way. Bounded like refineDrawn: past the budget a diagram keeps what it has.
  const pairs = (d.links.length * (d.links.length - 1)) / 2;
  let budget = REFINE_BUDGET;
  for (let pass = 0; pass < TERM_ROUNDS && best.n > 0; pass++) {
    const before = best.n;
    for (const block of termBlocks(d, best.lanes[0])) {
      const rest = termBlocks(d, best.lanes[0]).filter((b) => b[0] !== block[0]);
      for (let j = 0; j <= rest.length && budget >= pairs * 2; j++) {
        budget -= pairs * 2;
        const next = score([...rest.slice(0, j), block, ...rest.slice(j)].flat());
        if (next.n < best.n) best = next;
      }
    }
    if (best.n >= before) break;
  }
  return refineDrawn(d, best.lanes, placed);
}

const REFINE_PASSES = 4;
/** Line pairs checked, at most, by the drawn refinement: past it a diagram keeps laneOrder's. */
const REFINE_BUDGET = 20_000_000;

/** ``lanes`` improved by moves judged on the drawing itself: each block of terms, each table with
 * its columns moved to the place in its lane where the drawn lines cross least, then each column
 * inside its table's block, then each product; passes repeat while one gains, at most
 * REFINE_PASSES. */
function refineDrawn(
  d: LaneDiagram,
  lanes: string[][],
  placed: (lanes: string[][]) => (id: string) => At,
): string[][] {
  const tableOf = new Map<string, string>();
  for (const k of d.links)
    if (d.lane.get(k.to) === "table" && d.lane.get(k.from) !== "term") tableOf.set(k.from, k.to);
  // Blocks in the column lane's order: a table and its columns, or a column with no table.
  const toBlocks = (l: string[][]) => {
    const out: { table: string | null; members: string[] }[] = [];
    for (const c of l[1]) {
      const t = tableOf.get(c) ?? null;
      const last = out[out.length - 1];
      if (t !== null && last && last.table === t) last.members.push(c);
      else out.push({ table: t, members: [c] });
    }
    // A table with no column drawn stands as a block of its own, after the table before it.
    l[2].forEach((t, i) => {
      if (out.some((b) => b.table === t)) return;
      const after = i === 0 ? -1 : out.findIndex((b) => b.table === l[2][i - 1]);
      out.splice(after + 1, 0, { table: t, members: [] });
    });
    return out;
  };
  let termRow = termBlocks(d, lanes[0]);
  const fromBlocks = (
    bs: { table: string | null; members: string[] }[],
    products: string[],
    terms: string[][] = termRow,
  ) => [
    terms.flat(),
    bs.flatMap((b) => b.members),
    bs.flatMap((b) => (b.table ? [b.table] : [])),
    products,
  ];
  const pairs = (d.links.length * (d.links.length - 1)) / 2;
  let budget = REFINE_BUDGET;
  const count = (l: string[][]) => {
    budget -= pairs;
    return drawnCrossings(d.links, placed(l));
  };
  let blocks = toBlocks(lanes);
  let products = lanes[3];
  let best = count(lanes);
  // Every place for item ``i`` of ``row``: the first where the drawing crosses least.
  const moveEach = <T>(row: T[], build: (r: T[]) => string[][]): T[] => {
    let cur = [...row];
    for (const v of row) {
      if (budget < pairs * cur.length) return cur;
      const rest = cur.filter((x) => x !== v);
      let bestRow = cur;
      for (let j = 0; j <= rest.length; j++) {
        const cand = [...rest.slice(0, j), v, ...rest.slice(j)];
        const n = count(build(cand));
        if (n < best) {
          best = n;
          bestRow = cand;
        }
      }
      cur = bestRow;
    }
    return cur;
  };
  for (let pass = 0; pass < REFINE_PASSES && best > 0; pass++) {
    const before = best;
    termRow = moveEach(termRow, (ts) => fromBlocks(blocks, products, ts));
    blocks = moveEach(blocks, (bs) => fromBlocks(bs, products));
    for (let i = 0; i < blocks.length; i++) {
      const members = moveEach(blocks[i].members, (ms) =>
        fromBlocks(
          blocks.map((x, j) => (j === i ? { ...x, members: ms } : x)),
          products,
        ),
      );
      blocks = blocks.map((x, j) => (j === i ? { ...x, members } : x));
    }
    products = moveEach(products, (ps) => fromBlocks(blocks, ps));
    if (best >= before) break;
  }
  return fromBlocks(blocks, products);
}

/** What a focused node brings forward: the nodes and the lines between them. */
export interface Connected {
  nodes: Set<string>;
  links: Set<string>;
}

/** Everything connected to ``id`` along the lanes: its own path out in each direction, never
 * doubling back, so a table reached from a term does not bring that table's other columns. A term
 * brings its broader and narrower terms, and each of those the columns, tables and products it
 * reaches; a term link that is not a rank edge, its other end alone. */
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
  const inLane = (x: string, k: LaneLink) => at(k.from) === term && at(k.to) === term && other(x, k) !== null;
  const chain = (from: string[]) => [
    ...walk(from, (x, k) => (inLane(x, k) && k.to === x ? k.from : null)),
    ...walk(from, (x, k) => (inLane(x, k) && k.from === x ? k.to : null)),
  ];
  const across = (from: string[], dir: 1 | -1) =>
    walk(from, (x, k) => {
      const to = other(x, k);
      return to !== null && Math.sign(at(to) - at(x)) === dir ? to : null;
    });
  if (at(id) === term) across(chain([id]), -1);
  else {
    across([id], -1);
    chain(across([id], 1).filter((x) => at(x) === term));
  }
  for (const k of d.arcs)
    if (k.from === id || k.to === id) {
      links.add(k.id);
      nodes.add(k.from).add(k.to);
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
    out.push(
inTerms(k) ? dotted(k.to, k.from, k.label) : `  ${idOf(k.to)} --> ${idOf(k.from)}`
    );
  for (const k of d.arcs) out.push(dotted(k.from, k.to, k.label));
  return out.join("\n");
}
