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
import { edgeClass } from "./layeredLayout";

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
 * lines inside the term lane cross nothing outside it. Same diagram, same order. */
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

const PROJECTIONS = 3;

/** The lanes' order for a drawing whose first pass put each node at ``first`` (the term lane as
 * ELK laid it out on its own, a grid of rows): the terms' order is how their lines meet the
 * column lane, which in a row of terms depends on where the lines go. Each candidate term order
 * -- top to bottom with a row left to right, and right to left, then each term at the height its
 * lines reach the term lane's right edge, from the best order so far -- is ordered by laneOrder,
 * the other lanes spread evenly over the height they had, and the one whose lines cross least as
 * drawn is kept. */
export function placedLaneOrder(d: LaneDiagram, first: (id: string) => At): string[][] {
  const terms = d.graph.nodes.filter((n) => d.lane.get(n.id) === "term").map((n) => n.id);
  const byRow = (dir: 1 | -1) =>
    [...terms].sort((p, q) => first(p).y - first(q).y || dir * (first(p).x - first(q).x));
  // Where each node of the other lanes would be drawn: at its lane's x, spread over the lane's
  // first height in the order given.
  const placed = (lanes: string[][]) => {
    const at = new Map<string, At>();
    for (const row of lanes.slice(1)) {
      if (row.length === 0) continue;
      const ys = row.map((id) => first(id).y);
      const lo = Math.min(...ys);
      const hi = Math.max(...ys);
      row.forEach((id, i) =>
        at.set(id, { x: first(id).x, y: lo + ((i + 0.5) * (hi - lo)) / row.length }),
      );
    }
    for (const t of terms) at.set(t, first(t));
    return (id: string) => at.get(id) as At;
  };
  const score = (order: string[]) => {
    const lanes = laneOrder(d, order);
    return { lanes, n: drawnCrossings(d.links, placed(lanes)) };
  };
  let best = score(byRow(1));
  const other = score(byRow(-1));
  if (other.n < best.n) best = other;
  const edge = Math.max(...terms.map((t) => first(t).x));
  for (let r = 0; r < PROJECTIONS && best.n > 0; r++) {
    const at = placed(best.lanes);
    const reach = new Map<string, number>();
    for (const t of terms) {
      const p = first(t);
      const ends = d.links.filter((k) => k.from === t && d.lane.get(k.to) !== "term").map((k) => at(k.to));
      if (ends.length === 0) {
        reach.set(t, p.y);
        continue;
      }
      const q = { x: ends.reduce((s, e) => s + e.x, 0) / ends.length, y: ends.reduce((s, e) => s + e.y, 0) / ends.length };
      reach.set(t, p.y + ((q.y - p.y) * (edge - p.x)) / (q.x - p.x));
    }
    const next = score(
      [...terms].sort((p, q) => (reach.get(p) as number) - (reach.get(q) as number) || first(p).x - first(q).x),
    );
    if (next.n >= best.n) break;
    best = next;
  }
  return refineDrawn(d, best.lanes, placed);
}

const REFINE_PASSES = 4;
/** Line pairs checked, at most, by the drawn refinement: past it a diagram keeps laneOrder's. */
const REFINE_BUDGET = 20_000_000;

/** ``lanes`` improved by moves judged on the drawing itself: each table with its columns moved to
 * the place in its lane where the drawn lines cross least, then each column inside its table's
 * block, then each product; passes repeat while one gains, at most REFINE_PASSES. */
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
  const fromBlocks = (bs: { table: string | null; members: string[] }[], products: string[]) => [
    lanes[0],
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
