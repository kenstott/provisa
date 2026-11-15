// Copyright (c) 2026 Kenneth Stott
// Canary: 8e3f1a6b-5c27-4d90-b4e8-1f7a9c2d5e30
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174, REQ-2239 (amended 2026-11-11, TERMS SIT LEVEL WITH THEIR COLUMNS; THE SCORE IS THE ANGLE
// OF THE LINES; EACH SIBLING TERM ON ITS OWN ROW; amended 2026-11-12, COLUMNS ARE FIXED; TABLES,
// PRODUCTS AND TERMS ALIGN TO THEM): the lanes preset. Every lane but the last (the ``beside`` lane,
// the terms) is sorted left to right, one lane at a time, by a sort and not a search (sortLanes): the
// first lane keeps the order it is given; each lane after it is sorted by the places of what its nodes
// join in the lanes to their left (the mean when a node joins several; those lanes spread evenly over
// the tallest lane's height for the sort), nodes tied on that key next to those reaching the same
// nodes two lanes on (tables sharing terms), then in the order given. The lane just left of the beside
// lane (the columns) is then fixed: packed, in its sorted order, from the top (alignedRows). Every
// other lane is aligned to it: a node at the midpoint of the top and bottom nodes it reaches there
// through the lanes between (a table its columns, a folded table its more-columns node, a product its
// tables' columns); nodes of one lane whose midpoints fall closer than the node gap keep their order
// and move apart by the least; a node reaching nothing sits a node gap below the one before it. The
// beside lane is placed, not sorted (placeRows), scored by how far its lines to the lane on its left
// are from horizontal: a node joining one node there sits level with it, one joining several level
// with the one leaving its lines flattest in total (fewer crossings, then the higher, on a tie), the
// children of one parent (and a parent joining nothing on its left, with them) a block on adjacent
// rows placed where its lines are flattest in total; a block never takes a row another has taken.
// Each node has its own row. The lines between two nodes of the beside lane run beside it, on its
// right, never through its rows (besideRoutes). Each lane is boxed round its own nodes and those
// lines, titled as a group is, the boxes a gap apart.

import type { CyInstance, CyLayoutOptions } from "../graph/cytoscape-types";
import type { CyCanvas } from "./cyCanvas";
import type { Core, ElementDefinition } from "./core";
import { edgeElement, textWidth, type GraphEdgeSpec } from "./model";
import { SIZE, groupHue } from "./tokens";
import type { At } from "./tipPlacement";
import { segmentCoords } from "./taxi";
import { fitClear } from "./viewport";

export interface LanesOptions {
  /** The lanes, left to right. */
  lanes: { id: string; title: string }[];
  /** The lane whose own lines (between two of its nodes) run beside it, on its right. */
  beside?: string;
  /** A glance (a card): the lanes sit closer. */
  tight?: boolean;
}

/** The space between two lanes' boxes; a glance's lanes sit closer. */
export const LANE_GAP = 56;
const TIGHT_LANE_GAP = 24;
/** A lane's box round its nodes, and the room above them for its title. */
export const LANE_PAD = 16;
/** A lane box's dashed border, as drawn past its size (stylesheet). */
const LANE_EDGE = 2;
const LANE_TITLE = 12;
/** The least gap between two nodes of a lane. */
export const NODE_GAP = 14;
/** How far right of its lane's widest node the first line beside the lane runs, and the step out
 * to each next one. */
const TRACK_INSET = 14;
const TRACK_STEP = 12;
/** The space between a line's run beside the lane and its name, and between two names. */
const NAME_GAP = 4;
/** A line's name: its text background's padding (stylesheet) and its line height. */
const NAME_PAD = 2;
const NAME_LINE = 1.25;

/** A node as placeLanes reads it: its size. */
export interface LaneItem {
  id: string;
  w: number;
  h: number;
}

/** A line between two nodes, and its name (drawn beside it when it runs beside its lane). */
export interface LaneLine {
  id: string;
  source: string;
  target: string;
  label?: string;
}

/** Two keys equal within the float tolerance: a relative 1e-14 (a node with no key, Infinity, equals
 * only another with none). */
const same = (p: number, q: number) =>
  p === q ||
  (Number.isFinite(p) &&
    Number.isFinite(q) &&
    Math.abs(p - q) <= 1e-14 * Math.max(Math.abs(p), Math.abs(q)));

/** ``lanes`` (left to right) each in its drawn order. The first keeps its order, and so does the
 * ``beside`` lane (placeRows places it). Each other lane is sorted, once, by the places (each lane but
 * the beside lane spread evenly, spreadRows) of what its nodes join in the lanes to their left: a
 * node by the mean height of those; a node joining nothing to its left by the nodes of its own lane it
 * joins that are sorted (the mean of theirs), and one joining neither after the rest. Nodes tied on that key (the tables of one
 * product) stand next to those reaching the most of the same nodes two or more lanes on (the terms
 * their columns bind): each after the first is the one sharing most with the one before it, ties and
 * none shared in the order given. Placing a lane moves no lane to its left, so each lane is sorted on
 * its left neighbours' final places. */
export function sortLanes(
  lanes: { id: string; items: LaneItem[] }[],
  lines: LaneLine[],
  beside?: string,
  gap: number = LANE_GAP,
  overlay: ReadonlySet<string> = new Set(),
): { id: string; items: LaneItem[] }[] {
  const laneOf = new Map<string, number>();
  lanes.forEach((l, i) => l.items.forEach((n) => laneOf.set(n.id, i)));
  const near = new Map<string, string[]>();
  const join = (a: string, b: string) => near.set(a, [...(near.get(a) ?? []), b]);
  for (const k of lines) {
    if (!laneOf.has(k.source) || !laneOf.has(k.target)) continue;
    join(k.source, k.target);
    join(k.target, k.source);
  }
  /** The nodes two or more lanes right of ``id`` reached by stepping right. */
  const reach = (id: string): Set<string> => {
    const from = laneOf.get(id) as number;
    const out = new Set<string>();
    const seen = new Set([id]);
    const todo = [id];
    while (todo.length > 0) {
      const at = todo.pop() as string;
      for (const m of near.get(at) ?? []) {
        if (seen.has(m) || (laneOf.get(m) as number) <= (laneOf.get(at) as number)) continue;
        seen.add(m);
        todo.push(m);
        if ((laneOf.get(m) as number) >= from + 2) out.add(m);
      }
    }
    return out;
  };
  const out = lanes.map((l) => ({ id: l.id, items: [...l.items] }));
  for (let i = 1; i < out.length; i++) {
    if (out[i].id === beside) continue;
    const at = placeAcross(out, lines, beside, gap, overlay, spreadRows(out, beside));
    const y = (id: string) => (at.get(id) as At).y;
    const mean = (ys: number[]) => ys.reduce((s, v) => s + v, 0) / ys.length;
    const key = new Map<string, number>();
    for (const n of out[i].items) {
      const ys = (near.get(n.id) ?? []).filter((m) => (laneOf.get(m) as number) < i).map(y);
      if (ys.length > 0) key.set(n.id, mean(ys));
    }
    // A node joining nothing to its left takes its place from the nodes of its lane it joins, nearest
    // the keyed ones first, in the order given.
    for (let grew = true; grew; ) {
      grew = false;
      for (const n of out[i].items) {
        if (key.has(n.id)) continue;
        const ks = (near.get(n.id) ?? [])
          .filter((m) => laneOf.get(m) === i && key.has(m))
          .map((m) => key.get(m) as number);
        if (ks.length === 0) continue;
        key.set(n.id, mean(ks));
        grew = true;
      }
    }
    const sorted = out[i].items
      .map((n, order) => ({ n, k: key.get(n.id) ?? Infinity, order }))
      .sort((p, q) => (same(p.k, q.k) ? 0 : p.k - q.k) || p.order - q.order);
    const items: LaneItem[] = [];
    for (let a = 0; a < sorted.length; ) {
      let b = a + 1;
      while (b < sorted.length && same(sorted[b].k, sorted[a].k)) b++;
      items.push(
        ...chainShared(
          sorted.slice(a, b).map((x) => x.n),
          reach,
        ),
      );
      a = b;
    }
    out[i].items = items;
  }
  return out;
}

/** ``tied`` (in the order given) with each after the first the one sharing most of ``reach`` with
 * the one before it; ties, and none shared, in the order given. */
function chainShared(tied: LaneItem[], reach: (id: string) => Set<string>): LaneItem[] {
  if (tied.length < 3) return tied;
  const reached = new Map(tied.map((n) => [n.id, reach(n.id)]));
  const shared = (a: string, b: string) => {
    const rb = reached.get(b) as Set<string>;
    let n = 0;
    for (const x of reached.get(a) as Set<string>) if (rb.has(x)) n++;
    return n;
  };
  const left = [...tied];
  const out = [left.shift() as LaneItem];
  while (left.length > 0) {
    const last = out[out.length - 1].id;
    let pick = 0;
    for (let j = 1; j < left.length; j++)
      if (shared(last, left[j].id) > shared(last, left[pick].id)) pick = j;
    out.push(left.splice(pick, 1)[0]);
  }
  return out;
}

/** The lines between two nodes of ``lane`` (``laneOf``) given its order: each on a track beside the
 * lane, the first track nearest it. A line takes the first track where it meets no line it shares no
 * node with along its span (lines from one node share a track, a comb), the shortest lines first, so a
 * line nests inside a longer one and two lines cross only where their spans interleave. */
function besideTracks(order: string[], lines: LaneLine[]): Map<string, number> {
  const row = new Map(order.map((id, i) => [id, i]));
  const spans = lines
    .filter((k) => row.has(k.source) && row.has(k.target) && k.source !== k.target)
    .map((k) => {
      const a = row.get(k.source) as number;
      const b = row.get(k.target) as number;
      return { k, lo: Math.min(a, b), hi: Math.max(a, b) };
    })
    .sort((p, q) => p.hi - p.lo - (q.hi - q.lo) || p.lo - q.lo || (p.k.id < q.k.id ? -1 : 1));
  const tracks: (typeof spans)[] = [];
  const out = new Map<string, number>();
  const shares = (p: LaneLine, q: LaneLine) =>
    [p.source, p.target].some((id) => id === q.source || id === q.target);
  for (const s of spans) {
    let t = 0;
    while (
      t < tracks.length &&
      tracks[t].some((o) => o.lo <= s.hi && s.lo <= o.hi && !shares(o.k, s.k))
    )
      t++;
    if (t === tracks.length) tracks.push([]);
    tracks[t].push(s);
    out.set(s.k.id, t);
  }
  return out;
}

/** Where each node of ``lanes`` (left to right, each in its order top to bottom) is drawn. Each node
 * has its own row; each lane is as wide as its widest node, its nodes centred in it, and as wide again
 * as the tracks of the lines beside it (the ``beside`` lane, its ``lines`` inside it); the lanes' boxes
 * stand ``gap`` apart. The heights are alignedRows' (the fixed lane packed, the others aligned to it);
 * the beside lane's rows are placed by placeRows on the places of the lanes to its left; its
 * ``overlay`` lines place nothing. */
export function placeLanes(
  lanes: { id: string; items: LaneItem[] }[],
  lines: LaneLine[] = [],
  beside?: string,
  gap: number = LANE_GAP,
  overlay: ReadonlySet<string> = new Set(),
): Map<string, At> {
  return placeAcross(lanes, lines, beside, gap, overlay, alignedRows(lanes, lines, beside));
}

/** The lanes placed as placeLanes places them, with ``rows`` the height of each node of every lane
 * but the beside lane. */
function placeAcross(
  lanes: { id: string; items: LaneItem[] }[],
  lines: LaneLine[],
  beside: string | undefined,
  gap: number,
  overlay: ReadonlySet<string>,
  rows: Map<string, number>,
): Map<string, At> {
  const out = new Map<string, At>();
  let left = 0;
  for (const { id, items } of lanes) {
    if (items.length === 0) continue;
    const width = Math.max(...items.map((n) => n.w));
    const ys = id === beside ? placeRows(items, lines, overlay, (m) => out.get(m)?.y) : rows;
    for (const n of items) out.set(n.id, { x: left + width / 2, y: ys.get(n.id) as number });
    let room = 0;
    if (id === beside) {
      const placed = items.map((n) => ({ ...n, ...(out.get(n.id) as At) }));
      const routes = routesBeside(placed, lines);
      const right = left + width;
      for (const path of routes.values()) room = Math.max(room, path[1].x - right);
      for (const b of besideNames(routes, lines).values()) room = Math.max(room, b.x2 - right);
    }
    left += width + room + 2 * (LANE_PAD + LANE_EDGE) + gap;
  }
  return out;
}

/** The heights sortLanes keys on: every lane but the ``beside`` lane spread over the height of the
 * tallest of them packed, its first node at the top, its last at the bottom, the space between
 * shared evenly (a lone node at the middle). The sort reads a lane's order from these, so a lane's
 * key does not wait on the lanes it orders. */
function spreadRows(
  lanes: { id: string; items: LaneItem[] }[],
  beside: string | undefined,
): Map<string, number> {
  const packed = (items: LaneItem[]) =>
    items.reduce((sum, n) => sum + n.h, 0) + NODE_GAP * Math.max(0, items.length - 1);
  const height = Math.max(0, ...lanes.filter((l) => l.id !== beside).map((l) => packed(l.items)));
  const out = new Map<string, number>();
  for (const { id, items } of lanes) {
    if (id === beside || items.length === 0) continue;
    const free = height - items.reduce((sum, n) => sum + n.h, 0);
    const step = items.length > 1 ? free / (items.length - 1) : 0;
    let top = items.length > 1 ? 0 : (height - items[0].h) / 2;
    for (const n of items) {
      out.set(n.id, top + n.h / 2);
      top += n.h + step;
    }
  }
  return out;
}

/** The height of each node of every lane but the ``beside`` lane (REQ-2239, amended 2026-11-12,
 * COLUMNS ARE FIXED; TABLES, PRODUCTS AND TERMS ALIGN TO THEM). The fixed lane, the one just left of
 * the beside lane (the last lane when there is none), is packed in its order from the top, a node gap
 * between rows. Every other lane is aligned to it: a node wants the midpoint of the top and bottom
 * nodes it reaches in the fixed lane along ``lines``, each step a lane nearer the fixed lane (a table
 * its columns, or its one more-columns node; a product its tables' columns). A lane keeps its order;
 * where two wanted places fall closer than the node gap, the nodes move apart by the least (the
 * summed square of the moves). A node reaching nothing in the fixed lane wants nothing: it keeps its
 * place in the order, a node gap below the node before it (above the node after it when none is
 * before it); a lane where no node reaches the fixed lane is packed from the top. */
function alignedRows(
  lanes: { id: string; items: LaneItem[] }[],
  lines: LaneLine[],
  beside: string | undefined,
): Map<string, number> {
  const out = new Map<string, number>();
  const at = lanes.findIndex((l) => l.id === beside);
  const fixed = at >= 0 ? at - 1 : lanes.length - 1;
  if (fixed >= 0) stack(lanes[fixed].items, new Map(), out);
  const laneOf = new Map<string, number>();
  lanes.forEach((l, i) => l.items.forEach((n) => laneOf.set(n.id, i)));
  const near = new Map<string, string[]>();
  const join = (a: string, b: string) => near.set(a, [...(near.get(a) ?? []), b]);
  for (const k of lines) {
    if (!laneOf.has(k.source) || !laneOf.has(k.target)) continue;
    join(k.source, k.target);
    join(k.target, k.source);
  }
  /** The heights of the fixed lane's nodes ``id`` reaches, each step a lane nearer it. */
  const reached = (id: string): number[] => {
    const toward = (a: number, b: number) =>
      Math.sign(b - a) === Math.sign(fixed - a) && Math.abs(fixed - b) < Math.abs(fixed - a);
    const ys: number[] = [];
    const seen = new Set([id]);
    const todo = [id];
    while (todo.length > 0) {
      const from = todo.pop() as string;
      for (const m of near.get(from) ?? []) {
        if (seen.has(m) || !toward(laneOf.get(from) as number, laneOf.get(m) as number)) continue;
        seen.add(m);
        if (laneOf.get(m) === fixed) ys.push(out.get(m) as number);
        else todo.push(m);
      }
    }
    return ys;
  };
  lanes.forEach(({ id, items }, i) => {
    if (i === fixed || id === beside) return;
    const want = new Map<string, number>();
    for (const n of items) {
      const ys = reached(n.id);
      if (ys.length > 0) want.set(n.id, (Math.min(...ys) + Math.max(...ys)) / 2);
    }
    stack(items, want, out);
  });
  return out;
}

/** ``items`` (one lane, in order) each at the height it ``want``s, moved apart by the least (the
 * summed square of the moves) so that each two keep their order a node gap apart; one wanting no
 * place sits a node gap below the one before it (above the one after it when none is before it), and
 * a lane wanting none is packed from the top. Into ``out``. */
function stack(items: LaneItem[], want: Map<string, number>, out: Map<string, number>): void {
  if (items.length === 0) return;
  // The packed offset of each node from the first: the least distance the order allows.
  const off: number[] = [0];
  for (let i = 1; i < items.length; i++)
    off.push(off[i - 1] + items[i - 1].h / 2 + NODE_GAP + items[i].h / 2);
  const wanting = items.flatMap((n, i) => (want.has(n.id) ? [i] : []));
  const y: (number | undefined)[] = items.map(() => undefined);
  if (wanting.length === 0) items.forEach((_, i) => (y[i] = items[0].h / 2 + off[i]));
  else {
    // Less its offset, each wanted place must not fall: pool adjacent violators to their mean.
    const pools: { sum: number; n: number; from: number }[] = [];
    for (const [k, i] of wanting.entries()) {
      pools.push({ sum: (want.get(items[i].id) as number) - off[i], n: 1, from: k });
      while (pools.length > 1) {
        const q = pools[pools.length - 1];
        const p = pools[pools.length - 2];
        if (p.sum / p.n <= q.sum / q.n) break;
        p.sum += q.sum;
        p.n += q.n;
        pools.pop();
      }
    }
    pools.forEach((p, j) => {
      const to = j + 1 < pools.length ? pools[j + 1].from : wanting.length;
      for (let k = p.from; k < to; k++) y[wanting[k]] = p.sum / p.n + off[wanting[k]];
    });
    const first = wanting[0];
    for (let i = first - 1; i >= 0; i--) y[i] = (y[first] as number) - (off[first] - off[i]);
    for (let i = first + 1; i < items.length; i++)
      if (y[i] === undefined) y[i] = (y[i - 1] as number) + (off[i] - off[i - 1]);
  }
  items.forEach((n, i) => out.set(n.id, y[i] as number));
}

/** ``x <= y`` within the float tolerance. */
const atMost = (x: number, y: number) => x <= y || same(x, y);

/** The centre height of each of ``items`` (one lane), each on its own row, a node gap between rows.
 * ``yOf`` gives the place of a node already drawn to the left, which is what a node's lines out of
 * the lane run to. A line's score is how far it is from horizontal: its vertical run (the lanes stand
 * a fixed distance apart). The lane's own lines (``lines`` with both ends in it, ``overlay`` aside)
 * run from a parent to its children: the children of one parent, and a parent with no line out of
 * the lane with its children, move as one block, its rows adjacent in the order of their lines' ends
 * (a parent above its children), ties in the order given. Each unit (a node alone or a block) is
 * placed once, where its lines' summed run is least among the places no unit placed before it has
 * taken; ties to the fewer crossings with the lines already placed, then a place level with one of its
 * lines, then the higher. The units go one at a time: nodes with one line out first (level with its
 * end), then those with several, then blocks, then those with none, which sit nearest the nodes of the
 * lane they join (below the rest when they join none); within each, from the top, then in the order
 * given. */
export function placeRows(
  items: LaneItem[],
  lines: LaneLine[],
  overlay: ReadonlySet<string>,
  yOf: (id: string) => number | undefined,
): Map<string, number> {
  const index = new Map(items.map((n, i) => [n.id, i]));
  const size = new Map(items.map((n) => [n.id, n.h]));
  const wants = new Map<string, number[]>(items.map((n) => [n.id, []]));
  const children = new Map<string, string[]>();
  const kin = new Map<string, string[]>(items.map((n) => [n.id, []]));
  const seen = new Set<string>();
  for (const k of lines) {
    const s = index.has(k.source);
    const g = index.has(k.target);
    if (s && g) {
      if (k.source === k.target) continue;
      (kin.get(k.source) as string[]).push(k.target);
      (kin.get(k.target) as string[]).push(k.source);
      const pair = `${k.source}>${k.target}`;
      if (overlay.has(k.id) || seen.has(pair)) continue;
      seen.add(pair);
      children.set(k.source, [...(children.get(k.source) ?? []), k.target]);
    } else if (s || g) {
      const y = yOf(s ? k.target : k.source);
      if (y !== undefined) (wants.get(s ? k.source : k.target) as number[]).push(y);
    }
  }
  const root = new Map(items.map((n) => [n.id, n.id]));
  const find = (id: string): string => {
    const r = root.get(id) as string;
    return r === id ? id : find(r);
  };
  const union = (a: string, b: string) => {
    const [p, q] = [find(a), find(b)];
    if (p !== q) root.set(q, p);
  };
  for (const [p, kids] of children) {
    if (kids.length > 1) for (const k of kids) union(kids[0], k);
    if ((wants.get(p) as number[]).length === 0) for (const k of kids) union(p, k);
  }

  /** The height that leaves ``ys`` flattest in total: their median (the upper of the two middle). */
  const flattest = (ys: number[]) => [...ys].sort((p, q) => p - q)[Math.floor((ys.length - 1) / 2)];
  // A member's place in its block: by its lines' ends; one with none by its children's, above them.
  const depth = new Map<string, number>();
  const keyOf = new Map<string, number>();
  const key = (id: string, path: Set<string> = new Set()): number => {
    const known = keyOf.get(id);
    if (known !== undefined) return known;
    const ys = wants.get(id) as number[];
    let k = Infinity;
    if (ys.length > 0) k = flattest(ys);
    else if (!path.has(id))
      for (const c of children.get(id) ?? [])
        if (find(c) === find(id)) k = Math.min(k, key(c, new Set([...path, id])));
    keyOf.set(id, k);
    return k;
  };
  const depthOf = (id: string, path: Set<string> = new Set()): number => {
    const known = depth.get(id);
    if (known !== undefined) return known;
    let d = 0;
    for (const [p, kids] of children)
      if (kids.includes(id) && find(p) === find(id) && !path.has(p))
        d = Math.max(d, depthOf(p, new Set([...path, id])) + 1);
    depth.set(id, d);
    return d;
  };
  const groups = new Map<string, string[]>();
  for (const n of items) groups.set(find(n.id), [...(groups.get(find(n.id)) ?? []), n.id]);
  const units = [...groups.values()].map((ids) => {
    const members = [...ids].sort(
      (p, q) =>
        (same(key(p), key(q)) ? 0 : key(p) - key(q)) ||
        depthOf(p) - depthOf(q) ||
        (index.get(p) as number) - (index.get(q) as number),
    );
    const off: number[] = [];
    let h = 0;
    for (const id of members) {
      off.push(h + (size.get(id) as number) / 2);
      h += (size.get(id) as number) + NODE_GAP;
    }
    const lineCount = members.reduce((s, id) => s + (wants.get(id) as number[]).length, 0);
    const kind =
      members.length > 1 ? (lineCount > 0 ? 2 : 3) : lineCount === 1 ? 0 : lineCount > 1 ? 1 : 3;
    return { members, off, h: h - NODE_GAP, kind, first: index.get(members[0]) as number };
  });
  const ideal = (u: (typeof units)[number]) =>
    u.kind === 3
      ? Infinity
      : key(u.members.find((id) => (wants.get(id) as number[]).length > 0) as string);
  units.sort(
    (p, q) =>
      p.kind - q.kind || (same(ideal(p), ideal(q)) ? 0 : ideal(p) - ideal(q)) || p.first - q.first,
  );

  const out = new Map<string, number>();
  const taken: [number, number][] = [];
  const fits = (top: number, h: number) =>
    taken.every(([a, b]) => atMost(top + h + NODE_GAP, a) || atMost(b + NODE_GAP, top));
  /** Pairs of lines out of the lane crossing, ``u``'s at ``top`` against those placed. */
  const crossings = (u: (typeof units)[number], top: number) => {
    let n = 0;
    u.members.forEach((id, i) => {
      const y = top + u.off[i];
      for (const c of wants.get(id) as number[])
        for (const [o, oy] of out)
          for (const oc of wants.get(o) as number[])
            if (!same(c, oc) && !same(y, oy) && (c - oc) * (y - oy) < 0) n++;
    });
    return n;
  };
  for (const u of units) {
    const level: number[] = [];
    u.members.forEach((id, i) =>
      (wants.get(id) as number[]).forEach((c) => level.push(c - u.off[i])),
    );
    let score: (top: number) => number;
    if (u.kind === 3) {
      const near = u.members
        .flatMap((id) => kin.get(id) as string[])
        .map((m) => out.get(m))
        .filter((y): y is number => y !== undefined);
      const bottom = Math.max(0, ...taken.map(([, b]) => b + NODE_GAP));
      const aim =
        near.length > 0 ? near.reduce((s, v) => s + v, 0) / near.length - u.h / 2 : bottom;
      level.push(aim);
      score = (top) => Math.abs(top - aim);
    } else
      score = (top) =>
        u.members.reduce(
          (s, id, i) =>
            s + (wants.get(id) as number[]).reduce((t, c) => t + Math.abs(top + u.off[i] - c), 0),
          0,
        );
    const tops = [
      ...level.map((top) => ({ top, level: 0 })),
      ...taken.flatMap(([a, b]) => [
        { top: a - NODE_GAP - u.h, level: 1 },
        { top: b + NODE_GAP, level: 1 },
      ]),
    ]
      .filter((c) => fits(c.top, u.h))
      .map((c) => ({
        ...c,
        score: score(c.top),
        crossings: u.kind === 3 ? 0 : crossings(u, c.top),
      }));
    tops.sort(
      (p, q) =>
        (same(p.score, q.score) ? 0 : p.score - q.score) ||
        p.crossings - q.crossings ||
        p.level - q.level ||
        p.top - q.top,
    );
    const top = tops[0].top;
    u.members.forEach((id, i) => out.set(id, top + u.off[i]));
    taken.push([top, top + u.h]);
  }
  return out;
}

/** The route of each line between two nodes of ``lane`` as drawn on ``cy``: out of its first node's
 * right side to its track beside the lane, down (or up) the track, and into its other node's right
 * side, by id. */
export function besideRoutes(cy: Core, lane: string): Map<string, At[]> {
  return routesBeside(
    placedItems(cy).filter((n) => n.lane === lane),
    drawnLines(cy),
  );
}

/** The lines drawn on ``cy``, with their names. */
function drawnLines(cy: Core): LaneLine[] {
  const lines: LaneLine[] = [];
  cy.edges().forEach((e) => {
    lines.push({
      id: e.id(),
      source: e.data("source") as string,
      target: e.data("target") as string,
      label: (e.data("label") as string | undefined) ?? "",
    });
  });
  return lines;
}

/** The routes of those of ``lines`` between two of ``items`` (one lane, placed), as besideRoutes. */
function routesBeside(
  items: (LaneItem & { x: number; y: number })[],
  all: LaneLine[],
): Map<string, At[]> {
  const out = new Map<string, At[]>();
  if (items.length === 0) return out;
  const byId = new Map(items.map((n) => [n.id, n]));
  const lines = all.filter((k) => byId.has(k.source) && byId.has(k.target));
  const order = [...items].sort((p, q) => p.y - q.y).map((n) => n.id);
  const tracks = besideTracks(order, lines);
  const right = Math.max(...items.map((n) => n.x + n.w / 2));
  for (const k of lines) {
    const t = tracks.get(k.id);
    if (t === undefined) continue;
    const x = right + TRACK_INSET + t * TRACK_STEP;
    const s = byId.get(k.source) as (typeof items)[number];
    const g = byId.get(k.target) as (typeof items)[number];
    out.set(k.id, [
      { x: s.x + s.w / 2, y: s.y },
      { x, y: s.y },
      { x, y: g.y },
      { x: g.x + g.w / 2, y: g.y },
    ]);
  }
  return out;
}

/** A box drawn on the canvas. */
export interface NameBox {
  x1: number;
  y1: number;
  x2: number;
  y2: number;
}

const overlaps = (p: NameBox, q: NameBox) =>
  p.x1 < q.x2 + NAME_GAP &&
  q.x1 < p.x2 + NAME_GAP &&
  p.y1 < q.y2 + NAME_GAP &&
  q.y1 < p.y2 + NAME_GAP;

/** Where the name of each of ``lines`` running beside its lane on ``routes`` (besideRoutes) is
 * drawn, by id: right of its run down its track (``NAME_GAP`` past it), growing away from the lane,
 * centred on that run's middle. A name never lies on another name or on any line's route: it moves
 * along its own run, a name's height at a time, nearest the middle first (above before below), to
 * the first place clear of them; where its run has none, it moves right past what it meets and
 * tries again. Names go in turn from the track nearest the lane, each down its track from the top,
 * then by id. A line with no name has no box. */
export function besideNames(routes: Map<string, At[]>, lines: LaneLine[]): Map<string, NameBox> {
  const label = new Map(lines.map((k) => [k.id, k.label ?? ""]));
  const size = SIZE.edgeLabel;
  const h = size * NAME_LINE + 2 * NAME_PAD;
  const thin = (a: At, b: At): NameBox => ({
    x1: Math.min(a.x, b.x),
    x2: Math.max(a.x, b.x),
    y1: Math.min(a.y, b.y),
    y2: Math.max(a.y, b.y),
  });
  const taken: NameBox[] = [];
  routes.forEach((p) => {
    for (let i = 1; i < p.length; i++) taken.push(thin(p[i - 1], p[i]));
  });
  const named = [...routes]
    .filter(([id]) => (label.get(id) ?? "") !== "")
    .map(([id, p]) => ({
      id,
      x: p[1].x,
      lo: Math.min(p[1].y, p[2].y),
      hi: Math.max(p[1].y, p[2].y),
    }))
    .sort((p, q) => p.x - q.x || p.lo + p.hi - (q.lo + q.hi) || (p.id < q.id ? -1 : 1));
  const out = new Map<string, NameBox>();
  for (const n of named) {
    const w = textWidth(label.get(n.id) as string, size) + 2 * NAME_PAD;
    const mid = (n.lo + n.hi) / 2;
    const ys = [mid];
    for (let k = 1; mid - k * (h + NAME_GAP) >= n.lo || mid + k * (h + NAME_GAP) <= n.hi; k++)
      for (const y of [mid - k * (h + NAME_GAP), mid + k * (h + NAME_GAP)])
        if (y >= n.lo && y <= n.hi) ys.push(y);
    let x1 = n.x + NAME_GAP;
    let placed: NameBox | undefined;
    while (placed === undefined) {
      const at = (y: number): NameBox => ({ x1, x2: x1 + w, y1: y - h / 2, y2: y + h / 2 });
      const y = ys.find((c) => taken.every((t) => !overlaps(at(c), t)));
      if (y !== undefined) placed = at(y);
      else
        x1 =
          Math.max(...taken.filter((t) => overlaps(at(mid), t)).map((t) => t.x2)) + NAME_GAP + 1e-6;
    }
    taken.push(placed);
    out.set(n.id, placed);
  }
  return out;
}

/** Lays ``cy`` out in lanes; ``done`` once it is placed. */
export function runLanes(
  cy: Core,
  canvas: CyCanvas,
  opts: LanesOptions,
  overlay: GraphEdgeSpec[],
  done: () => void,
): void {
  // Nothing is placed by a layout: the order is a sort, the beside lane's rows are placed by their
  // lines, and the places follow from them. The null
  // layout only gives the canvas its one run, which ends the core cleanly if it goes first.
  canvas.run(cy.layout({ name: "null" } as CyLayoutOptions), () => {
    cy.add(overlay.map((e) => edgeElement(e)) as unknown as ElementDefinition[]);
    const items = placedItems(cy);
    const lines = drawnLines(cy);
    const gap = opts.tight ? TIGHT_LANE_GAP : LANE_GAP;
    const laid = new Set(overlay.map((e) => e.id));
    const lanes = sortLanes(
      opts.lanes.map((l) => ({ id: l.id, items: items.filter((n) => n.lane === l.id) })),
      lines,
      opts.beside,
      gap,
      laid,
    );
    const where = placeLanes(lanes, lines, opts.beside, gap, laid);
    cy.batch(() => where.forEach((p, id) => cy.getElementById(id).position(p)));
    if (opts.beside !== undefined) routeBeside(cy, opts.beside);
    boxLanes(cy, opts);
    fitClear(cy as unknown as CyInstance, 24);
    done();
  });
}

/** The lines inside ``lane`` drawn on their routes beside it (besideRoutes), as right-angled
 * segments from each node's right side. */
function routeBeside(cy: Core, lane: string): void {
  const routes = besideRoutes(cy, lane);
  const names = besideNames(routes, drawnLines(cy));
  routes.forEach((path, id) => {
    const [s, p1, p2, t] = path;
    const c1 = segmentCoords(p1, s, t);
    const c2 = segmentCoords(p2, s, t);
    const e = cy.getElementById(id);
    const half = (end: string) => (cy.getElementById(end).data("w") as number) / 2;
    e.style({
      "curve-style": "segments",
      "edge-distances": "endpoints",
      "source-endpoint": `${half(e.data("source") as string)}px 0px`,
      "target-endpoint": `${half(e.data("target") as string)}px 0px`,
      "segment-weights": `${c1.weight} ${c2.weight}`,
      "segment-distances": `${c1.distance} ${c2.distance}`,
    });
    // Its name beside its run down the track (besideNames): cytoscape centres a segments edge's
    // label on the middle of its two bends, so the margins move it from there.
    const b = names.get(id);
    if (b !== undefined)
      e.style({
        "text-margin-x": (b.x1 + b.x2) / 2 - (p1.x + p2.x) / 2,
        "text-margin-y": (b.y1 + b.y2) / 2 - (p1.y + p2.y) / 2,
      });
  });
}

/** The nodes laid out (not a lane's box): each one's lane, size and place. */
function placedItems(cy: Core): (LaneItem & { lane: string; x: number; y: number })[] {
  const out: (LaneItem & { lane: string; x: number; y: number })[] = [];
  cy.nodes('[kind = "node"], [kind = "more"]').forEach((n) => {
    const p = n.position();
    out.push({
      id: n.id(),
      lane: n.data("lane") as string,
      w: n.data("w") as number,
      h: n.data("h") as number,
      x: p.x,
      y: p.y,
    });
  });
  return out;
}

/** Each lane boxed round its own nodes and the lines beside it, its title above them. */
function boxLanes(cy: Core, opts: LanesOptions): void {
  const items = placedItems(cy);
  const routes =
    opts.beside === undefined ? new Map<string, At[]>() : besideRoutes(cy, opts.beside);
  const names = [...besideNames(routes, drawnLines(cy)).values()];
  const trackRight = Math.max(
    -Infinity,
    ...[...routes.values()].map((p) => p[1].x),
    ...names.map((b) => b.x2),
  );
  for (const lane of opts.lanes) {
    const inLane = items.filter((n) => n.lane === lane.id);
    if (inLane.length === 0) continue;
    const x1 = Math.min(...inLane.map((n) => n.x - n.w / 2)) - LANE_PAD;
    const nodesRight = Math.max(...inLane.map((n) => n.x + n.w / 2));
    const x2 = (lane.id === opts.beside ? Math.max(nodesRight, trackRight) : nodesRight) + LANE_PAD;
    const own = lane.id === opts.beside ? names : [];
    const y1 =
      Math.min(...inLane.map((n) => n.y - n.h / 2), ...own.map((b) => b.y1)) -
      LANE_PAD -
      LANE_TITLE;
    const y2 = Math.max(...inLane.map((n) => n.y + n.h / 2), ...own.map((b) => b.y2)) + LANE_PAD;
    cy.add({
      group: "nodes",
      data: {
        id: `lane:${lane.id}`,
        kind: "lane",
        title: lane.title.toUpperCase(),
        hue: groupHue(lane.id),
        w: x2 - x1,
        h: y2 - y1,
      },
      position: { x: (x1 + x2) / 2, y: (y1 + y2) / 2 },
    });
  }
}
