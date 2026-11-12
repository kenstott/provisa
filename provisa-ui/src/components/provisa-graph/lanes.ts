// Copyright (c) 2026 Kenneth Stott
// Canary: 8e3f1a6b-5c27-4d90-b4e8-1f7a9c2d5e30
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174, REQ-2239 (amended 2026-11-11, EACH LANE IS SORTED BY THE LANE ON ITS LEFT; EACH SIBLING
// TERM ON ITS OWN ROW): the lanes preset. The lanes are ordered left to right, one lane at a time, by a
// sort and not a search (sortLanes): the first lane keeps the order it is given; each lane after it is
// sorted by the places of what its nodes join in the lanes to their left (the mean when a node joins
// several), ties in the order given, so the same graph always draws the same way. Then the lanes are
// placed from that order (placeLanes): side by side left to right, each as wide as its widest node,
// every node on its own row, every lane spread over the height of the tallest, so a lane short of nodes
// is not packed at the middle. The lines between two nodes of one lane (the term lane's rank lines and
// the links laid over it) run beside that lane, on its right, never through its rows (besideRoutes).
// Each lane is boxed round its own nodes and those lines, titled as a group is, the boxes a gap apart.

import type { CyInstance, CyLayoutOptions } from "../graph/cytoscape-types";
import type { CyCanvas } from "./cyCanvas";
import type { Core, ElementDefinition } from "./core";
import { edgeElement, type GraphEdgeSpec } from "./model";
import { groupHue } from "./tokens";
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

/** A node as placeLanes reads it: its size. */
export interface LaneItem {
  id: string;
  w: number;
  h: number;
}

/** A line between two nodes. */
export interface LaneLine {
  id: string;
  source: string;
  target: string;
}

/** Two keys equal within the float tolerance: a relative 1e-14 (a node with no key, Infinity, equals
 * only another with none). */
const same = (p: number, q: number) =>
  p === q ||
  (Number.isFinite(p) &&
    Number.isFinite(q) &&
    Math.abs(p - q) <= 1e-14 * Math.max(Math.abs(p), Math.abs(q)));

/** ``lanes`` (left to right) each in its drawn order. The first keeps its order. Each lane after it is
 * sorted, once, by the places (as placeLanes draws them) of what its nodes join in the lanes to their
 * left: a node by the mean height of those; a node joining nothing to its left by the nodes of its own
 * lane it joins that are sorted (the mean of theirs), and one joining neither after the rest; ties in
 * the order given. Placing a lane moves no lane to its left, so each lane is sorted on its left
 * neighbours' final places. */
export function sortLanes(
  lanes: { id: string; items: LaneItem[] }[],
  lines: LaneLine[],
  beside?: string,
  gap: number = LANE_GAP,
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
  const out = lanes.map((l) => ({ id: l.id, items: [...l.items] }));
  for (let i = 1; i < out.length; i++) {
    const at = placeLanes(out, lines, beside, gap);
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
    out[i].items = out[i].items
      .map((n, order) => ({ n, k: key.get(n.id) ?? Infinity, order }))
      .sort((p, q) => (same(p.k, q.k) ? 0 : p.k - q.k) || p.order - q.order)
      .map((x) => x.n);
  }
  return out;
}

/** The lines between two nodes of ``lane`` (``laneOf``) given its order: each on a track beside the
 * lane, the first track nearest it. A line takes the first track where it meets no line it shares no
 * node with along its span (lines from one node share a track, a comb), the shortest lines first, so a
 * line nests inside a longer one and two lines cross only where their spans interleave. */
function besideTracks(
  order: string[],
  lines: LaneLine[],
): Map<string, number> {
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
 * stand ``gap`` apart. Every lane is spread over the height of the tallest packed lane: its first node
 * at the top, its last at the bottom, the space between shared evenly; a lane of one node sits at the
 * middle. Spreading keeps each lane's order. */
export function placeLanes(
  lanes: { id: string; items: LaneItem[] }[],
  lines: LaneLine[] = [],
  beside?: string,
  gap: number = LANE_GAP,
): Map<string, At> {
  const packed = (items: LaneItem[]) =>
    items.reduce((sum, n) => sum + n.h, 0) + NODE_GAP * Math.max(0, items.length - 1);
  const height = Math.max(0, ...lanes.map((l) => packed(l.items)));
  const out = new Map<string, At>();
  let left = 0;
  for (const { id, items } of lanes) {
    if (items.length === 0) continue;
    const width = Math.max(...items.map((n) => n.w));
    const free = height - items.reduce((sum, n) => sum + n.h, 0);
    const step = items.length > 1 ? free / (items.length - 1) : 0;
    let top = items.length > 1 ? 0 : (height - items[0].h) / 2;
    for (const n of items) {
      out.set(n.id, { x: left + width / 2, y: top + n.h / 2 });
      top += n.h + step;
    }
    const tracks =
      id === beside
        ? new Set(besideTracks(items.map((n) => n.id), lines).values()).size
        : 0;
    const room = tracks > 0 ? TRACK_INSET + (tracks - 1) * TRACK_STEP : 0;
    left += width + room + 2 * (LANE_PAD + LANE_EDGE) + gap;
  }
  return out;
}

/** The route of each line between two nodes of ``lane`` as drawn on ``cy``: out of its first node's
 * right side to its track beside the lane, down (or up) the track, and into its other node's right
 * side, by id. */
export function besideRoutes(cy: Core, lane: string): Map<string, At[]> {
  const items = placedItems(cy).filter((n) => n.lane === lane);
  const out = new Map<string, At[]>();
  if (items.length === 0) return out;
  const byId = new Map(items.map((n) => [n.id, n]));
  const lines: LaneLine[] = [];
  cy.edges().forEach((e) => {
    const k = { id: e.id(), source: e.data("source") as string, target: e.data("target") as string };
    if (byId.has(k.source) && byId.has(k.target)) lines.push(k);
  });
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

/** Lays ``cy`` out in lanes; ``done`` once it is placed. */
export function runLanes(
  cy: Core,
  canvas: CyCanvas,
  opts: LanesOptions,
  overlay: GraphEdgeSpec[],
  done: () => void,
): void {
  // Nothing is placed by a layout: the order is a sort and the places follow from it. The null
  // layout only gives the canvas its one run, which ends the core cleanly if it goes first.
  canvas.run(cy.layout({ name: "null" } as CyLayoutOptions), () => {
    cy.add(overlay.map((e) => edgeElement(e)) as unknown as ElementDefinition[]);
    const items = placedItems(cy);
    const lines: LaneLine[] = [];
    cy.edges().forEach((e) => {
      lines.push({ id: e.id(), source: e.data("source") as string, target: e.data("target") as string });
    });
    const gap = opts.tight ? TIGHT_LANE_GAP : LANE_GAP;
    const lanes = sortLanes(
      opts.lanes.map((l) => ({ id: l.id, items: items.filter((n) => n.lane === l.id) })),
      lines,
      opts.beside,
      gap,
    );
    const where = placeLanes(lanes, lines, opts.beside, gap);
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
  besideRoutes(cy, lane).forEach((path, id) => {
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
  const routes = opts.beside === undefined ? new Map<string, At[]>() : besideRoutes(cy, opts.beside);
  const trackRight = Math.max(-Infinity, ...[...routes.values()].map((p) => p[1].x));
  for (const lane of opts.lanes) {
    const inLane = items.filter((n) => n.lane === lane.id);
    if (inLane.length === 0) continue;
    const x1 = Math.min(...inLane.map((n) => n.x - n.w / 2)) - LANE_PAD;
    const nodesRight = Math.max(...inLane.map((n) => n.x + n.w / 2));
    const x2 = (lane.id === opts.beside ? Math.max(nodesRight, trackRight) : nodesRight) + LANE_PAD;
    const y1 = Math.min(...inLane.map((n) => n.y - n.h / 2)) - LANE_PAD - LANE_TITLE;
    const y2 = Math.max(...inLane.map((n) => n.y + n.h / 2)) + LANE_PAD;
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
