// Copyright (c) 2026 Kenneth Stott
// Canary: 8e3f1a6b-5c27-4d90-b4e8-1f7a9c2d5e30
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174, REQ-2239: the lanes preset. ELK's layered algorithm in fixed lane partitions places
// the diagram once; one lane (the stacked lane) is a compound laid out top-down by the lines inside
// it. Then the view orders each lane given those places (``order``: the barycentre of each node's
// neighbours, crossings cut), settles the stacked lane (``settle``), and the lanes are placed here
// from that order alone (placeLanes): side by side left to right, each as wide as its widest node,
// every lane spread over the height of the tallest, so a lane short of nodes is not packed at the
// middle. Each lane is boxed round its own nodes and titled as a group is, the boxes a gap apart;
// the lines laid over the placed nodes are added last.

import type { CyInstance, CyLayoutOptions } from "../graph/cytoscape-types";
import type { CyCanvas } from "./cyCanvas";
import type { Core, ElementDefinition, NodeSingular } from "./core";
import { edgeElement, type Element, type GraphEdgeSpec } from "./model";
import { groupHue } from "./tokens";
import type { At } from "./tipPlacement";
import { fitClear } from "./viewport";

export interface LanesOptions {
  /** The lanes, left to right. */
  lanes: { id: string; title: string }[];
  /** The lane laid out top-down by the lines inside it, as one compound. */
  stacked?: string;
  /** A glance (a card): the lanes sit closer. */
  tight?: boolean;
  /** Each lane's order given the first pass's places: the stacked lane's first, then the rest
   * left to right. */
  order?: (at: (id: string) => At) => string[][];
  /** Where the stacked lane's nodes go, once the second pass has placed the rest. */
  settle?: (
    stackedOrder: string[],
    at: (id: string) => At,
    widthOf: (id: string) => number,
  ) => Map<string, At>;
}

export const frameId = (lane: string) => `frame:${lane}`;

/** ``elements`` with the stacked lane's nodes inside their compound. */
export function laneElements(elements: Element[], opts: LanesOptions): Element[] {
  const stacked = opts.stacked;
  if (stacked === undefined) return elements;
  const inStack = elements.filter((e) => e.group === "nodes" && e.data.lane === stacked);
  if (inStack.length === 0) return elements;
  return [
    { group: "nodes", data: { id: frameId(stacked), kind: "frame" }, classes: "" },
    ...elements.map((e) =>
      e.group === "nodes" && e.data.lane === stacked
        ? { ...e, data: { ...e.data, parent: frameId(stacked) } }
        : e,
    ),
  ];
}

/** The space between two lanes' boxes; a glance's lanes sit closer. */
export const LANE_GAP = 56;
const TIGHT_LANE_GAP = 24;
/** A lane's box round its nodes, and the room above them for its title. */
export const LANE_PAD = 16;
const LANE_TITLE = 12;
/** The least gap between two nodes of a lane, as the stacked lane's ELK spacing. */
export const NODE_GAP = 14;

/** A node as placeLanes reads it: its size, and in the stacked lane where ``settle`` put it. */
export interface LaneItem {
  id: string;
  w: number;
  h: number;
}

/** A run of nodes placed as one: a node of an ordinary lane, or a row of the stacked lane (nodes
 * whose heights overlap where ``settle`` put them), moved together so its shape holds. */
interface Band {
  ids: string[];
  /** Each node's place inside the band, from the band's top left. */
  at: Map<string, At>;
  w: number;
  h: number;
}

/** The stacked lane's rows, in order down the lane: nodes joined by a line inside the lane
 * (``links``) stay together, and so do nodes whose spans overlap where ``settle`` put them. */
function stackedBands(
  items: LaneItem[],
  settled: (id: string) => At,
  links: [string, string][],
): Band[] {
  const span = new Map(
    items.map((n) => {
      const p = settled(n.id);
      return [n.id, { n, x1: p.x - n.w / 2, y1: p.y - n.h / 2, y2: p.y + n.h / 2 }] as const;
    }),
  );
  const up = new Map(items.map((n) => [n.id, n.id]));
  const root = (id: string): string => {
    const u = up.get(id) as string;
    return u === id ? id : root(u);
  };
  for (const [a, b] of links) if (up.has(a) && up.has(b)) up.set(root(a), root(b));
  const joined = new Map<string, string[]>();
  for (const n of items) joined.set(root(n.id), [...(joined.get(root(n.id)) ?? []), n.id]);
  const groups = [...joined.values()]
    .map((ids) => ({
      ids,
      y1: Math.min(...ids.map((id) => span.get(id)!.y1)),
      y2: Math.max(...ids.map((id) => span.get(id)!.y2)),
    }))
    .sort((a, b) => a.y1 - b.y1);
  const rows: string[][] = [];
  let bottom = -Infinity;
  for (const g of groups) {
    if (rows.length > 0 && g.y1 < bottom) {
      rows[rows.length - 1].push(...g.ids);
      bottom = Math.max(bottom, g.y2);
    } else {
      rows.push([...g.ids]);
      bottom = g.y2;
    }
  }
  return rows.map((ids) => {
    const row = ids.map((id) => span.get(id)!);
    const x1 = Math.min(...row.map((s) => s.x1));
    const y1 = Math.min(...row.map((s) => s.y1));
    const at = new Map(
      row.map((s) => [s.n.id, { x: s.x1 - x1 + s.n.w / 2, y: s.y1 - y1 + s.n.h / 2 }] as const),
    );
    return {
      ids,
      at,
      w: Math.max(...row.map((s) => s.x1 + s.n.w)) - x1,
      h: Math.max(...row.map((s) => s.y2)) - y1,
    };
  });
}

/** Where each node of ``lanes`` (left to right, each in its order top to bottom) is drawn. Each
 * lane is as wide as its widest node (or row), its nodes centred in it; the lanes' boxes stand
 * ``gap`` apart. Every lane is spread over the height of the tallest packed lane: its first node at
 * the top, its last at the bottom, the space between shared evenly; a lane of one node sits at the
 * middle. ``settled`` gives the stacked lane's places, whose rows (``links`` the lines inside it)
 * keep their shape. */
export function placeLanes(
  lanes: { id: string; items: LaneItem[] }[],
  stacked: string | undefined,
  settled: (id: string) => At,
  links: [string, string][] = [],
  gap: number = LANE_GAP,
): Map<string, At> {
  const banded = lanes.map((l) => ({
    id: l.id,
    bands:
      l.id === stacked
        ? stackedBands(l.items, settled, links)
        : l.items.map((n) => ({
            ids: [n.id],
            at: new Map([[n.id, { x: n.w / 2, y: n.h / 2 }]]),
            w: n.w,
            h: n.h,
          })),
  }));
  const packed = (bands: Band[]) =>
    bands.reduce((sum, b) => sum + b.h, 0) + NODE_GAP * Math.max(0, bands.length - 1);
  const height = Math.max(0, ...banded.map((l) => packed(l.bands)));
  const out = new Map<string, At>();
  let left = 0;
  for (const { bands } of banded) {
    if (bands.length === 0) continue;
    const width = Math.max(...bands.map((b) => b.w));
    const free = height - bands.reduce((sum, b) => sum + b.h, 0);
    const step = bands.length > 1 ? free / (bands.length - 1) : 0;
    let top = bands.length > 1 ? 0 : (height - bands[0].h) / 2;
    for (const b of bands) {
      const x0 = left + (width - b.w) / 2;
      b.at.forEach((p, id) => out.set(id, { x: x0 + p.x, y: top + p.y }));
      top += b.h + step;
    }
    left += width + 2 * LANE_PAD + gap;
  }
  return out;
}

/** Lays ``cy`` out in lanes; ``done`` once every pass has placed it. */
export function runLanes(
  cy: Core,
  canvas: CyCanvas,
  opts: LanesOptions,
  overlay: GraphEdgeSpec[],
  done: () => void,
): void {
  const ids = opts.lanes.map((l) => l.id);
  const partition = (lane: string) => String(ids.indexOf(lane));
  const stacked = opts.stacked;
  const layout = cy.layout({
    name: "elk",
    fit: false,
    nodeLayoutOptions: (n: NodeSingular) => {
      if (stacked !== undefined && n.id() === frameId(stacked))
        return {
          "elk.partitioning.partition": partition(stacked),
          "elk.algorithm": "layered",
          "elk.direction": "DOWN",
          "elk.aspectRatio": "0.4",
          "elk.layered.spacing.nodeNodeBetweenLayers": "36",
          "elk.spacing.nodeNode": String(NODE_GAP),
        };
      if (n.data("lane") === stacked) return {};
      return { "elk.partitioning.partition": partition(n.data("lane") as string) };
    },
    elk: {
      algorithm: "layered",
      "elk.direction": "RIGHT",
      "elk.partitioning.activate": "true",
      // One graph, so every lane lines up across all of it.
      "elk.separateConnectedComponents": "false",
      "elk.layered.spacing.nodeNodeBetweenLayers": "90",
      "elk.spacing.nodeNode": "12",
      "elk.layered.nodePlacement.strategy": "NETWORK_SIMPLEX",
    },
  } as CyLayoutOptions);

  const at = (id: string) => ({ ...cy.getElementById(id).position() });
  const widthOf = (id: string) => cy.getElementById(id).data("w") as number;
  const placed = () => cy.nodes('[kind = "node"], [kind = "more"]');

  canvas.run(layout, () => {
    const nodes = placed();
    const laneOf = new Map(nodes.map((n) => [n.id(), n.data("lane") as string]));
    // Each node's place in its lane: the view's order, else top to bottom as ELK put it.
    const rank = new Map<string, number>();
    const rows = opts.order?.(at) ?? [];
    const stackOrder = stacked === undefined ? [] : (rows[0] ?? []);
    for (const row of rows) row.forEach((id, i) => rank.set(id, i));
    const settled =
      stacked !== undefined && opts.settle ? opts.settle(stackOrder, at, widthOf) : new Map<string, At>();
    const lanes = ids.map((lane) => ({
      id: lane,
      items: nodes
        .filter((n) => laneOf.get(n.id()) === lane)
        .map((n) => ({ id: n.id(), w: n.data("w") as number, h: n.data("h") as number, y: n.position("y") }))
        .sort((a, b) => (rank.get(a.id) ?? Infinity) - (rank.get(b.id) ?? Infinity) || a.y - b.y),
    }));
    const where = placeLanes(
      lanes,
      stacked,
      (id) => settled.get(id) ?? at(id),
      cy.edges().map((e) => [e.source().id(), e.target().id()] as [string, string]),
      opts.tight ? TIGHT_LANE_GAP : LANE_GAP,
    );
    cy.batch(() => where.forEach((p, id) => cy.getElementById(id).position(p)));
    cy.add(overlay.map((e) => edgeElement(e)) as unknown as ElementDefinition[]);
    boxLanes(cy, opts);
    fitClear(cy as unknown as CyInstance, 24);
    done();
  });
}

/** Each lane boxed round its own nodes, its title above them. */
function boxLanes(cy: Core, opts: LanesOptions): void {
  const placed = cy.nodes('[kind = "node"], [kind = "more"]');
  for (const lane of opts.lanes) {
    const inLane = placed.filter((n) => n.data("lane") === lane.id);
    if (inLane.empty()) continue;
    const xs = inLane.map((n) => [n.position("x") - (n.data("w") as number) / 2, n.position("x") + (n.data("w") as number) / 2]);
    const ys = inLane.map((n) => [n.position("y") - (n.data("h") as number) / 2, n.position("y") + (n.data("h") as number) / 2]);
    const x1 = Math.min(...xs.map((x) => x[0])) - LANE_PAD;
    const x2 = Math.max(...xs.map((x) => x[1])) + LANE_PAD;
    const y1 = Math.min(...ys.map((y) => y[0])) - LANE_PAD - LANE_TITLE;
    const y2 = Math.max(...ys.map((y) => y[1])) + LANE_PAD;
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
