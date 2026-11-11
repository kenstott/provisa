// Copyright (c) 2026 Kenneth Stott
// Canary: 8e3f1a6b-5c27-4d90-b4e8-1f7a9c2d5e30
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174, REQ-2239: the lanes preset. ELK's layered algorithm in fixed lane partitions, placed
// strictly left to right; one lane (the stacked lane) is a compound laid out top-down by the lines
// inside it. A second pass orders each lane by the first's placement (the view's ``order``), the
// stacked lane is settled (the view's ``settle``), the lines laid over the placed nodes are added,
// and each lane is boxed and titled as a group is.

import type { CyLayoutOptions } from "../graph/cytoscape-types";
import type { CyCanvas } from "./cyCanvas";
import type { Core, ElementDefinition, NodeSingular } from "./core";
import { edgeElement, type Element, type GraphEdgeSpec } from "./model";
import { SIZE, groupHue } from "./tokens";
import type { At } from "./tipPlacement";

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
  const layout = (ordered: boolean) =>
    cy.layout({
      name: "elk",
      fit: true,
      padding: 24,
      nodeLayoutOptions: (n: NodeSingular) => {
        if (stacked !== undefined && n.id() === frameId(stacked))
          return {
            "elk.partitioning.partition": partition(stacked),
            "elk.algorithm": "layered",
            "elk.direction": "DOWN",
            "elk.aspectRatio": "0.4",
            "elk.layered.spacing.nodeNodeBetweenLayers": "36",
            "elk.spacing.nodeNode": "14",
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
        "elk.layered.spacing.nodeNodeBetweenLayers": opts.tight ? "40" : "90",
        "elk.spacing.nodeNode": "12",
        "elk.layered.nodePlacement.strategy": "NETWORK_SIMPLEX",
        // The second pass keeps each lane in the order the first pass's places ask for.
        ...(ordered ? { "elk.layered.crossingMinimization.strategy": "INTERACTIVE" } : {}),
      },
    } as CyLayoutOptions);

  const at = (id: string) => cy.getElementById(id).position();

  // The second pass's order: each lane spread over the height of the longest.
  const reorder = (): string[] => {
    if (!opts.order) return [];
    const [first, ...lanes] = opts.order(at);
    const span = Math.max(1, ...lanes.map((row) => row.length));
    for (const row of lanes)
      row.forEach((id, i) => {
        const n = cy.getElementById(id);
        n.position({
          x: n.position("x"),
          y: ((i + 0.5) * span * SIZE.nodeHeight * 2) / row.length,
        });
      });
    return first ?? [];
  };

  const boxLanes = () => {
    const placed = cy.nodes('[kind = "node"], [kind = "more"]');
    if (placed.empty()) return;
    const top = placed.boundingBox({}).y1;
    const bottom = placed.boundingBox({}).y2;
    for (const lane of opts.lanes) {
      const inLane = placed.filter((n) => n.data("lane") === lane.id);
      if (inLane.empty()) continue;
      const bb = inLane.boundingBox({});
      const pad = 16;
      cy.add({
        group: "nodes",
        data: {
          id: `lane:${lane.id}`,
          kind: "lane",
          title: lane.title.toUpperCase(),
          hue: groupHue(lane.id),
          w: bb.x2 - bb.x1 + pad * 2,
          h: bottom - top + pad * 2 + 12,
        },
        position: { x: (bb.x1 + bb.x2) / 2, y: (top + bottom) / 2 - 6 },
      });
    }
  };

  canvas.run(layout(false), () => {
    const first = reorder();
    canvas.run(layout(true), () => {
      if (opts.settle) {
        const moved = opts.settle(first, at, (id) => cy.getElementById(id).data("w") as number);
        cy.batch(() => moved.forEach((p, id) => cy.getElementById(id).position(p)));
      }
      cy.add(overlay.map((e) => edgeElement(e)) as unknown as ElementDefinition[]);
      boxLanes();
      cy.fit(undefined, 24);
      done();
    });
  });
}
