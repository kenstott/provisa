// Copyright (c) 2026 Kenneth Stott
// Canary: 4b4d674d-ff06-4dd7-be33-5f088c395355
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Collapse to Supernode: a node related to a collapsed group's members is tied to the group's
// supernode for the layout, and a redraw after the supernodes were dragged apart settles each such
// node beside the supernode it connects to, the dragged supernodes where they were put.

import { describe, it, expect } from "vitest";
import cytoscape from "cytoscape";
import "../canvas/cytoscape-extensions";
import {
  buildClusterElements,
  buildClusterMetaEdges,
  swapToLayoutMetaEdges,
  cidToId,
} from "../graph-clusters";
import type { GEdge, GNode } from "../graph-model";
import type { CyInstance } from "../cytoscape-types";
import { LAYOUT_OPTIONS } from "../canvas/canvas-types";
import { anchoredPositions } from "../canvas/canvas-helpers";

const REGIONS = ["North", "South", "East", "West"];

/** A schema level: a node with no value of one stays free, where a property's lacking nodes form a
 * group of their own (frameMeta.test.ts). */
const LEVEL = "schema_L1";

/** A weblogs-like result: customers carrying a region, each with two orders, each order with an
 * item of a product -- the orders, items and products carry no region. */
function weblogs() {
  const nodes = new Map<string, GNode>();
  const edges = new Map<string, GEdge>();
  let id = 0;
  const node = (label: string, properties: Record<string, unknown>) => {
    const n: GNode = { id: ++id, label, tableLabel: label, properties };
    nodes.set(`${label}:${n.id}`, n);
    return n;
  };
  const edge = (type: string, a: GNode, b: GNode) => {
    const identity = `e${edges.size}`;
    edges.set(identity, {
      identity,
      start: a.id,
      end: b.id,
      type,
      properties: {},
      startNode: a,
      endNode: b,
    });
  };
  const owner = new Map<string, string>(); // order/item/product key -> its customer's region
  REGIONS.forEach((region) => {
    for (let c = 0; c < 3; c++) {
      const customer = node("customers", { scl1: region });
      for (let o = 0; o < 2; o++) {
        const order = node("orders", { total: o });
        edge("PLACED", customer, order);
        const item = node("order_items", { qty: 1 });
        edge("CONTAINS", order, item);
        const product = node("products", { sku: `${region}-${c}-${o}` });
        edge("OF", item, product);
        for (const n of [order, item, product]) owner.set(`${n.label}:${n.id}`, region);
      }
    }
  });
  return { nodes, edges, owner };
}

const supernode = (region: string) => `__collapsed_${LEVEL}_${cidToId(region)}`;

describe("the grouping transform, every group collapsed", () => {
  const { nodes, edges } = weblogs();
  const collapsed = new Set(REGIONS);

  it("ties an edge to a collapsed member to its group's supernode", () => {
    const meta = buildClusterMetaEdges(nodes, edges, LEVEL, undefined, collapsed, false);
    const placed = meta.filter((e) => e.data._metaType === "PLACED");
    expect(new Set(placed.map((e) => e.data.source))).toEqual(new Set(REGIONS.map(supernode)));
    // every order is tied to a supernode, none to a customer that is no longer drawn
    expect(placed.every((e) => String(e.data.target).startsWith("orders:"))).toBe(true);
    expect(placed).toHaveLength(REGIONS.length * 3 * 2);
  });

  it("ends an edge on the supernode in either routing", () => {
    const forLayout = buildClusterMetaEdges(nodes, edges, LEVEL, undefined, collapsed, false);
    const forPorts = buildClusterMetaEdges(nodes, edges, LEVEL, undefined, collapsed, true);
    expect(forPorts.map((e) => e.data.id)).toEqual(forLayout.map((e) => e.data.id));
  });
});

function canvas() {
  const { nodes, edges, owner } = weblogs();
  const collapsed = new Set(REGIONS);
  const cy = cytoscape({
    headless: true,
    styleEnabled: true,
    elements: [
      ...buildClusterElements(nodes, edges, LEVEL, undefined, collapsed),
      // after a layout the canvas holds the port routing (GraphCanvas.computeHulls)
      ...buildClusterMetaEdges(nodes, edges, LEVEL, undefined, collapsed, true),
    ],
  }) as unknown as CyInstance;
  return { cy, nodes, edges, owner, collapsed };
}

describe("a redraw after the supernodes were dragged apart", () => {
  it("puts the layout's edges in place of the ones the canvas holds", () => {
    const { cy, nodes, edges, collapsed } = canvas();
    const before = cy.edges("[?_metaEdge]").length;
    swapToLayoutMetaEdges(cy, nodes, edges, LEVEL, new Map(), collapsed);
    expect(cy.edges("[?_metaEdge]").length).toBe(before);
  });

  it("settles each related node beside its supernode, the supernodes where they were put", () => {
    const { cy, nodes, edges, owner, collapsed } = canvas();
    // Everything in a central tangle, the supernodes dragged out to the four corners.
    cy.nodes().forEach((n, i) => {
      n.position({ x: (i % 7) * 3, y: (i % 5) * 3 });
    });
    const corners: Record<string, { x: number; y: number }> = {
      North: { x: 0, y: -1200 },
      South: { x: 0, y: 1200 },
      East: { x: 1200, y: 0 },
      West: { x: -1200, y: 0 },
    };
    for (const region of REGIONS) {
      cy.getElementById(supernode(region)).position(corners[region]).lock();
    }

    swapToLayoutMetaEdges(cy, nodes, edges, LEVEL, new Map(), collapsed);
    // The nudge's layout (use-graph-layout.ts nudgeLayout), run to its end without animation.
    cy.layout({
      ...LAYOUT_OPTIONS.force,
      idealEdgeLength: () => 80,
      randomize: false,
      animate: false,
      numIter: 6000,
      gravity: 0,
      fit: false,
      packComponents: false,
      fixedNodeConstraint: anchoredPositions(cy, new Set(REGIONS.map(supernode))),
    }).run();

    for (const region of REGIONS) {
      expect(cy.getElementById(supernode(region)).position()).toEqual(corners[region]);
    }
    const nearest = (p: { x: number; y: number }) =>
      REGIONS.reduce((best, r) =>
        Math.hypot(p.x - corners[r].x, p.y - corners[r].y) <
        Math.hypot(p.x - corners[best].x, p.y - corners[best].y)
          ? r
          : best,
      );
    const astray = [...owner].filter(
      ([key, region]) => nearest(cy.getElementById(key).position()) !== region,
    );
    expect(astray).toEqual([]);
    // ...and beside it: an order is one edge from its supernode
    const farthestOrder = Math.max(
      ...[...owner]
        .filter(([key]) => key.startsWith("orders:"))
        .map(([key, region]) => {
          const p = cy.getElementById(key).position();
          return Math.hypot(p.x - corners[region].x, p.y - corners[region].y);
        }),
    );
    expect(farthestOrder).toBeLessThan(400);
  });
});
