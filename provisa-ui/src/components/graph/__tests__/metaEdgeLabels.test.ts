// Copyright (c) 2026 Kenneth Stott
// Canary: 905c18d1-cb50-47bd-897b-225262fd9d69
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Reported 2026-11-17 (Explore > Cypher, three neighbourhoods collapsed, at 1900): two
// cluster-to-cluster edge labels drew over each other. A label is never rotated, so it is as wide
// as its text; the layout gave every edge the same length (the edge-distance setting), shorter
// than the label, so two labels on edges that meet at a supernode ran into each other and onto
// the supernodes. The force layout now gives a labelled cluster edge room for its label.

import { describe, expect, it } from "vitest";
import cytoscape from "cytoscape";
import "../../provisa-graph/core";
import { buildClusterElements, buildClusterMetaEdges } from "../graph-clusters";
import type { GEdge, GNode } from "../graph-model";
import { LAYOUT_OPTIONS } from "../canvas/canvas-types";
import { buildGraphStylesheet } from "../graph-stylesheet";
import { graphTokens } from "../../provisa-graph/tokens";
import { textWidth } from "../../provisa-graph/model";

const LEVEL = "schema_L1";
const HOODS = ["Millbrook", "Oak Hill", "Riverside"];

/** Inspections in each neighbourhood with a building permit in the next one: three clusters, each
 * pair joined by six IS_BUILDING_PERMIT edges, as in the report. */
function neighbourhoods() {
  const nodes = new Map<string, GNode>();
  const edges = new Map<string, GEdge>();
  let id = 0;
  const node = (label: string, hood: string) => {
    const n: GNode = { id: ++id, label, tableLabel: label, properties: { scl1: hood } };
    nodes.set(`${label}:${n.id}`, n);
    return n;
  };
  HOODS.forEach((hood, h) => {
    const next = HOODS[(h + 1) % HOODS.length];
    for (let i = 0; i < 6; i++) {
      const a = node("Inspections", hood);
      const b = node("BuildingPermits", next);
      const identity = `e${edges.size}`;
      edges.set(identity, {
        identity,
        start: a.id,
        end: b.id,
        type: "IS_BUILDING_PERMIT",
        properties: {},
        startNode: a,
        endNode: b,
      });
    }
  });
  return { nodes, edges };
}

type Box = { x1: number; x2: number; y1: number; y2: number };
const overlaps = (a: Box, b: Box) => a.x1 < b.x2 && b.x1 < a.x2 && a.y1 < b.y2 && b.y1 < a.y2;
const none = { current: {} };

/** The layout the canvas runs with groups on (use-graph-layout.ts runLayout), to its end. */
type Core = ReturnType<typeof cytoscape>;

function laidOut(): Core {
  const { nodes, edges } = neighbourhoods();
  const collapsed = new Set(HOODS);
  const cy = cytoscape({
    headless: true,
    styleEnabled: true,
    style: buildGraphStylesheet(
      {
        colorOverridesRef: none,
        sizeOverridesRef: none,
        labelPropertyRef: none,
        relLineOverridesRef: none,
      },
      graphTokens(false),
    ) as never,
    elements: [
      ...buildClusterElements(nodes, edges, LEVEL, undefined, collapsed),
      ...buildClusterMetaEdges(nodes, edges, LEVEL, undefined, collapsed, false),
    ],
  });
  cy.layout({
    ...LAYOUT_OPTIONS.force,
    nestingFactor: 0.1,
    nodeRepulsion: () => 25000,
  } as never).run();
  return cy;
}

/** A label's box: at the middle of its (straight, single) edge, as wide as its 9px text and
 * the box's 2px padding, never rotated. */
function labelBox(cy: Core, edgeId: string): Box {
  const e = cy.getElementById(edgeId);
  const s = e.source().position();
  const t = e.target().position();
  const mx = (s.x + t.x) / 2;
  const my = (s.y + t.y) / 2;
  const w = textWidth(e.data("label") as string, 9) + 4;
  const h = 9 * 1.2 + 4;
  return { x1: mx - w / 2, x2: mx + w / 2, y1: my - h / 2, y2: my + h / 2 };
}

function nodeBox(cy: Core, nodeId: string): Box {
  const n = cy.getElementById(nodeId);
  const p = n.position();
  const r = n.width() / 2;
  return { x1: p.x - r, x2: p.x + r, y1: p.y - r, y2: p.y + r };
}

describe("cluster-to-cluster edge labels", () => {
  it("never overlap one another or a supernode, over many layouts", () => {
    for (let run = 0; run < 20; run++) {
      const cy = laidOut();
      const metas = cy.edges("[?_metaEdge]").map((e) => e.id());
      expect(metas).toHaveLength(3);
      const supers = cy.nodes("[?_collapsed]").map((n) => n.id());
      expect(supers).toHaveLength(3);
      for (let i = 0; i < metas.length; i++) {
        const a = labelBox(cy, metas[i]);
        for (let j = i + 1; j < metas.length; j++) {
          expect(overlaps(a, labelBox(cy, metas[j])), `run ${run}: ${metas[i]} / ${metas[j]}`).toBe(
            false,
          );
        }
        for (const s of supers) {
          expect(overlaps(a, nodeBox(cy, s)), `run ${run}: ${metas[i]} on ${s}`).toBe(false);
        }
      }
      cy.destroy();
    }
  });
});
