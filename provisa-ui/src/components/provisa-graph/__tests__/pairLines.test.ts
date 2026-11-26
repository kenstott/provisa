// Copyright (c) 2026 Kenneth Stott
// Canary: 9b4e2d71-6c3a-4f85-8e19-2a7d5c0b6f34
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277: the lines between one pair of nodes, either way, are drawn apart and every label is
// drawn clear of the others. The maintainer collapsed SHELTER and PET-STORE on the Relationships
// ERD: PETS_OF_BREED and BREED_INFO, one each way between the two boxes, were drawn almost on top
// of each other and PETS_OF_BREED's label covered BREED_INFO's.
//
// The real cytoscape, headless: the lines' paths and the labels' boxes are read back from the
// styles the routing set, the way cytoscape draws them.

import cytoscape from "cytoscape";
import { describe, expect, it } from "vitest";
import type { CyElement, CyInstance } from "../../graph/cytoscape-types";
import { graphElements, type GraphSpec } from "../model";
import { bendPairs, labelSize, PAIR_BEND } from "../pairs";
import { graphStylesheet } from "../stylesheet";
import { applyTaxiLanes } from "../taxi";
import { runLanes, type LanesOptions } from "../lanes";
import type { Core } from "../core";
import type { CyCanvas } from "../cyCanvas";
import { graphTokens } from "../tokens";

type Pt = { x: number; y: number };

/** The ERD with both domains collapsed: the two relationships, one each way, between the boxes. */
const COLLAPSED: GraphSpec = {
  groups: [
    { id: "d:shelter", label: "shelter" },
    { id: "d:pet-store", label: "pet-store" },
  ],
  nodes: [],
  edges: [
    { id: "rp:breed-info", source: "d:pet-store", target: "d:shelter", label: "BREED_INFO" },
    { id: "rp:pets-of-breed", source: "d:shelter", target: "d:pet-store", label: "PETS_OF_BREED" },
  ],
};

function drawn(spec: GraphSpec, at: Record<string, Pt>, routing: "bezier" | "taxi") {
  const cy = cytoscape({
    headless: true,
    styleEnabled: true,
    elements: graphElements(spec, (t) => t, "all"),
    style: graphStylesheet(graphTokens(false)),
  } as unknown as Parameters<typeof cytoscape>[0]) as unknown as CyInstance;
  for (const [id, p] of Object.entries(at)) cy.getElementById(id).position(p);
  // As ProvisaGraph routes the lines once a layout has placed the nodes.
  cy.$(".pg-edge").style("curve-style", routing);
  if (routing === "taxi") applyTaxiLanes(cy);
  else bendPairs(cy);
  return cy;
}

const px = (v: unknown) => parseFloat(String(v));
const nums = (v: unknown) => String(v).replaceAll("px", "").trim().split(/\s+/).map(Number);

/** The points a line passes through between its ends, and where cytoscape anchors its label. */
function path(edge: CyElement): { pts: Pt[]; anchor: Pt } {
  const sp = edge.source().position();
  const tp = edge.target().position();
  const curve = String(edge.style("curve-style"));
  if (curve === "segments") {
    const [sox, soy] = nums(edge.style("source-endpoint"));
    const [tox, toy] = nums(edge.style("target-endpoint"));
    const s = { x: sp.x + sox, y: sp.y + soy };
    const t = { x: tp.x + tox, y: tp.y + toy };
    const vx = t.x - s.x;
    const vy = t.y - s.y;
    const len = Math.hypot(vx, vy);
    const ds = nums(edge.style("segment-distances"));
    const pts = nums(edge.style("segment-weights")).map((w, i) => ({
      x: s.x + vx * w + (-vy / len) * ds[i],
      y: s.y + vy * w + (vx / len) * ds[i],
    }));
    // Two corner points: the label sits at the middle of the run between them.
    return { pts, anchor: { x: (pts[0].x + pts[1].x) / 2, y: (pts[0].y + pts[1].y) / 2 } };
  }
  // A curve: one control point the given distance to the line's own left of its middle; the
  // curve, and the label, pass half that distance from the middle.
  expect(curve).toBe("unbundled-bezier");
  const d = px(edge.style("control-point-distances"));
  const vx = tp.x - sp.x;
  const vy = tp.y - sp.y;
  const len = Math.hypot(vx, vy);
  const n = { x: -vy / len, y: vx / len };
  const mid = { x: (sp.x + tp.x) / 2, y: (sp.y + tp.y) / 2 };
  return {
    pts: [{ x: mid.x + n.x * d, y: mid.y + n.y * d }],
    anchor: { x: mid.x + (n.x * d) / 2, y: mid.y + (n.y * d) / 2 },
  };
}

/** The box a line's label is drawn in: at its anchor, moved by its margins. */
function labelBox(edge: CyElement) {
  const { anchor } = path(edge);
  const c = {
    x: anchor.x + px(edge.style("text-margin-x")),
    y: anchor.y + px(edge.style("text-margin-y")),
  };
  const { w, h } = labelSize(String(edge.data("label")));
  return { x1: c.x - w / 2, x2: c.x + w / 2, y1: c.y - h / 2, y2: c.y + h / 2 };
}

const overlaps = (a: ReturnType<typeof labelBox>, b: ReturnType<typeof labelBox>) =>
  a.x1 < b.x2 && b.x1 < a.x2 && a.y1 < b.y2 && b.y1 < a.y2;

const lines = (cy: CyInstance) => {
  const out: CyElement[] = [];
  cy.edges(".pg-edge").forEach((e) => {
    out.push(e);
  });
  return out;
};

/** The least distance between two lines' paths, point by point. */
const apart = (a: Pt[], b: Pt[]) =>
  Math.min(...a.flatMap((p) => b.map((q) => Math.hypot(p.x - q.x, p.y - q.y))));

// The two boxes as the maintainer saw them, one above the other; and side by side; and slanted.
const PLACES: Record<string, Record<string, Pt>> = {
  stacked: { "d:shelter": { x: 0, y: 0 }, "d:pet-store": { x: 0, y: 300 } },
  "side by side": { "d:shelter": { x: 0, y: 0 }, "d:pet-store": { x: 400, y: 0 } },
  slanted: { "d:shelter": { x: 0, y: 0 }, "d:pet-store": { x: 300, y: 200 } },
};

describe("two lines in opposite directions between one pair of nodes (REQ-2277)", () => {
  for (const [name, at] of Object.entries(PLACES)) {
    it(`bezier, ${name}: the two curves bend apart and both labels are drawn clear`, () => {
      const cy = drawn(COLLAPSED, at, "bezier");
      const [a, b] = lines(cy);
      expect(apart(path(a).pts, path(b).pts)).toBeGreaterThanOrEqual(PAIR_BEND - 1e-6);
      expect(overlaps(labelBox(a), labelBox(b))).toBe(false);
    });

    it(`taxi, ${name}: the two routes are distinct and both labels are drawn clear`, () => {
      const cy = drawn(COLLAPSED, at, "taxi");
      const [a, b] = lines(cy);
      expect(apart(path(a).pts, path(b).pts)).toBeGreaterThan(0);
      expect(overlaps(labelBox(a), labelBox(b))).toBe(false);
    });
  }

  it("three lines between one pair: every curve apart, no label on another", () => {
    const spec: GraphSpec = {
      nodes: [
        { id: "a", type: "table", name: "a" },
        { id: "b", type: "table", name: "b" },
      ],
      edges: [
        { id: "e1", source: "a", target: "b", label: "OWNS" },
        { id: "e2", source: "b", target: "a", label: "OWNED_BY" },
        { id: "e3", source: "a", target: "b", label: "MANAGES" },
      ],
    };
    const cy = drawn(spec, { a: { x: 0, y: 0 }, b: { x: 0, y: 400 } }, "bezier");
    const [e1, e2, e3] = lines(cy);
    for (const [p, q] of [
      [e1, e2],
      [e1, e3],
      [e2, e3],
    ]) {
      expect(apart(path(p).pts, path(q).pts)).toBeGreaterThanOrEqual(PAIR_BEND - 1e-6);
      expect(overlaps(labelBox(p), labelBox(q))).toBe(false);
    }
  });

  it("a line alone between its nodes keeps the stylesheet's own curve and label", () => {
    const spec: GraphSpec = {
      nodes: [
        { id: "a", type: "table", name: "a" },
        { id: "b", type: "table", name: "b" },
      ],
      edges: [{ id: "e1", source: "a", target: "b", label: "OWNS" }],
    };
    const cy = drawn(spec, { a: { x: 0, y: 0 }, b: { x: 0, y: 400 } }, "bezier");
    const [e1] = lines(cy);
    expect(String(e1.style("curve-style"))).toBe("bezier");
  });
});

// The lead's decision (identical behaviour on every graph surface): the lanes preset bends the lines
// of one pair apart and keeps their labels clear, as the ERD does. A table and its column joined
// both ways ran on one straight line between the lanes, one label on the other.
describe("the lanes preset: two lines between one pair of nodes (REQ-2277)", () => {
  const headless: CyCanvas = {
    container: null as unknown as HTMLDivElement,
    run(layout, then) {
      layout.one("layoutstop", () => then?.());
      layout.run();
    },
    end(cy) {
      cy.destroy();
    },
  };
  const SPEC: GraphSpec = {
    nodes: [
      { id: "t", type: "table", name: "orders", lane: "table" },
      { id: "c", type: "column", name: "customer_id", lane: "column" },
      { id: "g1", type: "term", name: "Customer", lane: "term" },
      { id: "g2", type: "term", name: "Buyer", lane: "term" },
    ],
    edges: [
      { id: "has", source: "t", target: "c", label: "HAS_COLUMN" },
      { id: "of", source: "c", target: "t", label: "COLUMN_OF" },
      { id: "means", source: "c", target: "g1", label: "MEANS" },
      { id: "broader", source: "g1", target: "g2", label: "BROADER" },
      { id: "narrower", source: "g2", target: "g1", label: "NARROWER" },
    ],
  };
  const OPTS: LanesOptions = {
    lanes: ["table", "column", "term"].map((id) => ({ id, title: id })),
    beside: "term",
  };

  async function laid(): Promise<CyInstance> {
    const cy = cytoscape({
      headless: true,
      styleEnabled: true,
      elements: graphElements(SPEC, (t) => t, "all"),
      style: graphStylesheet(graphTokens(false)),
    } as unknown as Parameters<typeof cytoscape>[0]) as unknown as Core;
    await new Promise<void>((done) => runLanes(cy, headless, OPTS, [], done));
    return cy as unknown as CyInstance;
  }
  const edge = (cy: CyInstance, id: string) => cy.getElementById(id) as unknown as CyElement;

  it("the two lines between the lanes bend apart and both labels are drawn clear", async () => {
    const cy = await laid();
    const a = edge(cy, "has");
    const b = edge(cy, "of");
    expect(apart(path(a).pts, path(b).pts)).toBeGreaterThanOrEqual(PAIR_BEND - 1e-6);
    expect(overlaps(labelBox(a), labelBox(b))).toBe(false);
  });

  it("the lines beside the term lane keep their own tracks; a line alone keeps its curve", async () => {
    const cy = await laid();
    expect(String(edge(cy, "broader").style("curve-style"))).toBe("segments");
    expect(String(edge(cy, "narrower").style("curve-style"))).toBe("segments");
    expect(String(edge(cy, "means").style("curve-style"))).toBe("bezier");
  });
});
