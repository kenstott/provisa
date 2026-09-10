// Copyright (c) 2026 Kenneth Stott
// Canary: b28bfee0-41cc-4c75-975f-4a73cd8a794a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect } from "vitest";
import cytoscape from "cytoscape";
import { applyTaxiLanes, clearTaxiLanes, laneRoute } from "../components/erd/sections/erd-taxi";
import type { CyInstance, CyElement } from "../components/graph/cytoscape-types";

function graph(
  edges: Array<[string, string]>,
  positions: Record<string, { x: number; y: number }>,
) {
  const nodes = Object.keys(positions).map((id) => ({ data: { id } }));
  const cy = cytoscape({
    headless: true,
    styleEnabled: true,
    elements: [
      ...nodes,
      ...edges.map(([source, target], i) => ({
        data: { id: `e${i}`, source, target },
        classes: "erd-rel",
      })),
    ],
  }) as unknown as CyInstance;
  for (const [id, pos] of Object.entries(positions)) cy.getElementById(id).position(pos);
  return cy;
}

// Rebuild the lane's corner points from the segment coordinates, the way cytoscape does.
function corners(edge: CyElement, route: NonNullable<ReturnType<typeof laneRoute>>) {
  const parse = (s: string) => s.split(" ").map(Number);
  const [sox, soy] = parse(route.sourceEndpoint.replaceAll("px", ""));
  const [tox, toy] = parse(route.targetEndpoint.replaceAll("px", ""));
  const sp = edge.source().position();
  const tp = edge.target().position();
  const s = { x: sp.x + sox, y: sp.y + soy };
  const t = { x: tp.x + tox, y: tp.y + toy };
  const vx = t.x - s.x;
  const vy = t.y - s.y;
  const len = Math.hypot(vx, vy);
  const ws = parse(route.weights);
  const ds = parse(route.distances);
  const pts = ws.map((w, i) => ({
    x: s.x + vx * w + (-vy / len) * ds[i],
    y: s.y + vy * w + (vx / len) * ds[i],
  }));
  return { s, t, pts };
}

describe("laneRoute", () => {
  it("is a right-angle Z between offset boundary endpoints on a slanted vertical pair", () => {
    const cy = graph([["a", "b"]], { a: { x: 0, y: 0 }, b: { x: 100, y: 300 } });
    const edge = cy.edges(".erd-rel")[0];
    const route = laneRoute(edge, 6, -6, 0.46);
    expect(route).not.toBeNull();
    const { s, t, pts } = corners(edge, route!);
    // Endpoints: on the bottom edge of a, 6px right of centre; on the top edge of b, 6px left.
    expect(s).toEqual({ x: 6, y: 15 });
    expect(t).toEqual({ x: 94, y: 285 });
    expect(pts[0].x).toBeCloseTo(s.x, 6);
    expect(pts[1].x).toBeCloseTo(t.x, 6);
    expect(pts[0].y).toBeCloseTo(pts[1].y, 6);
    expect(pts[0].y).toBeCloseTo(15 + 0.46 * 270, 6);
  });

  it("lays a horizontal pair's lane across y", () => {
    const cy = graph([["a", "b"]], { a: { x: 0, y: 0 }, b: { x: 300, y: -40 } });
    const edge = cy.edges(".erd-rel")[0];
    const { s, t, pts } = corners(edge, laneRoute(edge, -6, -6, 0.5)!);
    expect(s).toEqual({ x: 15, y: -6 });
    expect(t).toEqual({ x: 285, y: -46 });
    expect(pts[0].y).toBeCloseTo(s.y, 6);
    expect(pts[1].y).toBeCloseTo(t.y, 6);
    expect(pts[0].x).toBeCloseTo(pts[1].x, 6);
  });

  it("returns null when the nodes overlap along the corridor", () => {
    const cy = graph([["a", "b"]], { a: { x: 0, y: 0 }, b: { x: 0, y: 20 } });
    expect(laneRoute(cy.edges(".erd-rel")[0], 6, 6, 0.5)).toBeNull();
  });
});

describe("applyTaxiLanes", () => {
  it("gives each edge of a shared pair its own lane, symmetric about the centre line", () => {
    const cy = graph(
      [
        ["a", "b"],
        ["a", "b"],
        ["b", "a"],
      ],
      { a: { x: 0, y: 0 }, b: { x: 10, y: 300 } },
    );
    applyTaxiLanes(cy);
    const edges = cy.edges(".erd-rel");
    expect(edges.map((e) => e.style("curve-style"))).toEqual(["segments", "segments", "segments"]);
    // Three lanes close up to fit a 30px headless node: 30 / 4 = 7.5px apart.
    expect(edges.map((e) => e.style("source-endpoint"))).toEqual([
      "-7.5px 15px",
      "0px 15px",
      "7.5px -15px",
    ]);
  });

  it("gives edges to different neighbours distinct join points on the shared node", () => {
    const cy = graph(
      [
        ["a", "b"],
        ["a", "c"],
      ],
      { a: { x: 0, y: 0 }, b: { x: -50, y: 300 }, c: { x: 50, y: 300 } },
    );
    applyTaxiLanes(cy);
    const edges = cy.edges(".erd-rel");
    // Ordered by where the other end lies, so the two runs do not cross.
    expect(edges.map((e) => e.style("source-endpoint"))).toEqual(["-5px 15px", "5px 15px"]);
    expect(edges.map((e) => e.style("target-endpoint"))).toEqual(["0px -15px", "0px -15px"]);
  });

  it("closes the lanes up when a side has no room for the full gap", () => {
    const cy = graph(
      [
        ["a", "b"],
        ["a", "b"],
        ["a", "b"],
        ["a", "b"],
      ],
      { a: { x: 0, y: 0 }, b: { x: 0, y: 300 } },
    );
    applyTaxiLanes(cy);
    // A 30px node fits four lanes at 6px, not 12px.
    expect(cy.edges(".erd-rel").map((e) => e.style("source-endpoint"))).toEqual([
      "-9px 15px",
      "-3px 15px",
      "3px 15px",
      "9px 15px",
    ]);
  });

  it("falls back to taxi routing when the nodes overlap along the corridor", () => {
    const cy = graph([["a", "b"]], { a: { x: 0, y: 0 }, b: { x: 0, y: 20 } });
    applyTaxiLanes(cy);
    const edge = cy.edges(".erd-rel")[0];
    expect(edge.style("curve-style")).toBe("taxi");
    expect(edge.style("source-endpoint")).toBe("outside-to-node");
  });

  it("clearTaxiLanes restores the default endpoints", () => {
    const cy = graph(
      [
        ["a", "b"],
        ["a", "b"],
      ],
      { a: { x: 0, y: 0 }, b: { x: 0, y: 300 } },
    );
    applyTaxiLanes(cy);
    clearTaxiLanes(cy);
    cy.edges(".erd-rel").forEach((e) => {
      expect(e.style("source-endpoint")).toBe("outside-to-node");
      expect(e.style("edge-distances")).toBe("intersection");
    });
  });
});
