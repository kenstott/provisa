// Copyright (c) 2026 Kenneth Stott
// Canary: 8e3d1b56-7a92-4c0f-b4e8-2f6c9a1d5e73
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239 (amended 2026-11-27): the flow preset, lineage's. What no line reaches is packed under
// the laid-out part; a group's fold node sits under the group's other nodes.

import { describe, expect, it } from "vitest";
import { cytoscape, type Core } from "../core";
import { flowParts, foldsLast, layerGap, packUnder } from "../flow";
import { textWidth } from "../model";
import { SIZE } from "../tokens";

const node = (id: string, x: number, y: number, parent?: string, kind = "node") => ({
  data: { id, kind, ...(parent ? { parent } : {}) },
  position: { x, y },
});

function core(): Core {
  return cytoscape({
    headless: true,
    styleEnabled: true,
    style: [{ selector: "node", style: { width: 100, height: 40 } }],
    elements: [
      { data: { id: "g1", kind: "group" } },
      node("a", 0, 0, "g1"),
      node("b", 300, 0),
      { data: { id: "ab", source: "a", target: "b" } },
      { data: { id: "g2", kind: "group" } },
      node("c", 0, 0, "g2"),
      node("fold", 0, -100, "g2", "more"),
      node("d", 0, 0),
    ],
  }) as Core;
}

describe("the flow preset", () => {
  it("splits the top-level things by whether a line reaches them", () => {
    const { laid, isolated } = flowParts(core());
    expect(laid.sort()).toEqual(["b", "g1"]);
    expect(isolated.sort()).toEqual(["d", "g2"]);
  });

  it("puts a group's fold node under its other nodes", () => {
    const cy = core();
    foldsLast(cy);
    const fold = cy.getElementById("fold").position("y");
    expect(fold).toBeGreaterThan(cy.getElementById("c").position("y") + 40);
  });

  it("packs what no line reaches under the laid-out part, each group with its nodes", () => {
    const cy = core();
    const before = cy.getElementById("c").position("y") - cy.getElementById("fold").position("y");
    packUnder(cy, ["g2", "d"], 1.6);
    const laidBottom = cy.getElementById("a").union(cy.getElementById("b")).boundingBox({}).y2;
    for (const id of ["c", "fold", "d"])
      expect(cy.getElementById(id).boundingBox({}).y1).toBeGreaterThan(laidBottom);
    // The group moved whole.
    expect(cy.getElementById("c").position("y") - cy.getElementById("fold").position("y")).toBe(
      before,
    );
  });
});

// REQ-2239 (amended 2026-11-27): lineage labels its lines with their transforms; the layers stand
// far enough apart that the widest label is clear of the nodes at both ends.
describe("layerGap", () => {
  it("keeps the least gap when every label is short", () => {
    expect(layerGap(["", "SUM"])).toBe(72);
  });

  it("widens to the widest label, with an arrowhead's room at each end", () => {
    const long = "SUM, COUNT, CAST, UPPER, SUBSTRING(1, 3) · 5 columns";
    const gap = layerGap(["SUM", long]);
    expect(gap).toBeGreaterThan(140);
    expect(gap).toBeGreaterThanOrEqual(textWidth(long, SIZE.edgeLabel) + 2 * 2 + 2 * 12);
  });
});
