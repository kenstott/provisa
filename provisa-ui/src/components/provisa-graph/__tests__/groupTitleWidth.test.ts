// Copyright (c) 2026 Kenneth Stott
// Canary: c6938c5a-3d09-4257-a4db-071c0bb430bf
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239 (amended 2026-11-28): a group's title wider than its nodes ran past its box's sides. ELK
// is given each group's title width as its least width, and the drawn box takes the same width, so
// a group is always at least as wide as its title and a neighbour never touches it.

import { describe, expect, it } from "vitest";
import { cytoscape, type Core } from "../core";
import type { RunningLayout } from "../cyCanvas";
import { LAYOUT_FAIL } from "../elkLayout";
import { flowLayout } from "../flow";
import { GROUP_PAD, graphElements, groupMinWidth, textWidth, type GraphSpec } from "../model";
import { GROUPED_ELK_LAYOUT } from "../presets";
import { graphStylesheet } from "../stylesheet";
import { SIZE, graphTokens } from "../tokens";

const LONG = "Regional finance controllers and reviewers";

const SPEC: GraphSpec = {
  groups: [
    { id: "long", label: LONG },
    { id: "short", label: "ops" },
  ],
  nodes: [
    { id: "a", type: "role", name: "a", parent: "long" },
    { id: "b", type: "role", name: "b", parent: "short" },
    { id: "c", type: "role", name: "c" },
  ],
  edges: [
    { id: "ac", source: "a", target: "c", arrow: "target" },
    { id: "bc", source: "b", target: "c", arrow: "target" },
  ],
};

function drawn(): Core {
  return cytoscape({
    headless: true,
    styleEnabled: true,
    elements: graphElements(SPEC) as unknown as Parameters<typeof cytoscape>[0]["elements"],
    style: graphStylesheet(graphTokens(false)) as unknown as Parameters<
      typeof cytoscape
    >[0]["style"],
  }) as Core;
}

/** layout run on cy, to its end. */
function laidOut(cy: Core, layout: object): Promise<Core> {
  return new Promise((resolve, reject) => {
    const l = cy.layout(layout as Parameters<Core["layout"]>[0]) as unknown as RunningLayout;
    l.one("layoutstop", () => resolve(cy));
    // A failed layout fails the test, with ELK's error.
    l.one(LAYOUT_FAIL, (_evt, error) => reject(error));
    l.run();
  });
}

const titleWidth = textWidth(LONG.toUpperCase(), SIZE.groupLabel);

const overlap = (
  p: { x1: number; x2: number; y1: number; y2: number },
  q: { x1: number; x2: number; y1: number; y2: number },
) => p.x1 < q.x2 && q.x1 < p.x2 && p.y1 < q.y2 && q.y1 < p.y2;

describe("a group is at least as wide as its title", () => {
  it("asks for a width past its title's, padding included, and no less than a one-node box", () => {
    expect(groupMinWidth(LONG.toUpperCase())).toBeGreaterThan(titleWidth);
    expect(groupMinWidth("OPS")).toBe(SIZE.nodeWidth + 2 * GROUP_PAD);
  });

  for (const [name, layout] of [
    ["flow (lineage, roles)", flowLayout(undefined, 72)],
    ["layered (ERD)", GROUPED_ELK_LAYOUT],
  ] as const) {
    it(`is laid out title-wide, its neighbours clear of it: ${name}`, async () => {
      const cy = drawn();
      const eles = cy.elements();
      await laidOut(cy, { ...layout, eles });
      const long = cy.getElementById("long").boundingBox({ includeLabels: false });
      const others = ["short", "c"].map((id) =>
        cy.getElementById(id).boundingBox({ includeLabels: false }),
      );
      const min = groupMinWidth(LONG.toUpperCase());
      cy.destroy();
      // The drawn box is the width ELK kept for it (its border's width aside).
      expect(long.w).toBeGreaterThanOrEqual(min);
      expect(long.w).toBeGreaterThan(titleWidth);
      for (const o of others) expect(overlap(long, o)).toBe(false);
    });
  }
});
