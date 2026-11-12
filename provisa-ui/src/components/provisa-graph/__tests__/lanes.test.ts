// Copyright (c) 2026 Kenneth Stott
// Canary: 7e1c4a93-2d58-4b06-9f3e-c5a8b1d7e240
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239 (maintainer, 2026-11-11 screenshots): the lanes preset as ProvisaGraph places it, measured
// on the positions it leaves -- ELK headless, the pet-store Ontology graph. Each lane is ordered by
// the barycentre of its neighbours, so the drawing crosses less than the second ELK pass left it;
// every lane is spread over the tallest lane's height; each lane's box bounds its own nodes and no
// two boxes meet; a node is wide enough for its label, which starts after the icon.

import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import cytoscape from "cytoscape";
import elk from "cytoscape-elk";
import {
  laneDiagram,
  drawnCrossings,
  placedLaneOrder,
  stackTerms,
  LANES,
  type At,
} from "../../graph3d/laneDiagram";
import { laneGraph } from "../../graph3d/laneGraph";
import { petStoreOntology } from "../../graph3d/__tests__/crossingFixtures";
import type { GraphData } from "../../graph3d/graphModel";
import { NODE_MAX_WIDTH, TEXT_LEFT, TEXT_RIGHT, graphElements, textWidth } from "../model";
import { graphStylesheet } from "../stylesheet";
import { SIZE, graphTokens } from "../tokens";
import { laneElements, runLanes, LANE_GAP, type LanesOptions } from "../lanes";
import type { Core } from "../core";
import type { CyCanvas } from "../cyCanvas";

type CyExt = Parameters<typeof cytoscape.use>[0];
try {
  cytoscape.use(((elk as { default?: CyExt }).default ?? elk) as CyExt);
} catch {
  /* already registered */
}

/** The pet-store graph as the second ELK pass drew it, before J1206 (origin/main 2c6f10d75d, run
 * headless as below): 24 pairs of lines crossed. */
const CROSSINGS_BEFORE = 24;

const headlessCanvas: CyCanvas = {
  container: null as unknown as HTMLDivElement,
  run(layout, then) {
    layout.one("layoutstop", () => then?.());
    layout.run();
  },
  end(cy) {
    cy.destroy();
  },
};

/** ``data`` laid out as LaneStage lays it out, headless. */
async function laidOut(data: GraphData) {
  const d = laneDiagram(data, data, new Set());
  const graph = laneGraph(d, (n) => `+${n} columns`);
  const opts: LanesOptions = {
    lanes: LANES.map((l) => ({ id: l, title: l })),
    stacked: "term",
    order: (at) => placedLaneOrder(d, at),
    settle: (order, at, widthOf) => stackTerms(d, order, at, widthOf),
  };
  const elements = laneElements(
    graphElements(graph, (t) => t),
    opts,
  );
  const cy = cytoscape({
    headless: true,
    styleEnabled: true,
    elements: elements as unknown as Parameters<typeof cytoscape>[0]["elements"],
    style: graphStylesheet(graphTokens(false)) as unknown as Parameters<
      typeof cytoscape
    >[0]["style"],
  }) as unknown as Core;
  await new Promise<void>((done) =>
    runLanes(
      cy,
      headlessCanvas,
      opts,
      graph.edges.filter((e) => e.overlay),
      done,
    ),
  );
  const at = (id: string): At => cy.getElementById(id).position();
  return { d, cy, at };
}

const box = (cy: Core, id: string) => {
  const n = cy.getElementById(id);
  const p = n.position();
  const w = n.data("w") as number;
  const h = n.data("h") as number;
  return { x1: p.x - w / 2, x2: p.x + w / 2, y1: p.y - h / 2, y2: p.y + h / 2 };
};

describe("the lanes preset on the pet-store Ontology graph", () => {
  it("crosses fewer lines than the second ELK pass drew", async () => {
    const { d, cy, at } = await laidOut(petStoreOntology());
    const n = drawnCrossings(d.links, at);
    cy.destroy();
    expect(n).toBeLessThan(CROSSINGS_BEFORE);
  });

  it("spreads every lane over the height of the tallest", async () => {
    const { d, cy } = await laidOut(petStoreOntology());
    const laneOf = (id: string) => cy.getElementById(id).data("lane") as string;
    const placed = cy.nodes('[kind = "node"], [kind = "more"]').map((n) => n.id());
    const spans = LANES.map((l) => {
      const ids = placed.filter((id) => laneOf(id) === l);
      const bs = ids.map((id) => box(cy, id));
      return {
        l,
        n: ids.length,
        y1: Math.min(...bs.map((b) => b.y1)),
        y2: Math.max(...bs.map((b) => b.y2)),
      };
    }).filter((s) => s.n > 1);
    const top = Math.min(...spans.map((s) => s.y1));
    const bottom = Math.max(...spans.map((s) => s.y2));
    cy.destroy();
    expect(d.graph.nodes.length).toBeGreaterThan(40);
    for (const s of spans) {
      expect(s.y1, `${s.l} lane's top`).toBeCloseTo(top, 0);
      expect(s.y2, `${s.l} lane's bottom`).toBeCloseTo(bottom, 0);
    }
  });

  it("boxes each lane round its own nodes alone, the boxes a gap apart", async () => {
    const { cy } = await laidOut(petStoreOntology());
    // The boxes as drawn: their padding and border included, their titles not.
    const hulls = cy.nodes('[kind = "lane"]').map((n) => {
      const b = n.boundingBox({ includeLabels: false });
      return { lane: n.id().slice("lane:".length), x1: b.x1, x2: b.x2, y1: b.y1, y2: b.y2 };
    });
    const placed = cy.nodes('[kind = "node"], [kind = "more"]').map((n) => ({
      lane: n.data("lane") as string,
      b: box(cy, n.id()),
    }));
    cy.destroy();
    expect(hulls.length).toBe(4);
    hulls.sort((a, b) => a.x1 - b.x1);
    for (let i = 1; i < hulls.length; i++)
      expect(
        hulls[i].x1 - hulls[i - 1].x2,
        `${hulls[i - 1].lane} | ${hulls[i].lane}`,
      ).toBeGreaterThanOrEqual(LANE_GAP);
    for (const { lane, b } of placed) {
      const own = hulls.find((h) => h.lane === lane)!;
      expect(
        b.x1 >= own.x1 && b.x2 <= own.x2 && b.y1 >= own.y1 && b.y2 <= own.y2,
        `${lane} node in its box`,
      ).toBe(true);
      for (const h of hulls.filter((x) => x.lane !== lane))
        expect(b.x2 <= h.x1 || b.x1 >= h.x2, `${lane} node outside ${h.lane}'s box`).toBe(true);
    }
  });
});

describe("a node's label", () => {
  // The Pet Health graph's table, whose name has no space to wrap at: drawn over the icon before.
  const LONG = "shelter.assignments_by_employee";
  const spec = {
    nodes: [
      { id: "t:long", type: "table" as const, name: LONG },
      { id: "t:short", type: "table" as const, name: "pets" },
      {
        id: "c:col",
        type: "column" as const,
        name: "shelter.assignments_by_employee.employee_assignment_history_id",
      },
    ],
    edges: [],
  };
  const drawn = () =>
    cytoscape({
      headless: true,
      styleEnabled: true,
      elements: graphElements(spec, (t) => t) as unknown as Parameters<
        typeof cytoscape
      >[0]["elements"],
      style: graphStylesheet(graphTokens(false)) as unknown as Parameters<
        typeof cytoscape
      >[0]["style"],
    });

  it("is in a node wide enough for it", () => {
    const cy = drawn();
    const w = cy.getElementById("t:long").data("w") as number;
    cy.destroy();
    expect(w).toBeGreaterThanOrEqual(TEXT_LEFT + textWidth(LONG, SIZE.name) + TEXT_RIGHT);
    expect(w).toBeGreaterThan(SIZE.nodeWidth);
  });

  it("starts right of the icon and ends inside the node, however wide its text runs", () => {
    const cy = drawn();
    cy.nodes().forEach((n) => {
      const w = n.data("w") as number;
      const room = parseFloat(n.style("text-max-width") as string);
      const shift = parseFloat(n.style("text-margin-x") as string);
      const iconRight = -w / 2 + parseFloat(n.style("background-position-x") as string) + SIZE.icon;
      // The label is centred at the node's centre plus its shift and is at most its room wide:
      // it wraps anywhere rather than run past it.
      expect(n.style("text-halign")).toBe("center");
      expect(n.style("text-overflow-wrap"), n.id()).toBe("anywhere");
      expect(shift - room / 2, `${n.id()} label's left edge`).toBeGreaterThan(iconRight);
      expect(shift + room / 2, `${n.id()} label's right edge`).toBeLessThanOrEqual(w / 2);
    });
    cy.destroy();
  });

  it("wraps a name past the widest node, and the node grows in height for it", () => {
    const cy = drawn();
    const col = cy.getElementById("c:col");
    const w = col.data("w") as number;
    const h = col.data("h") as number;
    cy.destroy();
    expect(w).toBe(NODE_MAX_WIDTH);
    expect(h).toBeGreaterThan(SIZE.nodeHeight);
  });
});

describe("the canvas", () => {
  it("has no dot grid", () => {
    const css = readFileSync(resolve(__dirname, "../provisa-graph.css"), "utf8");
    const stage = css.slice(
      css.indexOf(".pg-stage {"),
      css.indexOf("}", css.indexOf(".pg-stage {")),
    );
    expect(stage).toContain("background-color: var(--pg-canvas)");
    expect(css).not.toMatch(/radial-gradient|--pg-grid|background-size/);
    expect(Object.keys(graphTokens(false))).not.toContain("grid");
    expect(Object.keys(graphTokens(true))).not.toContain("grid");
  });
});
