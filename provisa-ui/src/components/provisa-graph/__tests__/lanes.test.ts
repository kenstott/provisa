// Copyright (c) 2026 Kenneth Stott
// Canary: 7e1c4a93-2d58-4b06-9f3e-c5a8b1d7e240
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239 (maintainer, 2026-11-11 screenshots; amended 2026-11-11, EACH LANE IS SORTED BY THE LANE
// ON ITS LEFT), REQ-2174 (amended 2026-11-11, EACH SIBLING TERM ON ITS OWN ROW): the lanes preset as
// ProvisaGraph places it, measured on the positions it leaves (headless), on the pet-store and Pet
// Health Ontology graphs. The first lane keeps its order and each lane after it is sorted by the places
// of what its nodes join on their left, ties by the order given; every lane is spread over the tallest
// lane's height; each term has its own row and the lines between terms run beside the lane; each lane's
// box bounds its own nodes and no two boxes meet; a node is wide enough for its label, which starts
// after the icon.

import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import cytoscape from "cytoscape";
import elk from "cytoscape-elk";
import { laneDiagram, drawnCrossings, LANES, type At } from "../../graph3d/laneDiagram";
import { laneGraph } from "../../graph3d/laneGraph";
import { petHealthOntology, petStoreOntology } from "../../graph3d/__tests__/crossingFixtures";
import type { GraphData } from "../../graph3d/graphModel";
import { NODE_MAX_WIDTH, TEXT_LEFT, TEXT_RIGHT, graphElements, textWidth } from "../model";
import { graphStylesheet } from "../stylesheet";
import { SIZE, graphTokens } from "../tokens";
import { besideRoutes, runLanes, LANE_GAP, type LanesOptions } from "../lanes";
import type { Core } from "../core";
import type { CyCanvas } from "../cyCanvas";

type CyExt = Parameters<typeof cytoscape.use>[0];
try {
  cytoscape.use(((elk as { default?: CyExt }).default ?? elk) as CyExt);
} catch {
  /* already registered */
}

/** Pairs of lines crossing before the lanes were spread (J1206's baseline, run headless as below):
 * the pet-store graph 24 as the second ELK pass drew it (origin/main 2c6f10d75d), Pet Health 42. */
const CROSSINGS_BEFORE = { petStore: 24, petHealth: 42 };
/** The pet-store graph at most as crossed as J1206's barycentre sweeps left it (6a4b0d1db0: 20). */
const PET_STORE_SWEPT = 20;

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
    beside: "term",
  };
  const elements = graphElements(graph, (t) => t);
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
  /** Each line as drawn: the lines inside the term lane by the route beside it, the rest straight. */
  const routes = besideRoutes(cy, "term");
  return { d, cy, at, route: (id: string) => routes.get(id) };
}

/** Each lane's nodes top to bottom as drawn. */
const drawnLanes = (cy: Core) =>
  LANES.map((l) =>
    cy
      .nodes('[kind = "node"], [kind = "more"]')
      .filter((n) => n.data("lane") === l)
      .map((n) => n.id())
      .sort((a, b) => cy.getElementById(a).position().y - cy.getElementById(b).position().y),
  );

const box = (cy: Core, id: string) => {
  const n = cy.getElementById(id);
  const p = n.position();
  const w = n.data("w") as number;
  const h = n.data("h") as number;
  return { x1: p.x - w / 2, x2: p.x + w / 2, y1: p.y - h / 2, y2: p.y + h / 2 };
};

describe("each lane sorted by the lane on its left (REQ-2239, amended 2026-11-11)", () => {
  const FIXTURES = { petStore: petStoreOntology, petHealth: petHealthOntology };

  it("draws no line from a table to its own columns across another (Pet Health)", async () => {
    const { d, cy, at } = await laidOut(petHealthOntology());
    const own = d.links.filter((k) => d.lane.get(k.to) === "table" && d.lane.get(k.from) !== "term");
    const n = drawnCrossings(own, at);
    cy.destroy();
    expect(own.length).toBeGreaterThan(30);
    expect(n).toBe(0);
  });

  for (const [name, fixture] of Object.entries(FIXTURES))
    it(`draws each lane in the sort of its left neighbours' places (${name})`, async () => {
      const { cy } = await laidOut(fixture());
      // The order the nodes are given in: the stable key.
      const given = cy
        .nodes('[kind = "node"], [kind = "more"]')
        .map((n) => n.id());
      const laneAt = new Map<string, number>();
      cy.nodes('[kind = "node"], [kind = "more"]').forEach((n) => {
        laneAt.set(n.id(), LANES.indexOf(n.data("lane")));
      });
      const left = new Map<string, number[]>();
      const y = (id: string) => cy.getElementById(id).position().y;
      cy.edges().forEach((e) => {
        const [a, b] = [e.data("source") as string, e.data("target") as string];
        for (const [p, q] of [
          [a, b],
          [b, a],
        ])
          if ((laneAt.get(q) as number) < (laneAt.get(p) as number))
            left.set(p, [...(left.get(p) ?? []), y(q)]);
      });
      const key = (id: string) => {
        const ys = left.get(id) ?? [];
        return ys.length === 0 ? Infinity : ys.reduce((s, v) => s + v, 0) / ys.length;
      };
      const close = (p: number, q: number) =>
        p === q ||
        (Number.isFinite(p) &&
          Number.isFinite(q) &&
          Math.abs(p - q) <= 1e-14 * Math.max(Math.abs(p), Math.abs(q)));
      const drawn = drawnLanes(cy);
      cy.destroy();
      expect(drawn[0]).toEqual(given.filter((id) => laneAt.get(id) === 0));
      for (let l = 1; l < LANES.length; l++) {
        const sorted = given
          .filter((id) => laneAt.get(id) === l)
          .map((id, i) => ({ id, k: key(id), i }))
          .sort((p, q) => (close(p.k, q.k) ? 0 : p.k - q.k) || p.i - q.i)
          .map((x) => x.id);
        expect(drawn[l], `the ${LANES[l]} lane`).toEqual(sorted);
      }
    });

  it("crosses fewer lines than before the lanes were spread, counted as drawn", async () => {
    const health = await laidOut(petHealthOntology());
    const h = drawnCrossings(health.d.links, health.at, health.route);
    health.cy.destroy();
    const store = await laidOut(petStoreOntology());
    const s = drawnCrossings(store.d.links, store.at, store.route);
    store.cy.destroy();
    expect(h).toBeLessThan(CROSSINGS_BEFORE.petHealth);
    expect(s).toBeLessThan(CROSSINGS_BEFORE.petStore);
    expect(s).toBeLessThanOrEqual(PET_STORE_SWEPT);
  });

  it("draws the same graph the same way twice", async () => {
    for (const fixture of Object.values(FIXTURES)) {
      const one = await laidOut(fixture());
      const first = drawnLanes(one.cy);
      one.cy.destroy();
      const two = await laidOut(fixture());
      const second = drawnLanes(two.cy);
      two.cy.destroy();
      expect(second).toEqual(first);
    }
  });
});

describe("each sibling term on its own row (REQ-2174, amended 2026-11-11)", () => {
  for (const [name, fixture] of Object.entries({
    petStore: petStoreOntology,
    petHealth: petHealthOntology,
  }))
    it(`gives every term a row and runs the lines between terms beside the lane (${name})`, async () => {
      const { d, cy } = await laidOut(fixture());
      const terms = cy
        .nodes('[kind = "node"]')
        .filter((n) => n.data("lane") === "term")
        .map((n) => ({ id: n.id(), b: box(cy, n.id()) }));
      const inside = cy.edges().filter((e) => {
        const lane = (id: string) => cy.getElementById(id).data("lane");
        return lane(e.data("source")) === "term" && lane(e.data("target")) === "term";
      });
      const styles = inside.map((e) => ({
        id: e.id(),
        curve: e.style("curve-style") as string,
      }));
      const routes = besideRoutes(cy, "term");
      cy.destroy();
      // Dog and Cat under Pet: siblings.
      expect(d.links.filter((k) => k.to === "term:dog" || k.to === "term:cat")).toHaveLength(2);
      // No two terms share a row: their spans down the lane are apart.
      const spans = terms.map((t) => t.b).sort((p, q) => p.y1 - q.y1);
      for (let i = 1; i < spans.length; i++)
        expect(spans[i].y1, "a term's row below the last").toBeGreaterThanOrEqual(spans[i - 1].y2);
      // Each line between terms runs beside the lane: past every term's right edge, save where it
      // leaves and meets its own two terms.
      const right = Math.max(...terms.map((t) => t.b.x2));
      expect(styles.length).toBeGreaterThanOrEqual(6);
      for (const { id, curve } of styles) {
        expect(curve, id).toBe("segments");
        const path = routes.get(id) as At[];
        expect(path.length, id).toBe(4);
        expect(path[0].x, `${id} leaves from a term's right side`).toBeLessThanOrEqual(right);
        for (const p of path.slice(1, -1)) expect(p.x, `${id} beside the lane`).toBeGreaterThan(right);
        // Its run down the lane passes through no term's row.
        for (const t of terms)
          expect(path[1].x > t.b.x2, `${id} clear of ${t.id}`).toBe(true);
      }
    });
});

describe("the lanes preset on the pet-store Ontology graph", () => {

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
      // A fold node's sentence wraps at its spaces, never inside a word ("thi s view").
      {
        id: "more:t",
        type: "more" as const,
        kind: "more" as const,
        name: "3 columns with no term in this view",
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
      // it wraps at its spaces, or anywhere when a word of it is wider than the room.
      expect(n.style("text-halign")).toBe("center");
      const words = String(n.data("label")).split(/\s+/);
      const wider = words.some((x) => textWidth(x, SIZE.name) > room);
      expect(n.style("text-overflow-wrap"), n.id()).toBe(wider ? "anywhere" : "whitespace");
      expect(shift + room / 2, `${n.id()} label's right edge`).toBeLessThanOrEqual(w / 2);
      expect(shift - room / 2, `${n.id()} label's left edge`).toBeGreaterThanOrEqual(-w / 2);
      // A fold node has no icon.
      if (n.data("kind") === "node")
        expect(shift - room / 2, `${n.id()} label's left edge`).toBeGreaterThan(iconRight);
    });
    cy.destroy();
  });

  it("wraps a sentence at its spaces and a name with none wherever it must", () => {
    const cy = drawn();
    const more = cy.getElementById("more:t").style("text-overflow-wrap");
    const long = cy.getElementById("c:col").style("text-overflow-wrap");
    cy.destroy();
    expect(more).toBe("whitespace");
    expect(long).toBe("anywhere");
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
