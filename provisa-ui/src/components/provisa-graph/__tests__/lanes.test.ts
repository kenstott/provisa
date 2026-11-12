// Copyright (c) 2026 Kenneth Stott
// Canary: 7e1c4a93-2d58-4b06-9f3e-c5a8b1d7e240
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239 (maintainer, 2026-11-11 screenshots; amended 2026-11-11, TERMS SIT LEVEL WITH THEIR
// COLUMNS; THE SCORE IS THE ANGLE OF THE LINES), REQ-2174 (amended 2026-11-11, EACH SIBLING TERM ON ITS
// OWN ROW): the lanes preset as ProvisaGraph places it, measured on the positions it leaves (headless),
// on the pet-store and Pet Health Ontology graphs. Products, tables and columns are sorted by the
// places of what they join on their left, tied tables that share terms side by side, and spread over
// the tallest lane's height; the term lane is placed: a term bound to one column level with it, one
// bound to several level with the column leaving its lines flattest, the children of one parent a
// block on adjacent rows placed where its lines are flattest, no term or block left a free place that
// flattens its lines. Each term has its own row and the lines between terms run beside the lane; each
// lane's box bounds its own nodes and no two boxes meet; a node is wide enough for its label, which
// starts after the icon. Crossings are counted (drawnCrossings), not gated.

import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import cytoscape from "cytoscape";
import { laneDiagram, drawnCrossings, LANES, type At, type Lane } from "../../graph3d/laneDiagram";
import { laneGraph } from "../../graph3d/laneGraph";
import { petHealthOntology, petStoreOntology } from "../../graph3d/__tests__/crossingFixtures";
import type { GraphData } from "../../graph3d/graphModel";
import { NODE_MAX_WIDTH, TEXT_LEFT, TEXT_RIGHT, graphElements, textWidth } from "../model";
import { graphStylesheet } from "../stylesheet";
import { SIZE, graphTokens } from "../tokens";
import { GLOSSARY_REL_TYPES } from "../../../api/glossary";
import { besideRoutes, runLanes, LANE_GAP, NODE_GAP, type LanesOptions } from "../lanes";
import type { Core } from "../core";
import type { CyCanvas } from "../cyCanvas";

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
  const overlay = new Set(graph.edges.filter((e) => e.overlay).map((e) => e.id));
  return { d, cy, at, route: (id: string) => routes.get(id), overlay };
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

/** Two numbers equal within the float tolerance (a relative 1e-14). */
const close = (p: number, q: number) =>
  p === q ||
  (Number.isFinite(p) &&
    Number.isFinite(q) &&
    Math.abs(p - q) <= 1e-14 * Math.max(Math.abs(p), Math.abs(q)));

/** The term lane as drawn: each term's place and size, the columns it binds, the terms its rank lines
 * join it to, and the blocks that move as one (the children of one parent, with a parent that binds
 * no column of its own). */
function termLane(cy: Core, overlay: Set<string>) {
  const lane = (id: string) => cy.getElementById(id).data("lane") as string;
  const terms = cy
    .nodes('[kind = "node"]')
    .filter((n) => n.data("lane") === "term")
    .map((n) => ({
      id: n.id(),
      y: n.position().y,
      h: n.data("h") as number,
    }));
  const cols = new Map<string, number[]>(terms.map((t) => [t.id, []]));
  const children = new Map<string, string[]>();
  cy.edges().forEach((e) => {
    const [s, g] = [e.data("source") as string, e.data("target") as string];
    for (const [a, b] of [
      [s, g],
      [g, s],
    ])
      if (lane(a) === "term" && lane(b) === "column")
        cols.get(a)?.push(cy.getElementById(b).position().y);
    if (lane(s) === "term" && lane(g) === "term" && !overlay.has(e.id()))
      children.set(s, [...(children.get(s) ?? []), g]);
  });
  const root = new Map(terms.map((t) => [t.id, t.id]));
  const find = (id: string): string => {
    const r = root.get(id) as string;
    return r === id ? id : find(r);
  };
  const union = (a: string, b: string) => root.set(find(a), find(b));
  for (const [p, kids] of children) {
    if (kids.length > 1) kids.forEach((k) => union(kids[0], k));
    if ((cols.get(p) ?? []).length === 0) kids.forEach((k) => union(p, k));
  }
  const blocks = new Map<string, string[]>();
  for (const t of terms) blocks.set(find(t.id), [...(blocks.get(find(t.id)) ?? []), t.id]);
  const inBlock = (id: string) => (blocks.get(find(id)) as string[]).length > 1;
  const byId = new Map(terms.map((t) => [t.id, t]));
  /** The summed vertical offset of ``ids``' lines with each moved by ``dy``. */
  const score = (ids: string[], dy = 0) =>
    ids.reduce(
      (s, id) =>
        s + (cols.get(id) as number[]).reduce((u, c) => u + Math.abs(byId.get(id)!.y + dy - c), 0),
      0,
    );
  /** Whether ``ids`` moved by ``dy`` overlap no other term's row (a node gap kept). */
  const free = (ids: string[], dy: number) =>
    ids.every((id) => {
      const t = byId.get(id)!;
      return terms
        .filter((o) => !ids.includes(o.id))
        .every(
          (o) =>
            t.y + dy + t.h / 2 + NODE_GAP <= o.y - o.h / 2 + 1e-9 ||
            t.y + dy - t.h / 2 >= o.y + o.h / 2 + NODE_GAP - 1e-9,
        );
    });
  return { terms, cols, children, blocks: [...blocks.values()], inBlock, score, free, byId };
}

describe("terms sit level with their columns (REQ-2239, amended 2026-11-11)", () => {
  const FIXTURES = { petStore: petStoreOntology, petHealth: petHealthOntology };

  for (const [name, fixture] of Object.entries(FIXTURES)) {
    it(`draws no line from a table to its own columns across another (${name})`, async () => {
      const { d, cy, at } = await laidOut(fixture());
      const own = d.links.filter(
        (k) => d.lane.get(k.to) === "table" && d.lane.get(k.from) !== "term",
      );
      const n = drawnCrossings(own, at);
      cy.destroy();
      expect(own.length).toBeGreaterThan(30);
      expect(n).toBe(0);
    });

    it(`sorts the product, table and column lanes by their left neighbours' places (${name})`, async () => {
      const { cy } = await laidOut(fixture());
      const given = cy.nodes('[kind = "node"], [kind = "more"]').map((n) => n.id());
      const laneAt = new Map<string, number>();
      cy.nodes('[kind = "node"], [kind = "more"]').forEach((n) => {
        laneAt.set(n.id(), LANES.indexOf(n.data("lane") as Lane));
      });
      const left = new Map<string, number[]>();
      const y = (id: string) => cy.getElementById(id).position().y;
      cy.edges().forEach((e) => {
        const [a, b] = [e.data("source") as string, e.data("target") as string];
        for (const [p, q] of [
          [a, b],
          [b, a],
        ])
          if ((laneAt.get(q) as number) === (laneAt.get(p) as number) - 1)
            left.set(p, [...(left.get(p) ?? []), y(q)]);
      });
      const key = (id: string) => {
        const ys = left.get(id) ?? [];
        return ys.length === 0 ? Infinity : ys.reduce((s, v) => s + v, 0) / ys.length;
      };
      const drawn = drawnLanes(cy);
      cy.destroy();
      expect(drawn[0]).toEqual(given.filter((id) => laneAt.get(id) === 0));
      for (const l of [1, 2]) {
        const keys = drawn[l].map(key);
        for (let i = 1; i < keys.length; i++)
          expect(
            close(keys[i], keys[i - 1]) || keys[i] > keys[i - 1],
            `${LANES[l]} lane: ${drawn[l][i]} after ${drawn[l][i - 1]}`,
          ).toBe(true);
      }
    });

    it(`puts tied tables that share terms next to each other (${name})`, async () => {
      const { cy } = await laidOut(fixture());
      const lane = (id: string) => cy.getElementById(id).data("lane") as string;
      const termsOf = new Map<string, Set<string>>();
      const tableOf = new Map<string, string>();
      cy.edges().forEach((e) => {
        const [s, g] = [e.data("source") as string, e.data("target") as string];
        if (lane(s) === "table" && lane(g) === "column") tableOf.set(g, s);
      });
      cy.edges().forEach((e) => {
        const [s, g] = [e.data("source") as string, e.data("target") as string];
        for (const [c, t] of [
          [s, g],
          [g, s],
        ])
          if (lane(c) === "column" && lane(t) === "term" && tableOf.has(c)) {
            const tb = tableOf.get(c) as string;
            termsOf.set(tb, (termsOf.get(tb) ?? new Set()).add(t));
          }
      });
      const productY = (tb: string) => {
        const ps = cy
          .edges()
          .filter((e) => e.data("target") === tb && lane(e.data("source") as string) === "product")
          .map((e) => cy.getElementById(e.data("source") as string).position().y);
        return ps.length === 0 ? Infinity : ps.reduce((s, v) => s + v, 0) / ps.length;
      };
      const tables = drawnLanes(cy)[1];
      cy.destroy();
      const shared = (a: string, b: string) =>
        [...(termsOf.get(a) ?? [])].filter((t) => termsOf.get(b)?.has(t)).length;
      // In each run of tables tied on their product, a table is followed by one it shares a term
      // with whenever any table still to come in the run shares one with it.
      let runs = 0;
      for (let i = 0; i < tables.length; ) {
        let j = i + 1;
        while (j < tables.length && close(productY(tables[j]), productY(tables[i]))) j++;
        const run = tables.slice(i, j);
        if (run.length > 2) runs++;
        for (let k = 0; k + 1 < run.length; k++) {
          const best = Math.max(...run.slice(k + 1).map((b) => shared(run[k], b)));
          if (best > 0)
            expect(shared(run[k], run[k + 1]), `${run[k]} then ${run[k + 1]}`).toBe(best);
        }
        i = j;
      }
      expect(runs).toBeGreaterThan(0);
    });

    it(`sets each term bound to one column level with it (${name})`, async () => {
      const { cy, overlay } = await laidOut(fixture());
      const t = termLane(cy, overlay);
      cy.destroy();
      const single = t.terms.filter((x) => t.cols.get(x.id)!.length === 1 && !t.inBlock(x.id));
      expect(single.length).toBeGreaterThan(1);
      for (const x of single) expect(x.y, x.id).toBeCloseTo(t.cols.get(x.id)![0], 9);
    });

    it(`sets each term bound to several columns level with the one leaving its lines flattest (${name})`, async () => {
      const { cy, overlay } = await laidOut(fixture());
      const t = termLane(cy, overlay);
      cy.destroy();
      const multi = t.terms.filter((x) => t.cols.get(x.id)!.length > 1 && !t.inBlock(x.id));
      expect(multi.length).toBeGreaterThan(2);
      for (const x of multi) {
        const ys = t.cols.get(x.id)!;
        expect(
          ys.some((c) => close(c, x.y)),
          `${x.id} level with one of its columns`,
        ).toBe(true);
        const now = t.score([x.id]);
        for (const c of ys)
          if (t.free([x.id], c - x.y))
            expect(t.score([x.id], c - x.y), `${x.id} at its column ${c}`).toBeGreaterThanOrEqual(
              now - 1e-14 * now,
            );
      }
    });

    it(`leaves no term or block a free place that flattens its lines (${name})`, async () => {
      const { cy, overlay } = await laidOut(fixture());
      const t = termLane(cy, overlay);
      cy.destroy();
      const units = t.blocks.filter((b) => b.some((id) => t.cols.get(id)!.length > 0));
      for (const ids of units) {
        const now = t.score(ids);
        // Every place the unit could take: each of its lines level, and against each other row.
        const tops = ids.map((id) => t.byId.get(id)!.y);
        const moves = new Set<number>();
        ids.forEach((id, i) => t.cols.get(id)!.forEach((c) => moves.add(c - tops[i])));
        const lo = Math.min(...ids.map((id) => t.byId.get(id)!.y - t.byId.get(id)!.h / 2));
        const hi = Math.max(...ids.map((id) => t.byId.get(id)!.y + t.byId.get(id)!.h / 2));
        for (const o of t.terms.filter((o) => !ids.includes(o.id))) {
          moves.add(o.y - o.h / 2 - NODE_GAP - hi);
          moves.add(o.y + o.h / 2 + NODE_GAP - lo);
        }
        for (const dy of moves)
          if (t.free(ids, dy))
            expect(t.score(ids, dy), `${ids.join("+")} moved ${dy}`).toBeGreaterThanOrEqual(
              now - 1e-14 * now,
            );
      }
    });

    it(`keeps a parent's children next to each other, each on its own row (${name})`, async () => {
      const { cy, overlay } = await laidOut(fixture());
      const t = termLane(cy, overlay);
      cy.destroy();
      const order = [...t.terms].sort((p, q) => p.y - q.y).map((x) => x.id);
      let families = 0;
      for (const [, kids] of t.children) {
        if (kids.length < 2) continue;
        families++;
        const at = kids.map((k) => order.indexOf(k)).sort((p, q) => p - q);
        expect(at[at.length - 1] - at[0], kids.join(",")).toBe(kids.length - 1);
      }
      expect(families).toBeGreaterThan(1);
      const rows = [...t.terms].sort((p, q) => p.y - q.y);
      for (let i = 1; i < rows.length; i++)
        expect(
          rows[i].y - rows[i].h / 2,
          `${rows[i].id} below ${rows[i - 1].id}`,
        ).toBeGreaterThanOrEqual(rows[i - 1].y + rows[i - 1].h / 2 + NODE_GAP - 1e-9);
    });

    it(`draws the same graph the same way twice (${name})`, async () => {
      const where = (cy: Core) =>
        cy.nodes().map((n) => [n.id(), n.position().x, n.position().y] as const);
      const one = await laidOut(fixture());
      const first = where(one.cy);
      one.cy.destroy();
      const two = await laidOut(fixture());
      const second = where(two.cy);
      two.cy.destroy();
      expect(second).toEqual(first);
    });
  }
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
        return (
          lane(e.data("source") as string) === "term" && lane(e.data("target") as string) === "term"
        );
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
        for (const p of path.slice(1, -1))
          expect(p.x, `${id} beside the lane`).toBeGreaterThan(right);
        // Its run down the lane passes through no term's row.
        for (const t of terms) expect(path[1].x > t.b.x2, `${id} clear of ${t.id}`).toBe(true);
      }
    });
});

/** J1227: a term page's graph, as the maintainer's 2026-11-12 screenshots drew it: ``a`` joined to
 * ``b`` by a line named ``label``, ``b`` bound to a column. */
function twoTerms(label: string): GraphData {
  return {
    nodes: [
      { id: "term:test", type: "term", label: "test" },
      { id: "term:breed", type: "term", label: "animal breed name" },
      { id: "t:animals", type: "table", label: "shelter.animals" },
      {
        id: "c:animals.breed",
        type: "column",
        label: "shelter.animals.breed",
        parent: "t:animals",
      },
    ],
    edges: [
      { id: "term_edge:test>breed", a: "term:test", b: "term:breed", kind: "term_edge", label },
      { id: "term_binding:breed", a: "term:breed", b: "c:animals.breed", kind: "term_binding" },
    ],
  };
}

/** Three terms in a PART_OF chain (test3 of test2 of test1), the middle one bound to a column. */
function partOfChain(): GraphData {
  const g = twoTerms("PART_OF");
  return {
    nodes: [
      { id: "term:test1", type: "term", label: "test1" },
      { id: "term:test2", type: "term", label: "test2" },
      { id: "term:test3", type: "term", label: "test3" },
      ...g.nodes.filter((n) => !n.id.startsWith("term:")),
    ],
    edges: [
      {
        id: "term_edge:3>2",
        a: "term:test3",
        b: "term:test2",
        kind: "term_edge",
        label: "PART_OF",
      },
      {
        id: "term_edge:2>1",
        a: "term:test2",
        b: "term:test1",
        kind: "term_edge",
        label: "PART_OF",
      },
      { id: "term_binding:2", a: "term:test2", b: "c:animals.breed", kind: "term_binding" },
    ],
  };
}

/** A hub term joined to one term by each edge type a glossary has (a comb on one track), and a
 * chain under the first of them (stacked segments). */
function everyType(): GraphData {
  const g = twoTerms("RELATED_TO");
  const nodes: GraphData["nodes"] = [
    { id: "term:hub", type: "term", label: "hub" },
    ...g.nodes.filter((n) => !n.id.startsWith("term:")),
  ];
  const edges: GraphData["edges"] = [
    { id: "term_binding:hub", a: "term:hub", b: "c:animals.breed", kind: "term_binding" },
  ];
  GLOSSARY_REL_TYPES.forEach((type, i) => {
    nodes.push({ id: `term:k${i}`, type: "term", label: `kin ${i}` });
    edges.push({
      id: `term_edge:k${i}`,
      a: `term:k${i}`,
      b: "term:hub",
      kind: "term_edge",
      label: type,
    });
  });
  nodes.push({ id: "term:deep", type: "term", label: "deep" });
  edges.push({
    id: "term_edge:deep",
    a: "term:deep",
    b: "term:k0",
    kind: "term_edge",
    label: "PREFERRED_TERM_FOR",
  });
  return { nodes, edges };
}

/** The box each line beside the term lane draws its name in: centred on its anchor (cytoscape's
 * middle of a two-bend segments edge, the middle of its run down the track) moved by its text
 * margins, as wide as its text and as tall as its font, its background's padding round both. */
function besideLabels(cy: Core) {
  const routes = besideRoutes(cy, "term");
  const out: { id: string; text: string; b: ReturnType<typeof box> }[] = [];
  routes.forEach((path, id) => {
    const e = cy.getElementById(id);
    const text = String(e.data("label") ?? "");
    if (text === "") return;
    const pad = px(e.style("text-background-padding"));
    const size = px(e.style("font-size"));
    const x = (path[1].x + path[2].x) / 2 + px(e.style("text-margin-x"));
    const y = (path[1].y + path[2].y) / 2 + px(e.style("text-margin-y"));
    const w = textWidth(text, size) + 2 * pad;
    const h = size * 1.25 + 2 * pad;
    out.push({ id, text, b: { x1: x - w / 2, x2: x + w / 2, y1: y - h / 2, y2: y + h / 2 } });
  });
  return out;
}

/** A style's length in px. */
const px = (v: unknown) => parseFloat(String(v));

const meets = (p: ReturnType<typeof box>, q: ReturnType<typeof box>) =>
  p.x1 < q.x2 && q.x1 < p.x2 && p.y1 < q.y2 && q.y1 < p.y2;

describe("the name of a line beside the term lane (J1227)", () => {
  const fixtures: Record<string, () => GraphData> = {
    relatedTo: () => twoTerms("RELATED_TO"),
    preferredTermFor: () => twoTerms("PREFERRED_TERM_FOR"),
    partOfChain,
    everyType,
    petHealth: petHealthOntology,
    petStore: petStoreOntology,
  };
  for (const [name, fixture] of Object.entries(fixtures))
    it(`is drawn clear of every node, of every other name, and inside the term lane (${name})`, async () => {
      const { cy } = await laidOut(fixture());
      const labels = besideLabels(cy);
      const nodes = cy.nodes('[kind = "node"], [kind = "more"]').map((n) => ({
        id: n.id(),
        b: box(cy, n.id()),
      }));
      const lane = box(cy, "lane:term");
      const starts = labels.map((l) => {
        const path = besideRoutes(cy, "term").get(l.id) as At[];
        return {
          id: l.id,
          track: path[1].x,
          lo: Math.min(path[1].y, path[2].y),
          hi: Math.max(path[1].y, path[2].y),
        };
      });
      cy.destroy();
      expect(labels.length, "lines beside the lane with names").toBeGreaterThan(0);
      for (const l of labels) {
        for (const n of nodes)
          expect(meets(l.b, n.b), `${l.text} (${l.id}) over ${n.id}`).toBe(false);
        expect(
          l.b.x1 >= lane.x1 && l.b.x2 <= lane.x2 && l.b.y1 >= lane.y1 && l.b.y2 <= lane.y2,
          `${l.text} (${l.id}) inside the term lane's box`,
        ).toBe(true);
        // On the outer side of its own track, beside its run down it.
        const s = starts.find((x) => x.id === l.id)!;
        expect(l.b.x1, `${l.id} right of its track`).toBeGreaterThan(s.track);
        const mid = (l.b.y1 + l.b.y2) / 2;
        expect(mid >= s.lo - 1e-9 && mid <= s.hi + 1e-9, `${l.id} beside its run`).toBe(true);
      }
      for (let i = 0; i < labels.length; i++)
        for (let j = i + 1; j < labels.length; j++)
          expect(meets(labels[i].b, labels[j].b), `${labels[i].id} | ${labels[j].id}`).toBe(false);
    });

  it("keeps the names of lines that do not run beside the lane where they were", async () => {
    const { cy } = await laidOut(petHealthOntology());
    const routes = besideRoutes(cy, "term");
    const others = cy.edges().filter((e) => !routes.has(e.id()));
    const margins = others.map((e) => px(e.style("text-margin-x")));
    cy.destroy();
    expect(margins.length).toBeGreaterThan(0);
    for (const m of margins) expect(m).toBe(0);
  });
});

describe("the lanes preset on the pet-store Ontology graph", () => {
  it("spreads every lane but the term lane over the height of the tallest", async () => {
    const { d, cy } = await laidOut(petStoreOntology());
    const laneOf = (id: string) => cy.getElementById(id).data("lane") as string;
    const placed = cy.nodes('[kind = "node"], [kind = "more"]').map((n) => n.id());
    const spans = LANES.filter((l) => l !== "term")
      .map((l) => {
        const ids = placed.filter((id) => laneOf(id) === l);
        const bs = ids.map((id) => box(cy, id));
        return {
          l,
          n: ids.length,
          y1: Math.min(...bs.map((b) => b.y1)),
          y2: Math.max(...bs.map((b) => b.y2)),
        };
      })
      .filter((s) => s.n > 1);
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
