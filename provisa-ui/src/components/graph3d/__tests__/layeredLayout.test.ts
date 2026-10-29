// Copyright (c) 2026 Kenneth Stott
// Canary: b41e8a07-93c5-4d2f-8e6a-5c7d1f20a963
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174: the layered layout -- levels below the deepest parent, every parent kept, crossings cut,
// cycle-closing edges ignored, ranks top-down and flows left-to-right, lateral edges placing nothing --
// and the layout a view opens in.

import { describe, expect, it } from "vitest";
import {
  chosenLayout,
  layoutGraph,
  openingLayout,
  type EdgeKind,
  type GraphData,
} from "../graphModel";
import { crossings, layerLevels, orderLevels } from "../layeredLayout";
import { laneDiagram, laneOrder } from "../laneDiagram";
import { petStoreFlows, petStoreOntology, threeLevels } from "./crossingFixtures";

const node = (id: string) => ({ id, type: "term" as const, label: id });
const edge = (a: string, b: string, kind: EdgeKind, label?: string) => ({
  id: `${kind}:${a}>${b}`,
  a,
  b,
  kind,
  label,
});
const graph = (ids: string[], edges: ReturnType<typeof edge>[]): GraphData => ({
  nodes: ids.map(node),
  edges,
});
const at = (g: GraphData, id: string, layout = layoutGraph(g, "layered")) =>
  layout.get(id) as [number, number, number];

describe("layered placement", () => {
  it("puts a node with two parents one level below the deeper, under both", () => {
    // child KIND_OF p1 and PART_OF p2; p2 is itself a kind of root, so p2 is the deeper parent.
    const g = graph(
      ["root", "p1", "p2", "child"],
      [
        edge("p2", "root", "term_edge", "KIND_OF"),
        edge("child", "p1", "term_edge", "KIND_OF"),
        edge("child", "p2", "term_edge", "PART_OF"),
      ],
    );
    const p = layoutGraph(g, "layered");
    expect(at(g, "root", p)[1]).toBeGreaterThan(at(g, "p2", p)[1]);
    expect(at(g, "p2", p)[1]).toBeGreaterThan(at(g, "child", p)[1]);
    expect(at(g, "p1", p)[1]).toBe(at(g, "root", p)[1]);
    const lo = Math.min(at(g, "p1", p)[0], at(g, "p2", p)[0]);
    const hi = Math.max(at(g, "p1", p)[0], at(g, "p2", p)[0]);
    expect(at(g, "child", p)[0]).toBeGreaterThanOrEqual(lo);
    expect(at(g, "child", p)[0]).toBeLessThanOrEqual(hi);
  });

  it("runs ranks top-down: the domain above its table above its column", () => {
    const g = graph(
      ["dom", "tbl", "col"],
      [edge("tbl", "dom", "in_domain"), edge("tbl", "col", "has_column")],
    );
    const p = layoutGraph(g, "layered");
    expect(at(g, "dom", p)[1]).toBeGreaterThan(at(g, "tbl", p)[1]);
    expect(at(g, "tbl", p)[1]).toBeGreaterThan(at(g, "col", p)[1]);
  });

  it("runs flows left-to-right: lineage source before target, DERIVED_FROM source first", () => {
    const g = graph(
      ["src", "mid", "out", "base"],
      [
        edge("src", "mid", "lineage"),
        edge("mid", "out", "lineage"),
        edge("out", "base", "term_edge", "DERIVED_FROM"),
      ],
    );
    const p = layoutGraph(g, "layered");
    // base is what out is derived from: it comes before out.
    expect(at(g, "src", p)[0]).toBeLessThan(at(g, "mid", p)[0]);
    expect(at(g, "mid", p)[0]).toBeLessThan(at(g, "out", p)[0]);
    expect(at(g, "base", p)[0]).toBeLessThan(at(g, "out", p)[0]);
  });

  it("ignores an edge that closes a cycle", () => {
    const g = graph(
      ["a", "b", "c"],
      [
        edge("a", "b", "lineage"),
        edge("b", "c", "lineage"),
        edge("c", "a", "lineage"),
      ],
    );
    const { levels } = layerLevels(g);
    expect(levels).toEqual([["a"], ["b"], ["c"]]);
  });

  it("orders levels to cut crossings against the naive order", () => {
    // Id order puts a1 a2 over b1 b2 with a1-b2 and a2-b1: two edges crossing.
    const g = graph(
      ["a1", "a2", "b1", "b2"],
      [edge("a1", "b2", "lineage"), edge("a2", "b1", "lineage")],
    );
    const naive = [
      ["a1", "a2"],
      ["b1", "b2"],
    ];
    const edges: [string, string][] = [
      ["a1", "b2"],
      ["a2", "b1"],
    ];
    expect(crossings(naive, edges)).toBe(1);
    const { levels, edges: kept } = layerLevels(g);
    expect(crossings(levels, kept)).toBe(0);
  });

  it("lateral edges never move a node", () => {
    const base = graph(["x", "y", "z"], [edge("y", "x", "term_edge", "KIND_OF")]);
    const withLateral: GraphData = {
      nodes: base.nodes,
      edges: [
        ...base.edges,
        edge("z", "x", "term_edge", "RELATED_TO"),
        edge("z", "y", "term_edge", "SYNONYM_OF"),
      ],
    };
    expect([...layoutGraph(withLateral, "layered")]).toEqual([...layoutGraph(base, "layered")]);
  });
});

describe("the layout a view opens in", () => {
  it("is layered when every drawn rank edge is one class and acyclic", () => {
    const g = graph(["a", "b"], [edge("b", "a", "term_edge", "KIND_OF")]);
    expect(openingLayout(g)).toEqual({ layout: "layered", layeredPossible: true });
  });

  it("is layered for flows alone", () => {
    const g = graph(["a", "b"], [edge("a", "b", "lineage")]);
    expect(openingLayout(g).layout).toBe("layered");
  });

  it("is force where ranks and flows are mixed", () => {
    const g = graph(
      ["a", "b", "c"],
      [edge("b", "a", "term_edge", "KIND_OF"), edge("b", "c", "lineage")],
    );
    expect(openingLayout(g)).toEqual({ layout: "force", layeredPossible: true });
  });

  it("is force, Layered impossible, where a cycle closes", () => {
    const g = graph(["a", "b"], [edge("a", "b", "lineage"), edge("b", "a", "lineage")]);
    expect(openingLayout(g)).toEqual({ layout: "force", layeredPossible: false });
    expect(chosenLayout(g, "layered")).toBe("force");
  });

  it("is force where nothing ranks or flows", () => {
    const g = graph(["a", "b"], [edge("a", "b", "term_edge", "RELATED_TO")]);
    expect(openingLayout(g).layout).toBe("force");
  });

  it("yields to the reader's choice where Layered is possible", () => {
    const g = graph(
      ["a", "b", "c"],
      [edge("b", "a", "term_edge", "KIND_OF"), edge("b", "c", "lineage")],
    );
    expect(chosenLayout(g, "layered")).toBe("layered");
    expect(chosenLayout(g, null)).toBe("force");
  });
});

// The crossings the barycenter sweeps this replaced (24 sweeps, adjacent levels only, no transpose)
// left on each fixture, counted the same way (a long edge at every level it passes), measured on
// that code before it changed.
const OLD_FLOWS = 31;
const OLD_THREE = 10;
const OLD_LANES = 95;

function permutations<T>(xs: T[]): T[][] {
  if (xs.length <= 1) return [xs];
  return xs.flatMap((x, i) =>
    permutations([...xs.slice(0, i), ...xs.slice(i + 1)]).map((p) => [x, ...p]),
  );
}

describe("crossing minimisation", () => {
  it("counts a long edge at every level it passes", () => {
    // a0>c0 passes level 1 through a stand-in after b0: a1>b0 and a0>(stand-in) cross once.
    const levels = [["a0", "a1"], ["b0"], ["c0"]];
    expect(crossings(levels, [["a1", "b0"], ["b0", "c0"], ["a0", "c0"]])).toBe(1);
    // Adjacent-level edges alone: as before.
    expect(crossings([["a0", "a1"], ["b0", "b1"]], [["a0", "b1"], ["a1", "b0"]])).toBe(1);
  });

  it("cuts the pet-store graph's crossings (terms > columns > tables > product)", () => {
    const g = petStoreFlows();
    const { levels, edges } = layerLevels(g);
    expect(levels.map((l) => l.length)).toEqual([17, 32, 12, 1]);
    const c = crossings(levels, edges);
    expect(c).toBeLessThanOrEqual(26);
    expect(c).toBeLessThan(OLD_FLOWS);
  });

  it("finds the best order of a three-level graph with a long edge", () => {
    const g = threeLevels();
    const { levels, edges } = layerLevels(g);
    expect(levels.map((l) => l.length)).toEqual([4, 5, 4]);
    // Every order of every level tried: the fewest crossings any order has.
    let best = Infinity;
    for (const a of permutations(levels[0]))
      for (const b of permutations(levels[1]))
        for (const c of permutations(levels[2])) best = Math.min(best, crossings([a, b, c], edges));
    expect(best).toBe(1);
    expect(crossings(levels, edges)).toBe(best);
    expect(crossings(levels, edges)).toBeLessThan(OLD_THREE);
  });

  it("gives the same order for the same graph, whatever order its nodes and edges come in", () => {
    const g = petStoreFlows();
    const shuffled: GraphData = { nodes: [...g.nodes].reverse(), edges: [...g.edges].reverse() };
    expect(layerLevels(shuffled).levels).toEqual(layerLevels(g).levels);
  });

  it("keeps a fixed level's order and every node once", () => {
    const levels = [["a1", "a0"], ["b0", "b1", "b2"]];
    const edges: [string, string][] = [["a0", "b0"], ["a1", "b2"], ["a0", "b1"]];
    const out = orderLevels(levels, edges, new Set([0]));
    expect(out[0]).toEqual(["a1", "a0"]);
    expect([...out[1]].sort()).toEqual(["b0", "b1", "b2"]);
    expect(crossings(out, edges)).toBe(0);
  });
});

describe("the lane diagram's order", () => {
  const data = petStoreOntology();
  const d = laneDiagram(data, data, new Set());
  const terms = d.graph.nodes.filter((n) => d.lane.get(n.id) === "term").map((n) => n.id);
  const order = laneOrder(d, terms);
  const at = new Map<string, number>();
  order.forEach((row, l) => row.forEach((id) => at.set(id, l)));
  const edges = d.links
    .filter((k) => at.get(k.from) !== at.get(k.to))
    .map((k) => [k.from, k.to] as [string, string]);

  it("cuts the crossings of the pet-store Ontology graph, its terms in the order given", () => {
    expect(order.map((l) => l.length)).toEqual([17, 32, 12, 1]);
    expect(order[0]).toEqual(terms);
    const c = crossings(order, edges);
    expect(c).toBeLessThanOrEqual(95);
    expect(c).toBeLessThan(OLD_LANES);
  });

  it("puts a table's columns side by side", () => {
    const tableOf = new Map(d.graph.nodes.filter((n) => n.parent).map((n) => [n.id, n.parent]));
    const runs = order[1].map((id) => tableOf.get(id));
    // Each table's columns are one run: a table never reappears after another's.
    const seen = new Set<string | undefined>();
    runs.forEach((t, i) => {
      if (i > 0 && runs[i - 1] !== t) expect(seen.has(t)).toBe(false);
      seen.add(t);
    });
  });
});
