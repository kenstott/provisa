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
import { crossings, layerLevels } from "../layeredLayout";

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
