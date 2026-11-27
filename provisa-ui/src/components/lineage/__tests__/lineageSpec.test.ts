// Copyright (c) 2026 Kenneth Stott
// Canary: 1f6a9c38-4e27-4b5d-8a90-c3e5d7b2f164
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239 (amended 2026-11-27): the column lineage as the one graph component draws it, the way
// the 2D ontology graph is drawn, keeping lineage's own: a relation open is a group of its fields,
// collapsed one node; a field's role is its node type; a line carries its transform; an indirect
// line (through views, or reported by OpenLineage) is dashed.

import { describe, expect, it } from "vitest";
import type { LineageGraphData } from "../../../api/lineage";
import { buildLineageModel } from "../lineage-layout";
import { lineageSpec, lineageTrace, type LineageWords } from "../lineageSpec";

const WORDS: LineageWords = {
  reported: "reported",
  columns: (n) => `${n} columns`,
  unused: (n) => `+${n} unused`,
  materialized: "materialized",
  feedbackCycle: "feedback",
  errorCycle: "error",
};

const GRAPH: LineageGraphData = {
  nodes: [
    { id: "raw.a.x", column: "x", relation: "raw.a", kind: "source", materialized: false },
    { id: "raw.a.y", column: "y", relation: "raw.a", kind: "source", materialized: false },
    { id: "raw.a.z", column: "z", relation: "raw.a", kind: "source", materialized: false },
    { id: "mid.m.x", column: "x", relation: "mid.m", kind: "derived", materialized: true },
    { id: "out.o.total", column: "total", relation: "out.o", kind: "derived", materialized: false },
  ],
  edges: [
    { source: "raw.a.x", target: "mid.m.x", transform: "", ops: [] },
    {
      source: "mid.m.x",
      target: "out.o.total",
      transform: "sum",
      ops: [{ name: "sum", kind: "aggregate" }],
    },
    {
      source: "raw.a.y",
      target: "out.o.total",
      transform: "",
      ops: [],
      origin: "openlineage",
    },
  ],
  outputs: ["out.o.total"],
  cycles: [],
} as unknown as LineageGraphData;

const spec = (collapsed: string[] = []) =>
  lineageSpec(buildLineageModel(GRAPH, new Set(collapsed)), WORDS, (rel, col) =>
    rel === "raw.a" && col === "x" ? "the x" : null,
  );

describe("lineageSpec", () => {
  it("draws an open relation as a group of its fields, each typed by its role", () => {
    const s = spec();
    expect(s.groups?.map((g) => g.id).sort()).toEqual(["rel:mid.m", "rel:out.o", "rel:raw.a"]);
    const node = (id: string) => s.nodes.find((n) => n.id === id);
    expect(node("raw.a.x")).toMatchObject({ type: "source", parent: "rel:raw.a", name: "x" });
    expect(node("mid.m.x")).toMatchObject({ type: "intermediate", parent: "rel:mid.m" });
    expect(node("out.o.total")).toMatchObject({ type: "output", parent: "rel:out.o" });
    // The registered description and the field's state are on its card.
    expect(node("raw.a.x")?.description).toBe("the x");
    expect(node("mid.m.x")?.description).toBe("materialized");
  });

  it("folds the fields no line touches into one node of the group", () => {
    const fold = spec().nodes.find((n) => n.id === "rel:raw.a:unused");
    expect(fold).toMatchObject({ kind: "more", parent: "rel:raw.a", name: "+1 unused" });
  });

  it("names each transform on its line, its arrow at the field made", () => {
    const s = spec();
    const sum = s.edges.find((e) => e.source === "mid.m.x");
    expect(sum).toMatchObject({ target: "out.o.total", label: "sum", arrow: "target" });
    expect(sum?.dashed).toBeUndefined();
  });

  it("dashes a line OpenLineage reported, and says so", () => {
    const reported = spec().edges.find((e) => e.source === "raw.a.y");
    expect(reported).toMatchObject({ dashed: true, label: "reported" });
  });

  it("draws a collapsed relation as one node, its lines rolled up onto it", () => {
    const s = spec(["raw.a"]);
    expect(s.groups?.some((g) => g.id === "rel:raw.a")).toBe(false);
    expect(s.nodes.find((n) => n.id === "rel:raw.a")).toMatchObject({
      type: "table",
      description: "3 columns",
      data: { relation: "raw.a", collapsed: "yes" },
    });
    expect(s.nodes.some((n) => n.parent === "rel:raw.a")).toBe(false);
    expect(s.edges.filter((e) => e.source === "rel:raw.a").map((e) => e.target).sort()).toEqual([
      "mid.m.x",
      "out.o.total",
    ]);
  });
});

describe("lineageTrace", () => {
  it("lights everything upstream and downstream of a field, and nothing beside it", () => {
    const s = spec();
    const trace = lineageTrace(s, "mid.m.x");
    expect([...trace.nodes].sort()).toEqual(["mid.m.x", "out.o.total", "raw.a.x"]);
    expect(trace.links.size).toBe(2);
    expect(trace.nodes.has("raw.a.y")).toBe(false);
  });
});
