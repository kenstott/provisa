// Copyright (c) 2026 Kenneth Stott
// Canary: 6f1a8d43-2c95-4e07-a3b8-9d7e5c2f1a60
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1665: the box/row model the lineage DAG is drawn from. A relation is one box whose rows are
// its touched columns; the final projection is the result box; a collapsed relation folds its
// edges onto the box; untouched columns are counted, not drawn.

import { describe, expect, it } from "vitest";
import * as layout from "../lineage-layout";
import {
  HEADER_H,
  ROW_H,
  buildLineageModel,
  edgeText,
  lanesFrom,
  rowPosition,
  viewsBetween,
} from "../lineage-layout";
import type { LineageGraphData } from "../../../api/lineage";

const op = (name: string, kind: "sql_function" | "identity" = "sql_function") => ({ name, kind });

const GRAPH: LineageGraphData = {
  nodes: [
    {
      id: "orders.amount",
      column: "amount",
      relation: "orders",
      kind: "source",
      materialized: false,
    },
    { id: "orders.qty", column: "qty", relation: "orders", kind: "source", materialized: false },
    { id: "orders.note", column: "note", relation: "orders", kind: "source", materialized: false },
    { id: "cte.total", column: "total", relation: "cte", kind: "derived", materialized: false },
    { id: "total_out", column: "total_out", relation: null, kind: "derived", materialized: false },
  ],
  edges: [
    { source: "orders.amount", target: "cte.total", transform: "amount * qty", ops: [op("*")] },
    { source: "orders.qty", target: "cte.total", transform: "amount * qty", ops: [op("*")] },
    {
      source: "cte.total",
      target: "total_out",
      transform: "total",
      ops: [op("total", "identity")],
    },
  ],
  outputs: ["total_out"],
  cycles: [],
};

describe("buildLineageModel", () => {
  it("boxes columns by relation, lands the final projection in the result box, counts untouched columns", () => {
    const m = buildLineageModel(GRAPH, undefined);
    const byId = Object.fromEntries(m.boxes.map((b) => [b.id, b]));
    expect(Object.keys(byId).sort()).toEqual(["rel:cte", "rel:orders", "rel:result"]);
    expect(byId["rel:orders"].rows.map((r) => r.label)).toEqual(["amount", "qty"]);
    expect(byId["rel:orders"].unused).toBe(1); // note: no edge touches it
    expect(byId["rel:result"].rows.map((r) => r.label)).toEqual(["total_out"]);
    expect(byId["rel:result"].rows[0].output).toBe(true);
    // Rows sit under the header, one row-height apart, inside the box's own height.
    const box = byId["rel:orders"];
    expect(box.height).toBeGreaterThan(HEADER_H + 2 * ROW_H);
    const p0 = rowPosition(box, { x: 100, y: 100 }, 0);
    const p1 = rowPosition(box, { x: 100, y: 100 }, 1);
    expect(p1.y - p0.y).toBe(ROW_H);
    expect(p0.y).toBeGreaterThan(100 - box.height / 2 + HEADER_H);
  });

  it("draws an output column as a row even when no edge touches it", () => {
    const m = buildLineageModel({ ...GRAPH, outputs: ["orders.note"] }, undefined);
    const orders = m.boxes.find((b) => b.id === "rel:orders")!;
    expect(orders.rows.map((r) => r.label)).toEqual(["amount", "qty", "note"]);
    expect(orders.unused).toBe(0);
    expect(orders.rows.find((r) => r.label === "note")!.output).toBe(true);
  });

  it("draws every column as a row for Complete Lineage, where all a role can see is in use", () => {
    const m = buildLineageModel(GRAPH, undefined, { allColumns: true });
    const orders = m.boxes.find((b) => b.id === "rel:orders")!;
    expect(orders.rows.map((r) => r.label)).toEqual(["amount", "qty", "note"]);
    expect(orders.unused).toBe(0);
  });

  it("names the result box after the view when one is given", () => {
    const m = buildLineageModel(GRAPH, undefined, { resultLabel: "sales_totals" });
    expect(m.boxes.some((b) => b.id === "rel:sales_totals" && b.label === "sales_totals")).toBe(
      true,
    );
  });

  it("draws row-to-row edges labelled by their transform", () => {
    const m = buildLineageModel(GRAPH, undefined);
    expect(m.edges.map((e) => [e.source, e.target, e.label])).toEqual([
      ["orders.amount", "cte.total", "*"],
      ["orders.qty", "cte.total", "*"],
      ["cte.total", "total_out", ""], // identity: no formula to name
    ]);
  });

  it("orders a downstream box's rows by the rows that feed them, so copies draw as parallel wires", () => {
    const copy: LineageGraphData = {
      nodes: [
        { id: "s.a", column: "a", relation: "s", kind: "source", materialized: false },
        { id: "s.b", column: "b", relation: "s", kind: "source", materialized: false },
        { id: "s.c", column: "c", relation: "s", kind: "source", materialized: false },
        // listed in the reverse order of their sources
        { id: "d.c", column: "c", relation: "d", kind: "derived", materialized: false },
        { id: "d.b", column: "b", relation: "d", kind: "derived", materialized: false },
        { id: "d.a", column: "a", relation: "d", kind: "derived", materialized: false },
      ],
      edges: ["a", "b", "c"].map((c) => ({
        source: `s.${c}`,
        target: `d.${c}`,
        transform: c,
        ops: [op(c, "identity")],
      })),
      outputs: [],
      cycles: [],
    };
    const m = buildLineageModel(copy, undefined);
    expect(m.boxes.find((b) => b.id === "rel:d")!.rows.map((r) => r.label)).toEqual([
      "a",
      "b",
      "c",
    ]);
  });

  it("lanes boxes around the given relations and their inputs: consumers +1 (REQ-1667, REQ-2174)", () => {
    const chain: LineageGraphData = {
      nodes: [
        { id: "a.x", column: "x", relation: "a", kind: "source", materialized: false },
        { id: "b.x", column: "x", relation: "b", kind: "derived", materialized: false },
        { id: "m.x", column: "x", relation: "m", kind: "derived", materialized: false },
        { id: "m.y", column: "y", relation: "m", kind: "source", materialized: false },
        { id: "c.x", column: "x", relation: "c", kind: "derived", materialized: false },
        { id: "lone.z", column: "z", relation: "lone", kind: "source", materialized: false },
      ],
      edges: [
        { source: "a.x", target: "b.x", transform: "", ops: [] },
        { source: "b.x", target: "m.x", transform: "", ops: [] },
        { source: "a.x", target: "m.y", transform: "", ops: [] }, // a also feeds m through b
        { source: "m.x", target: "c.x", transform: "", ops: [] },
      ],
      outputs: ["m.x", "m.y", "lone.z"],
      cycles: [],
    };
    const m = buildLineageModel(chain, undefined, {
      lanesAround: { members: new Set(["m", "lone"]), inputs: new Set(["a"]) },
    });
    const lanes = Object.fromEntries(m.boxes.map((b) => [b.label, b.lane]));
    // a is m's input; b, a view m reads it through, is folded into the a -> m edge (no box).
    expect(lanes).toEqual({ a: -1, m: 0, c: 1, lone: 0 });
  });

  // REQ-2174 (amended 2026-11-08): a view between an input and a member is on the edge.
  describe("a view between an input and a member", () => {
    const col = (relation: string, kind: "source" | "derived" = "derived") => ({
      id: `${relation}.x`,
      column: "x",
      relation,
      kind,
      materialized: false,
    });
    const edge = (source: string, target: string, name: string) => ({
      source: `${source}.x`,
      target: `${target}.x`,
      transform: name,
      ops: [op(name)],
    });
    const around = { members: new Set(["m"]), inputs: new Set(["a"]) };
    const one: LineageGraphData = {
      nodes: [col("a", "source"), col("v1"), col("m")],
      edges: [edge("a", "v1", "upper"), edge("v1", "m", "trim")],
      outputs: ["m.x"],
      cycles: [],
    };
    const two: LineageGraphData = {
      nodes: [col("a", "source"), col("v1"), col("v2"), col("m")],
      edges: [edge("a", "v1", "upper"), edge("v1", "v2", "trim"), edge("v2", "m", "lower")],
      outputs: ["m.x"],
      cycles: [],
    };

    it("is drawn as no box and no lane", () => {
      const m = buildLineageModel(one, undefined, { lanesAround: around });
      expect(m.boxes.map((b) => b.relation).sort()).toEqual(["a", "m"]);
      expect(m.boxes.flatMap((b) => b.rows.map((r) => r.id)).sort()).toEqual(["a.x", "m.x"]);
    });

    it("leaves the input-to-member column edge, naming the view and keeping the transforms", () => {
      const m = buildLineageModel(one, undefined, { lanesAround: around });
      expect(m.edges).toHaveLength(1);
      expect(m.edges[0]).toMatchObject({ source: "a.x", target: "m.x", via: ["v1"] });
      expect(m.edges[0].label).toBe("upper trim");
      expect(edgeText(m.edges[0])).toBe("upper trim via v1");
    });

    it("names both views of a chain, in the order the column passes through them", () => {
      const m = buildLineageModel(two, undefined, { lanesAround: around });
      expect(m.boxes.map((b) => b.relation).sort()).toEqual(["a", "m"]);
      expect(m.edges).toHaveLength(1);
      expect(m.edges[0]).toMatchObject({ source: "a.x", target: "m.x", via: ["v1", "v2"] });
      expect(edgeText(m.edges[0])).toBe("upper trim lower via v1, v2");
    });

    it("names a view as the page names its relation", () => {
      const m = buildLineageModel(one, undefined, {
        lanesAround: around,
        relationLabel: (r) => `sales.${r}`,
      });
      expect(edgeText(m.edges[0])).toBe("upper trim via sales.v1");
    });

    it("leaves a direct input-to-member edge as it is, with no via", () => {
      const direct: LineageGraphData = {
        nodes: [col("a", "source"), col("m")],
        edges: [edge("a", "m", "upper")],
        outputs: ["m.x"],
        cycles: [],
      };
      const m = buildLineageModel(direct, undefined, { lanesAround: around });
      expect(m.edges).toHaveLength(1);
      expect(m.edges[0].via).toEqual([]);
      expect(edgeText(m.edges[0])).toBe("upper");
    });

    it("finds the views between, not the inputs, members or what lies beyond an input", () => {
      expect(
        viewsBetween(
          [
            ["landing", "a"],
            ["a", "v1"],
            ["v1", "m"],
          ],
          ["m"],
          ["a"],
        ),
      ).toEqual(new Set(["v1"]));
    });

    it("has no BETWEEN_LANE", () => {
      expect("BETWEEN_LANE" in layout).toBe(false);
    });
  });

  // REQ-2174 (amended 2026-11-07): a product's inputs are its Inputs lane, wherever the graph
  // puts them; only what lies beyond an input is "Upstream, hop n".
  describe("lanesFrom", () => {
    it("pins an input read through a view to the Inputs lane, the view with no lane", () => {
      // input -> view -> member: by path length the view would be -1 and the input -2.
      const lane = lanesFrom(
        [
          ["input", "view"],
          ["view", "member"],
        ],
        ["member"],
        ["input"],
      );
      expect(Object.fromEntries(lane)).toEqual({ member: 0, input: -1 });
    });

    it("pins every input to -1, near or far", () => {
      const lane = lanesFrom(
        [
          ["near", "member"],
          ["far", "v1"],
          ["v1", "v2"],
          ["v2", "member"],
        ],
        ["member"],
        ["near", "far"],
      );
      expect(lane.get("near")).toBe(-1);
      expect(lane.get("far")).toBe(-1);
      expect(lane.has("v1")).toBe(false);
      expect(lane.has("v2")).toBe(false);
    });

    it("counts the hops beyond the inputs by the longest path down to one", () => {
      // landing -> raw (input) -> member; origin feeds landing, and raw directly too.
      const lane = lanesFrom(
        [
          ["raw", "member"],
          ["landing", "raw"],
          ["origin", "landing"],
          ["origin", "raw"],
        ],
        ["member"],
        ["raw"],
      );
      expect(lane.get("raw")).toBe(-1);
      expect(lane.get("landing")).toBe(-2); // Upstream, hop 1
      expect(lane.get("origin")).toBe(-3); // hop 2: its longer path runs through landing
    });

    it("lanes what is beyond an input read through a view by its hops to the input", () => {
      const lane = lanesFrom(
        [
          ["landing", "raw"],
          ["raw", "view"],
          ["view", "member"],
        ],
        ["member"],
        ["raw"],
      );
      expect(Object.fromEntries(lane)).toEqual({
        member: 0,
        raw: -1,
        landing: -2,
      });
    });

    it("leaves what reaches lane 0 from no input, and the unconnected, with no lane", () => {
      const lane = lanesFrom(
        [
          ["orphanView", "member"],
          ["member", "reader"],
          ["x", "y"],
        ],
        ["member"],
        [],
      );
      expect(Object.fromEntries(lane)).toEqual({
        member: 0,
        reader: 1,
      });
    });
  });

  it("folds a collapsed relation's edges onto its box and reports the count", () => {
    const m = buildLineageModel(GRAPH, new Set(["orders"]));
    const orders = m.boxes.find((b) => b.id === "rel:orders")!;
    expect(orders.collapsed).toBe(true);
    expect(orders.rows).toHaveLength(2); // kept for the header count, not drawn
    const folded = m.edges.find((e) => e.source === "rel:orders");
    expect(folded).toMatchObject({ target: "cte.total", count: 2, label: "" });
  });
});
