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
import { HEADER_H, ROW_H, buildLineageModel, rowPosition } from "../lineage-layout";
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

  it("lanes boxes by distance from the given relations: contributors negative, consumers +1 (REQ-1667)", () => {
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
        { source: "a.x", target: "m.y", transform: "", ops: [] }, // a also feeds m directly: still -2
        { source: "m.x", target: "c.x", transform: "", ops: [] },
      ],
      outputs: ["m.x", "m.y", "lone.z"],
      cycles: [],
    };
    const m = buildLineageModel(chain, undefined, { lanesAround: new Set(["m", "lone"]) });
    const lanes = Object.fromEntries(m.boxes.map((b) => [b.label, b.lane]));
    expect(lanes).toEqual({ a: -2, b: -1, m: 0, c: 1, lone: 0 });
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
