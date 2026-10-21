// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2124: the relationships map, joint cloud and drift strata models.

import { describe, expect, it } from "vitest";
import { buildRelations, edgesOf, visibleEdges } from "../relations/relationsModel";
import { buildJoint, jointTargets, MAX_CATEGORIES } from "../joint/jointModel";
import { buildStrata, strataColumns, STRATA_BINS, type StrataRunInput } from "../drift/driftModel";

const col = (name: string, family: string, extra: Record<string, unknown> = {}) => ({
  column_name: name,
  family,
  row_count: 1000,
  null_count: 0,
  null_share: 0,
  ...extra,
});

describe("buildRelations", () => {
  const results = {
    columns: [col("a", "numeric"), col("b", "numeric"), col("c", "text"), col("lonely", "text")],
    correlations: [
      { column_name: "a", other_column: "b", measure: "spearman", value: -0.8, rows: 990 },
      { column_name: "b", other_column: "a", measure: "spearman", value: -0.8, rows: 990 },
      { column_name: "c", other_column: "a", measure: "correlation_ratio", value: 0.2, rows: 990 },
      { column_name: "c", other_column: "b", measure: "spearman", value: null, rows: 0 },
    ],
    dependencies: [
      {
        column_name: "c",
        parent_1: "a",
        parent_2: "customer.region",
        uncertainty: 0.6,
        mutual_information: 0.4,
        in_network: true,
      },
      {
        column_name: "c",
        parent_1: "b",
        parent_2: null,
        uncertainty: 0.3,
        mutual_information: 0.2,
        in_network: false,
      },
    ],
  };

  it("joins columns by correlations and the dependency network, once per pair", () => {
    const m = buildRelations(results);
    expect(m.edges.map((e) => e.id).sort()).toEqual([
      "correlation_ratio:a|c",
      "dependence:a>c",
      "dependence:customer.region>c",
      "spearman:a|b",
    ]);
    const neg = m.edges.find((e) => e.kind === "spearman");
    expect(neg).toMatchObject({ value: -0.8, strength: 0.8 });
  });

  it("adds a parent table's column as a foreign node and places every node", () => {
    const m = buildRelations(results);
    expect(m.nodes.find((n) => n.id === "customer.region")?.family).toBe("foreign");
    expect(m.nodes.map((n) => n.id)).toContain("lonely");
    for (const n of m.nodes) expect(Math.hypot(n.x, n.y, n.z)).toBeLessThanOrEqual(6.0001);
    expect(buildRelations(results)).toEqual(m);
  });

  it("filters edges by strength and kind, and lists a node's strongest first", () => {
    const m = buildRelations(results);
    expect(
      visibleEdges(m, 0.5, new Set(["spearman", "correlation_ratio", "dependence"])).map(
        (e) => e.id,
      ),
    ).toEqual(["spearman:a|b", "dependence:a>c", "dependence:customer.region>c"]);
    expect(visibleEdges(m, 0, new Set(["dependence"]))).toHaveLength(2);
    expect(edgesOf(m.edges, "a").map((e) => e.id)).toEqual([
      "spearman:a|b",
      "dependence:a>c",
      "correlation_ratio:a|c",
    ]);
  });
});

describe("buildJoint", () => {
  const joint = (t: number | null, tv: string | null, p1: string, p2: number, n: number) => ({
    column_name: "amount",
    parent_1: "plan",
    parent_2: "age",
    target_bucket: t,
    target_value: tv,
    parent_1_value: p1,
    parent_1_bucket: null,
    parent_2_value: null,
    parent_2_bucket: p2,
    row_count: n,
  });

  it("puts deciles and categories on axes and scores each cell's lift", () => {
    const m = buildJoint(
      {
        joint_counts: [
          joint(0, null, "free", 0, 300),
          joint(9, null, "pro", 9, 300),
          joint(0, null, "pro", 0, 100),
          joint(null, null, "free", 9, 100),
        ],
        dependencies: [
          { column_name: "amount", in_network: true, mutual_information: 0.5, uncertainty: 0.25 },
        ],
      },
      "amount",
    );
    if (!m) throw new Error("no model");
    expect(m.axes[1].kind).toBe("decile");
    expect(m.axes[1].slots).toHaveLength(11);
    expect(m.axes[1].slots[10].value).toBeNull();
    expect(m.axes[0]).toMatchObject({ column: "plan", kind: "category" });
    expect(m.axes[0].slots.map((s) => s.value)).toEqual(["free", "pro"]);
    expect(m.total).toBe(800);
    const top = m.cells.find((c) => c.y === 9 && c.x === 1 && c.z === 9);
    // 300 rows where independence gives 800 · (300/800) · (400/800) · (400/800)
    expect(top?.lift).toBeCloseTo(300 / 75, 6);
    expect(m.uncertainty).toBe(0.25);
  });

  it("folds categories past the cap into one slot", () => {
    const rows = Array.from({ length: MAX_CATEGORIES + 3 }, (_, i) => ({
      column_name: "city",
      parent_1: "country",
      parent_2: null,
      target_value: `c${i}`,
      target_bucket: null,
      parent_1_value: "x",
      parent_1_bucket: null,
      row_count: 100 - i,
    }));
    const m = buildJoint({ joint_counts: rows }, "city");
    expect(m?.axes[1].slots).toHaveLength(MAX_CATEGORIES);
    expect(m?.axes[1].slots[MAX_CATEGORIES - 1]).toEqual({ value: "4", other: true });
    expect(m?.axes[2].kind).toBe("single");
    expect(m?.cells.reduce((s, c) => s + c.count, 0)).toBe(
      rows.reduce((s, r) => s + r.row_count, 0),
    );
  });

  it("lists the columns that have joint counts", () => {
    expect(
      jointTargets({
        joint_counts: [
          joint(0, null, "a", 0, 1),
          { ...joint(0, null, "a", 0, 1), column_name: "x" },
        ],
      }),
    ).toEqual(["amount", "x"]);
    expect(buildJoint({ joint_counts: [] }, "amount")).toBeNull();
  });
});

describe("buildStrata", () => {
  const sketch = (column: string, lo: number, hi: number) =>
    Array.from({ length: 101 }, (_, i) => ({
      column_name: column,
      measure: "value",
      q: i / 100,
      value: lo + ((hi - lo) * i) / 100,
    }));

  const runs: StrataRunInput[] = [
    {
      runId: "r1",
      runTime: "2026-10-01T00:00:00Z",
      results: {
        columns: [col("v", "numeric", { null_share: 0.1, null_count: 100 })],
        quantiles: sketch("v", 0, 10),
      },
    },
    {
      runId: "r2",
      runTime: "2026-10-02T00:00:00Z",
      results: {
        columns: [col("v", "numeric"), col("s", "text")],
        quantiles: sketch("v", 5, 15),
        top_values: [
          { column_name: "s", kind: "frequency", value: "a", row_count: 600 },
          { column_name: "s", kind: "frequency", value: "b", row_count: 400 },
        ],
        drift: [{ column_name: "v", drifting: true, drift_reason: "psi" }],
      },
    },
  ];

  it("cuts every run's sketch into the same bins", () => {
    const m = buildStrata(runs, "v");
    expect(m.kind).toBe("sketch");
    expect(m.labels).toHaveLength(STRATA_BINS);
    for (const r of m.runs) expect(r.shares.reduce((a, b) => a + b, 0)).toBeCloseTo(1, 6);
    expect(m.runs[0].medianT).toBeCloseTo(1 / 3, 6);
    expect(m.runs[1].medianT).toBeCloseTo(2 / 3, 6);
    expect(m.runs[0].nullShare).toBe(0.1);
    expect(m.runs.map((r) => r.driftReason)).toEqual([null, "psi"]);
  });

  it("counts categories and leaves a run without the column empty", () => {
    const m = buildStrata(runs, "s");
    expect(m.kind).toBe("categories");
    expect(m.labels).toEqual(["a", "b"]);
    expect(m.runs[0].shares).toEqual([]);
    expect(m.runs[1].shares).toEqual([0.6, 0.4]);
  });

  it("lists the columns any run can draw, newest run's order first", () => {
    expect(strataColumns(runs)).toEqual(["v", "s"]);
  });
});
