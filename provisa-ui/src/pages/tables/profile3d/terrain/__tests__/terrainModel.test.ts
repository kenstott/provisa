// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2123: the terrain model turns one run's result rows into ridges.

import { describe, expect, it } from "vitest";
import {
  buildTerrain,
  countPeaks,
  heightShare,
  orderColumns,
  sketchCdf,
  SKETCH_BINS,
  tOfValue,
} from "../terrainModel";

const sketchRows = (column: string, sketch: number[]) =>
  sketch.map((value, i) => ({ column_name: column, measure: "value", q: i / 100, value }));

const linear = (lo: number, hi: number) =>
  Array.from({ length: 101 }, (_, i) => lo + ((hi - lo) * i) / 100);

const columnRow = (name: string, family: string, extra: Record<string, unknown> = {}) => ({
  column_name: name,
  family,
  data_type: family === "text" ? "varchar" : "double",
  row_count: 1000,
  null_count: 0,
  null_share: 0,
  distinct_count: 500,
  ...extra,
});

describe("sketchCdf", () => {
  it("interpolates between sketch points and clamps at the ends", () => {
    const s = linear(0, 100);
    expect(sketchCdf(s, -1)).toBe(0);
    expect(sketchCdf(s, 100)).toBe(1);
    expect(sketchCdf(s, 25)).toBeCloseTo(0.25, 6);
    expect(sketchCdf(s, 50.5)).toBeCloseTo(0.505, 6);
  });
});

describe("buildTerrain", () => {
  it("re-cuts a numeric column's sketch into bins that hold every non-null row", () => {
    const [c] = buildTerrain({
      columns: [
        columnRow("amount", "numeric", { null_count: 100, null_share: 0.1, skewness: 0.1 }),
      ],
      quantiles: sketchRows("amount", linear(0, 100)),
    });
    expect(c.binning).toBe("sketch");
    expect(c.ordered).toBe(true);
    expect(c.smooth).toBe(true);
    expect(c.bins).toHaveLength(SKETCH_BINS);
    expect(c.bins.reduce((s, b) => s + b.count, 0)).toBeCloseTo(900, 6);
    expect(c.quantiles).toEqual({ p5: 5, p50: 50, p95: 95 });
    expect(c.beyondLow + c.beyondHigh).toBe(0);
    expect(tOfValue(c, 50)).toBeCloseTo(0.5, 6);
    expect(c.flags.map((f) => f.key)).toEqual(["flagNulls"]);
  });

  it("leaves values past 3×IQR off the ridge and counts them", () => {
    const sketch = linear(10, 20);
    sketch[100] = 10_000;
    const [c] = buildTerrain({
      columns: [columnRow("latency", "numeric")],
      quantiles: sketchRows("latency", sketch),
    });
    expect(c.hi).toBeLessThan(10_000);
    expect(c.beyondHigh).toBeGreaterThan(0);
    expect(c.bins.reduce((s, b) => s + b.count, 0) + c.beyondHigh).toBeCloseTo(1000, 6);
    expect(c.flags.find((f) => f.key === "flagBeyond")?.kind).toBe("warn");
  });

  it("gives a small-range integer column one step per value", () => {
    const sketch = Array.from({ length: 101 }, (_, i) => Math.floor(i / 20.2) + 1);
    const [c] = buildTerrain({
      columns: [columnRow("rating", "numeric", { integer_only: true })],
      quantiles: sketchRows("rating", sketch),
    });
    expect(c.integerBins).toBe(true);
    expect(c.smooth).toBe(false);
    expect(c.bins.map((b) => b.label)).toEqual(["1", "2", "3", "4", "5"]);
  });

  it("uses exact frequencies in value order for a low-cardinality numeric column", () => {
    const [c] = buildTerrain({
      columns: [columnRow("qty", "numeric", { null_count: 10, null_share: 0.01 })],
      quantiles: sketchRows("qty", linear(1, 3)),
      top_values: [
        { column_name: "qty", kind: "frequency", rank: 1, value: "2", row_count: 600 },
        { column_name: "qty", kind: "frequency", rank: 2, value: "10", row_count: 300 },
        { column_name: "qty", kind: "frequency", rank: 3, value: "1", row_count: 90 },
        { column_name: "qty", kind: "frequency", rank: 4, value: null, row_count: 10 },
      ],
    });
    expect(c.binning).toBe("frequency");
    expect(c.bins.map((b) => [b.label, b.count])).toEqual([
      ["1", 90],
      ["2", 600],
      ["10", 300],
    ]);
  });

  it("shows a text column's top values most common first, the rest as one bar", () => {
    const [c] = buildTerrain({
      columns: [columnRow("country", "text", { distinct_count: 40 })],
      top_values: [
        { column_name: "country", kind: "top", rank: 2, value: "GB", row_count: 200 },
        { column_name: "country", kind: "top", rank: 1, value: "US", row_count: 500 },
      ],
    });
    expect(c.binning).toBe("top");
    expect(c.ordered).toBe(false);
    expect(c.bins.map((b) => [b.label, b.count])).toEqual([
      ["US", 500],
      ["GB", 200],
      ["…", 300],
    ]);
    expect(c.flags.map((f) => f.key)).toContain("flagDistinct");
  });

  it("flags a dominant category", () => {
    const [c] = buildTerrain({
      columns: [columnRow("status", "text", { distinct_count: 2 })],
      top_values: [
        { column_name: "status", kind: "frequency", rank: 1, value: "OK", row_count: 950 },
        { column_name: "status", kind: "frequency", rank: 2, value: "ERR", row_count: 50 },
      ],
    });
    expect(c.flags).toContainEqual({
      kind: "warn",
      key: "flagDominant",
      params: { value: "OK", pct: "95%" },
    });
  });

  it("draws no ridge for a column whose values the role may not see", () => {
    const [c] = buildTerrain({
      columns: [
        {
          column_name: "ssn",
          family: "text",
          data_type: "varchar",
          null_share: 0.3,
          row_count: null,
          null_count: null,
          distinct_count: null,
        },
      ],
    });
    expect(c.restricted).toBe(true);
    expect(c.bins).toEqual([]);
    expect(c.rows).toBeNull();
    expect(c.flags.map((f) => f.key)).toEqual(["flagNulls", "flagRestricted"]);
  });

  it("marks a column the run found drifting", () => {
    const [c] = buildTerrain({
      columns: [columnRow("amount", "numeric")],
      quantiles: sketchRows("amount", linear(0, 1)),
      drift: [
        {
          scope: "column",
          column_name: "amount",
          measure: "mean",
          drifting: true,
          drift_reason: "psi",
        },
        {
          scope: "table",
          column_name: null,
          measure: "row_count",
          drifting: true,
          drift_reason: "distance",
        },
      ],
    });
    expect(c.driftReason).toBe("psi");
    expect(c.flags[0]).toEqual({ kind: "warn", key: "flagDrifting", params: { reason: "psi" } });
  });
});

describe("countPeaks", () => {
  it("counts separated peaks and ignores noise", () => {
    expect(countPeaks([1, 5, 9, 5, 1, 1, 1, 5, 9, 5, 1], false)).toBe(2);
    expect(countPeaks([1, 3, 5, 7, 9, 7, 5, 3, 1], false)).toBe(1);
  });
});

describe("heightShare / orderColumns", () => {
  const cols = buildTerrain({
    columns: [
      columnRow("a", "text", { null_share: 0.1, distinct_count: 3 }),
      columnRow("b", "text", { null_share: 0.5, distinct_count: 9 }),
    ],
    top_values: [
      { column_name: "a", kind: "frequency", rank: 1, value: "x", row_count: 400 },
      { column_name: "b", kind: "frequency", rank: 1, value: "y", row_count: 100 },
    ],
  });

  it("scales a bin to its column's peak or to the largest share of rows", () => {
    expect(heightShare(cols[1], 100, "peak", false, 0.4)).toBe(1);
    expect(heightShare(cols[1], 100, "share", false, 0.4)).toBeCloseTo(0.25, 6);
  });

  it("orders columns by the chosen measure, schema order breaking ties", () => {
    expect(orderColumns(cols, "schema")).toEqual(["a", "b"]);
    expect(orderColumns(cols, "nulls")).toEqual(["b", "a"]);
    expect(orderColumns(cols, "distinct")).toEqual(["b", "a"]);
  });
});
