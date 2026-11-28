// Copyright (c) 2026 Kenneth Stott
// Canary: 8b41d6e3-29c7-4f5a-a0de-73c15e9f2b86
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239: the words both lineage views put on an edge beside its transforms -- the count of the
// columns rolled into it (plural per the language) and the views it reads through -- are the
// reader's language. Transform names (SUM, CAST) stay as the SQL writes them.

import { beforeAll, describe, expect, it } from "vitest";
import type { LineageGraphData, LineageEdge, ProductLineage } from "../../../api/lineage";
import i18n from "../../../i18n";
import { productLineageSpec } from "../../../pages/data-products/tabs/productLineageSpec";
import { buildLineageModel, edgeText, edgeWords, type EdgeWords } from "../lineage-layout";

const op = (name: string, kind: "sql_function" | "identity" = "sql_function") => ({ name, kind });
const col = (relation: string, column: string, kind = "derived") => ({
  id: `${relation}.${column}`,
  column,
  relation,
  kind,
  materialized: false,
});

// o.a, o.b roll into one drawn edge to r when both boxes are collapsed: SUM and COUNT, 2 columns.
const ROLLED: LineageGraphData = {
  nodes: [col("o", "a", "source"), col("o", "b", "source"), col("r", "total"), col("r", "n")],
  edges: [
    { source: "o.a", target: "r.total", transform: "SUM(a)", ops: [op("SUM")] },
    { source: "o.b", target: "r.n", transform: "COUNT(b)", ops: [op("COUNT")] },
  ],
  outputs: ["r.total", "r.n"],
  cycles: [],
} as unknown as LineageGraphData;

// a.x reaches the member m.x through the view v1: the view is folded onto the edge.
const THROUGH_VIEW: LineageGraphData = {
  nodes: [col("a", "x", "source"), col("v1", "x"), col("m", "x")],
  edges: [
    { source: "a.x", target: "v1.x", transform: "upper(x)", ops: [op("upper")] },
    { source: "v1.x", target: "m.x", transform: "x", ops: [op("x", "identity")] },
  ],
  outputs: ["m.x"],
  cycles: [],
} as unknown as LineageGraphData;

const fn = (source: string, target: string, name: string): LineageEdge => ({
  source,
  target,
  transform: `${name}(x)`,
  ops: [{ name, kind: "sql_function" }],
});
// raw.days feeds staging.d (CAST), a view folded onto the edge to the member (UPPER).
const PRODUCT = {
  nodes: [
    { id: "raw.days.placed", relation: "raw.days", column: "placed", kind: "source" },
    { id: "staging.d.day", relation: "staging.d", column: "day", kind: "derived" },
    { id: "sales.revenue.day", relation: "sales.revenue", column: "day", kind: "derived" },
  ].map((n) => ({ ...n, materialized: false })),
  edges: [
    fn("raw.days.placed", "staging.d.day", "CAST"),
    fn("staging.d.day", "sales.revenue.day", "UPPER"),
  ],
  outputs: [],
  members: ["sales.revenue"],
  consumers: [],
  inputs: [{ relation: "raw.days", kind: "source_table" }],
  dq: { expectations: [], promises: [], related: [], unattached: [] },
} as unknown as ProductLineage;

const rolledText = (words: EdgeWords) =>
  edgeText(buildLineageModel(ROLLED, new Set(["o", "r"])).edges[0], words);
const viewText = (words: EdgeWords) =>
  edgeText(
    buildLineageModel(THROUGH_VIEW, undefined, {
      lanesAround: { inputs: new Set(["a"]), members: new Set(["m"]) },
    }).edges[0],
    words,
  );
const productText = (words: EdgeWords) =>
  productLineageSpec(PRODUCT, (r) => r, words).spec.edges.find(
    (e) => e.id === "raw.days->sales.revenue",
  )?.label;

describe("lineage edge text in English", () => {
  const en = edgeWords(i18n.getFixedT("en"));

  it("counts one column and two columns", () => {
    expect(en.columns(1)).toBe("1 column");
    expect(en.columns(2)).toBe("2 columns");
  });

  it("names the columns rolled into a SQL lineage edge", () => {
    expect(rolledText(en)).toBe("SUM, COUNT · 2 columns");
  });

  it("names the views a SQL lineage edge reads through", () => {
    expect(viewText(en)).toBe("upper via v1");
  });

  it("names the views a product lineage edge reads through", () => {
    expect(productText(en)).toBe("CAST, UPPER via staging.d");
  });
});

describe("lineage edge text in German", () => {
  let de: EdgeWords;
  beforeAll(async () => {
    await i18n.loadLanguages("de");
    de = edgeWords(i18n.getFixedT("de"));
  });

  it("counts one column and two columns", () => {
    expect(de.columns(1)).toBe("1 Spalte");
    expect(de.columns(2)).toBe("2 Spalten");
  });

  it("names the columns rolled into a SQL lineage edge, the transforms as written", () => {
    expect(rolledText(de)).toBe("SUM, COUNT · 2 Spalten");
  });

  it("names the views a SQL lineage edge reads through", () => {
    expect(viewText(de)).toBe("upper über v1");
  });

  it("names the views a product lineage edge reads through", () => {
    expect(productText(de)).toBe("CAST, UPPER über staging.d");
  });
});
