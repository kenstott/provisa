// Copyright (c) 2026 Kenneth Stott
// Canary: 4635fa55-dcbc-4936-81ac-9b29263c04a9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239 (amended 2026-11-27): both lineage views label their edges with their transforms. A
// product's Lineage tab draws one edge per relation hop in the lanes preset; the edge names each
// distinct transform of the column edges behind it, then any views it reads through.
//
// The real cytoscape, headless (jsdom has no 2D context).

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render } from "../../../../test-utils/render";
import type { LineageEdge, LineageNode, ProductLineage } from "../../../../api/lineage";

const held = vi.hoisted(() => ({ cores: [] as unknown[] }));
vi.mock("cytoscape", async (importOriginal) => {
  type Real = ((...a: unknown[]) => unknown) & { use: (e: unknown) => void };
  const real = (await importOriginal<{ default: Real }>()).default;
  const make = (...args: unknown[]) => {
    if (typeof args[0] === "string") return real(...args);
    const o = args[0] as Record<string, unknown>;
    const cy = real({ ...o, container: undefined, headless: true, styleEnabled: true });
    held.cores.push(cy);
    return cy;
  };
  return { default: Object.assign(make, { use: real.use.bind(real) }) };
});

import { LineageTab } from "../LineageTab";
import { productLineageSpec } from "../productLineageSpec";
import { edgeWords } from "../../../../components/lineage/lineage-layout";
import i18n from "../../../../i18n";

const EN = edgeWords(i18n.getFixedT("en"));

interface Core {
  edges(): {
    map<T>(
      fn: (e: { id(): string; data(k: string): unknown; style(k: string): unknown }) => T,
    ): T[];
  };
}

const node = (id: string, relation: string): LineageNode => ({
  id,
  relation,
  column: id.slice(relation.length + 1),
  kind: "source",
  materialized: false,
});
const fn = (source: string, target: string, name: string): LineageEdge => ({
  source,
  target,
  transform: `${name}(x)`,
  ops: [{ name, kind: "sql_function" }],
});
const copy = (source: string, target: string): LineageEdge => ({
  source,
  target,
  transform: "x",
  ops: [{ name: "x", kind: "identity" }],
});

// raw.orders feeds the member sales.revenue directly (SUM, COUNT, a plain copy); raw.days feeds
// staging.d (CAST), a view folded onto the edge to the member (UPPER).
const LINEAGE = {
  nodes: [
    node("raw.orders.amount", "raw.orders"),
    node("raw.orders.id", "raw.orders"),
    node("raw.orders.region", "raw.orders"),
    node("raw.days.placed", "raw.days"),
    node("staging.d.day", "staging.d"),
    node("sales.revenue.total", "sales.revenue"),
    node("sales.revenue.n", "sales.revenue"),
    node("sales.revenue.region", "sales.revenue"),
    node("sales.revenue.day", "sales.revenue"),
  ],
  edges: [
    fn("raw.orders.amount", "sales.revenue.total", "SUM"),
    fn("raw.orders.id", "sales.revenue.n", "COUNT"),
    copy("raw.orders.region", "sales.revenue.region"),
    fn("raw.days.placed", "staging.d.day", "CAST"),
    fn("staging.d.day", "sales.revenue.day", "UPPER"),
  ],
  outputs: [],
  members: ["sales.revenue"],
  consumers: [],
  inputs: [
    { relation: "raw.orders", kind: "source_table" },
    { relation: "raw.days", kind: "source_table" },
  ],
  dq: { expectations: [], promises: [], related: [], unattached: [] },
} as unknown as ProductLineage;

beforeEach(() => {
  held.cores = [];
  vi.stubGlobal(
    "ResizeObserver",
    class {
      observe() {}
      disconnect() {}
    },
  );
});
afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("a product's lineage edges (lanes preset)", () => {
  it("name each distinct transform of the hop, then the views read through", () => {
    const { spec } = productLineageSpec(LINEAGE, (r) => r, EN);
    const label = (id: string) => spec.edges.find((e) => e.id === id)?.label;
    expect(label("raw.orders->sales.revenue")).toBe("SUM, COUNT");
    expect(label("raw.days->sales.revenue")).toBe("CAST, UPPER via staging.d");
  });

  it("leave a hop of plain copies unlabelled", () => {
    const { spec } = productLineageSpec(
      { ...LINEAGE, edges: [copy("raw.orders.region", "sales.revenue.region")] },
      (r) => r,
      EN,
    );
    expect(spec.edges).toHaveLength(1);
    expect(spec.edges[0].label).toBeUndefined();
  });

  it("are drawn with those labels on the canvas", async () => {
    render(<LineageTab productId="p" loading={false} error="" graph={LINEAGE} />);
    await vi.waitFor(() => expect(held.cores.length).toBeGreaterThan(0));
    const core = held.cores[held.cores.length - 1] as Core;
    await vi.waitFor(() => expect(core.edges().map((e) => e.id()).length).toBe(2));
    const drawn = Object.fromEntries(
      core.edges().map((e) => {
        expect(e.style("label")).toBe(e.data("label"));
        return [e.id(), e.data("label")];
      }),
    );
    expect(drawn).toEqual({
      "raw.orders->sales.revenue": "SUM, COUNT",
      "raw.days->sales.revenue": "CAST, UPPER via staging.d",
    });
  });
});
