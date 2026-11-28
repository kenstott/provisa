// Copyright (c) 2026 Kenneth Stott
// Canary: 404d7282-1865-46ef-8e7e-a1aa57b391a2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239 (amended 2026-11-27): both lineage views label their edges with their transforms. The
// Lineage page draws in the flow preset; each drawn line carries its transform as its label, a line
// rolled up from a collapsed relation names each distinct transform behind it.
//
// The real cytoscape, headless (jsdom has no 2D context), with elkjs's answer held until the test
// gives it.

import { act } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render } from "../../../test-utils/render";
import type { LineageGraphData } from "../../../api/lineage";

const elk = vi.hoisted(() => ({
  asked: [] as { graph: unknown; resolve: (g: unknown) => void }[],
  cores: [] as unknown[],
}));

vi.mock("elkjs/lib/elk.bundled.js", () => ({
  default: class {
    layout(graph: unknown) {
      return new Promise((resolve) => elk.asked.push({ graph, resolve }));
    }
  },
}));

vi.mock("cytoscape", async (importOriginal) => {
  type Real = ((...a: unknown[]) => unknown) & { use: (e: unknown) => void };
  const real = (await importOriginal<{ default: Real }>()).default;
  const make = (...args: unknown[]) => {
    if (typeof args[0] === "string") return real(...args);
    const o = args[0] as Record<string, unknown>;
    const cy = real({ ...o, container: undefined, headless: true, styleEnabled: true });
    elk.cores.push(cy);
    return cy;
  };
  return { default: Object.assign(make, { use: real.use.bind(real) }) };
});

import { LineageDag } from "../LineageDag";

interface ElkGraph {
  id: string;
  x?: number;
  y?: number;
  children?: ElkGraph[];
}
const placed = (graph: ElkGraph): ElkGraph => ({
  ...graph,
  x: graph.x ?? 0,
  y: graph.y ?? 0,
  children: graph.children?.map((c, i) => ({ ...placed(c), x: i * 200, y: i * 200 })),
});

interface Core {
  edges(): { map<T>(fn: (e: { data(k: string): unknown; style(k: string): unknown }) => T): T[] };
}

const col = (id: string, relation: string, kind: "source" | "derived") => ({
  id,
  column: id.slice(relation.length + 1),
  relation,
  kind,
  materialized: false,
});

const GRAPH: LineageGraphData = {
  nodes: [
    col("orders.amount", "orders", "source"),
    col("orders.id", "orders", "source"),
    col("orders.placed", "orders", "source"),
    col("revenue.total", "revenue", "derived"),
    col("revenue.orders", "revenue", "derived"),
    col("revenue.day", "revenue", "derived"),
  ],
  edges: [
    {
      source: "orders.amount",
      target: "revenue.total",
      transform: "SUM(amount)",
      ops: [{ name: "SUM", kind: "sql_function" }],
    },
    {
      source: "orders.id",
      target: "revenue.orders",
      transform: "COUNT(id)",
      ops: [{ name: "COUNT", kind: "sql_function" }],
    },
    {
      source: "orders.placed",
      target: "revenue.day",
      transform: "CAST(placed AS DATE)",
      ops: [{ name: "CAST", kind: "sql_function" }],
    },
  ],
  outputs: ["revenue.total", "revenue.orders", "revenue.day"],
  cycles: [],
};

/** The labels the drawn lines carry, as the stylesheet renders them. */
async function drawnLabels(collapsed?: ReadonlySet<string>): Promise<string[]> {
  render(<LineageDag graph={GRAPH} collapsedRelations={collapsed} />);
  await act(async () => {
    await vi.waitFor(() => expect(elk.asked).toHaveLength(1));
  });
  await act(async () => elk.asked[0].resolve(placed(elk.asked[0].graph as ElkGraph)));
  const core = elk.cores[elk.cores.length - 1] as Core;
  return core
    .edges()
    .map((e) => {
      // The stylesheet's label is the edge's label: what a reader sees on the line.
      expect(e.style("label")).toBe(e.data("label"));
      return String(e.data("label"));
    })
    .sort();
}

beforeEach(() => {
  elk.asked = [];
  elk.cores = [];
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

describe("the Lineage page's edges (flow preset)", () => {
  it("label each line with its transform", async () => {
    expect(await drawnLabels()).toEqual(["CAST", "COUNT", "SUM"]);
  });

  it("label a line rolled up from collapsed relations with each transform behind it", async () => {
    expect(await drawnLabels(new Set(["orders", "revenue"]))).toEqual([
      "SUM, COUNT, CAST · 3 columns",
    ]);
  });
});
