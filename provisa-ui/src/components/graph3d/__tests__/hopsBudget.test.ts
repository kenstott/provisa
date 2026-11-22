// Copyright (c) 2026 Kenneth Stott
// Canary: c7d80040-a741-4f7a-a4b2-f653c8013928
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174 (amended 2026-11-12, A HOP IS A STEP BETWEEN TERMS): All follows the connected part to
// its end, under the limit on how many nodes a graph draws (REQ-2143's NODE_BUDGET): past it, how
// many is said instead, and the reads stop once the terms read alone are past it.

import { describe, expect, it, vi } from "vitest";
import { loadOntology } from "../useOntology";
import type { LayerId, QueryScope } from "../graphModel";
import { reader, server, world } from "./termServer";
import en from "../../../i18n/locales/en/graph3d.json";

const wire = vi.hoisted(() => ({
  read: null as null | ((q: string) => Promise<unknown>),
  reader: null as null | (() => unknown),
}));
vi.mock("../../../api/metaGraph", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/metaGraph")>()),
  metaReader: async () => wire.reader!(),
  dataGraphql: async (q: string) => wire.read!(q),
}));
vi.mock("../neighbourhood", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../neighbourhood")>()),
  NODE_BUDGET: 12,
}));

// Twenty terms in a chain, each IS a column of a table of its own that a product of its own holds:
// a term drawn is four nodes, with its column, table and product.
const IDS = Array.from({ length: 20 }, (_, i) => i + 1);
const CHAIN = world({
  products: IDS.map((id) => ({ id: `p${id}`, name: `product ${id}` })),
  tables: IDS.map((id) => ({ id: 100 + id, name: `t${id}`, product: `p${id}`, columns: ["c"] })),
  terms: IDS.map((id) => ({ id, name: `term ${id}`, columns: [[100 + id, "c"]] })),
  links: IDS.slice(1).map((id): [number, number, string] => [id - 1, id, "PART_OF"]),
});

const scope = (depth: number): QueryScope => ({
  layers: new Set<LayerId>(["ontology", "columns", "data"]),
  search: [],
  hiddenStatuses: [],
  edgeTypes: [],
  domains: [],
  orphans: true,
  tags: [],
  sensitivities: [],
  focus: "term:1",
  depth,
});

describe("All, under the node budget", () => {
  it("draws a focus whose hops stay within it", async () => {
    wire.reader = reader;
    wire.read = server(CHAIN).read;
    const loaded = await loadOntology("analyst", scope(1));
    if (loaded === null || loaded.over !== undefined) throw new Error("not drawn");
    // Terms 1 and 2, each with its column, table and product.
    expect(loaded.data.nodes).toHaveLength(8);
  });

  it("says, past it, that more than the budget would be drawn, and reads no further", async () => {
    wire.reader = reader;
    const srv = server(CHAIN);
    wire.read = srv.read;
    const loaded = await loadOntology("analyst", scope(Infinity));
    expect(loaded).toMatchObject({ over: 12, hops: true });
    // The count, the root, and a hop a term until the terms read are past the budget: not all 20.
    const hopsRead = srv.sent.filter((s) => s.query.startsWith("query OntologyHop")).length;
    expect(hopsRead).toBe(12);
  });

  it("says so in English", () => {
    expect(en.graph3d.overBudgetHops).toBe("More than {{count}} to draw; choose fewer hops");
    expect(en.graph3d.hopsAll).toBe("All");
  });
});
