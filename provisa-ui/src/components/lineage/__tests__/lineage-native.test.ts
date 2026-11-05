// Copyright (c) 2026 Kenneth Stott
// Canary: 2a7e9c40-6b13-4d85-9f2e-1c0b8d6a4e57
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2116: the native hop beneath a source column -- a box per native address carrying its
// source's logo and type label, the table it lands in saying how it is served -- and every element
// linking to where it is defined, as far as the user's rights open that page.

import { describe, expect, it } from "vitest";
import { buildLineageModel, servedText } from "../lineage-layout";
import { lineageLinks } from "../lineage-links";
import type { LineageGraphData, LineageNative } from "../../../api/lineage";
import type { CapabilityRequirement } from "../../../lib/capabilities";

const NATIVE: LineageNative = {
  relation: "sales.customers",
  source: "crm",
  type: "openapi",
  hints: { brand: "stripe" },
  address: [
    { what: "operation", value: "GET /v2/customers" },
    { what: "rows", value: "data.items" },
  ],
  served: { mode: "virtual" },
};

const GRAPH: LineageGraphData = {
  nodes: [
    {
      id: "native:sales.customers.postalCode",
      column: "postalCode",
      relation: "native:sales.customers",
      kind: "native",
      materialized: false,
      source_id: "crm",
      source_schema: "public",
      source_table: "Customers",
    },
    {
      id: "sales.customers.postal_code",
      column: "postal_code",
      relation: "sales.customers",
      kind: "source",
      materialized: false,
    },
    {
      id: "sales.report.postal_code",
      column: "postal_code",
      relation: "sales.report",
      kind: "derived",
      materialized: false,
    },
    { id: "e.score", column: "score", relation: "e", kind: "command", materialized: false },
  ],
  edges: [
    {
      source: "native:sales.customers.postalCode",
      target: "sales.customers.postal_code",
      transform: "postalCode",
      ops: [{ name: "naming", kind: "rename" }],
    },
    {
      source: "sales.customers.postal_code",
      target: "sales.report.postal_code",
      transform: "c.postal_code",
      ops: [{ name: "postal_code", kind: "identity" }],
    },
    {
      source: "sales.customers.postal_code",
      target: "e.score",
      transform: "score_it(...)",
      ops: [{ name: "score_it", kind: "command" }],
    },
  ],
  outputs: ["sales.report.postal_code"],
  natives: { "native:sales.customers": NATIVE },
};

const TABLES = [
  { sourceId: "crm", domainId: "sales", tableName: "customers" },
  { sourceId: "__derived__", domainId: "sales", tableName: "report" },
];

const everything = () => true;

describe("the native hop", () => {
  it("draws a native box with its source's brand and address above the raw field", () => {
    const model = buildLineageModel(GRAPH, undefined);
    const box = model.boxes.find((b) => b.relation === "native:sales.customers")!;
    expect(box.label).toBe("Stripe · crm");
    expect(box.native?.typeLabel).toBe("Stripe");
    expect(box.native?.lines).toEqual(["operation GET /v2/customers", "rows data.items"]);
    expect(box.native?.logo).toMatch(/^data:image\/svg\+xml/);
    expect(box.rows.map((r) => [r.label, r.role])).toEqual([["postalCode", "native"]]);
    const rename = model.edges.find((e) => e.source === "native:sales.customers.postalCode")!;
    expect(rename.label).toBe("naming");
  });

  it("names how the table it lands in is served", () => {
    const model = buildLineageModel(GRAPH, undefined);
    const table = model.boxes.find((b) => b.relation === "sales.customers")!;
    expect(table.label).toBe("sales.customers · virtual");
    expect(
      servedText({
        ...NATIVE,
        served: { mode: "replica", cache_ttl: -1, last_load: null, next_load: null },
      }),
    ).toBe("replica, never expires");
  });
});

describe("each element links to where it is defined", () => {
  it("opens a native box's source, a table, a view and a command", () => {
    const links = lineageLinks(GRAPH, TABLES, everything);
    expect(links.relation("native:sales.customers")).toBe("/sources?expanded=crm");
    expect(links.node("sales.customers.postal_code")).toBe("/tables?open=crm.customers");
    expect(links.relation("sales.report")).toBe("/views?open=__derived__.report");
    expect(links.node("e.score")).toBe("/commands?command=score_it");
    expect(links.relation("result")).toBeNull();
  });

  it("offers a link only where the user's rights open the page", () => {
    const tablesOnly = (req: CapabilityRequirement) => req.capability === "table_registration";
    const links = lineageLinks(GRAPH, TABLES, tablesOnly);
    expect(links.relation("native:sales.customers")).toBeNull();
    expect(links.relation("sales.report")).toBe("/views?open=__derived__.report");
    const none = lineageLinks(GRAPH, TABLES, () => false);
    expect(none.node("sales.customers.postal_code")).toBeNull();
    expect(none.command("score_it")).toBeNull();
  });
});

describe("a native box without its source's metadata (a user without source registration)", () => {
  const bare: LineageGraphData = { ...GRAPH, natives: undefined };

  it("is named by its source and the names the source gives it, never by its internal key", () => {
    const model = buildLineageModel(bare, undefined);
    const labels = model.boxes.map((b) => b.label);
    expect(labels.some((l) => l.includes("native:"))).toBe(false);
    const box = model.boxes.find((b) => b.relation === "native:sales.customers")!;
    expect(box.label).toBe("crm · public.Customers");
    expect(box.native).toBeNull();
  });

  it("reads the rename across its edge: the source's field, then the registered column", () => {
    const model = buildLineageModel(bare, undefined);
    const rows = (relation: string) =>
      model.boxes.find((b) => b.relation === relation)!.rows.map((r) => [r.label, r.role]);
    expect(rows("native:sales.customers")).toEqual([["postalCode", "native"]]);
    expect(rows("sales.customers")).toContainEqual(["postal_code", "source"]);
    const rename = model.edges.find((e) => e.source === "native:sales.customers.postalCode")!;
    expect(rename.target).toBe("sales.customers.postal_code");
    expect(rename.label).toBe("naming");
  });

  it("names an object with no schema or table (an API, a topic) by its source alone", () => {
    const api: LineageGraphData = {
      ...bare,
      nodes: bare.nodes.map((n) =>
        n.kind === "native" ? { ...n, source_schema: null, source_table: null } : n,
      ),
    };
    const model = buildLineageModel(api, undefined);
    expect(model.boxes.find((b) => b.relation === "native:sales.customers")?.label).toBe("crm");
  });

  it("keeps the catalog's name for registered tables beside it", () => {
    const catalog = (relation: string) => relation.replace("sales.", "Sales.");
    const model = buildLineageModel(bare, undefined, { relationLabel: catalog });
    const label = (relation: string) => model.boxes.find((b) => b.relation === relation)?.label;
    expect(label("native:sales.customers")).toBe("crm · public.Customers");
    expect(label("sales.report")).toBe("Sales.report");
  });
});
