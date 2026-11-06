// Copyright (c) 2026 Kenneth Stott
// Canary: 3e7b9d24-6c18-4f5a-a0d3-8b2e5f1c7a96
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174 (EVERY LANE NODE INFORMS AND LINKS; ONE ONTOLOGY GRAPH): what the one graph's read
// gives each node -- a term its definition, status and stewards; a column its type, description,
// mask and tags; a table its source, description, quality and tags; a product its status and
// owner -- whether reached from a term's refs or from a product's datasets, the product a
// product's snippet is focused on among them.

import { describe, expect, it } from "vitest";
import {
  productMapGraph,
  productNode,
  termNode,
  type ProductRow,
  type TermRow,
} from "../ontologyData";

const TERM: TermRow = {
  id: 7,
  name: "Customer",
  definition: "Someone who buys.",
  refCount: 1,
  deprecated: false,
  retired: false,
  status: "approved",
  grounding: "grounded",
  experts: [
    { userId: "ada", kind: "expert" },
    { userId: "bob", kind: "author" },
  ],
  refs: [
    {
      columnKey: "1:email",
      columnName: "email",
      tableId: 1,
      tableName: "customers",
      domainId: "sales",
      column: {
        dataType: "varchar",
        description: "Where we write.",
        masks: '[{"to": ["analyst"], "mask": {"type": "constant", "value": null}}]',
        sensitivity: [{ tags: "pii" }],
        tags: [{ tagId: "contact" }],
      },
      registeredTable: {
        id: 1,
        sourceId: "sales_pg",
        description: "Every customer.",
        checkedBy: [
          { latestOutcome: "pass", latestRunAt: "2026-10-26T10:00:00Z" },
          { latestOutcome: "fail", latestRunAt: "2026-10-27T10:00:00Z" },
        ],
        tags: [{ tagId: "gold" }],
        product: {
          id: "c360",
          name: "customer_360",
          domainId: "sales",
          status: "live",
          ownerRole: "sales_steward",
        },
      },
    },
  ],
};

const PRODUCT: ProductRow = {
  id: "c360",
  name: "customer_360",
  domainId: "sales",
  status: "live",
  ownerRole: "sales_steward",
  memberTables: [
    {
      id: 2,
      domainId: "sales",
      tableName: "orders",
      checkedBy: null,
      tags: null,
      tableColumns: [
        {
          columnName: "total",
          columnKey: "2:total",
          dataType: "numeric",
          glossaryRef: [{ termId: 7 }],
          tags: [{ tagId: "finance" }],
        },
      ],
    },
  ],
};

// A product's snippet reads by the Product map's rules (REQ-2174, ONE GRAPH, RULES PER PRESET).
const g = productMapGraph([TERM], [], [PRODUCT]);
const byId = new Map(g.nodes.map((n) => [n.id, n]));

describe("each node's facts", () => {
  it("gives a term its definition and stewards (its experts)", () => {
    expect(byId.get(termNode(7))).toMatchObject({
      definition: "Someone who buys.",
      info: { stewards: ["ada"] },
    });
  });

  it("gives a column its type, description, whether it is masked, and its sensitivity", () => {
    expect(byId.get("c:1:email")?.info).toEqual({
      dataType: "varchar",
      description: "Where we write.",
      masked: true,
      sensitive: ["pii"],
    });
  });

  it("gives a table its source, description and its checkers' worst latest outcome", () => {
    expect(byId.get("t:1")?.info).toEqual({
      source: "sales_pg",
      description: "Every customer.",
      quality: "fail",
      checkedAt: "2026-10-27T10:00:00Z",
    });
    // No checker scans it: no quality is said.
    expect(byId.get("t:2")?.info).toBeUndefined();
  });

  it("gives a product its status and owner", () => {
    expect(byId.get(productNode("c360"))?.info).toEqual({ status: "live", owner: "sales_steward" });
  });
});

describe("tags, which the tag filter judges", () => {
  it("are read on columns and tables, from a term's refs and a product's datasets alike", () => {
    expect(byId.get("c:1:email")?.tags).toEqual(["contact"]);
    expect(byId.get("t:1")?.tags).toEqual(["gold"]);
    expect(byId.get("c:2:total")?.tags).toEqual(["finance"]);
    // Read, with none: judged as carrying none.
    expect(byId.get("t:2")?.tags).toEqual([]);
  });
});

describe("a product's snippet", () => {
  it("joins the product to its tables, their columns and the terms those are", () => {
    const edges = g.edges.map((e) => `${e.kind}:${e.a}>${e.b}`);
    expect(edges).toEqual(
      expect.arrayContaining([
        "member:p:c360>t:1",
        "member:p:c360>t:2",
        "has_column:t:2>c:2:total",
        "term_binding:term:7>c:2:total",
        "term_binding:term:7>c:1:email",
      ]),
    );
  });
});
