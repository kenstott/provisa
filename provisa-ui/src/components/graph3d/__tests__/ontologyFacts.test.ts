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
  ontologyGraph,
  productNode,
  termNode,
  type ProductRow,
  type TermRow,
} from "../ontologyData";

const TERM: TermRow = {
  id: 7,
  name: "Customer",
  definition: "Someone who buys.",
  ref_count: 1,
  deprecated: false,
  retired: false,
  live: true,
  experts: [
    { user_id: "ada", kind: "expert" },
    { user_id: "bob", kind: "author" },
  ],
  refs: [
    {
      column_key: "1:email",
      column_name: "email",
      table_id: 1,
      table_name: "customers",
      domain_id: "sales",
      column: {
        data_type: "varchar",
        description: "Where we write.",
        masks: '[{"to": ["analyst"], "mask": {"type": "constant", "value": null}}]',
        sensitivity: [{ tags: "pii" }],
        tags: [{ tag_id: "contact" }],
      },
      table: {
        id: 1,
        source_id: "sales_pg",
        description: "Every customer.",
        checked_by: [
          { latest_outcome: "pass", latest_run_at: "2026-10-26T10:00:00Z" },
          { latest_outcome: "fail", latest_run_at: "2026-10-27T10:00:00Z" },
        ],
        tags: [{ tag_id: "gold" }],
        product: {
          id: "c360",
          name: "customer_360",
          domain_id: "sales",
          status: "live",
          owner_role: "sales_steward",
        },
      },
    },
  ],
};

const PRODUCT: ProductRow = {
  id: "c360",
  name: "customer_360",
  domain_id: "sales",
  status: "live",
  owner_role: "sales_steward",
  member_tables: [
    {
      id: 2,
      domain_id: "sales",
      table_name: "orders",
      checked_by: null,
      tags: null,
      table_columns: [
        {
          column_name: "total",
          column_key: "2:total",
          data_type: "numeric",
          glossary_ref: [{ term_id: 7 }],
          tags: [{ tag_id: "finance" }],
        },
      ],
    },
  ],
};

const g = ontologyGraph([TERM], [], [PRODUCT]);
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
