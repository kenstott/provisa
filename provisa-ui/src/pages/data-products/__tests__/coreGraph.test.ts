// Copyright (c) 2026 Kenneth Stott
// Canary: a97c07c8-57eb-484b-ac29-ad77f8a6b645
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The product page's knowledge graph card is drawn from the panel's own meta read (its core):
// the product, its member tables, its commands and the terms bound to its columns -- the same graph
// the product's graph query draws for the card, read once.

import { describe, expect, it } from "vitest";
import { coreGraph, type ProductCore } from "../productCore";
import { panelGraph } from "../../../components/graph3d/graphModel";
import { productNode } from "../../../components/graph3d/productGraphData";
import type { DataProduct } from "../../../types/admin";

const PRODUCT = { id: "c360", domainId: "sales", name: "customer_360" } as unknown as DataProduct;
const table = (id: number, name: string, over: Record<string, unknown> = {}) => ({
  id,
  source_id: "crm",
  domain_id: "sales",
  schema_name: "crm",
  table_name: name,
  description: null,
  dq_contract: null,
  table_columns: [],
  profile_results: [],
  ...over,
});

const CORE: ProductCore = {
  product: PRODUCT,
  tables: [
    table(1, "customers", {
      table_columns: [
        {
          column_name: "email",
          column_key: "1:email",
          glossary_ref: [{ term_id: 7, term: { id: 7, name: "Customer" } }],
        },
      ],
    }),
    table(2, "orders"),
    table(3, "orders_dq", {
      dq_contract: "name: orders_dq",
      checks: { id: 2, domain_id: "sales", table_name: "orders" },
    }),
  ] as never,
  commands: [{ name: "refresh_c360", description: null, domain_id: "sales" }] as never,
  tags: [],
  terms: [],
  edges: [],
  queries: { graphql: "", sql: "", jsonapi: "" },
};

describe("the knowledge graph card, from the panel's core", () => {
  it("draws the product, its member tables, its commands and its bound terms", () => {
    const g = panelGraph(coreGraph(CORE), productNode("c360"));
    expect(g.nodes.map((n) => `${n.type}:${n.label}`).sort()).toEqual([
      "command:refresh_c360",
      "product:customer_360",
      "table:sales.customers",
      "table:sales.orders",
      "term:Customer",
    ]);
  });
});
