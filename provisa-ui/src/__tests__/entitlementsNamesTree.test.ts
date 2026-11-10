// Copyright (c) 2026 Kenneth Stott
// Canary: 9f175e23-d08d-44b6-bb35-bf70ab9e952d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2219: the picker's tree of the names table. An identifier is a node's dotted SQL name; a
// shorter one selects what is beneath it, so checking a node stands for its children, and
// unchecking one child of a checked parent keeps the parent's other children.

import { describe, it, expect } from "vitest";
import {
  allIds,
  buildNamesTree,
  checkNode,
  covers,
  uncheckNode,
} from "../components/entitlements/namesTree";

const DOMAINS = [{ id: "finance" }, { id: "sales-ops" }];
const TABLES = [
  {
    domainId: "finance",
    tableName: "revenue_raw",
    alias: "revenue",
    columns: [{ columnName: "amount" }, { columnName: "cust_id", alias: "customer" }],
  },
  { domainId: "finance", tableName: "costs", columns: [{ columnName: "amount" }] },
  { domainId: "sales-ops", tableName: "orders", columns: [] },
];

describe("the names tree", () => {
  it("names domains by SQL name, tables and columns by alias, to the depth asked", () => {
    const depth3 = buildNamesTree(DOMAINS, TABLES, 3);
    expect(depth3.map((d) => d.id)).toEqual(["finance", "sales_ops"]);
    expect(depth3[0].children.map((t) => t.id)).toEqual(["finance.costs", "finance.revenue"]);
    expect(depth3[0].children[1].children.map((c) => c.id)).toEqual([
      "finance.revenue.amount",
      "finance.revenue.customer",
    ]);
    expect(buildNamesTree(DOMAINS, TABLES, 2)[0].children[1].children).toEqual([]);
    expect(buildNamesTree(DOMAINS, TABLES, 1)[0].children).toEqual([]);
    expect(allIds(depth3).has("finance.revenue.customer")).toBe(true);
  });

  it("a shorter identifier covers what is beneath it, ignoring case, * any segment", () => {
    expect(covers("finance", "finance.revenue.amount")).toBe(true);
    expect(covers("FINANCE.Revenue", "finance.revenue")).toBe(true);
    expect(covers("finance.*.amount", "finance.costs.amount")).toBe(true);
    expect(covers("finance.revenue", "finance")).toBe(false);
    expect(covers("finance.revenue", "finance.revenue_raw")).toBe(false);
  });

  it("checking a node lists it and drops what it now covers", () => {
    const [finance] = buildNamesTree(DOMAINS, TABLES, 2);
    expect(checkNode(["finance.costs", "sales_ops"], finance)).toEqual(["sales_ops", "finance"]);
  });

  it("unchecking a child of a checked domain keeps the domain's other children", () => {
    const roots = buildNamesTree(DOMAINS, TABLES, 3);
    const revenue = roots[0].children[1];
    expect(uncheckNode(["finance", "sales_ops"], revenue, roots)).toEqual([
      "sales_ops",
      "finance.costs",
    ]);
    const amount = revenue.children[0];
    expect(uncheckNode(["finance"], amount, roots)).toEqual([
      "finance.costs",
      "finance.revenue.customer",
    ]);
  });

  it("unchecking a node listed itself removes it and nothing else", () => {
    const roots = buildNamesTree(DOMAINS, TABLES, 2);
    expect(uncheckNode(["finance.costs", "sales_ops"], roots[0].children[0], roots)).toEqual([
      "sales_ops",
    ]);
  });
});
