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
import type { NamesNode } from "../api/entitlements";
import {
  allIds,
  checkNode,
  covers,
  toDepth,
  uncheckNode,
} from "../components/entitlements/namesTree";

const leaf = (id: string): NamesNode => ({ id, label: id.split(".").pop() ?? id, children: [] });

// The server's tree (GET /admin/entitlements/kinds): domains by SQL name, tables and columns by
// the names they are queried as, each level ordered.
const TREE: NamesNode[] = [
  {
    id: "finance",
    label: "finance",
    children: [
      { id: "finance.costs", label: "costs", children: [leaf("finance.costs.amount")] },
      {
        id: "finance.revenue",
        label: "revenue",
        children: [leaf("finance.revenue.amount"), leaf("finance.revenue.customer")],
      },
    ],
  },
  { id: "sales_ops", label: "sales_ops", children: [leaf("sales_ops.orders")] },
];

describe("the names tree", () => {
  it("is the server's tree, cut to the depth asked", () => {
    const depth3 = toDepth(TREE, 3);
    expect(depth3).toEqual(TREE);
    expect(depth3[0].children[1].children.map((c) => c.id)).toEqual([
      "finance.revenue.amount",
      "finance.revenue.customer",
    ]);
    expect(toDepth(TREE, 2)[0].children[1].children).toEqual([]);
    expect(toDepth(TREE, 2)[0].children.map((t) => t.id)).toEqual([
      "finance.costs",
      "finance.revenue",
    ]);
    expect(toDepth(TREE, 1)[0].children).toEqual([]);
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
    const [finance] = toDepth(TREE, 2);
    expect(checkNode(["finance.costs", "sales_ops"], finance)).toEqual(["sales_ops", "finance"]);
  });

  it("unchecking a child of a checked domain keeps the domain's other children", () => {
    const roots = toDepth(TREE, 3);
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
    const roots = toDepth(TREE, 2);
    expect(uncheckNode(["finance.costs", "sales_ops"], roots[0].children[0], roots)).toEqual([
      "sales_ops",
    ]);
  });
});
