// Copyright (c) 2026 Kenneth Stott
// Canary: 6b4d327b-f55a-4dd6-a785-a3740b2d98be
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1659 (amended 2026-11-28): the Data Products page's products and domains for a role set.

import { describe, expect, it } from "vitest";
import type { DataProduct } from "../../../types/admin";
import { productScope } from "../productScope";

const product = (id: string, domainId: string, status: string) =>
  ({ id, domainId, status }) as unknown as DataProduct;
const PRODUCTS = [
  product("a", "sales", "live"),
  product("b", "logistics", "draft"),
  product("c", "finance", "deprecated"),
  product("d", "finance", "retired"),
];
const role = (rights: string[], domains: string[]) => ({
  effectiveCapabilities: rights,
  domain_access: domains,
});
const ids = (ps: DataProduct[]) => ps.map((p) => p.id);

describe("productScope", () => {
  it("is the offered products alone for a role with no product right", () => {
    const s = productScope(PRODUCTS, [role(["table_registration"], ["finance", "hr"])]);
    expect(ids(s.products)).toEqual(["a"]);
    expect(s.domains).toEqual(["sales"]);
  });

  it("adds every product of a domain a product right reaches, and the domain itself", () => {
    const s = productScope(PRODUCTS, [role(["data_product_read"], ["finance", "hr"])]);
    expect(ids(s.products)).toEqual(["a", "c", "d"]);
    expect(s.domains).toEqual(["finance", "hr", "sales"]);
  });

  it("pairs a right with its own role's domains, never another role's", () => {
    const s = productScope(PRODUCTS, [
      role(["data_product_rw"], ["logistics"]),
      role(["table_registration"], ["finance"]),
    ]);
    expect(ids(s.products)).toEqual(["a", "b"]);
    expect(s.domains).toEqual(["logistics", "sales"]);
  });

  it("is every product for a product right on every domain", () => {
    const s = productScope(PRODUCTS, [role(["data_product_read"], ["*"])]);
    expect(ids(s.products)).toEqual(["a", "b", "c", "d"]);
    expect(s.domains).toEqual(["finance", "logistics", "sales"]);
  });
});
