// Copyright (c) 2026 Kenneth Stott
// Canary: 1f6b8d42-7c3e-4a95-b0d1-e9a2c5f74b38
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174 (amended 2026-11-07), REQ-2219 (amended 2026-11-23): who owns a product, and so sees
// its Consumers card -- a person named among its owners, acting as a role carrying
// data_product_owner, or a session holding data_product_rw reaching its domain; no one else.

import { describe, expect, it } from "vitest";
import { isProductOwner, type ActingRole } from "../productOwner";

const PRODUCT = { owners: ["vera"], domainId: "pet-store" };
const role = (id: string, caps: string[] = [], domains: string[] = []): ActingRole => ({
  id,
  effectiveCapabilities: caps as ActingRole["effectiveCapabilities"],
  domain_access: domains,
});
const OWNERS = role("data_product_owner", ["data_product_owner"]);

describe("owning a data product", () => {
  it("is being named among its owners, acting as a role carrying data_product_owner", () => {
    expect(isProductOwner(PRODUCT, "vera", [OWNERS])).toBe(true);
    expect(isProductOwner(PRODUCT, "vera", [role("analyst"), OWNERS])).toBe(true);
  });

  it("is not being named while acting as no role carrying the capability", () => {
    expect(isProductOwner(PRODUCT, "vera", [role("analyst", ["data_product_read"])])).toBe(false);
  });

  it("is not holding the capability without being named", () => {
    expect(isProductOwner(PRODUCT, "sam", [OWNERS])).toBe(false);
    expect(isProductOwner(PRODUCT, null, [OWNERS])).toBe(false);
  });

  it("is holding data_product_rw in its domain, or in every domain", () => {
    expect(
      isProductOwner(PRODUCT, "sam", [role("steward", ["data_product_rw"], ["pet-store"])]),
    ).toBe(true);
    expect(isProductOwner(PRODUCT, "sam", [role("admin", ["data_product_rw"], ["*"])])).toBe(true);
  });

  it("is not data_product_rw in another domain", () => {
    expect(
      isProductOwner(PRODUCT, "sam", [role("steward", ["data_product_rw"], ["finance"])]),
    ).toBe(false);
  });

  it("is not reading the product, nor a role's domain without the right", () => {
    expect(
      isProductOwner(PRODUCT, "sam", [role("analyst", ["data_product_read"], ["pet-store"])]),
    ).toBe(false);
    expect(isProductOwner(PRODUCT, "vera", [])).toBe(false);
  });

  it("is never a role named like a person when the product has no owners", () => {
    expect(isProductOwner({ owners: [], domainId: "pet-store" }, "vera", [OWNERS])).toBe(false);
  });
});
