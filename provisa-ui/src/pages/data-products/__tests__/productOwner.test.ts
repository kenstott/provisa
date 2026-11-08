// Copyright (c) 2026 Kenneth Stott
// Canary: 1f6b8d42-7c3e-4a95-b0d1-e9a2c5f74b38
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174 (amended 2026-11-07), REQ-2219: who owns a product, and so sees its Consumers card -- a
// session acting as its owner_role, or holding data_product_rw reaching its domain; no one else.

import { describe, expect, it } from "vitest";
import { isProductOwner, type ActingRole } from "../productOwner";

const PRODUCT = { ownerRole: "vet_lead", domainId: "pet-store" };
const role = (id: string, caps: string[] = [], domains: string[] = []): ActingRole => ({
  id,
  effectiveCapabilities: caps as ActingRole["effectiveCapabilities"],
  domain_access: domains,
});

describe("owning a data product", () => {
  it("is acting as its owner_role", () => {
    expect(isProductOwner(PRODUCT, [role("vet_lead")])).toBe(true);
    expect(isProductOwner(PRODUCT, [role("analyst"), role("vet_lead")])).toBe(true);
  });

  it("is holding data_product_rw in its domain, or in every domain", () => {
    expect(isProductOwner(PRODUCT, [role("steward", ["data_product_rw"], ["pet-store"])])).toBe(
      true,
    );
    expect(isProductOwner(PRODUCT, [role("admin", ["data_product_rw"], ["*"])])).toBe(true);
  });

  it("is not data_product_rw in another domain", () => {
    expect(isProductOwner(PRODUCT, [role("steward", ["data_product_rw"], ["finance"])])).toBe(
      false,
    );
  });

  it("is not reading the product, nor a role's domain without the right", () => {
    expect(isProductOwner(PRODUCT, [role("analyst", ["data_product_read"], ["pet-store"])])).toBe(
      false,
    );
    expect(isProductOwner(PRODUCT, [])).toBe(false);
  });

  it("is never a role of the same name as an absent owner_role", () => {
    expect(isProductOwner({ ownerRole: null, domainId: "pet-store" }, [role("vet_lead")])).toBe(
      false,
    );
  });
});
