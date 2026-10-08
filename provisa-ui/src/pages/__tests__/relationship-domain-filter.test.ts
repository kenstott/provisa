// Copyright (c) 2026 Kenneth Stott
// Canary: 1955dd2f-f8af-4fa4-8a3f-4ee095a8172a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect } from "vitest";
import { relationshipInCheckedDomains } from "../relationshipDomainFilter";

describe("relationshipInCheckedDomains", () => {
  const sales = new Set(["sales"]);

  it("keeps a cross-domain relationship when one end is checked", () => {
    expect(relationshipInCheckedDomains(sales, ["sales", "finance", null])).toBe(true);
    expect(relationshipInCheckedDomains(sales, ["finance", "sales", null])).toBe(true);
  });

  it("keeps a relationship whose owner domain is checked", () => {
    expect(relationshipInCheckedDomains(sales, ["finance", "ops", "sales"])).toBe(true);
  });

  it("drops a relationship with no checked end", () => {
    expect(relationshipInCheckedDomains(sales, ["finance", "ops", null])).toBe(false);
    expect(relationshipInCheckedDomains(sales, [undefined, undefined, null])).toBe(false);
  });

  it("shows everything when nothing narrows", () => {
    expect(relationshipInCheckedDomains(null, ["finance", "ops", null])).toBe(true);
  });
});
