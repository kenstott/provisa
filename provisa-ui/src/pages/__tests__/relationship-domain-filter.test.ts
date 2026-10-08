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
