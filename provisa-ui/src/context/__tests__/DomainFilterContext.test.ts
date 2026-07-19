// Copyright (c) 2026 Kenneth Stott
// Canary: 1b6ac6c3-a496-4550-a64b-766568c5093e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// A freshly-created view's domain must become visible without a reload: mergeCheckedDomains defaults
// any newly-appeared domain to checked while preserving domains the user explicitly unchecked.

import { describe, it, expect } from "vitest";
import { mergeCheckedDomains } from "../DomainFilterContext";

describe("mergeCheckedDomains", () => {
  it("returns null when there is no persisted state (caller checks everything)", () => {
    expect(mergeCheckedDomains(["a", "b"], null, null)).toBeNull();
    expect(mergeCheckedDomains(["a", "b"], ["a"], null)).toBeNull();
  });

  it("defaults a newly-appeared domain to checked", () => {
    // sales was known+checked; the just-created view's domain 'marketing' is new → auto-checked.
    const merged = mergeCheckedDomains(["sales", "marketing"], ["sales"], ["sales"]);
    expect(merged).toEqual(new Set(["sales", "marketing"]));
  });

  it("keeps a domain the user explicitly unchecked unchecked", () => {
    // 'ops' is known but not in the checked set → user turned it off; stays off.
    const merged = mergeCheckedDomains(["sales", "ops"], ["sales"], ["sales", "ops"]);
    expect(merged).toEqual(new Set(["sales"]));
  });

  it("drops persisted domains no longer available", () => {
    const merged = mergeCheckedDomains(["sales"], ["sales", "gone"], ["sales", "gone"]);
    expect(merged).toEqual(new Set(["sales"]));
  });

  it("checks a new domain even when others were unchecked", () => {
    const merged = mergeCheckedDomains(
      ["sales", "ops", "marketing"],
      ["sales"],
      ["sales", "ops"],
    );
    // ops stays off (known+unchecked); marketing is new → on.
    expect(merged).toEqual(new Set(["sales", "marketing"]));
  });
});
