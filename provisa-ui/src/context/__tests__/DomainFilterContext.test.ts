// Copyright (c) 2026 Kenneth Stott
// Canary: 1b6ac6c3-a496-4550-a64b-766568c5093e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// A freshly-created view's domain must become visible without a reload: mergeCheckedDomains defaults
// any newly-appeared domain to checked while preserving domains the user explicitly unchecked.

import { beforeEach, describe, it, expect } from "vitest";
import { mergeCheckedDomains, restoreCheckedDomains } from "../DomainFilterContext";
import {
  CHECKED_DOMAINS_KEY,
  KNOWN_DOMAINS_KEY,
  ROLE_DOMAIN_KEYS,
} from "../../lib/domainFilterKeys";
import { storageKey as scopedKey } from "../../lib/browserStorage";

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
    const merged = mergeCheckedDomains(["sales", "ops", "marketing"], ["sales"], ["sales", "ops"]);
    // ops stays off (known+unchecked); marketing is new → on.
    expect(merged).toEqual(new Set(["sales", "marketing"]));
  });
});

// One browser, two demos: the pet store left its domains in localStorage; the weblogs demo
// declares "shop". Each page load restores the filter from what the last one saved.
describe("restoreCheckedDomains across page loads", () => {
  const PET_STORE = ["pet-store", "shelter", "meta", "ops"];
  const WEBLOGS = ["shop", "meta", "ops", "default"];

  beforeEach(() => {
    localStorage.clear();
    localStorage.setItem(scopedKey(CHECKED_DOMAINS_KEY), JSON.stringify(PET_STORE));
    localStorage.setItem(scopedKey(KNOWN_DOMAINS_KEY), JSON.stringify(PET_STORE));
  });

  it("keeps a new demo's domain checked on every load, not only the first", () => {
    expect(restoreCheckedDomains(WEBLOGS, ROLE_DOMAIN_KEYS)).toEqual(new Set(WEBLOGS));
    // The reload: shop is known now, and must still be checked -- it was never unchecked.
    expect(restoreCheckedDomains(WEBLOGS, ROLE_DOMAIN_KEYS)).toEqual(new Set(WEBLOGS));
  });

  it("still keeps a domain the user unchecked unchecked across loads", () => {
    restoreCheckedDomains(WEBLOGS, ROLE_DOMAIN_KEYS);
    const unchecked = WEBLOGS.filter((d) => d !== "ops");
    localStorage.setItem(scopedKey(CHECKED_DOMAINS_KEY), JSON.stringify(unchecked)); // toggleDomain("ops")
    expect(restoreCheckedDomains(WEBLOGS, ROLE_DOMAIN_KEYS)).toEqual(new Set(unchecked));
    expect(restoreCheckedDomains(WEBLOGS, ROLE_DOMAIN_KEYS)).toEqual(new Set(unchecked));
  });

  it("checks the first demo's domains again when it comes back", () => {
    restoreCheckedDomains(WEBLOGS, ROLE_DOMAIN_KEYS);
    expect(restoreCheckedDomains(PET_STORE, ROLE_DOMAIN_KEYS)).toEqual(new Set(PET_STORE));
  });
});
