// Copyright (c) 2026 Kenneth Stott
// Canary: 9d3e6b21-4a7f-4c58-8e1b-2f6c0a9d5e47
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1591 / REQ-1723: the navbar's narrowing, the one rule the term list and the Ontology View
// both follow.

import { describe, expect, it } from "vitest";
import { viewDomainsOf } from "../viewDomains";

describe("the navbar's narrowing", () => {
  const all = ["shelter", "pet_store", "sales"];

  it("is the checked domains when some but not all are checked", () => {
    const checked = new Set(["shelter"]);
    expect(viewDomainsOf(all, checked, true)).toBe(checked);
  });

  it("is none with every domain checked, or none checked", () => {
    expect(viewDomainsOf(all, new Set(all), true)).toBeNull();
    expect(viewDomainsOf(all, new Set(), true)).toBeNull();
  });

  it("is none where domains are off", () => {
    expect(viewDomainsOf(all, new Set(["shelter"]), false)).toBeNull();
  });
});
