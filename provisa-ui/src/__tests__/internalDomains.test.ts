// Copyright (c) 2026 Kenneth Stott
// Canary: 3a9d2c71-5e46-4f08-b1c7-92e0d6f4a813
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A system domain is read from the Domains list, and the same list feeds the pickers that put
// something into a domain: none of them may offer one.

import { describe, it, expect } from "vitest";
import { assignableDomainIds, isInternalDomain } from "../lib/internalDomains";

describe("internal domains", () => {
  it("no picker offers a domain Provisa generates for itself", () => {
    expect(assignableDomainIds(["meta", "ontology", "ops", "", "pet-store"])).toEqual([
      "pet-store",
    ]);
  });

  it("names ontology with meta and ops", () => {
    expect(["meta", "ontology", "ops"].every(isInternalDomain)).toBe(true);
    expect(isInternalDomain("pet-store")).toBe(false);
  });
});
