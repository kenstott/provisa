// Copyright (c) 2026 Kenneth Stott
// Canary: 5e09b7d3-c1a4-4e62-97f0-2d6a8b13f4c7
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2155: the form offers what a role's parents hold, all selected, for a person to unselect.

import { describe, expect, it } from "vitest";
import { parentRights, reachable, withParents } from "../roleParents";
import type { Capability, Role } from "../../../types/auth";

function role(
  id: string,
  capabilities: Capability[],
  domains: string[],
  regions: string[] = [],
): Role {
  return {
    id,
    description: `${id}.`,
    capabilities,
    effectiveCapabilities: capabilities,
    effectiveDomains: domains,
    effectiveResidencyValues: regions,
    demonstrated: [],
    domain_access: domains,
  };
}

const ROLES = [
  role("analyst", ["usage", "query_development"], ["*"]),
  role("sales", ["usage", "write"], ["sales", "emea"]),
  role("resident", ["data_residency"], ["sales"], ["eu", "us"]),
];
const FORM = {
  parents: [] as string[],
  capabilities: [] as Capability[],
  domainAccess: [] as string[],
  residencyValues: [] as string[],
};

describe("what a role's parents hold", () => {
  it("is nothing to narrow by for a role with none", () => {
    expect(parentRights([], ROLES)).toBeNull();
    expect(reachable("anything", null)).toBe(true);
  });

  it("is the union of what its parents hold", () => {
    const held = parentRights(["analyst", "sales"], ROLES);
    expect([...(held?.capabilities ?? [])].sort()).toEqual([
      "query_development",
      "usage",
      "write",
    ]);
  });

  it("reaches every domain with * and no other with a list", () => {
    expect(reachable("hr", parentRights(["analyst"], ROLES))).toBe(true);
    expect(reachable("hr", parentRights(["sales"], ROLES))).toBe(false);
    expect(reachable("*", parentRights(["sales"], ROLES))).toBe(false);
    expect(reachable("emea", parentRights(["sales"], ROLES))).toBe(true);
  });
});

describe("choosing a parent", () => {
  it("selects everything it holds", () => {
    const form = withParents(FORM, ["analyst"], ROLES);
    expect(form.capabilities).toEqual(["usage", "query_development"]);
    expect(form.domainAccess).toEqual(["*"]);
  });

  it("selects a listing parent's domains", () => {
    expect(withParents(FORM, ["sales"], ROLES).domainAccess).toEqual(["sales", "emea"]);
  });

  it("keeps what was unselected from a parent still chosen", () => {
    const chosen = { ...withParents(FORM, ["analyst"], ROLES), capabilities: ["usage"] };
    const both = withParents(chosen as typeof FORM, ["analyst", "sales"], ROLES);
    expect(both.capabilities).toEqual(["usage", "write"]);
  });

  it("drops what no parent holds any longer", () => {
    const chosen = withParents(FORM, ["analyst", "sales"], ROLES);
    expect(withParents(chosen, ["analyst"], ROLES).capabilities).toEqual([
      "usage",
      "query_development",
    ]);
  });

  it("replaces * with what a listing parent reaches", () => {
    const form = { ...FORM, domainAccess: ["*"] };
    expect(withParents(form, ["sales"], ROLES).domainAccess).toEqual(["sales", "emea"]);
  });

  it("takes data_residency's regions with it", () => {
    const form = withParents(FORM, ["resident"], ROLES);
    expect(form.residencyValues).toEqual(["eu", "us"]);
  });

  it("leaves no regions where data_residency is not held", () => {
    const form = { ...FORM, residencyValues: ["eu"] };
    expect(withParents(form, ["analyst"], ROLES).residencyValues).toEqual([]);
  });

  it("keeps the form as it is with no parent left", () => {
    const chosen = withParents(FORM, ["analyst"], ROLES);
    expect(withParents(chosen, [], ROLES).capabilities).toEqual(chosen.capabilities);
  });
});
