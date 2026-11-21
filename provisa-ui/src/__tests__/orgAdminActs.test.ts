// Copyright (c) 2026 Kenneth Stott
// Canary: 5d1c7e2a-93b4-4f60-a8e1-2c6b9f0d47a3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1327 (amended 2026-11-14, 2026-11-16): deleting an organization, granting org_admin and
// revoking it are an org_admin's acts; user_management alone allows none of them, and a control
// is shown only to a caller the server would accept. The client reads the rule as the server
// does (rights.org_admin_rights_missing, rights.admin_role_grant_missing): every right org_admin
// carries, or cross_org.

import { describe, it, expect } from "vitest";
import { administersOrg, conferrableRoles } from "../lib/capabilities";
import type { Role } from "../types/auth";

function role(id: string, effectiveCapabilities: string[]): Role {
  return {
    id,
    capabilities: effectiveCapabilities,
    effectiveCapabilities,
    demonstrated: [],
    domain_access: ["*"],
  } as unknown as Role;
}

const ORG_ADMIN = role("org_admin", ["user_management", "org_settings", "table_registration"]);
const ANALYST = role("analyst", ["query"]);
const ROLES = [ORG_ADMIN, ANALYST];

describe("administersOrg", () => {
  it("is false for a holder of user_management alone", () => {
    expect(administersOrg(ROLES, ["user_management"])).toBe(false);
  });

  it("is true for a holder of every right org_admin carries", () => {
    expect(administersOrg(ROLES, ["user_management", "org_settings", "table_registration"])).toBe(
      true,
    );
  });

  it("is true for the platform, which appoints an org's admins", () => {
    expect(administersOrg(ROLES, ["cross_org"])).toBe(true);
  });

  it("is false before the org's roles are read", () => {
    expect(administersOrg([], ["user_management", "org_settings", "table_registration"])).toBe(
      false,
    );
  });
});

describe("conferrableRoles", () => {
  it("leaves out a role carrying user_management for a caller lacking its other rights", () => {
    expect(conferrableRoles(ROLES, ["user_management"]).map((r) => r.id)).toEqual(["analyst"]);
  });

  it("reads rights, not the role's name", () => {
    const steward = role("people_lead", ["user_management", "access_config"]);
    expect(conferrableRoles([steward, ANALYST], ["user_management"]).map((r) => r.id)).toEqual([
      "analyst",
    ]);
  });

  it("offers every role to an org_admin", () => {
    expect(
      conferrableRoles(ROLES, ["user_management", "org_settings", "table_registration"]).map(
        (r) => r.id,
      ),
    ).toEqual(["org_admin", "analyst"]);
  });

  it("offers every role to the platform", () => {
    expect(conferrableRoles(ROLES, ["cross_org"]).map((r) => r.id)).toEqual([
      "org_admin",
      "analyst",
    ]);
  });
});
