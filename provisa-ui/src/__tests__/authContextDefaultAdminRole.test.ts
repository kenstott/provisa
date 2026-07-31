// Copyright (c) 2026 Kenneth Stott
// Canary: 49aaf19f-c497-4cdd-8e3a-16709eefdf1c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-1352: `DEFAULT_ADMIN_ROLE` is a second copy of org_admin's definition.
 *
 * The role's capabilities belong to `provisa/core/schema.sql` alone (REQ-1352), but a dev/no-auth
 * deployment answers `/auth/me` before any org schema is reachable, so the client needs a literal
 * to mirror what the server grants an unsecured caller. A literal copy drifts: it had gained
 * `ad_hoc_query` and `read_restricted`, which the seed does not grant, and never gained the
 * REQ-1349 pair `org_settings`/`observability` — so a dev-mode org administrator's capability set
 * admitted no admin route and the Admin navigation group was not rendered at all.
 *
 * Parsing the seed rather than restating it here is the point: a second hand-written list would
 * drift the same way.
 */

import { readFileSync } from "node:fs";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import { DEFAULT_ADMIN_ROLE } from "../context/AuthContext";

function seededOrgAdminCapabilities(): string[] {
  const sql = readFileSync(join(__dirname, "../../../provisa/core/schema.sql"), "utf8");
  const start = sql.indexOf("    'org_admin',");
  expect(start, "schema.sql must seed org_admin").toBeGreaterThan(-1);
  const block = sql.slice(start, sql.indexOf("ON CONFLICT", start));
  const list = block.slice(block.indexOf("'["), block.indexOf("]'") + 1);
  return (JSON.parse(list.slice(1)) as string[]).map(String);
}

describe("DEFAULT_ADMIN_ROLE", () => {
  it("is org_admin's schema.sql seed verbatim", () => {
    expect([...DEFAULT_ADMIN_ROLE.capabilities].sort()).toEqual(
      [...seededOrgAdminCapabilities()].sort(),
    );
  });

  it("carries the REQ-1349 org-scoped rights the Admin group needs", () => {
    expect(DEFAULT_ADMIN_ROLE.capabilities).toContain("org_settings");
    expect(DEFAULT_ADMIN_ROLE.capabilities).toContain("observability");
  });

  it("carries no platform-bypass or cross-org right", () => {
    // REQ-1297/REQ-1337: those belong to platform_admin. `cross_org` is what marks a role
    // control-plane, and isControlPlaneOnly() would sort this role off the data plane entirely.
    for (const right of ["admin", "superadmin", "platform_settings", "cross_org"]) {
      expect(DEFAULT_ADMIN_ROLE.capabilities).not.toContain(right);
    }
  });

  it("grants every domain", () => {
    expect(DEFAULT_ADMIN_ROLE.domain_access).toEqual(["*"]);
  });
});
