// Copyright (c) 2026 Kenneth Stott
// Canary: 5a4f10c7-9d2e-4c31-8f0b-6c1de7a94b52
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1361: the platform wildcard does not answer for an org's secrets, in the menu either.
//
// The server refuses the org vault to a platform admin by name, and SecretsTab renders no org half
// without a literal `org_settings`. The nav entry, gated through `hasCapability`, went on saying
// "Org Secrets" to the one caller the whole rule is about — a link to a page with nothing of that
// org on it. What a platform admin does reach at that route is the DEPLOYMENT's secrets service,
// so the entry stays and is named for what it actually opens.

import { describe, it, expect } from "vitest";
import { readFileSync } from "fs";
import { resolve } from "path";
import { NAV_GROUPS, labelKeyFor } from "../components/navGroups";
import { meetsRequirement } from "../lib/capabilities";

const admin = NAV_GROUPS.find((g) => g.id === "admin")!;
const secrets = admin.items.find((i) => i.to === "/admin/secrets")!;

describe("the Org Secrets nav entry", () => {
  it("is not opened on the org right by anything but the org right", () => {
    for (const held of [["admin"], ["superadmin"], ["platform_settings", "cross_org"]]) {
      expect(meetsRequirement(held, { capability: secrets.capability }), held.join()).toBe(false);
    }
  });

  it("names the deployment's half for a caller who holds only platform authority", () => {
    // platform_admin holds platform_settings and cross_org; the entry survives on the former.
    const platformAdmin = ["platform_settings", "cross_org"];
    expect(meetsRequirement(platformAdmin, secrets)).toBe(true);
    expect(labelKeyFor(secrets, platformAdmin)).toBe("navBar.itemPlatformSecrets");
  });

  it("names the org's half for the administrator of that org", () => {
    expect(meetsRequirement(["org_settings"], secrets)).toBe(true);
    expect(labelKeyFor(secrets, ["org_settings"])).toBe("navBar.itemOrgSecrets");
  });

  it("is absent entirely for a caller holding neither right", () => {
    expect(meetsRequirement(["usage"], secrets)).toBe(false);
  });

  it("opens every admin entry on the right it names and on nothing in its place", () => {
    // REQ-1327: the rule is the nav's, not one entry's. The retired wildcard strings open no
    // entry, and each entry opens for a caller holding exactly its own right.
    for (const item of admin.items) {
      expect(meetsRequirement(["admin", "superadmin"], item), item.to).toBe(false);
      expect(meetsRequirement([item.capability], item), item.to).toBe(true);
      expect(labelKeyFor(item, [item.capability]), item.to).toBe(item.labelKey);
    }
  });

  it("shows a platform administrator only the entries the platform rights open", () => {
    const platformAdmin = ["platform_settings", "cross_org"];
    const dataRights = ["org_settings", "observability", "user_management", "usage"];
    for (const item of admin.items) {
      const opens = meetsRequirement(platformAdmin, item);
      const namesPlatformRight =
        platformAdmin.includes(item.capability) ||
        (item.orCapability !== undefined && platformAdmin.includes(item.orCapability));
      expect(opens, item.to).toBe(namesPlatformRight);
      if (dataRights.includes(item.capability) && item.orCapability === undefined) {
        expect(opens, item.to).toBe(false);
      }
    }
  });

  it("titles the page for the half the caller reaches", () => {
    // The heading is the same statement as the menu label: a platform admin opening /admin/secrets
    // is looking at the deployment's secrets service, and "Org Secrets" over it would name a thing
    // that is not on the page.
    const source = readFileSync(resolve(__dirname, "..", "pages/AdminPage.tsx"), "utf-8");
    const at = source.indexOf("const heading =");
    expect(at).toBeGreaterThan(-1);
    const decl = source.slice(at, at + 300);
    expect(decl).toContain('!capabilities.includes("org_settings")');
    expect(decl).toContain('"Platform Secrets"');
  });
});
