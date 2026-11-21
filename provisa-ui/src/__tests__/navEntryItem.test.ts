// Copyright (c) 2026 Kenneth Stott
// Canary: 1103d863-c8dd-4c46-a716-ab88ceb73e8a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect, beforeEach } from "vitest";
import { NAV_GROUPS, entryItem } from "../components/navGroups";
import { LAST_SUBNAV_KEY, SESSION_SCOPED_KEYS } from "../lib/session";
import { storageKey as scopedKey } from "../lib/browserStorage";

const admin = NAV_GROUPS.find((g) => g.id === "admin")!;

function remember(to: string) {
  localStorage.setItem(scopedKey(LAST_SUBNAV_KEY), JSON.stringify({ admin: to }));
}

describe("entryItem", () => {
  beforeEach(() => localStorage.clear());

  it("honours the remembered item when the caller still holds its right", () => {
    remember("/admin/reports");
    expect(entryItem(admin, ["observability"])?.to).toBe("/admin/reports");
  });

  it("drops a remembered item the caller may not reach, landing on the first permitted tab", () => {
    // REQ-1349: the multitenant org_admin case — the remembered federation-engine tab needs
    // platform_settings, which they do not hold.
    remember("/admin/federation-engine");
    expect(entryItem(admin, ["org_settings", "observability"])?.to).toBe("/admin/overview");
  });

  it("never returns the first-listed item when its right is absent", () => {
    // /admin/orgs is items[0] and needs cross_org.
    expect(admin.items[0].to).toBe("/admin/orgs");
    expect(entryItem(admin, ["org_settings"])?.to).toBe("/admin/domains");
  });

  it("gives a platform administrator the first-listed item, on cross_org", () => {
    expect(entryItem(admin, ["platform_settings", "cross_org"])?.to).toBe("/admin/orgs");
  });

  it("gives the retired wildcard strings only the pages every member opens", () => {
    // REQ-1327: no capability stands in for another. My Secrets and Requests ask no right
    // (amended 2026-11-14), so they are all a wildcard string reaches, as for a caller with none.
    expect(entryItem(admin, ["admin"])?.to).toBe("/admin/my-secrets");
    expect(entryItem(admin, ["superadmin"])?.to).toBe("/admin/my-secrets");
  });

  it("enters on My Secrets when the caller holds no admin right", () => {
    // REQ-1560, REQ-1327: every member has a vault of their own, so Admin always has a page.
    expect(entryItem(admin, ["query_development"])?.to).toBe("/admin/my-secrets");
    expect(entryItem(admin, [])?.to).toBe("/admin/my-secrets");
  });

  it("hides the org-engine tab on a deployment without the commercial plugin", () => {
    // REQ-1412: its router ships in provisa_commercial, so an installed Provisa would link to a 404.
    remember("/admin/org-engine");
    expect(entryItem(admin, ["org_settings"], false)?.to).not.toBe("/admin/org-engine");
    expect(entryItem(admin, ["org_settings"], true)?.to).toBe("/admin/org-engine");
  });

  it("hides the deployment-wide federation engine on a hosted deployment", () => {
    // REQ-1512: hosted orgs get their engine from their plan, so the deployment-wide engine screen
    // is an installed-Provisa surface only — the same `billing` flag that says a deployment is
    // hosted is what removes it.
    remember("/admin/federation-engine");
    expect(entryItem(admin, ["platform_settings"], true)?.to).not.toBe("/admin/federation-engine");
    expect(entryItem(admin, ["platform_settings", "cross_org"], false)?.to).toBe(
      "/admin/federation-engine",
    );
  });

  it("clears the remembered item at session start so it cannot cross identities", () => {
    expect(SESSION_SCOPED_KEYS).toContain(LAST_SUBNAV_KEY);
  });
});
