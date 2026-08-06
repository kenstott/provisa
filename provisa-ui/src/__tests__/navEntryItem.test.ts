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
import { LAST_SUBNAV_KEY, SESSION_KEYS } from "../lib/session";

const admin = NAV_GROUPS.find((g) => g.id === "admin")!;

function remember(to: string) {
  localStorage.setItem(LAST_SUBNAV_KEY, JSON.stringify({ admin: to }));
}

describe("entryItem", () => {
  beforeEach(() => localStorage.clear());

  it("honours the remembered item when the caller still holds its right", () => {
    remember("/admin/system-health");
    expect(entryItem(admin, ["observability"])?.to).toBe("/admin/system-health");
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

  it("gives the platform wildcard the first-listed item", () => {
    expect(entryItem(admin, ["admin"])?.to).toBe("/admin/orgs");
  });

  it("returns undefined when the caller holds no admin surface", () => {
    expect(entryItem(admin, ["query_development"])).toBeUndefined();
    expect(entryItem(admin, [])).toBeUndefined();
  });

  it("clears the remembered item at session start so it cannot cross identities", () => {
    expect(SESSION_KEYS).toContain(LAST_SUBNAV_KEY);
  });
});
