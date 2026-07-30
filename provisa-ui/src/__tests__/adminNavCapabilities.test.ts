// Copyright (c) 2026 Kenneth Stott
// Canary: 4d0b7e59-1c86-4a72-9f35-6ba82c1d0e73
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1349 / REQ-1337: which right each Admin surface is gated on, asserted at the source.
//
// The nav decides what an administrator SEES and the route table decides what they can REACH by
// typing the URL. A disagreement between the two is not cosmetic: a nav entry gated more loosely
// than its route renders a link straight into "You do not have permission to view this page", and
// the reverse hides a surface the user is entitled to. Both tables are plain literals, so they are
// read from the source rather than by mounting NavBar with its router, tour and domain-filter
// context — what is asserted here is the table itself, which is the thing that was wrong.

import { describe, it, expect } from "vitest";
import { readFileSync } from "fs";
import { resolve } from "path";

const SRC = resolve(__dirname, "..");

function navBarAdminCapabilities(): Record<string, string> {
  const source = readFileSync(resolve(SRC, "components/NavBar.tsx"), "utf-8");
  const out: Record<string, string> = {};
  const entry = /to:\s*"(\/admin\/[^"]+)"[\s\S]*?capability:\s*"([a-z_]+)"/g;
  for (const m of source.matchAll(entry)) out[m[1]] = m[2];
  return out;
}

function routeCapabilities(): Record<string, string> {
  const source = readFileSync(resolve(SRC, "App.tsx"), "utf-8");
  const out: Record<string, string> = {};
  const entry = /\["(\/admin\/[^"]+)",\s*"([a-z_]+)"\]/g;
  for (const m of source.matchAll(entry)) out[m[1]] = m[2];
  return out;
}

// The surfaces an org administrator owns, and the right each is gated on. `org_settings` is
// "settings whose subject is the acting org"; `observability` is "read-only performance and
// health". Neither implies any reach into the deployment or into another org.
const ORG_SCOPED: Record<string, string> = {
  "/admin/overview": "observability",
  "/admin/system-health": "observability",
  "/admin/observability": "observability",
  "/admin/ai-models": "org_settings",
  "/admin/domains": "org_settings",
  "/admin/scheduled-tasks": "org_settings",
  "/admin/requests": "org_settings",
};

// Deployment-wide surfaces. A multitenant org_admin does not hold `platform_settings`
// (apply_tenancy_role_grants withdraws it), so none of these appear for one.
const DEPLOYMENT_WIDE = [
  "/admin/cache",
  "/admin/federation-engine",
  "/admin/encryption",
  "/admin/auth",
  "/admin/security",
];

describe("admin surface capabilities", () => {
  it("gates every org-scoped surface on the org-scoped right", () => {
    const routes = routeCapabilities();
    for (const [path, capability] of Object.entries(ORG_SCOPED)) {
      // /admin/requests carries its own <Route> rather than a table row, so it is checked below.
      if (path === "/admin/requests") continue;
      expect(routes[path], path).toBe(capability);
    }
  });

  it("gates the approvals route on org_settings", () => {
    const source = readFileSync(resolve(SRC, "App.tsx"), "utf-8");
    const requests = source.slice(source.indexOf('path="/admin/requests"'));
    expect(requests.slice(0, 400)).toContain('capability="org_settings"');
  });

  it("keeps every deployment-wide surface on platform_settings", () => {
    const routes = routeCapabilities();
    for (const path of DEPLOYMENT_WIDE) {
      expect(routes[path], path).toBe("platform_settings");
    }
  });

  it("gates cross-org administration on cross_org", () => {
    // Administering an org you are not acting in is the one thing org authority never covers.
    expect(routeCapabilities()["/admin/orgs"]).toBe("cross_org");
  });

  it("never gates an org-scoped surface on the platform wildcard", () => {
    // `admin` is platform bypass, held only by platform_admin. Gating an org's own surface on it
    // is what made the Admin tab empty for an org administrator in the first place.
    const routes = routeCapabilities();
    const nav = navBarAdminCapabilities();
    for (const path of Object.keys(ORG_SCOPED)) {
      expect(routes[path] ?? nav[path], path).not.toBe("admin");
      expect(routes[path] ?? nav[path], path).not.toBe("superadmin");
    }
  });

  it("shows an org administrator exactly the surfaces they can reach", () => {
    // Every nav entry that names a right must name the SAME right its route does, or the link
    // leads to a permission error (or hides a surface the user owns).
    const nav = navBarAdminCapabilities();
    const routes = routeCapabilities();
    for (const [path, capability] of Object.entries(nav)) {
      if (!(path in routes)) continue; // /admin/requests — its own Route, asserted above
      expect(routes[path], path).toBe(capability);
    }
  });

  it("lists every org-scoped surface in the nav", () => {
    // A surface an org administrator may reach but cannot see is unreachable in practice.
    const nav = navBarAdminCapabilities();
    for (const [path, capability] of Object.entries(ORG_SCOPED)) {
      expect(nav[path], path).toBe(capability);
    }
  });
});
