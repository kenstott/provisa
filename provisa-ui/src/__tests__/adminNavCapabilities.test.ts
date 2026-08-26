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
  const source = readFileSync(resolve(SRC, "components/navGroups.ts"), "utf-8");
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
  "/admin/metadata-export": "org_settings",
  "/admin/import": "org_settings", // REQ-1483: Hasura v2 / DDN import
  "/admin/tags": "org_settings",
  "/admin/reports": "observability", // REQ-1386: ops-domain report viewer (read-only)
  // REQ-1590: the org's own business glossary, on its own read right. Looking a term up to
  // understand a column is not administering the org, so every seeded role reaches the surface;
  // curation inside it is gated on `glossary_rw`.
  "/admin/glossary": "glossary_read", // REQ-1387
  "/admin/domains": "org_settings",
  // REQ-1487: the org's environments — its branches, the merges between them, and the repository
  // the model is projected into. All of it is the acting org's own model, so none of it is
  // deployment-wide.
  "/admin/environments": "environment_management", // REQ-1573
  // REQ-1558: the names an org holds and the values behind them are the org's, so an org_admin
  // owns them and a platform admin operating the control plane does not read them (REQ-1361).
  "/admin/secrets": "org_settings",
  // REQ-1560: a person's own vault. Not a grant an administrator makes — `usage` is the right every
  // seeded role carries, so every member of an org reaches their own secrets and nobody else's.
  "/admin/my-secrets": "usage",
  "/admin/scheduled-tasks": "org_settings",
  "/admin/requests": "org_settings",
  // REQ-1412: which engine lane the org runs on (shared / SaaS-isolated / its own external
  // coordinator) is the org's decision. The engine KIND stays deployment-wide, below.
  "/admin/org-engine": "org_settings",
  // REQ-1469: the org's plan, running bill and next charge. Billing is the org's own commercial
  // relationship, so an org_admin owns it without holding anything deployment-wide.
  "/admin/billing": "org_settings",
  // REQ-1349: the cache surface is the org's own cached results, response TTL and redirect policy.
  // The one deployment-wide half of it — which store the node writes to — is gated inside the page
  // on `platform_settings`, so the route itself belongs to the org.
  "/admin/cache": "org_settings",
};

// Deployment-wide surfaces. A multitenant org_admin does not hold `platform_settings`
// (apply_tenancy_role_grants withdraws it), so none of these appear for one.
const DEPLOYMENT_WIDE = [
  "/admin/federation-engine",
  "/admin/encryption",
  "/admin/auth",
  "/admin/security",
  // REQ-1466: the scheduled-downtime banner speaks for the whole deployment; one org's admin must
  // not be able to tell every other org's users that the platform is down.
  "/admin/maintenance",
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
      // The glossary's nav entry is the top-level NavBar link, asserted below.
      if (path === "/admin/glossary") continue;
      // REQ-1469: billing's nav entry is the account-menu item in NavBar.tsx, asserted below.
      if (path === "/admin/billing") continue;
      // The health table was merged into the dashboard, so /admin/system-health is a deep link to
      // /admin/overview's section rather than a nav entry of its own.
      if (path === "/admin/system-health") continue;
      expect(nav[path], path).toBe(capability);
    }
  });

  it("shows the glossary as a top-level nav entry gated on its route's right", () => {
    // REQ-1387: Glossary is a top-level menu item, not an Admin group item — but the gate
    // must still match the /admin/glossary route or the link 403s (or hides the surface).
    const source = readFileSync(resolve(SRC, "components/NavBar.tsx"), "utf-8");
    const linkAt = source.indexOf('to="/admin/glossary"');
    expect(linkAt).toBeGreaterThan(-1);
    const gate = source.slice(Math.max(0, linkAt - 200), linkAt);
    expect(gate).toContain('capability="glossary_read"'); // REQ-1590
  });

  it("gates every top-level nav link on the right its route requires", () => {
    // The Relationships link shipped ungated while /relationships requires create_relationship, so
    // an analyst saw the tab and landed on the permission error. Each of these routes carries its
    // own <Route> rather than a table row, so the gate is read out of App.tsx around the path.
    const navSource = readFileSync(resolve(SRC, "components/NavBar.tsx"), "utf-8");
    const appSource = readFileSync(resolve(SRC, "App.tsx"), "utf-8");
    for (const path of ["/sources", "/tables", "/relationships"]) {
      const routeAt = appSource.indexOf(`path="${path}"`);
      expect(routeAt, path).toBeGreaterThan(-1);
      const capability = /capability="([a-z_]+)"/.exec(
        appSource.slice(routeAt, routeAt + 400),
      )?.[1];
      const linkAt = navSource.indexOf(`to="${path}"`);
      expect(linkAt, path).toBeGreaterThan(-1);
      expect(navSource.slice(Math.max(0, linkAt - 300), linkAt), path).toContain(
        `capability="${capability}"`,
      );
    }
  });

  it("shows billing in the account menu gated on the deployment flag and its route's right", () => {
    // REQ-1469: billing sits in the person pulldown, not the Admin group. The item must be gated
    // both on `billing` (self-hosted deployments do not mount the routes) and on the same right
    // the /admin/billing route requires, or the link 404s or hides the surface.
    const source = readFileSync(resolve(SRC, "components/NavBar.tsx"), "utf-8");
    const itemAt = source.indexOf('data-testid="navbar-billing"');
    expect(itemAt).toBeGreaterThan(-1);
    const gate = source.slice(Math.max(0, itemAt - 300), itemAt);
    expect(gate).toContain("billing &&");
    expect(gate).toContain('hasCapability(capabilities, "org_settings")');
  });
});
