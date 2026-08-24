// Copyright (c) 2026 Kenneth Stott
// Canary: 0d0f5e22-e54b-4c15-a023-20edbe311131
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Nav structure + entry resolution, split from NavBar.tsx so that component
// file exports only components (react-refresh) — this is the single source the
// nav, the router capability table test, and App's group-entry navigation read.

import { hasCapability } from "../lib/capabilities";
import { LAST_SUBNAV_KEY } from "../lib/session";
import type { Capability } from "../types/auth";

export interface DropdownItem {
  to: string;
  labelKey: string;
  capability: Capability;
  comingSoon?: boolean;
  separatorBefore?: boolean;
  // REQ-1412: the surface exists only where the commercial plugin is installed — its router is
  // mounted by that plugin, so an installed Provisa would link to a 404. Resolved from the same
  // `billing` flag /auth/me reports for the billing tab.
  commercial?: boolean;
  // REQ-1512: the inverse of `commercial` — a surface an INSTALLED Provisa has and a hosted one
  // does not. The deployment-wide engine kind is fixed by the cluster the platform runs, so on
  // hosted this entry could only report a setting nobody may change or accept one the next deploy
  // overwrites. Resolved from the same `billing` flag, which is what says a deployment is hosted.
  installedOnly?: boolean;
}

export interface NavGroup {
  id: string;
  labelKey: string;
  items: DropdownItem[];
}

/** The group a route belongs to — the nav, the subnav and the admin rail all key off this. */
export function activeGroupId(pathname: string): string | null {
  for (const group of NAV_GROUPS) {
    if (
      group.items.some(
        (i) => !i.comingSoon && (pathname === i.to || pathname.startsWith(i.to + "/")),
      )
    ) {
      return group.id;
    }
  }
  return null;
}

export const NAV_GROUPS: NavGroup[] = [
  {
    id: "model",
    labelKey: "navBar.groupModel",
    items: [
      { to: "/views", labelKey: "navBar.itemViews", capability: "table_registration" },
      { to: "/metrics", labelKey: "navBar.itemMetrics", capability: "table_registration" }, // REQ-1317
      { to: "/commands", labelKey: "navBar.itemCommands", capability: "admin" },
      { to: "/lineage", labelKey: "navBar.itemLineage", capability: "admin" }, // REQ-1160/1161
    ],
  },
  {
    id: "security",
    labelKey: "navBar.groupSecurity",
    items: [
      { to: "/security/roles", labelKey: "navBar.itemRoles", capability: "access_config" },
      { to: "/security/rls", labelKey: "navBar.itemRlsRules", capability: "access_config" },
    ],
  },
  {
    id: "explore",
    labelKey: "navBar.groupExplore",
    items: [
      { to: "/schema", labelKey: "navBar.itemSchema", capability: "query_development" },
      {
        to: "/nl",
        labelKey: "navBar.itemNl",
        capability: "query_development",
        separatorBefore: true,
      },
      { to: "/query", labelKey: "navBar.itemGraphql", capability: "query_development" },
      { to: "/graph", labelKey: "navBar.itemCypher", capability: "query_development" },
      { to: "/sql", labelKey: "navBar.itemSql", capability: "query_development" },
      { to: "/grpc", labelKey: "navBar.itemGrpc", capability: "query_development" },
      { to: "/jsonapi", labelKey: "navBar.itemJsonApi", capability: "query_development" },
      { to: "/openapi", labelKey: "navBar.itemOpenApi", capability: "query_development" },
      { to: "/explore", labelKey: "navBar.itemExplore", capability: "query_development" },
    ],
  },
  {
    id: "admin",
    labelKey: "navBar.groupAdmin",
    items: [
      // REQ-1337: administering orgs other than the one being acted in is `cross_org`.
      { to: "/admin/orgs", labelKey: "navBar.itemOrgs", capability: "cross_org" },
      // Team management is org administration, so it lives in this group; its route stays /team.
      { to: "/team", labelKey: "navBar.team", capability: "user_management" },
      // REQ-1349: the org-scoped rights. `observability` is read-only performance and health;
      // `org_settings` covers the surfaces whose subject is the acting org itself. org_admin holds
      // both in either tenancy mode, so an org administrator gets a useful Admin tab without any
      // reach into deployment-wide settings or into another org.
      // The dashboard: the catalog counts and the component health table, which were two nav
      // entries showing one page's worth of read-only status between them.
      { to: "/admin/overview", labelKey: "navBar.itemDashboard", capability: "observability" },
      { to: "/admin/domains", labelKey: "navBar.itemDomains", capability: "org_settings" },
      {
        // REQ-1573: environments have their own right rather than riding on org_settings — a
        // developer manages them and holds no org settings, and an analyst holds neither.
        to: "/admin/environments",
        labelKey: "navBar.itemEnvironments",
        capability: "environment_management",
      },
      // REQ-1337: cache storage, the federation engine and the encryption/auth providers are
      // DEPLOYMENT-WIDE settings, so each is gated on the `platform_settings` RIGHT rather than on a
      // role name. The seed grants it to platform_admin always and to org_admin only in a
      // single-tenant deployment (apply_tenancy_role_grants), so a multitenant org_admin never sees
      // these entries.
      { to: "/admin/cache", labelKey: "navBar.itemCache", capability: "org_settings" },
      {
        to: "/admin/scheduled-tasks",
        labelKey: "navBar.itemScheduler",
        capability: "org_settings",
      },
      {
        to: "/admin/federation-engine",
        labelKey: "navBar.itemFederation",
        capability: "platform_settings",
        installedOnly: true, // REQ-1512
      },
      // REQ-1412: which engine lane THIS org runs on (shared / isolated / external) is the org's
      // own setting, not the deployment's — org_settings, so a multitenant org_admin has it. Hosted
      // deployments only: choosing among coordinators the PLATFORM offers is not a question an
      // installed Provisa has, and its router ships in the commercial plugin.
      {
        to: "/admin/org-engine",
        // Labelled "Federation" like the deployment-wide tab: the two never appear together --
        // that one needs platform_settings, this one org_settings -- and to a tenant administrator
        // the lane their queries run on IS the federation setting they have.
        labelKey: "navBar.itemFederation",
        capability: "org_settings",
        commercial: true,
      },
      { to: "/admin/security", labelKey: "navBar.itemSecurity", capability: "platform_settings" },
      // REQ-1560: two surfaces, named for whose they are. They are separate entries rather than one
      // Secrets page with a filter because the question "may I see this" is answered differently for
      // each: the org vault is an administrator's list of what the org connects to (org_settings),
      // and a person's own vault is nobody's to grant -- every member has one, so `usage`, the right
      // every seeded role carries. This supersedes REQ-1558's single sub-tab under Security.
      { to: "/admin/secrets", labelKey: "navBar.itemOrgSecrets", capability: "org_settings" },
      { to: "/admin/my-secrets", labelKey: "navBar.itemMySecrets", capability: "usage" },
      // REQ-1466: the scheduled-downtime banner is turned on and off for the whole deployment, so
      // platform_settings — the same right that gates the engine topology switch it announces.
      {
        to: "/admin/maintenance",
        labelKey: "navBar.itemMaintenance",
        capability: "platform_settings",
      },
      { to: "/admin/ai-models", labelKey: "navBar.itemAiModels", capability: "org_settings" },
      // REQ-1074: the catalog this org publishes to is the org's setting, not the deployment's.
      {
        to: "/admin/metadata-export",
        labelKey: "navBar.itemMetadataExport",
        capability: "org_settings",
      },
      // REQ-1483: importing a Hasura project rewrites the acting org's semantic layer.
      { to: "/admin/import", labelKey: "navBar.itemImport", capability: "org_settings" },
      // REQ-1374: the tag registry is the acting org's own metadata, so org_settings.
      { to: "/admin/tags", labelKey: "navBar.itemTags", capability: "org_settings" },
      { to: "/admin/reports", labelKey: "navBar.itemReports", capability: "observability" }, // REQ-1386
      // REQ-1387: the glossary route stays /admin/glossary (org_settings) but its nav entry is
      // the top-level Glossary link in NavBar.tsx, not an item of this group.
      {
        to: "/admin/observability",
        labelKey: "navBar.itemObservability",
        capability: "observability",
      },
      { to: "/admin/requests", labelKey: "navBar.itemRequests", capability: "org_settings" },
    ],
  },
];

// Remembers the last submenu item visited within each group so returning to a
// group restores that item instead of always landing on the first one.
// The key itself lives in lib/session so clearSessionState drops it at sign-in.
function readLastSubnav(): Record<string, string> {
  const raw = localStorage.getItem(LAST_SUBNAV_KEY);
  if (!raw) return {};
  const parsed: unknown = JSON.parse(raw);
  return parsed && typeof parsed === "object" ? (parsed as Record<string, string>) : {};
}

export function writeLastSubnav(groupId: string, to: string) {
  localStorage.setItem(LAST_SUBNAV_KEY, JSON.stringify({ ...readLastSubnav(), [groupId]: to }));
}

// The submenu item within a group to navigate to on entry: the remembered one (if still valid,
// available AND permitted) or the first non-comingSoon item the caller's rights admit.
//
// REQ-1349: the remembered item is a browser preference, not an entitlement. Rights change under
// it — a deployment flips to multitenant and org_admin loses `platform_settings`, an org grant is
// withdrawn — and the subnav item it names then disappears while the entry navigation still aimed
// at it. Entering the group landed on the denied route, whose NotAuthorized fallback replaces the
// whole page INCLUDING this subnav, so there was nothing left to click to reach a permitted tab.
// A preference that no longer resolves is dropped in favour of the first tab that does.
export function entryItem(
  group: NavGroup,
  capabilities: string[],
  commercial = true,
): DropdownItem | undefined {
  const reachable = group.items.filter(
    (i) =>
      !i.comingSoon &&
      hasCapability(capabilities, i.capability) &&
      !(i.commercial && !commercial) &&
      !(i.installedOnly && commercial),
  );
  const remembered = readLastSubnav()[group.id];
  return reachable.find((i) => i.to === remembered) ?? reachable[0];
}
