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

import { IMPORT_RIGHTS, meetsRequirement } from "../lib/capabilities";
import { LAST_SUBNAV_KEY } from "../lib/session";
import type { Capability } from "../types/auth";
import { getStoredJson, setStored } from "../lib/browserStorage";

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
  // A second right that also opens the entry, for a surface that carries two things gated
  // differently.
  orCapability?: Capability;
  // Further rights, any one of which also opens the entry (CapabilityRequirement.anyOf).
  anyOf?: Capability[];
  // REQ-1327: a right needed besides whichever of the above opened the entry. A platform page asks
  // the deployment check, `platform_settings` and `cross_org`, as its routes on the server do.
  andCapability?: Capability;
  // REQ-1922: the surface exists only where the platform declares regions (useRegionChoices).
  regionsOnly?: boolean;
  // REQ-1922: a demo sandbox's visitor may open it to read, though its capability is not theirs.
  sandboxReadable?: boolean;
  // REQ-1327: where the org has regions, the right that opens the entry to read them, a demo
  // sandbox's visitor reading them too (RegionReadableGate).
  regionsCapability?: Capability;
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
      // Gated with the rest of the Model group: the endpoints behind Commands require no capability
      // of their own, and `admin` is the platform wildcard, so naming it here hid the entry from
      // every org role.
      { to: "/commands", labelKey: "navBar.itemCommands", capability: "table_registration" },
      // REQ-1160/1161: gated with the rest of the Model group. `admin` is the platform wildcard, so
      // naming it here hid Lineage from every org role while the endpoint required nothing.
      { to: "/lineage", labelKey: "navBar.itemLineage", capability: "view_governance" },
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
      { to: "/sparql", labelKey: "navBar.itemSparql", capability: "query_development" },
      { to: "/sql", labelKey: "navBar.itemSql", capability: "query_development" },
      { to: "/grpc", labelKey: "navBar.itemGrpc", capability: "query_development" },
      { to: "/jsonapi", labelKey: "navBar.itemJsonApi", capability: "query_development" },
      { to: "/odata", labelKey: "navBar.itemOData", capability: "query_development" }, // REQ-2193
      { to: "/openapi", labelKey: "navBar.itemOpenApi", capability: "query_development" },
      { to: "/explore", labelKey: "navBar.itemExplore", capability: "query_development" },
      // REQ-2071: Python over the role's governed data, as a script and as a Jupyter notebook.
      { to: "/python", labelKey: "navBar.itemPython", capability: "query_development" },
      { to: "/notebook", labelKey: "navBar.itemNotebook", capability: "query_development" },
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
        // REQ-1573: environments have their own right rather than riding on org_settings. REQ-2155:
        // org_admin manages any; a developer manages the ones it owns; an analyst neither.
        to: "/admin/environments",
        labelKey: "navBar.itemEnvironments",
        capability: "environment_management",
        orCapability: "environment_owned_management",
      },
      // REQ-1337, REQ-1327: the federation engine and the encryption/auth providers are
      // DEPLOYMENT-WIDE settings, so each asks the deployment check, `platform_settings` and
      // `cross_org`: a single-tenant org_admin holds the first (apply_tenancy_role_grants) and never
      // the second, so it is not shown a page whose every call it would be refused.
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
        andCapability: "cross_org",
        installedOnly: true, // REQ-1512
        // REQ-1922, REQ-1327: where the org has regions the page is the engine the platform assigned
        // it in each, which the org reads.
        regionsCapability: "org_settings",
        sandboxReadable: true,
      },
      // REQ-1412: which engine lane THIS org runs on (shared / isolated / external) is the org's
      // own setting, not the deployment's — org_settings, so a multitenant org_admin has it. Hosted
      // deployments only: choosing among coordinators the PLATFORM offers is not a question an
      // installed Provisa has, and its router ships in the commercial plugin.
      {
        to: "/admin/org-engine",
        // Labelled "Federation" like the deployment-wide tab: on a hosted deployment, the only
        // place this one shows, that one is not offered, and to a tenant administrator the lane
        // their queries run on IS the federation setting they have.
        labelKey: "navBar.itemFederation",
        capability: "org_settings",
        commercial: true,
      },
      {
        to: "/admin/security",
        labelKey: "navBar.itemSecurity",
        capability: "platform_settings",
        andCapability: "cross_org",
      },
      // REQ-1560: two surfaces, named for whose they are. They are separate entries rather than one
      // Secrets page with a filter because the question "may I see this" is answered differently for
      // each: the org vault is an administrator's list of what the org connects to (org_settings),
      // and a person's own vault is nobody's to grant -- every member has one, so `usage`, the right
      // every seeded role carries. This supersedes REQ-1558's single sub-tab under Security.
      // REQ-1361, REQ-1327: an org's secrets answer to the administrator of that org and to nobody
      // above it. The deployment's choice of secrets SERVICE is a platform setting, under Security.
      { to: "/admin/secrets", labelKey: "navBar.itemOrgSecrets", capability: "org_settings" },
      { to: "/admin/my-secrets", labelKey: "navBar.itemMySecrets", capability: "usage" },
      // REQ-1576: the mail transport platform communications go out through. Deployment-wide, and
      // an invitation is the platform's message rather than any org's, so platform_settings.
      // REQ-1923: the page also holds the organisation's mail platforms (the client its sources
      // sign in to Google or Microsoft with), which are the organisation's: org_settings opens it
      // too, and each half of the page shows only to a holder of its own right.
      {
        to: "/admin/email",
        labelKey: "navBar.itemEmail",
        capability: "platform_settings",
        orCapability: "org_settings",
      },
      // REQ-1466: the scheduled-downtime banner is turned on and off for the whole deployment, so
      // the deployment check — the same one that gates the engine topology switch it announces.
      {
        to: "/admin/maintenance",
        labelKey: "navBar.itemMaintenance",
        capability: "platform_settings",
        andCapability: "cross_org",
      },
      // REQ-1913: every operator setting, editable, on the deployment check its routes ask.
      {
        to: "/admin/deployment-settings",
        labelKey: "navBar.itemDeploymentSettings",
        capability: "platform_settings",
        andCapability: "cross_org",
      },
      // REQ-1922: the platform's regions, the default grant and each org's grant. A node reads them
      // before it opens any store, so they are the platform administrator's, applied on restart. A
      // demo sandbox shows the regions read-only, to explain them.
      {
        to: "/admin/platform",
        labelKey: "navBar.itemPlatform",
        capability: "platform_settings",
        andCapability: "cross_org",
        sandboxReadable: true,
      },
      // REQ-1922: the regions the platform granted the org and their stores, read-only. Shown only
      // where the platform declares regions: with none, region is not a concept anywhere a user sees.
      {
        to: "/admin/regions",
        labelKey: "navBar.itemRegions",
        capability: "org_settings",
        regionsOnly: true,
      },
      { to: "/admin/ai-models", labelKey: "navBar.itemAiModels", capability: "org_settings" },
      // REQ-1984: the registries every Node install of the organisation goes through.
      {
        to: "/admin/package-registries",
        labelKey: "navBar.itemPackageRegistries",
        capability: "org_settings",
      },
      // REQ-2098: the registries a code source's built image is pushed to.
      {
        to: "/admin/container-registries",
        labelKey: "navBar.itemContainerRegistries",
        capability: "org_settings",
      },
      // REQ-2122: the catalogs this org connects to once, for export and import.
      { to: "/admin/catalogs", labelKey: "navBar.itemCatalogs", capability: "org_settings" },
      // REQ-2185: the documents the org's assistants draw on and cite, curated with the glossary
      // (term finding adds them), so the glossary's curation right (REQ-1327).
      {
        to: "/admin/reference-library",
        labelKey: "navBar.itemReferenceLibrary",
        capability: "glossary_rw",
      },
      // REQ-1074: the catalog this org publishes to is the org's setting, not the deployment's.
      {
        to: "/admin/metadata-export",
        labelKey: "navBar.itemMetadataExport",
        capability: "org_settings",
      },
      // REQ-1483, REQ-1327: an import writes objects of several layers; any one of their rights
      // opens the page, and applying checks each declared object's own right.
      {
        to: "/admin/import",
        labelKey: "navBar.itemImport",
        capability: "table_registration",
        anyOf: IMPORT_RIGHTS,
      },
      // REQ-1374, REQ-1944: tags are edited by a table editor or a holder of sensitive_data, the
      // rights the tag mutations ask.
      {
        to: "/admin/tags",
        labelKey: "navBar.itemTags",
        capability: "table_registration",
        orCapability: "sensitive_data",
      },
      // REQ-2147, REQ-1327: classification is the steward's; the kinds are switched by the right
      // a classification decision asks.
      {
        to: "/admin/classification-kinds",
        labelKey: "navBar.itemClassificationKinds",
        capability: "sensitive_data",
      },
      { to: "/admin/reports", labelKey: "navBar.itemReports", capability: "observability" }, // REQ-1386
      // REQ-1387: the ontology route is /admin/ontology (glossary_read), and its nav entry is
      // the top-level Ontology link in NavBar.tsx, not an item of this group.
      {
        to: "/admin/observability",
        labelKey: "navBar.itemObservability",
        capability: "observability",
      },
      // REQ-1327: the exporter, the query engine's lifecycle and org-wide debug tracing act on the
      // deployment; the page above is the org's own traces and tracing.
      {
        to: "/admin/platform-observability",
        labelKey: "navBar.itemPlatformObservability",
        capability: "platform_settings",
        andCapability: "cross_org",
      },
      // REQ-1327: every member sees the requests it made and decides those whose own right it
      // holds (creation_requests_router), so the page asks `usage`, the right every role carries.
      { to: "/admin/requests", labelKey: "navBar.itemRequests", capability: "usage" },
    ],
  },
];

// Remembers the last submenu item visited within each group so returning to a
// group restores that item instead of always landing on the first one.
// The key itself lives in lib/session so clearSessionState drops it at sign-in.
function readLastSubnav(): Record<string, string> {
  const parsed = getStoredJson<unknown>(LAST_SUBNAV_KEY);
  return parsed && typeof parsed === "object" ? (parsed as Record<string, string>) : {};
}

export function writeLastSubnav(groupId: string, to: string) {
  setStored(LAST_SUBNAV_KEY, JSON.stringify({ ...readLastSubnav(), [groupId]: to }));
}

/** REQ-1922: whether a demo sandbox's visitor may open the page at ``path`` (its query string
 * aside) to read, though its capability is not theirs: an entry marked `sandboxReadable`. */
export function sandboxReadable(path: string): boolean {
  const page = path.split("?")[0];
  return NAV_GROUPS.some((g) => g.items.some((i) => i.to === page && i.sandboxReadable === true));
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
      meetsRequirement(capabilities, i) &&
      !(i.commercial && !commercial) &&
      !(i.installedOnly && commercial),
  );
  const remembered = readLastSubnav()[group.id];
  return reachable.find((i) => i.to === remembered) ?? reachable[0];
}
