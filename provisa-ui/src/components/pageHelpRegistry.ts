// Copyright (c) 2026 Kenneth Stott
// Canary: 8b453f94-977f-4b2a-ad61-4974c7e7b7a9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277 rule 8: every page has the page help (?) in its header. This registry holds each page's
// help, keyed by its route: the purpose line, any further paragraphs, and the documentation page it
// links. pageHelpGuard.test.ts fails a route of the app's route table without an entry here, an
// entry for no route, a key with no English string, and a docs link to no page or section.

/** Where a page draws its (?): in its PageTitle, at the end of the subnav above a work surface
 *  that has no PageTitle, or in a header of its own (RoutePageHelp placed by the page). */
export type PageHelpPlacement = "title" | "subnav" | "own";

export interface PageHelpEntry {
  /** i18n key of the page's purpose: one plain sentence. */
  purpose: string;
  /** i18n keys of further paragraphs: the rules a reader cannot see on the screen. */
  more?: readonly string[];
  /** The docs site page as /docs?page= takes it (a path under docs/ ending in /), with an
   *  optional #section. */
  docs: string;
  placement: PageHelpPlacement;
}

const p = (id: string) => `pageHelp.purpose.${id}`;

export const PAGE_HELP: Readonly<Record<string, PageHelpEntry>> = {
  "/sources": { purpose: p("sources"), docs: "sources/", placement: "title" },
  "/tables": { purpose: p("tables"), docs: "configuration/#tables", placement: "title" },
  "/views": {
    purpose: p("views"),
    docs: "configuration/#views-governed-computed-datasets",
    placement: "title",
  },
  "/relationships": {
    purpose: p("relationships"),
    docs: "configuration/#relationships",
    placement: "title",
  },
  "/metrics": { purpose: p("metrics"), docs: "modeling/#metrics", placement: "title" },
  "/commands": { purpose: p("commands"), docs: "commands/", placement: "title" },
  "/lineage": { purpose: p("lineage"), docs: "lineage/", placement: "title" },
  "/data-products": { purpose: p("dataProducts"), docs: "data-products/", placement: "title" },
  "/team": { purpose: p("team"), docs: "access-model/#grants", placement: "title" },
  "/security/roles": {
    purpose: p("roles"),
    docs: "security/#roles-granted-derived-and-meta",
    placement: "title",
  },
  "/security/rls": {
    purpose: p("rls"),
    docs: "security/#row-level-security-rls",
    placement: "own",
  },

  // Explore: the query editors are work surfaces without a PageTitle.
  "/schema": { purpose: p("schema"), docs: "modeling/", placement: "subnav" },
  "/nl": { purpose: p("nl"), docs: "api-reference/#post-querynl", placement: "subnav" },
  "/query": { purpose: p("graphql"), docs: "api-reference/#post-datagraphql", placement: "subnav" },
  "/graph": { purpose: p("cypher"), docs: "cypher/", placement: "subnav" },
  "/sparql": { purpose: p("sparql"), docs: "conformance/sparql/", placement: "subnav" },
  "/sql": { purpose: p("sql"), docs: "api-reference/#post-datasql", placement: "subnav" },
  "/grpc": {
    purpose: p("grpc"),
    docs: "integrations/#protobuf-grpc-port-50051",
    placement: "subnav",
  },
  "/jsonapi": { purpose: p("jsonapi"), docs: "conformance/jsonapi/", placement: "subnav" },
  "/odata": { purpose: p("odata"), docs: "odata/", placement: "title" },
  "/openapi": { purpose: p("openapi"), docs: "conformance/rest/", placement: "subnav" },
  "/explore": { purpose: p("explore"), docs: "conformance/mcp/", placement: "title" },
  "/python": { purpose: p("python"), docs: "python-client/", placement: "subnav" },
  "/notebook": {
    purpose: p("notebook"),
    docs: "python-client/#using-provisa-in-jupyter",
    placement: "subnav",
  },

  // Context.
  "/context/health": {
    purpose: p("contextHealth"),
    docs: "ontology/#to-review",
    placement: "title",
  },
  "/context/glossary": {
    purpose: p("glossary"),
    docs: "ontology/#building-a-glossary-that-grounds",
    placement: "title",
  },
  "/context/ontology": { purpose: p("ontology"), docs: "ontology/", placement: "title" },
  "/context/metrics": {
    purpose: p("contextMetrics"),
    docs: "modeling/#metrics",
    placement: "title",
  },
  "/context/tags": {
    purpose: p("contextTags"),
    docs: "admin/#tags-library-sections-and-objects",
    placement: "title",
  },
  "/context/classification": {
    purpose: p("contextClassification"),
    docs: "test-data/#profile-a-table",
    placement: "title",
  },
  "/context/documents": {
    purpose: "referenceLibrary.intro",
    more: [
      "referenceLibrary.helpWhat",
      "referenceLibrary.helpWho",
      "referenceLibrary.helpSees",
      "referenceLibrary.helpEmbedding",
    ],
    docs: "ontology/#finding-terms-in-documents-and-tables",
    placement: "title",
  },
  "/context/sync": { purpose: p("contextSync"), docs: "metadata-export/", placement: "title" },

  // Pages outside the navigation groups.
  "/admin/requests": {
    purpose: p("requests"),
    docs: "admin-graphql/#creation-requests",
    placement: "title",
  },
  "/welcome": { purpose: p("welcome"), docs: "quickstart/", placement: "own" },
  "/docs": { purpose: p("docs"), docs: "quickstart/", placement: "own" },

  // Admin: every route draws AdminPage, whose header (or the tab's own) is a PageTitle.
  "/admin/overview": { purpose: p("dashboard"), docs: "admin/", placement: "title" },
  "/admin/system-health": { purpose: p("dashboard"), docs: "admin/", placement: "title" },
  "/admin/mcp-server": { purpose: p("dashboard"), docs: "admin/", placement: "title" },
  "/admin/domains": {
    purpose: p("domains"),
    docs: "access-model/#domain-hierarchy",
    placement: "title",
  },
  "/admin/environments": { purpose: p("environments"), docs: "environments/", placement: "title" },
  "/admin/cache": { purpose: p("cache"), docs: "configuration/#cache", placement: "title" },
  "/admin/scheduled-tasks": {
    purpose: p("triggers"),
    docs: "architecture/#scheduled-triggers",
    placement: "title",
  },
  "/admin/federation-engine": { purpose: p("federation"), docs: "engines/", placement: "title" },
  "/admin/org-engine": {
    purpose: p("orgEngine"),
    docs: "engines/#engine-lifecycle-hosted-deployment-only",
    placement: "title",
  },
  "/admin/email": { purpose: p("email"), docs: "deployment/", placement: "title" },
  "/admin/maintenance": {
    purpose: p("maintenance"),
    docs: "cli/#provisa-maintenance-on",
    placement: "title",
  },
  "/admin/deployment-settings": {
    purpose: p("deploymentSettings"),
    docs: "configuration/",
    placement: "title",
  },
  "/admin/platform": {
    purpose: p("platform"),
    docs: "regions/#declaring-and-assigning-regions",
    placement: "title",
  },
  "/admin/regions": { purpose: p("regions"), docs: "regions/", placement: "title" },
  "/admin/billing": { purpose: p("billing"), docs: "hosted-sandbox/", placement: "title" },
  "/admin/observability": {
    purpose: p("observability"),
    docs: "integrations/#observability-otel",
    placement: "title",
  },
  "/admin/platform-observability": {
    purpose: p("platformObservability"),
    docs: "configuration/#telemetry-filters",
    placement: "title",
  },
  "/admin/orgs": {
    purpose: p("orgs"),
    docs: "access-model/#three-layers-of-administration",
    placement: "title",
  },
  "/admin/ai-models": {
    purpose: p("aiModels"),
    docs: "secrets/#org-ai-vendor-keys-as-secret-references",
    placement: "title",
  },
  "/admin/package-registries": {
    purpose: p("packageRegistries"),
    docs: "package-registries/",
    placement: "title",
  },
  "/admin/container-registries": {
    purpose: p("containerRegistries"),
    docs: "code-sources/#deployment",
    placement: "title",
  },
  "/admin/spark-cluster": {
    purpose: p("sparkCluster"),
    docs: "spark-connector/",
    placement: "title",
  },
  "/admin/classification-kinds": {
    purpose: p("classificationKinds"),
    docs: "test-data/#profile-a-table",
    placement: "title",
  },
  "/admin/reports": { purpose: p("reports"), docs: "admin/#reports-viewer", placement: "title" },
  "/admin/security": {
    purpose: p("security"),
    docs: "configuration/#authentication",
    placement: "title",
  },
  "/admin/encryption": {
    purpose: p("security"),
    docs: "configuration/#authentication",
    placement: "title",
  },
  "/admin/auth": {
    purpose: p("security"),
    docs: "configuration/#authentication",
    placement: "title",
  },
  "/admin/secrets": { purpose: p("orgSecrets"), docs: "secrets/", placement: "title" },
  "/admin/my-secrets": {
    purpose: p("mySecrets"),
    docs: "secrets/#two-vaults-one-service",
    placement: "title",
  },
  "/admin/import": { purpose: p("import"), docs: "import/", placement: "title" },
  "/admin/tags": {
    purpose: p("tags"),
    docs: "admin/#tags-library-sections-and-objects",
    placement: "title",
  },
};

/** The in-app Docs route for an entry's documentation page. */
export function pageHelpDocsHref(entry: PageHelpEntry): string {
  const [page, section] = entry.docs.split("#");
  return `/docs?page=${page}${section === undefined ? "" : `#${section}`}`;
}
