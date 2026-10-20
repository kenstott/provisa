// Copyright (c) 2026 Kenneth Stott
// Canary: 7a44944b-6555-4388-b469-4a238d24e8c1
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Tour anchors (test-utils/tourAnchor): the Model Deep Dive's steps on their pages. The views
// steps point at the section navigation (tourAnchors.nav) once the Views page is ready; metrics and
// commands are their pages' own "new" controls, offered with none declared; lineage is the
// statement graph the step deep-links (LINEAGE_DEMO_SQL), drawn for the inquiries tables the topic
// needs (tourPrerequisites.json).

import { describe, it, expect, vi } from "vitest";
import { render } from "../test-utils/render";
import { stepPageReady, stepRoute, walkTourStep } from "../test-utils/tourAnchor";

const hooks = vi.hoisted(() => {
  const refetch = async () => undefined;
  const none: never[] = [];
  return {
    list: (key: string) => ({ loading: false, [key]: none, refetch }),
    auth: {
      role: "admin",
      selectedRoles: [{ id: "admin" }],
      capabilities: ["admin", "table_registration", "view_governance"],
      domainAccess: ["*"],
      demonstrated: [],
      activeOrgId: "acme",
      loading: false,
    },
    graph: {
      nodes: [
        {
          id: "inquiries.id",
          column: "id",
          relation: "inquiries",
          kind: "source",
          materialized: false,
        },
        {
          id: "inquiry_count",
          column: "inquiry_count",
          relation: null,
          kind: "derived",
          materialized: false,
        },
      ],
      edges: [{ source: "inquiries.id", target: "inquiry_count", transform: "COUNT", ops: [] }],
      outputs: ["inquiry_count"],
      cycles: [],
    },
  };
});
// One value per hook across renders: a fresh array each render reruns the page's effects forever.
const LISTS = {
  sources: hooks.list("sources"),
  domains: hooks.list("domains"),
  tables: hooks.list("tables"),
  relationships: hooks.list("relationships"),
  roles: hooks.list("roles"),
  dataProducts: hooks.list("dataProducts"),
  metrics: hooks.list("metrics"),
};

vi.mock("../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../context/AuthContext")>()),
  useAuth: () => hooks.auth,
}));
vi.mock("../context/DomainFilterContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../context/DomainFilterContext")>()),
  useDomainFilter: () => FILTER,
}));
const FILTER = {
  checkedDomains: new Set<string>(),
  domains: [],
  domainsEnabled: true,
  setDomains: vi.fn(),
  selectedDomain: null,
  setSelectedDomain: vi.fn(),
  toggleDomain: vi.fn(),
};
vi.mock("../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useAdminQueries")>()),
  useSources: () => LISTS.sources,
  useDomains: () => LISTS.domains,
  useTables: () => LISTS.tables,
  useRelationships: () => LISTS.relationships,
  useAllRelationships: () => LISTS.relationships,
  useRoles: () => LISTS.roles,
  useDataProducts: () => LISTS.dataProducts,
}));
vi.mock("../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/admin")>()),
  fetchSettings: vi.fn().mockResolvedValue({
    redirect: { enabled: false, threshold: 10000, default_format: "json", ttl: 3600 },
    sampling: { default_sample_size: 1000 },
    cache: { default_ttl: 300 },
    naming: { domain_prefix: false, convention: "none" },
  }),
  fetchOrgRoles: vi
    .fn()
    .mockResolvedValue([{ id: "admin", capabilities: [], demonstrated: [], domain_access: ["*"] }]),
}));
vi.mock("../api/actions", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/actions")>()),
  fetchActions: vi.fn(async () => ({ functions: [], webhooks: [] })),
}));
vi.mock("../api/lineage", () => ({
  fetchLineageGraph: vi.fn(async () => hooks.graph),
  fetchFederationGraph: vi.fn(async () => hooks.graph),
}));
// jsdom draws no canvas; the step's anchor is the graph's container, not what is drawn in it. Every
// call on the stand-in answers another stand-in, and as a collection it is empty.
vi.mock("cytoscape", () => {
  const stub: unknown = new Proxy(function () {}, {
    get: (_t, prop) => {
      if (prop === "length") return 0;
      if (prop === Symbol.iterator) return function* () {};
      if (prop === "then") return undefined;
      if (prop === "map" || prop === "toArray") return () => [];
      if (prop === "forEach") return () => undefined;
      return stub;
    },
    apply: () => stub,
  });
  return { default: stub };
});

import { ViewsPage } from "../pages/ViewsPage";
import { MetricsPage } from "../pages/MetricsPage";
import { CommandsPage } from "../pages/CommandsPage";
import { LineagePage } from "../pages/LineagePage";

describe("the Model Deep Dive's steps on their pages", () => {
  for (const key of ["step20", "step21"]) {
    it(`is ready for ${key} on the Views page`, async () => {
      render(<ViewsPage />, { initialEntries: [stepRoute(key)] });
      expect(await stepPageReady(key)).toBeInTheDocument();
    });
  }
  it("finds stepMetrics's anchor", async () => {
    render(<MetricsPage />, { initialEntries: [stepRoute("stepMetrics")] });
    expect(await walkTourStep("stepMetrics")).toBeInTheDocument();
  });
  it("finds stepCommands's anchor", async () => {
    render(<CommandsPage />, { initialEntries: [stepRoute("stepCommands")] });
    expect(await walkTourStep("stepCommands")).toBeInTheDocument();
  });
  it("finds step22's anchor", async () => {
    render(<LineagePage />, { initialEntries: [stepRoute("step22")] });
    expect(await walkTourStep("step22")).toBeInTheDocument();
  });
});
