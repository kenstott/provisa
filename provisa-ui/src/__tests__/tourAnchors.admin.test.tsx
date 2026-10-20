// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Tour anchors (test-utils/tourAnchor): the Deep Dives' steps on the admin pages (AdminPage, one
// page per route), for a viewer holding every right the steps ask. Their topics need nothing of
// the org beyond its pages (tourPrerequisites.json).

import { afterEach, beforeEach, describe, it, expect, vi } from "vitest";
import { render } from "../test-utils/render";
import { stepPageReady, stepRoute, walkTourStep, walkTourSteps } from "../test-utils/tourAnchor";

// One value per hook across renders: a fresh array each render reruns the page's effects forever.
const hooks = vi.hoisted(() => {
  const refetch = async () => undefined;
  const none: never[] = [];
  return {
    none,
    list: (key: string) => ({ loading: false, [key]: none, refetch }),
    auth: {
      role: { id: "org_admin", capabilities: [] as string[], domain_access: ["*"] },
      capabilities: [
        "org_settings",
        "domain_admin",
        "environment_management",
        "glossary_read",
        "glossary_rw",
        "observability",
        "access_config",
      ],
      selectedRoles: ["org_admin"],
      user: { id: "u", username: "u" },
      activeOrgId: "acme",
      domainAccess: ["*"],
    },
  };
});
const SOURCES = hooks.list("sources");
const DOMAINS = hooks.list("domains");
const TABLES = hooks.list("tables");
const RELATIONSHIPS = hooks.list("relationships");
const RLS = hooks.list("rlsRules");
const ROLES = hooks.list("roles");

vi.mock("../hooks/useAdminQueries", async (orig) => ({
  ...(await orig<typeof import("../hooks/useAdminQueries")>()),
  useSources: () => SOURCES,
  useDomains: () => DOMAINS,
  useTables: () => TABLES,
  useRelationships: () => RELATIONSHIPS,
  useAllRelationships: () => RELATIONSHIPS,
  useRLSRules: () => RLS,
  useRoles: () => ROLES,
}));
vi.mock("../context/AuthContext", () => ({ useAuth: () => hooks.auth }));
vi.mock("../api/admin", async (orig) => ({
  ...(await orig<typeof import("../api/admin")>()),
  // The server always answers its otel block (settings_router), which the Observability page reads.
  fetchSettings: vi.fn(async () => ({
    naming: { use_domains: true },
    otel: {
      endpoint: "http://otel-collector:4317",
      service_name: "provisa",
      sample_rate: 0.1,
      log_level: "INFO",
      subsystem_traces: {},
    },
  })),
}));

// Panels inside the pages whose own content no step points at; each has its own tests.
vi.mock("../components/admin/MergeRequestsPanel", () => ({ MergeRequestsPanel: () => null }));
vi.mock("../components/admin/RepoBrowser", () => ({ RepoBrowser: () => null }));
vi.mock("../components/admin/RepoIntegrationPanel", () => ({ RepoIntegrationPanel: () => null }));
vi.mock("../components/admin/SyntheticDatasetsPanel", () => ({
  SyntheticDatasetsPanel: () => null,
}));
vi.mock("../components/admin/McpServerTab", () => ({ McpServerTab: () => null }));

import { AdminPage } from "../pages/AdminPage";

// What the admin pages ask of the server, answered as an org with nothing in it yet answers.
function answer(path: string): unknown {
  if (path.endsWith("/environments")) return { environments: [] };
  if (path.endsWith("/members")) return [];
  if (path.includes("/admin/actions")) return { functions: [], webhooks: [] };
  if (path.includes("/admin/glossary/terms")) return [];
  return {};
}

beforeEach(() => {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string) => {
      return new Response(JSON.stringify(answer(String(url))), {
        headers: { "Content-Type": "application/json" },
      });
    }),
  );
});
afterEach(() => vi.unstubAllGlobals());

describe("the Deep Dives' steps on the admin pages", () => {
  for (const key of [
    "stepDomains",
    "stepTags",
    "stepEnvKinds",
    "stepGlossary",
    "stepGlossaryTerm",
    "stepReports",
    "stepObservability",
    "stepCache",
    "stepScheduled",
  ]) {
    it(`finds ${key}'s anchor`, async () => {
      render(<AdminPage />, { initialEntries: [stepRoute(key)] });
      expect(await walkTourStep(key)).toBeInTheDocument();
    });
  }
  it("walks the environments page into its synthetic tab", async () => {
    render(<AdminPage />, { initialEntries: [stepRoute("stepEnvKinds")] });
    await walkTourSteps(["stepEnvKinds", "stepSynthetic", "stepSyntheticPrivacy"]);
  });
  it("is ready for step23", async () => {
    render(<AdminPage />, { initialEntries: [stepRoute("step23")] });
    expect(await stepPageReady("step23")).toBeInTheDocument();
  });
});
