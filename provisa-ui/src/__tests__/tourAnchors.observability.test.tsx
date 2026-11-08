// Copyright (c) 2026 Kenneth Stott
// Canary: 434bb357-0981-426d-91ca-8c4deba75389
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Tour anchors for a viewer holding exactly `observability` and no deployment-wide right (a cloud
// sandbox visitor): GET /admin/settings omits the otel block for such a caller (REQ-1349), so the
// Observability page paints the caller's own trace feed but not the debug-tracing tab.

import { afterEach, beforeEach, describe, it, expect, vi } from "vitest";
import { screen } from "@testing-library/react";
import { render } from "../test-utils/render";
import { stepPageReady, stepRoute, walkTourStep } from "../test-utils/tourAnchor";

// One value per hook across renders: a fresh array each render reruns the page's effects forever.
const hooks = vi.hoisted(() => {
  const refetch = async () => undefined;
  const none: never[] = [];
  return {
    refetch,
    list: (key: string) => ({ loading: false, [key]: none, refetch }),
    auth: {
      role: { id: "visitor", capabilities: [] as string[], domain_access: ["*"] },
      capabilities: ["observability"],
      demonstrated: [] as string[],
      selectedRoles: ["visitor"],
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
const HEALTH = { systemHealth: null, loading: false, error: undefined, refetch: hooks.refetch };

vi.mock("../hooks/useAdminQueries", async (orig) => ({
  ...(await orig<typeof import("../hooks/useAdminQueries")>()),
  useSources: () => SOURCES,
  useDomains: () => DOMAINS,
  useTables: () => TABLES,
  useRelationships: () => RELATIONSHIPS,
  useAllRelationships: () => RELATIONSHIPS,
  useRoles: () => ROLES,
}));
// AdminPage reads its RLS rules from the Security page's hooks, not useAdminQueries.
vi.mock("../hooks/useSecurityQueries", async (orig) => ({
  ...(await orig<typeof import("../hooks/useSecurityQueries")>()),
  useRLSRules: () => RLS,
}));
vi.mock("../hooks/useAdminOpsQueries", async (orig) => ({
  ...(await orig<typeof import("../hooks/useAdminOpsQueries")>()),
  useSystemHealth: () => HEALTH,
}));
vi.mock("../context/AuthContext", () => ({ useAuth: () => hooks.auth }));
vi.mock("../api/admin", async (orig) => ({
  ...(await orig<typeof import("../api/admin")>()),
  fetchSettings: vi.fn(async () => ({
    naming: { use_domains: true },
    federation: { enabled: false, subgraph_name: "provisa", role: null },
  })),
}));

import { AdminPage } from "../pages/AdminPage";
import type { McpServerStatus } from "../api/admin";

// GET /admin/mcp-server as it answers a caller holding observability: the full status, tool list
// included (provisa/api/mcp/status.py). step23's Dashboard lists those tools.
const MCP_SERVER: McpServerStatus = {
  enabled: false,
  port: null,
  transport: null,
  stdio_role: null,
  max_rows: 1000,
  tools: [],
  enable_env_var: "PROVISA_MCP_PORT",
  role_env_var: "PROVISA_MCP_ROLE",
};

function answer(url: string): object {
  if (url.includes("/traces/")) return { traces: [] };
  if (url.endsWith("/admin/mcp-server")) return MCP_SERVER;
  return {};
}

beforeEach(() => {
  vi.stubGlobal(
    "fetch",
    vi.fn(
      async (url: string) =>
        new Response(JSON.stringify(answer(String(url))), {
          headers: { "Content-Type": "application/json" },
        }),
    ),
  );
});
afterEach(() => vi.unstubAllGlobals());

describe("the observability steps for a role holding only observability, settings without otel", () => {
  for (const key of ["stepReports", "stepObservability"]) {
    it(`finds ${key}'s anchor`, async () => {
      render(<AdminPage />, { initialEntries: [stepRoute(key)] });
      expect(await walkTourStep(key)).toBeInTheDocument();
    });
  }
  it("shows the trace feed and no debug tab", async () => {
    render(<AdminPage />, { initialEntries: [stepRoute("stepObservability")] });
    await walkTourStep("stepObservability");
    expect(document.querySelector('[data-testid="observability-debug-trace-tab"]')).toBeNull();
  });
  it("is ready for step23", async () => {
    render(<AdminPage />, { initialEntries: [stepRoute("step23")] });
    expect(await stepPageReady("step23")).toBeInTheDocument();
  });
  it("paints step23's Dashboard through to its MCP tool list", async () => {
    render(<AdminPage />, { initialEntries: [stepRoute("step23")] });
    expect(await screen.findByTestId("mcp-tools")).toBeInTheDocument();
    expect(await stepPageReady("step23")).toBeInTheDocument();
  });
});
