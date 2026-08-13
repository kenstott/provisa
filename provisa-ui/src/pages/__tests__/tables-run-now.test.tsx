// Copyright (c) 2026 Kenneth Stott
// Canary: d810f435-8ff6-45df-a18b-04b25580f954
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-968: the forced-run control. A landed table can be rebuilt on demand — the operator says why,
// and that reason rides on the posted event as the audit record of who forced the rebuild.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { MemoryRouter } from "react-router-dom";
import { render, screen, waitFor } from "../../test-utils/render";
import userEvent from "@testing-library/user-event";
import type { RegisteredTable } from "../../types/admin";

vi.mock("../../context/DomainFilterContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../context/DomainFilterContext")>()),
  useDomainFilter: () => ({
    checkedDomains: new Set<string>(),
    domains: [],
    domainsEnabled: true,
    setDomains: vi.fn(),
    selectedDomain: null,
    setSelectedDomain: vi.fn(),
    toggleDomain: vi.fn(),
  }),
}));

vi.mock("../../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../context/AuthContext")>()),
  useAuth: () => ({
    role: "admin",
    selectedRoles: ["admin"],
    capabilities: ["admin"],
    domainAccess: ["*"],
  }),
}));

vi.mock("../../components/admin/FilterInput", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../components/admin/FilterInput")>()),
  FilterInput: () => null,
}));

vi.mock("../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/admin")>()),
  fetchSettings: vi.fn().mockResolvedValue({
    redirect: { enabled: false, threshold: 10000, default_format: "json", ttl: 3600 },
    sampling: { default_sample_size: 1000 },
    cache: { default_ttl: 300 },
    naming: { domain_prefix: false, convention: "none" },
  }),
}));

function table(
  id: number,
  tableName: string,
  serving: "live" | "scheduled" | "cache" | "frozen",
): RegisteredTable {
  return {
    id,
    sourceId: "sales-pg",
    domainId: "sales",
    schemaName: "sales",
    tableName,
    alias: null,
    description: null,
    cacheTtl: null,
    preferMaterialized: null,
    loadProtected: null,
    offPeakWindow: null,
    offPeakTz: null,
    refreshPolicySummary: { text: serving, serving, warning: null },
    gqlNamingConvention: null,
    watermarkColumn: null,
    changeSignal: null,
    probeQuery: null,
    probeType: null,
    columns: [],
    columnPresets: [],
    uniqueConstraints: [],
    apiEndpoint: null,
    viewSql: null,
    dqContract: null,
    materialize: false,
    mvRefreshInterval: 0,
    mvDebounceQuiet: 0,
    mvDebounceMaxDelay: 0,
    mvConsistency: "shared",
    mvPreprocess: null,
    mvBitemporalMode: null,
    mvBitemporalKey: [],
    mvPersist: "replace",
    mvPrimaryKey: [],
    mvIncremental: false,
    mvCalendar: null,
    mvGrain: null,
    mvAllowedLateness: 0,
    mvExpectedEvents: null,
    mvBusinessDayGrain: false,
    dataProduct: false,
    enableAggregates: false,
    enableGroupBy: false,
    canDeployToDb: false,
    live: null,
    implicitMeasures: [],
    implicitDimensions: [],
  };
}

// Stable identities: TablesPage's reload() is a useCallback keyed on the refetch fns and an effect
// is keyed on `tables`, so a fresh array or fn per render would loop.
const TABLES = [table(7, "orders", "scheduled"), table(8, "prices", "live")];
const SOURCES = [
  {
    id: "sales-pg",
    type: "postgresql",
    host: "localhost",
    port: 5432,
    database: "sales",
    username: "admin",
    dialect: "postgresql",
    cacheEnabled: false,
    cacheTtl: null,
    allowedDomains: [],
    namingConvention: null,
    path: null,
    description: "",
  },
];
const DOMAINS = [{ id: "sales", description: "Sales data" }];
const ROLES = [{ id: "admin", capabilities: ["admin"], domainAccess: ["*"] }];
const forceRegen = vi.fn();

vi.mock("../../hooks/useAdminOpsQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminOpsQueries")>()),
  usePurgeCacheByTable: () => ({ purgeCacheByTable: vi.fn(), loading: false }),
  useInvalidateFileSource: () => ({ invalidateFileSource: vi.fn(), loading: false }),
}));

vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useTables: () => ({ tables: TABLES, loading: false, refetch: vi.fn() }),
  useSources: () => ({ sources: SOURCES, loading: false, refetch: vi.fn() }),
  useDomains: () => ({ domains: DOMAINS, loading: false, refetch: vi.fn() }),
  useRoles: () => ({ roles: ROLES, loading: false, refetch: vi.fn() }),
  useAllRelationships: () => ({ relationships: [], loading: false, refetch: vi.fn() }),
  useForceRegen: () => ({ forceRegen, loading: false }),
}));

import { TablesPage } from "../TablesPage";

function renderPage() {
  return render(
    <MemoryRouter>
      <TablesPage />
    </MemoryRouter>,
  );
}

describe("TablesPage — forced run (REQ-968)", () => {
  beforeEach(() => {
    forceRegen.mockReset();
    forceRegen.mockResolvedValue({ success: true, message: "" });
  });

  it("offers the control on a landed table and not on a live one", async () => {
    renderPage();
    expect(await screen.findByTestId("tables-run-now-orders")).toBeInTheDocument();
    // Nothing lands a live table, so a forced run has no rows to rebuild.
    expect(screen.queryByTestId("tables-run-now-prices")).not.toBeInTheDocument();
  });

  it("sends the operator's reason with the table and confirms the queued run", async () => {
    renderPage();
    await userEvent.click(await screen.findByTestId("tables-run-now-orders"));

    const submit = await screen.findByTestId("run-now-submit");
    // The reason is the audit record, so an empty one cannot be submitted.
    expect(submit).toBeDisabled();

    await userEvent.type(await screen.findByTestId("run-now-reason"), "bad overnight load");
    await waitFor(() => expect(submit).toBeEnabled());
    await userEvent.click(submit);

    expect(forceRegen).toHaveBeenCalledWith(7, "bad overnight load");
    expect(await screen.findByTestId("tables-regen-queued")).toHaveTextContent("sales.orders");
  });

  it("reports a refusal instead of claiming the run was queued", async () => {
    forceRegen.mockResolvedValue({
      success: false,
      message: "sales.orders federates live — it has no landed rows to regenerate",
    });
    renderPage();
    await userEvent.click(await screen.findByTestId("tables-run-now-orders"));
    await userEvent.type(await screen.findByTestId("run-now-reason"), "why not");
    await userEvent.click(await screen.findByTestId("run-now-submit"));

    expect(await screen.findByTestId("tables-error")).toHaveTextContent(/no landed rows/);
    expect(screen.queryByTestId("tables-regen-queued")).not.toBeInTheDocument();
  });
});
