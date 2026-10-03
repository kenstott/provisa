// Copyright (c) 2026 Kenneth Stott
// Canary: 3ca68f49-0fc5-4cb7-a0e3-8290147e6d4c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-318: the Paging section of the table edit form. A REST endpoint table sets its paging type
// and parameters; a connection table sets max rows, which may only lower the operator's bound.
// Saving persists the paging through updateTablePaging only when it changed; paging the table
// cannot take blocks the save before any mutation runs.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor, within } from "../../test-utils/render";
import userEvent from "@testing-library/user-event";
import type { RegisteredTable, RoleTtl } from "../../types/admin";
import { NO_PAGING } from "../tables/paging";

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
  roleTtl: RoleTtl[],
  overrides: Partial<RegisteredTable> = {},
): RegisteredTable {
  const serving = "cache" as const;
  return {
    id,
    sourceId: "sales-pg",
    origin: "admin",
    domainId: "sales",
    schemaName: "sales",
    tableName,
    alias: null,
    description: null,
    cacheTtl: 60,
    roleTtl,
    pagingKind: null,
    pagination: null,
    pagingCeilingRows: null,
    replicate: null,
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
    graphqlFieldName: null,
    dqDataset: null,
    apiEndpoint: null,
    viewSql: null,
    dqContract: null,
    materialize: false,
    mvRefreshInterval: 0,
    mvDebounceQuiet: 0,
    mvDebounceMaxDelay: 0,
    pushDebounceQuiet: 0,
    pushDebounceMaxDelay: 5,
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
    productId: null, // REQ-1634
    enableAggregates: false,
    enableGroupBy: false,
    canDeployToDb: false,
    live: null,
    implicitMeasures: [],
    implicitDimensions: [],
    ...overrides,
  };
}

const TABLES = [
  table(7, "pets", [], {
    pagingKind: "endpoint",
    pagination: { ...NO_PAGING, type: "offset", maxPages: 3 },
  }),
  table(8, "issues", [], { pagingKind: "connection", pagingCeilingRows: 1000 }),
  table(9, "orders", []),
];
const SOURCES = [
  {
    id: "sales-pg",
    type: "postgresql",
    host: "localhost",
    port: 5432,
    database: "sales",
    username: "admin",
    dialect: "postgresql",
    cacheEnabled: true,
    cacheTtl: null,
    allowedDomains: [],
    namingConvention: null,
    path: null,
    description: "",
  },
];
const DOMAINS = [{ id: "sales", description: "Sales data" }];
const ROLES = [
  { id: "analyst", capabilities: [], domainAccess: ["*"] },
  { id: "trader", capabilities: [], domainAccess: ["*"] },
];
const ok = { success: true, message: "" };
const updateTable = vi.fn();
const updateTableNaming = vi.fn();
const updateTableCache = vi.fn();
const updateTableReplicate = vi.fn();
const updateTableLoadProtection = vi.fn();
const updateTableRoleTtl = vi.fn();
const updateTablePaging = vi.fn();

vi.mock("../../hooks/useAdminOpsQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminOpsQueries")>()),
  usePurgeCacheByTable: () => ({ purgeCacheByTable: vi.fn(), loading: false }),
  useUpdateTableRoleTtl: () => ({ updateTableRoleTtl, loading: false }),
  useUpdateTablePaging: () => ({ updateTablePaging, loading: false }),
}));

vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useTables: () => ({ tables: TABLES, loading: false, refetch: vi.fn() }),
  useSources: () => ({ sources: SOURCES, loading: false, refetch: vi.fn() }),
  useDomains: () => ({ domains: DOMAINS, loading: false, refetch: vi.fn() }),
  useRoles: () => ({ roles: ROLES, loading: false, refetch: vi.fn() }),
  useDataProducts: () => ({ dataProducts: [], loading: false, refetch: vi.fn() }),
  useAllRelationships: () => ({ relationships: [], loading: false, refetch: vi.fn() }),
  useAvailableColumnsMetadataLazy: () => async () => [],
  useRefreshPolicyPreview: () => async () => null,
  useUpdateTable: () => ({ updateTable, loading: false }),
  useUpdateTableNaming: () => ({ updateTableNaming, loading: false }),
  useUpdateTableCache: () => ({ updateTableCache, loading: false }),
  useUpdateTableReplicate: () => ({ updateTableReplicate, loading: false }),
  useUpdateTableLoadProtection: () => ({ updateTableLoadProtection, loading: false }),
}));

import { TablesPage } from "../TablesPage";

async function openEditor(name: string) {
  render(<TablesPage />);
  const row = (await screen.findByText(name)).closest("tr") as HTMLElement;
  await userEvent.click(row.querySelector("td") as HTMLElement);
  await userEvent.click(await screen.findByTestId("table-read-view-edit"));
  await userEvent.click(await screen.findByTestId("load-management-panel-toggle"));
}

describe("TablesPage — Paging (REQ-318)", () => {
  beforeEach(() => {
    for (const fn of [
      updateTable,
      updateTableNaming,
      updateTableCache,
      updateTableReplicate,
      updateTableLoadProtection,
      updateTableRoleTtl,
      updateTablePaging,
    ]) {
      fn.mockReset();
      fn.mockResolvedValue(ok);
    }
  });

  it("shows no Paging section for a table nothing pages", async () => {
    await openEditor("orders");
    expect(screen.queryByTestId("paging-field")).not.toBeInTheDocument();
  });

  it("saves a REST endpoint's paging when it changed", async () => {
    await openEditor("pets");
    const field = await screen.findByTestId("paging-field");
    const maxPages = await within(field).findByRole("textbox", { name: "Max pages" });
    await userEvent.clear(maxPages);
    await userEvent.type(maxPages, "5");
    await userEvent.click(screen.getByTestId("table-edit-save"));
    await waitFor(() =>
      expect(updateTablePaging).toHaveBeenCalledWith(7, {
        ...NO_PAGING,
        type: "offset",
        maxPages: 5,
      }),
    );
  });

  it("does not call updateTablePaging when the paging is unchanged", async () => {
    await openEditor("pets");
    await userEvent.click(screen.getByTestId("table-edit-save"));
    await waitFor(() => expect(updateTable).toHaveBeenCalled());
    expect(updateTablePaging).not.toHaveBeenCalled();
  });

  it("lets a connection table lower the operator's bound", async () => {
    await openEditor("issues");
    const rows = await screen.findByRole("textbox", { name: "Max rows per read" });
    expect(rows).toHaveAttribute("placeholder", "Default: 1000");
    await userEvent.type(rows, "200");
    await userEvent.click(screen.getByTestId("table-edit-save"));
    await waitFor(() =>
      expect(updateTablePaging).toHaveBeenCalledWith(8, { ...NO_PAGING, maxRows: 200 }),
    );
  });

  it("blocks the save when a connection table would raise the operator's bound", async () => {
    await openEditor("issues");
    await userEvent.type(await screen.findByRole("textbox", { name: "Max rows per read" }), "5000");
    expect(
      await screen.findByText("At most 1000: a table may lower the operator's bound, never raise it."),
    ).toBeInTheDocument();
    await userEvent.click(screen.getByTestId("table-edit-save"));
    expect(await screen.findByText("Fix the paging errors before saving.")).toBeInTheDocument();
    expect(updateTable).not.toHaveBeenCalled();
    expect(updateTablePaging).not.toHaveBeenCalled();
  });

  it("reports a server refusal in the reader's language", async () => {
    updateTablePaging.mockResolvedValue({
      success: false,
      message: "table 'issues': max_rows=900 is above graphql_remote.max_rows=800",
      code: "schema.paging_above_ceiling",
      params: { table: "issues", max_rows: 900, ceiling: 800 },
    });
    await openEditor("issues");
    await userEvent.type(await screen.findByRole("textbox", { name: "Max rows per read" }), "900");
    await userEvent.click(screen.getByTestId("table-edit-save"));
    expect(
      await screen.findByText(
        "Table issues: max_rows=900 is above graphql_remote.max_rows=800; a table may lower the operator's bound, never raise it.",
      ),
    ).toBeInTheDocument();
  });
});
