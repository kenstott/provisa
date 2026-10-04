// Copyright (c) 2026 Kenneth Stott
// Canary: 5d1a7c93-2b6e-4f08-8c4d-9e3b1f7a6c25
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1907: saving the table edit form persists the Role TTL list through updateTableRoleTtl
// (full replace), and an invalid row blocks the whole save before any mutation runs.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "../../test-utils/render";
import userEvent from "@testing-library/user-event";
import type { RegisteredTable, RoleTtl } from "../../types/admin";

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
    region: null,
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
    writeOps: ["delete", "insert", "update"],
    live: null,
    implicitMeasures: [],
    implicitDimensions: [],
    ...overrides,
  };
}

const TABLES = [
  table(7, "orders", [{ role: "analyst", ttl: 360 }]),
  // REQ-930: a ttl signal with no Cache TTL on the table or its source (the source sets none).
  table(9, "ticks", [], { cacheTtl: null, changeSignal: "ttl", replicate: 0 }),
  // Read live: a ttl table with no Cache TTL saves; the server raises if it ever lands.
  table(14, "live_ticks", [], { cacheTtl: null, changeSignal: "ttl" }),
  table(15, "mv_ticks", [], { cacheTtl: null, changeSignal: "ttl_probe", materialize: true }),
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

vi.mock("../../hooks/useAdminOpsQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminOpsQueries")>()),
  usePurgeCacheByTable: () => ({ purgeCacheByTable: vi.fn(), loading: false }),
  useUpdateTableRoleTtl: () => ({ updateTableRoleTtl, loading: false }),
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

async function openEditor(name = "orders") {
  render(<TablesPage />);
  const row = (await screen.findByText(name)).closest("tr") as HTMLElement;
  await userEvent.click(row.querySelector("td") as HTMLElement);
  await userEvent.click(await screen.findByTestId("table-read-view-edit"));
  // The load settings live in the collapsed "Load Management and Timeliness" panel.
  const toggle = await screen.findByTestId("load-management-panel-toggle");
  expect(toggle).toHaveAttribute("aria-expanded", "false");
  expect(toggle).toHaveTextContent("Load Management and Timeliness");
  await userEvent.click(toggle);
  expect(screen.getByTestId("load-management-help")).toHaveTextContent(
    "How current the data must be for each reader, balanced against load on the platform and upstream sources.",
  );
  return screen.findByTestId("role-ttl-field");
}

// The signal matrix lives in the tableTtlSignalError unit tests (roleTtl.test.ts), not here.
describe("TablesPage — Role TTL save (REQ-1907)", () => {
  beforeEach(() => {
    for (const fn of [
      updateTable,
      updateTableNaming,
      updateTableCache,
      updateTableReplicate,
      updateTableLoadProtection,
      updateTableRoleTtl,
    ]) {
      fn.mockReset();
      fn.mockResolvedValue(ok);
    }
  });

  it("sends the full role list when a row is added", async () => {
    await openEditor();
    await userEvent.click(await screen.findByRole("button", { name: "Add role TTL" }));
    await userEvent.click(screen.getByTestId("table-edit-save"));
    await waitFor(() =>
      expect(updateTableRoleTtl).toHaveBeenCalledWith(7, [
        { role: "analyst", ttl: 360 },
        { role: "trader", ttl: 60 },
      ]),
    );
  });

  it("does not call updateTableRoleTtl when the list is unchanged", async () => {
    await openEditor();
    await userEvent.click(screen.getByTestId("table-edit-save"));
    await waitFor(() => expect(updateTable).toHaveBeenCalled());
    expect(updateTableRoleTtl).not.toHaveBeenCalled();
  });

  it("blocks the whole save on an invalid row", async () => {
    await openEditor();
    await userEvent.clear(await screen.findByRole("textbox", { name: "TTL (seconds)" }));
    await userEvent.click(screen.getByTestId("table-edit-save"));
    expect(await screen.findByText("Fix the Role TTL rows before saving.")).toBeInTheDocument();
    expect(updateTable).not.toHaveBeenCalled();
    expect(updateTableRoleTtl).not.toHaveBeenCalled();
  });

  it("reports a server refusal", async () => {
    updateTableRoleTtl.mockResolvedValue({
      success: false,
      message: "Unknown role 'trader'",
      code: "schema.role_ttl_unknown_role",
      params: { role: "trader" },
    });
    await openEditor();
    await userEvent.click(await screen.findByRole("button", { name: "Add role TTL" }));
    await userEvent.click(screen.getByTestId("table-edit-save"));
    expect(await screen.findByText("Unknown role 'trader'")).toBeInTheDocument();
  });

  it("refuses a ttl signal with no Cache TTL, and saves once one is entered", async () => {
    await openEditor("ticks");
    const msg =
      "The ttl change signal judges staleness by the Cache TTL, so set a Cache TTL here or on the source.";
    expect(await screen.findByText(msg)).toBeInTheDocument();
    await userEvent.click(screen.getByTestId("table-edit-save"));
    expect(
      await screen.findByText("Fix the Load Management and Timeliness settings before saving."),
    ).toBeInTheDocument();
    expect(updateTable).not.toHaveBeenCalled();

    await userEvent.type(screen.getByRole("textbox", { name: /^Cache TTL/ }), "30");
    await waitFor(() => expect(screen.queryByText(msg)).toBeNull());
    await userEvent.click(screen.getByTestId("table-edit-save"));
    await waitFor(() => expect(updateTable).toHaveBeenCalled());
  });

  it.each(["live_ticks"])("saves the live-read ttl table %s with no Cache TTL", async (name) => {
    await openEditor(name);
    expect(screen.queryByText(/judges staleness by the Cache TTL/)).toBeNull();
    await userEvent.click(screen.getByTestId("table-edit-save"));
    await waitFor(() => expect(updateTable).toHaveBeenCalled());
    expect(
      screen.queryByText("Fix the Load Management and Timeliness settings before saving."),
    ).toBeNull();
  });

  it.each(["mv_ticks"])(
    "refuses landed table %s with a ttl signal and no Cache TTL",
    async (name) => {
      await openEditor(name);
      expect(await screen.findByText(/judges staleness by the Cache TTL/)).toBeInTheDocument();
      await userEvent.click(screen.getByTestId("table-edit-save"));
      expect(
        await screen.findByText("Fix the Load Management and Timeliness settings before saving."),
      ).toBeInTheDocument();
      expect(updateTable).not.toHaveBeenCalled();
    },
  );
});
