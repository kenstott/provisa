// Copyright (c) 2026 Kenneth Stott
// Canary: 30736ecd-d333-4adb-ae71-9a87ca916455
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A link to a table by its name (`?open=<source>.<table>`) opens its detail panel on the section
// (and, in Columns, the column tab) it names: a demo tour step names a table so, whatever id the
// org gave it, and shows what it describes where only the panel shows it (REQ-2010).

import { describe, it, expect, vi, beforeEach } from "vitest";
import { renderWithProviders, screen, within } from "../../test-utils/render";
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
    role: { id: "analyst" },
    // "Role: All": the user acts as both roles they hold.
    selectedRoles: [{ id: "analyst" }, { id: "trader" }],
    actingRoles: "analyst,trader",
    // REQ-1944: the table editor's right -- without it the page is the governance-only view.
    capabilities: ["admin", "table_registration"],
    domainAccess: ["*"],
  }),
}));

vi.mock("../../components/admin/FilterInput", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../components/admin/FilterInput")>()),
  FilterInput: () => null,
}));

const profileTable = vi.fn().mockResolvedValue({ columns: [], rows: [], rowCount: 0 });

vi.mock("../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/admin")>()),
  profileTable: (...args: unknown[]) => profileTable(...args),
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
    draft: false,
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
    writeGrants: [],
    uniqueConstraints: [],
    graphqlFieldName: null,
    dqDataset: null,
    qualityCheckers: [],
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
    businessKey: [],
    validTime: null,
    recordedTime: null,
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
  table(7, "orders", [], { region: "eu" }),
  table(8, "staging", [], { region: "eu", draft: true }),
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

const calls: string[] = [];
const setTableDraft = vi.fn(async (id: number, draft: boolean) => {
  calls.push(`draft ${id} ${draft}`);
  return ok;
});
const setTableRegion = vi.fn(async (id: number, region: string | null) => {
  calls.push(`region ${id} ${region}`);
  return ok;
});

vi.mock("../../hooks/useRegionQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useRegionQueries")>()),
  useRegionChoices: () => ({ regions: ["eu", "us"], connected: "eu" }),
  useSetTableDraft: () => setTableDraft,
  useSetTableRegion: () => setTableRegion,
}));

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

describe("TablesPage — a link opens a table's panel on a section", () => {
  beforeEach(() => localStorage.clear());

  it("opens the named table on its columns' Testing tab", async () => {
    renderWithProviders(<TablesPage />, {
      initialEntries: ["/tables?open=sales-pg.orders&section=columns&tab=testing"],
    });
    const panel = await screen.findByTestId("table-section-columns-panel");
    expect(within(panel).getByRole("tab", { selected: true })).toHaveTextContent(/test/i);
  });

  it("opens another section by its name", async () => {
    renderWithProviders(<TablesPage />, {
      initialEntries: ["/tables?open=sales-pg.orders&section=identity"],
    });
    expect(await screen.findByTestId("table-section-identity-panel")).toBeInTheDocument();
  });
});
