// Copyright (c) 2026 Kenneth Stott
// Canary: f947ff03-32b4-4f71-97bb-4ca4693268ea
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Creating a table or view: "+ Table" opens an empty form whatever table was selected or edited
// before it, and Duplicate (the panel head, beside Edit) opens the create form filled from the
// selected table: its settings and columns, under a new name (rule: panelDuplicateGuard.test.ts).

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, within } from "../../test-utils/render";
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
    // REQ-1944: the table editor's right -- without it the page is the governance-only view.
    capabilities: ["admin", "table_registration"],
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
    rowMaterialize: false,
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
    timeTravel: null,
    dqDataset: null,
    qualityCheckers: [],
    apiEndpoint: null,
    apiEndpointError: null,
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
    mvHoldOnFailingChecks: false,
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
    productIds: [], // REQ-1634
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
  table(7, "orders", [], {
    region: "eu",
    alias: "ord",
    description: "Customer orders ledger",
    // A column a graph links to (?column=).
    columns: [
      {
        id: 70,
        columnName: "email",
        visibleTo: [],
        masks: [],
        scope: "domain",
      } as unknown as RegisteredTable["columns"][number],
    ],
  }),
  table(8, "staging", [], { region: "eu", draft: true }),
  table(9, "order_totals", [], {
    viewSql: "SELECT 1 AS total",
    description: "Totals view",
  }),
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
  useTableDetails: () => ({ tables: TABLES, loading: false, refetch: vi.fn() }),
  useSources: () => ({ sources: SOURCES, loading: false, refetch: vi.fn() }),
  useDomains: () => ({ domains: DOMAINS, loading: false, refetch: vi.fn() }),
  useRoles: () => ({ roles: ROLES, loading: false, refetch: vi.fn() }),
  useDataProducts: () => ({ dataProducts: [], loading: false, refetch: vi.fn() }),
  useAllRelationships: () => ({ relationships: [], loading: false, refetch: vi.fn() }),
  useAvailableColumnsMetadataLazy: () => ({
    fetchColumns: async () => [],
    late: null,
    starting: false,
    startingTimedOut: false,
  }),
  useRefreshPolicyPreview: () => async () => null,
  useUpdateTable: () => ({ updateTable, loading: false }),
  useUpdateTableNaming: () => ({ updateTableNaming, loading: false }),
  useUpdateTableCache: () => ({ updateTableCache, loading: false }),
  useUpdateTableReplicate: () => ({ updateTableReplicate, loading: false }),
  useUpdateTableLoadProtection: () => ({ updateTableLoadProtection, loading: false }),
}));

import { TablesPage } from "../TablesPage";
import { expectHoldsNoneOf } from "../../test-utils/createForm";

const ORDERS_VALUES = ["orders", "ord", "Customer orders ledger", "email", "ord-copy"];

async function select(name: string) {
  const list = await screen.findByTestId("tables-list");
  const row = (await within(list).findByText(name)).closest("tr") as HTMLElement;
  await userEvent.click(row.querySelector("td:not(.list-select)") as HTMLElement);
  return row;
}
const panel = () => screen.getByTestId("tables-split-panel");

describe("+ Table opens an empty form", () => {
  beforeEach(() => localStorage.clear());

  it("holds none of the values of the table selected before it", async () => {
    render(<TablesPage />);
    await select("ord");
    await userEvent.click(await screen.findByTestId("tables-add-toggle"));
    await within(await screen.findByTestId("tables-split-panel")).findByTestId(
      "register-table-source-select",
    );
    expectHoldsNoneOf(panel(), ORDERS_VALUES);
  });

  it("holds none of them after the table was edited and the edit cancelled", async () => {
    render(<TablesPage />);
    await select("ord");
    await userEvent.click(await screen.findByTestId("table-read-view-edit"));
    await screen.findByTestId("table-draft-checkbox");
    await userEvent.click(screen.getByTestId("table-edit-cancel"));
    await userEvent.click(await screen.findByTestId("tables-add-toggle"));
    await within(await screen.findByTestId("tables-split-panel")).findByTestId(
      "register-table-source-select",
    );
    expectHoldsNoneOf(panel(), ORDERS_VALUES);
  });
});

describe("Duplicate, in a table's panel head", () => {
  beforeEach(() => localStorage.clear());

  it("is beside Edit and Delete, named Duplicate", async () => {
    render(<TablesPage />);
    await select("ord");
    const action = await within(panel()).findByTestId("table-read-view-duplicate");
    expect(action).toHaveAccessibleName("Duplicate");
  });

  it("opens the create form filled from the table, its alias a new name, columns and settings kept", async () => {
    render(<TablesPage />);
    await select("ord");
    await userEvent.click(await screen.findByTestId("table-read-view-duplicate"));
    const form = await within(panel()).findByTestId("table-section-identity-panel");
    expect(within(form).getByTestId("table-edit-table")).toHaveValue("orders");
    expect(within(form).getByDisplayValue("ord-copy")).toBeInTheDocument();
    expect(within(form).getByDisplayValue("Customer orders ledger")).toBeInTheDocument();
    // It is the create form: nothing is registered until it is saved, and it is not the saved table.
    expect(within(panel()).getByTestId("table-edit-save")).toBeInTheDocument();
    expect(within(panel()).queryByTestId("table-read-view-edit")).toBeNull();
  });

  it("names a view's copy by the view's own name", async () => {
    render(<TablesPage />);
    await select("order_totals");
    await userEvent.click(await screen.findByTestId("table-read-view-duplicate"));
    const form = await within(panel()).findByTestId("table-section-identity-panel");
    expect(within(form).getByTestId("table-edit-table")).toHaveValue("order_totals-copy");
    expect(within(form).getByDisplayValue("Totals view")).toBeInTheDocument();
  });
});
