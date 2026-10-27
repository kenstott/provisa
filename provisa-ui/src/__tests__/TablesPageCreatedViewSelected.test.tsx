// Copyright (c) 2026 Kenneth Stott
// Canary: b5cb4f89-2ac0-4386-abcf-930b5e6a4d6f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A view handed over from the SQL editor is created on the Tables/Views page: Save registers it,
// reloads the list, and selects the new view's row.

import { describe, it, expect, vi } from "vitest";
import { fireEvent, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { render } from "../test-utils/render";
import type { RegisteredTable } from "../types/admin";
import type { Registration } from "../pages/tables/draftTable";

const registerTable = vi.hoisted(() => vi.fn());

vi.mock("../context/DomainFilterContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../context/DomainFilterContext")>()),
  useDomainFilter: () => ({
    checkedDomains: new Set<string>(),
    domains: [],
    domainsEnabled: true,
    setDomains: vi.fn(),
    selectedDomain: null,
    setSelectedDomain: vi.fn(),
    toggleDomain: vi.fn(),
    ensureDomainChecked: vi.fn(),
  }),
}));
vi.mock("../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../context/AuthContext")>()),
  useAuth: () => ({
    role: "admin",
    selectedRoles: ["admin"],
    capabilities: ["admin", "table_registration"],
    domainAccess: ["*"],
  }),
}));
vi.mock("../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/admin")>()),
  fetchSettings: vi.fn().mockResolvedValue({
    redirect: { enabled: false, threshold: 10000, default_format: "json", ttl: 3600 },
    sampling: { default_sample_size: 1000 },
    cache: { default_ttl: 300 },
    naming: { domain_prefix: false, convention: "none" },
  }),
}));

function view(
  id: number,
  tableName: string,
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
    roleTtl: [],
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

const REFETCH = vi.fn().mockResolvedValue(undefined);
const NONE: never[] = [];
const TABLES = {
  tables: [
    view(7, "eod_totals", {
      sourceId: "sales-pg",
      domainId: "sales",
      schemaName: "views",
      viewSql: "SELECT 1 AS total",
    }),
  ],
  loading: false,
  refetch: REFETCH,
};
const SOURCES = [
  {
    id: "sales-pg",
    type: "postgresql",
    host: "localhost",
    port: 5432,
    database: "sales",
    username: "u",
    dialect: "postgresql",
    cacheEnabled: true,
    cacheTtl: null,
    allowedDomains: [],
    namingConvention: null,
    path: null,
    description: "",
  },
];
const DOMAINS = {
  domains: [{ id: "sales", description: "" }],
  loading: false,
  refetch: REFETCH,
};

vi.mock("../hooks/useRegionQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useRegionQueries")>()),
  useRegionChoices: () => ({ regions: [], connected: null }),
  useSetTableDraft: () => vi.fn(),
  useSetTableRegion: () => vi.fn(),
}));
vi.mock("../hooks/useAdminOpsQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useAdminOpsQueries")>()),
  usePurgeCacheByTable: () => ({ purgeCacheByTable: vi.fn(), loading: false }),
  useUpdateTableRoleTtl: () => ({ updateTableRoleTtl: vi.fn(), loading: false }),
}));
vi.mock("../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useAdminQueries")>()),
  useTables: () => TABLES,
  useSources: () => ({ sources: SOURCES, loading: false, refetch: REFETCH }),
  useDomains: () => DOMAINS,
  useRoles: () => ({ roles: NONE, loading: false, refetch: REFETCH }),
  useDataProducts: () => ({ dataProducts: NONE, loading: false, refetch: REFETCH }),
  useAllRelationships: () => ({ relationships: NONE, loading: false, refetch: REFETCH }),
  useAvailableColumnsMetadataLazy: () => async () => [],
  useRefreshPolicyPreview: () => async () => null,
  useRegisterTable: () => ({ registerTable, loading: false }),
  useUpdateTable: () => ({ updateTable: vi.fn(), loading: false }),
  useUpdateTableNaming: () => ({ updateTableNaming: vi.fn(), loading: false }),
  useUpdateTableCache: () => ({ updateTableCache: vi.fn(), loading: false }),
  useUpdateTableReplicate: () => ({ updateTableReplicate: vi.fn(), loading: false }),
  useUpdateTableLoadProtection: () => ({ updateTableLoadProtection: vi.fn(), loading: false }),
}));

import { TablesPage } from "../pages/TablesPage";

const REGISTRATION: Registration = {
  input: {
    sourceId: "sales-pg",
    domainId: "sales",
    schemaName: "views",
    tableName: "eod_totals",
    viewSql: "SELECT 1 AS total",
    columns: [{ name: "total", dataType: "integer", visibleTo: ["admin"] }],
  },
  pagination: null,
  pagingKind: null,
  pagingCeilingRows: null,
  rowRules: [],
};

describe("the Tables list's search", () => {
  it("lists the tables matching any word of the search", async () => {
    const before = TABLES.tables;
    TABLES.tables = ["orders", "customers", "invoices"].map((name, i) =>
      view(20 + i, name, {
        sourceId: "sales-pg",
        domainId: "sales",
        schemaName: "public",
        viewSql: null,
      }),
    );
    try {
      render(<TablesPage />, { initialEntries: ["/tables"] });
      await waitFor(() =>
        expect(document.querySelector('[data-table-row="sales-pg.customers"]')).not.toBeNull(),
      );
      fireEvent.change(screen.getByTestId("filter-input"), {
        target: { value: "orders invoices" },
      });
      await waitFor(() =>
        expect(document.querySelector('[data-table-row="sales-pg.customers"]')).toBeNull(),
      );
      expect(document.querySelector('[data-table-row="sales-pg.orders"]')).not.toBeNull();
      expect(document.querySelector('[data-table-row="sales-pg.invoices"]')).not.toBeNull();
    } finally {
      TABLES.tables = before;
    }
  });
});

describe("a view handed over from the SQL editor", () => {
  it("is selected in the list once Save registers it", async () => {
    registerTable.mockResolvedValue({
      success: true,
      message: "",
      code: "schema.table_registered",
      params: { table: "eod_totals", id: 7 },
    });
    render(<TablesPage />, {
      initialEntries: [{ pathname: "/views", state: { createView: REGISTRATION } }],
    });
    await userEvent.click(await screen.findByTestId("table-edit-save"));
    await waitFor(() => expect(registerTable).toHaveBeenCalledTimes(1));
    await waitFor(() => {
      const row = document.querySelector('[data-table-row="sales-pg.eod_totals"]');
      expect(row).not.toBeNull();
      expect(row).toHaveAttribute("aria-selected", "true");
    });
  });
});
