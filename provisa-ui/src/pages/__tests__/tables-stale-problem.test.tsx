// Copyright (c) 2026 Kenneth Stott
// Canary: 0c7d4e19-b2a6-4f53-9e8d-6a1f3c5b7d24
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A problem the page reports belongs to the action that met it. One met by an earlier table's save
// is not the Register Table form's problem: opening the form after it must not show "Table 7 not
// found" for a table the form is not about.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "../../test-utils/render";
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

const TABLES = [table(7, "orders")];
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
const ROLES = [{ id: "analyst", capabilities: [], domainAccess: ["*"] }];
// The table's save is answered as the server answers a table it no longer holds.
const notFound = {
  success: false,
  message: "Table 7 not found",
  code: "schema.table_not_found",
  params: { table: 7 },
};
const updateTable = vi.fn();

vi.mock("../../hooks/useRegionQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useRegionQueries")>()),
  useRegionChoices: () => ({ regions: [], connected: null }),
  useSetTableDraft: () => vi.fn(),
  useSetTableRegion: () => vi.fn(),
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
}));

import { TablesPage } from "../TablesPage";

describe("TablesPage — a problem stays with the action that met it", () => {
  beforeEach(() => {
    updateTable.mockReset();
    updateTable.mockResolvedValue(notFound);
  });

  it("does not carry a table's refused save into the Register Table form", async () => {
    render(<TablesPage />);
    const row = (await screen.findByText("orders")).closest("tr") as HTMLElement;
    await userEvent.click(row.querySelector("td:not(.list-select)") as HTMLElement);
    await userEvent.click(await screen.findByTestId("table-read-view-edit"));
    await userEvent.click(await screen.findByTestId("table-edit-save"));
    // The save's own form says why it failed.
    expect(await screen.findByText(/Table 7 not found/)).toBeInTheDocument();

    await userEvent.click(screen.getByTestId("table-edit-cancel"));
    await userEvent.click(screen.getByTestId("tables-add-toggle"));
    await screen.findByTestId("register-many-tables-submit");
    expect(screen.queryByText(/Table 7 not found/)).toBeNull();
  });
});
