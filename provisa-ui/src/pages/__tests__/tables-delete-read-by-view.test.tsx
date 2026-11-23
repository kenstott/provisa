// Copyright (c) 2026 Kenneth Stott
// Canary: 8e2a5c71-3d94-4b0f-a6e2-9c1d7f40b583
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1986 (amended 2026-11-23): a table a derived table reads is not deleted under it. The
// refusal lists the derived tables that read it, and the page shows that list where the delete
// was offered, each linked to its own panel, so they can be retired first.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, within } from "../../test-utils/render";
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
    registeredAt: "2026-11-01T00:00:00+00:00",
    cacheTtl: 60,
    roleTtl: [],
    pagingKind: null,
    pagination: null,
    pagingCeilingRows: null,
    replicate: null,
    region: null,
    withdrawn: false,
    sourceWithdrawn: false,
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
    mvRebuildEvery: null,
    incrementalOption: { available: true, code: null, reason: null },
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
  table(7, "stores_1"),
  table(9, "kept_1", { sourceId: "__derived__", schemaName: "views", viewSql: "SELECT 1" }),
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
const ROLES = [{ id: "analyst", capabilities: [], domainAccess: ["*"] }];
// The delete is answered as the server answers a table a code resolver's bridge view reads.
const readByView = {
  success: false,
  message: "Table 'stores_1' is still referred to by: table kept_1",
  code: "schema.table_has_dependents",
  params: {
    table: 7,
    name: "stores_1",
    dependents: [
      {
        kind: "table",
        id: 9,
        name: "kept_1",
        label: "kept_1",
        via: ["registered_tables.view_sql"],
      },
    ],
  },
};
const deleteTable = vi.fn();

vi.mock("../../hooks/useRegionQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useRegionQueries")>()),
  useRegionChoices: () => ({ regions: [], connected: null }),
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
  useDeleteTable: () => ({ deleteTable, loading: false }),
}));

import { TablesPage } from "../TablesPage";

describe("TablesPage — a table a derived table reads is not deleted under it", () => {
  beforeEach(() => {
    deleteTable.mockReset();
    deleteTable.mockResolvedValue(readByView);
  });

  it("lists the derived tables that read it where the delete was offered", async () => {
    render(<TablesPage />);
    const row = (await screen.findByText("stores_1")).closest("tr") as HTMLElement;
    await userEvent.click(row.querySelector("td:not(.list-select)") as HTMLElement);
    await userEvent.click(await screen.findByTestId("table-read-view-delete"));
    const confirm = await screen.findByRole("dialog");
    await userEvent.click(within(confirm).getByRole("button", { name: "Delete" }));

    expect(deleteTable).toHaveBeenCalledWith(7);
    expect(await screen.findByText("Cannot delete stores_1 yet")).toBeInTheDocument();
    const listed = screen.getByTestId("dependents-table");
    const link = within(listed).getByRole("link", { name: "kept_1" });
    expect(link.getAttribute("href")).toContain("9");
  });
});
