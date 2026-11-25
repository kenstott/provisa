// Copyright (c) 2026 Kenneth Stott
// Canary: 1a2b3c4d-5e6f-7890-abcd-ef1234567890
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2180 (amended 2026-11-24): loading the Tables or Views page as anonymous must not show an
// error banner. Reading suggestion counts never requires a suggester; anonymous gets an empty list.
// Creating a suggestion as anonymous is refused inside the suggest dialog, not as a page banner.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "../../test-utils/render";
import type { RegisteredTable, RoleTtl } from "../../types/admin";

vi.mock("react-router-dom", async (importOriginal) => ({
  ...(await importOriginal<typeof import("react-router-dom")>()),
  useNavigate: () => vi.fn(),
}));

vi.mock("../../context/DomainFilterContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../context/DomainFilterContext")>()),
  useDomainFilter: () => ({
    checkedDomains: new Set<string>(),
    domains: [],
    domainsEnabled: false,
    setDomains: vi.fn(),
    selectedDomain: null,
    setSelectedDomain: vi.fn(),
    toggleDomain: vi.fn(),
    ensureDomainChecked: vi.fn(),
  }),
}));

vi.mock("../../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../context/AuthContext")>()),
  useAuth: () => ({
    role: null,
    selectedRoles: [],
    capabilities: [],
    domainAccess: [],
    actingRoles: null,
    domainAccess: [],
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

vi.mock("../../hooks/useRegionQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useRegionQueries")>()),
  useRegionChoices: () => ({ regions: [], connected: null, error: null }),
}));

vi.mock("../../hooks/useAdminOpsQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminOpsQueries")>()),
  usePurgeCacheByTable: () => ({ purgeCacheByTable: vi.fn(), loading: false }),
  useUpdateTableRoleTtl: () => ({ updateTableRoleTtl: vi.fn(), loading: false }),
  useReplicaBuilds: () => ({ replicaBuilds: null }),
}));

function table(id: number, tableName: string, overrides: Partial<RegisteredTable> = {}): RegisteredTable {
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
    roleTtl: [] as RoleTtl[],
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
    productIds: [],
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

const TABLES = [table(1, "orders"), table(2, "invoices", { viewSql: "SELECT 1" })];

vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useTableDetails: () => ({ tables: TABLES, loading: false, error: null, refetch: async () => ({}) }),
  useSources: () => ({ sources: [], loading: false, refetch: async () => ({}) }),
  useDomains: () => ({ domains: [], loading: false, refetch: async () => ({}) }),
  useRoles: () => ({ roles: [], loading: false, refetch: async () => ({}) }),
  useDataProducts: () => ({ dataProducts: [], loading: false, refetch: async () => ({}) }),
  useListedNames: () => [],
  useAllRelationships: () => ({ relationships: [], loading: false, refetch: async () => ({}) }),
  useAvailableColumnsMetadataLazy: () => ({
    fetchColumns: async () => [],
    late: null,
    starting: false,
    startingTimedOut: false,
  }),
  useRefreshPolicyPreview: () => async () => null,
  useUpdateTable: () => ({ updateTable: vi.fn(), loading: false }),
  useGenerateTableDescription: () => ({ generateTableDescription: vi.fn() }),
  useGenerateColumnDescription: () => ({ generateColumnDescription: vi.fn() }),
  useColumnDependents: () => vi.fn(),
  useDeleteTable: () => ({ deleteTable: vi.fn() }),
  useDeleteTables: () => vi.fn(),
  useDeployViewToDb: () => ({ deployViewToDb: vi.fn() }),
  useSuggestTableAlias: () => ({ suggestTableAlias: vi.fn() }),
  useForceRegen: () => ({ forceRegen: vi.fn() }),
}));

// REQ-2180: suggestion counts — anonymous caller receives an empty list (no banner).
const mockSuggestionCounts = vi.fn();
vi.mock("../../api/productSuggestions", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/productSuggestions")>()),
  suggestionCounts: () => mockSuggestionCounts(),
}));

import { TablesPage } from "../TablesPage";

beforeEach(() => {
  mockSuggestionCounts.mockReset().mockResolvedValue([]);
  localStorage.clear();
});

describe("Tables and Views pages — anonymous user shows no banner on load (REQ-2180)", () => {
  it("Tables page: suggestion counts resolving with [] shows no error banner", async () => {
    render(<TablesPage />);
    await screen.findByTestId("tables-list");
    await waitFor(() => expect(mockSuggestionCounts).toHaveBeenCalled());
    expect(screen.queryByTestId("tables-error")).toBeNull();
  });

  it("Views page: suggestion counts resolving with [] shows no error banner", async () => {
    render(<TablesPage viewsOnly />);
    await screen.findByTestId("tables-list");
    await waitFor(() => expect(mockSuggestionCounts).toHaveBeenCalled());
    expect(screen.queryByTestId("tables-error")).toBeNull();
  });

  it("Tables page: a counts error not from sign-in still shows a banner", async () => {
    mockSuggestionCounts.mockRejectedValue(new Error("Service unavailable"));
    render(<TablesPage />);
    await screen.findByTestId("tables-list");
    await waitFor(() => expect(screen.queryByTestId("tables-error")).not.toBeNull());
    expect(screen.getByTestId("tables-error")).toHaveTextContent("Service unavailable");
  });
});
