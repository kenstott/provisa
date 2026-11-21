// Copyright (c) 2026 Kenneth Stott
// Canary: 6da25d9f-b859-4996-9107-0fd28ee26788
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1923/REQ-2234: a Wikidata table handed over from a source's Register tables step is created
// on the Tables page: Save registers it, with its SPARQL query; the page never leaves it unsaved.

import { describe, it, expect, vi } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { Route, Routes } from "react-router-dom";
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
  {
    id: "test",
    type: "sparql",
    host: "https://query.wikidata.org/sparql",
    port: 443,
    database: "",
    username: "",
    dialect: "sparql",
    cacheEnabled: true,
    cacheTtl: 86400,
    changeSignal: "ttl",
    allowedDomains: [],
    namingConvention: null,
    path: null,
    description: "",
    federationHintsJson: JSON.stringify({ brand: "wikidata" }),
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
  useTableDetails: () => TABLES,
  useSources: () => ({ sources: SOURCES, loading: false, refetch: REFETCH }),
  useDomains: () => DOMAINS,
  useRoles: () => ({ roles: NONE, loading: false, refetch: REFETCH }),
  useDataProducts: () => ({ dataProducts: NONE, loading: false, refetch: REFETCH }),
  useAllRelationships: () => ({ relationships: NONE, loading: false, refetch: REFETCH }),
  useAvailableColumnsMetadataLazy: () => ({
    fetchColumns: async () => [],
    late: null,
    starting: false,
    startingTimedOut: false,
  }),
  useRefreshPolicyPreview: () => async () => null,
  useRegisterTable: () => ({ registerTable, loading: false }),
  useUpdateTable: () => ({ updateTable: vi.fn(), loading: false }),
  useUpdateTableNaming: () => ({ updateTableNaming: vi.fn(), loading: false }),
  useUpdateTableCache: () => ({ updateTableCache: vi.fn(), loading: false }),
  useUpdateTableReplicate: () => ({ updateTableReplicate: vi.fn(), loading: false }),
  useUpdateTableLoadProtection: () => ({ updateTableLoadProtection: vi.fn(), loading: false }),
}));

import { TablesPage } from "../pages/TablesPage";

// What Continue on the Wikidata source's Register tables step hands over: the starter table chosen.
const WIKIDATA_TABLE: Registration = {
  input: {
    sourceId: "test",
    domainId: "sales",
    schemaName: "sparql",
    tableName: "countries",
    queryTemplate: "SELECT ?id ?name WHERE { }",
    discover: false,
    columns: [
      { name: "id", dataType: "varchar", visibleTo: ["admin"] },
      { name: "name", dataType: "varchar", visibleTo: ["admin"] },
    ],
  },
  pagination: null,
  pagingKind: null,
  pagingCeilingRows: null,
  rowRules: [],
};

describe("a Wikidata table handed over from its source's Register tables step", () => {
  it("opens to be created on the Tables page, and Save registers it with its query", async () => {
    registerTable.mockReset().mockResolvedValue({
      success: true,
      message: "",
      code: "schema.table_registered",
      params: { table: "countries", id: 9 },
    });
    render(
      <Routes>
        <Route path="/sources" element={<div data-testid="sources-page" />} />
        <Route path="/tables" element={<TablesPage />} />
      </Routes>,
      { initialEntries: [{ pathname: "/tables", state: { createTable: WIKIDATA_TABLE } }] },
    );
    await userEvent.click(await screen.findByTestId("table-edit-save"));
    await waitFor(() => expect(registerTable).toHaveBeenCalledTimes(1));
    expect(registerTable.mock.calls[0][0]).toMatchObject({
      sourceId: "test",
      schemaName: "sparql",
      tableName: "countries",
      queryTemplate: "SELECT ?id ?name WHERE { }",
    });
    expect(screen.queryByTestId("sources-page")).toBeNull();
  });
});
