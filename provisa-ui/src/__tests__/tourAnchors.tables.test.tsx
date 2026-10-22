// Copyright (c) 2026 Kenneth Stott
// Canary: 795c50d2-e73a-4970-9ea4-fb7b1a6fb303
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Tour anchors (test-utils/tourAnchor): the core tour's steps on the Tables page, in the state the
// core is offered in -- the org holds the pet store's tables (tourPrerequisites.json) -- and the
// Quality and Test data Deep Dives' steps there, offered with the checker's and the pets tables.

import { describe, it, expect, vi } from "vitest";
import { render } from "../test-utils/render";
import { stepPageReady, walkTourStep, walkTourSteps } from "../test-utils/tourAnchor";
import type { RegisteredTable, RoleTtl } from "../types/admin";

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

const SOURCES = [
  {
    id: "pet-store-sqlite",
    type: "sqlite",
    host: "",
    port: null,
    database: "",
    username: "",
    dialect: "sqlite",
    cacheEnabled: true,
    cacheTtl: null,
    allowedDomains: [],
    namingConvention: null,
    path: "pets.db",
    description: "",
  },
  {
    id: "dq-checker",
    type: "great_expectations",
    host: "localhost",
    port: 5439,
    database: "provisa",
    username: "org_admin",
    dialect: "postgresql",
    cacheEnabled: true,
    cacheTtl: null,
    allowedDomains: [],
    namingConvention: null,
    path: null,
    description: "",
  },
];

// One value per hook across renders: a fresh array each render reruns the page's effects forever.
const REFETCH = vi.fn();
const NONE: never[] = [];
const TABLES = {
  tables: [
    table(1, "pets", [], {
      sourceId: "pet-store-sqlite",
      domainId: "pet-store",
      schemaName: "pet_store",
    }),
    table(2, "pets_quality_dq_checker_dq", [], {
      sourceId: "dq-checker",
      domainId: "pet-store",
      schemaName: "pet_store",
    }),
  ],
  loading: false,
  refetch: REFETCH,
};
const DOMAINS = {
  domains: [{ id: "pet-store", description: "" }],
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
  useUpdateTable: () => ({ updateTable: vi.fn(), loading: false }),
  useUpdateTableNaming: () => ({ updateTableNaming: vi.fn(), loading: false }),
  useUpdateTableCache: () => ({ updateTableCache: vi.fn(), loading: false }),
  useUpdateTableReplicate: () => ({ updateTableReplicate: vi.fn(), loading: false }),
  useUpdateTableLoadProtection: () => ({ updateTableLoadProtection: vi.fn(), loading: false }),
}));

import { TablesPage } from "../pages/TablesPage";

describe("the core tour's steps on the Tables page", () => {
  it("is ready for step4", async () => {
    render(<TablesPage />, { initialEntries: ["/tables"] });
    expect(await stepPageReady("step4")).toBeInTheDocument();
  });
  for (const key of ["step5", "stepPreview"]) {
    it(`finds ${key}'s anchor`, async () => {
      render(<TablesPage />, { initialEntries: ["/tables"] });
      expect(await walkTourStep(key)).toBeInTheDocument();
    });
  }
});

describe("the Quality and Test data Deep Dives' steps on the Tables page", () => {
  it("walks the checker table into its Quality panel", async () => {
    render(<TablesPage />, { initialEntries: ["/tables?source=dq-checker"] });
    await walkTourSteps(["stepQualityTable", "stepQualityPanel"]);
  });
  // Each of the pets table's cards opens its own starting state (PETS_EDIT_OPEN), so each is
  // entered cold, as Back and resume enter it.
  for (const key of [
    "stepQualityProfile",
    "stepProfilerChecks",
    "stepTestDataMap",
    "stepTestDataProfiler",
    "stepFakes",
    "stepFakesFill",
  ]) {
    it(`finds ${key}'s anchor`, async () => {
      render(<TablesPage />, { initialEntries: ["/tables?source=pet-store-sqlite"] });
      expect(await walkTourStep(key)).toBeInTheDocument();
    });
  }
});
