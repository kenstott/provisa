// Copyright (c) 2026 Kenneth Stott
// Canary: 45809fec-ad93-4d01-99e1-2cac264c2201
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2109: a table or view that keeps history is badged HISTORY in the Tables and Views lists,
// beside DIMENSION and FACT, and its detail says so with the way to query it as of or as recorded.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, within } from "../../test-utils/render";
import userEvent from "@testing-library/user-event";
import type { RegisteredTable, RoleTtl } from "../../types/admin";
import i18n from "../../i18n";

const navigateTo = vi.fn();
vi.mock("react-router-dom", async (importOriginal) => ({
  ...(await importOriginal<typeof import("react-router-dom")>()),
  useNavigate: () => navigateTo,
}));

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

let TABLES = [
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
// REQ-2153: what another domain lists, names only.
const LISTED = [{ domainId: "hr", tableName: "staff", columnNames: ["id", "title"] }];
const ok = { success: true, message: "" };
const refetchTables = vi.fn();
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
  useTables: () => ({ tables: TABLES, loading: false, refetch: refetchTables }),
  useSources: () => ({ sources: SOURCES, loading: false, refetch: async () => ({}) }),
  useDomains: () => ({ domains: DOMAINS, loading: false, refetch: async () => ({}) }),
  useRoles: () => ({ roles: ROLES, loading: false, refetch: async () => ({}) }),
  useDataProducts: () => ({ dataProducts: [], loading: false, refetch: async () => ({}) }),
  useListedNames: () => LISTED,
  useAllRelationships: () => ({ relationships: [], loading: false, refetch: async () => ({}) }),
  useAvailableColumnsMetadataLazy: () => async () => [],
  useRefreshPolicyPreview: () => async () => null,
  useUpdateTable: () => ({ updateTable, loading: false }),
  useUpdateTableNaming: () => ({ updateTableNaming, loading: false }),
  useUpdateTableCache: () => ({ updateTableCache, loading: false }),
  useUpdateTableReplicate: () => ({ updateTableReplicate, loading: false }),
  useUpdateTableLoadProtection: () => ({ updateTableLoadProtection, loading: false }),
}));

// The form that creates or redefines a view, as the page sees it: it reports the view it made.
vi.mock("../tables/ViewDefinitionForm", () => ({
  ViewDefinitionForm: (props: {
    editing: RegisteredTable | null;
    onSuccess: (created?: string) => void;
  }) => (
    <div data-testid="view-form-stub">
      {props.editing ? `redefining ${props.editing.tableName}` : "a new view"}
      <button onClick={() => props.onSuccess(props.editing ? undefined : "rev_by_region")}>
        saved
      </button>
    </div>
  ),
}));

// The short register step, as the page sees it: it reports the table it registered, by the id the
// server gave it.
vi.mock("../tables/RegisterTableForm", () => ({
  RegisterTableForm: (props: { onSuccess: (registered: { id: number } | null) => void }) => (
    <button onClick={() => props.onSuccess({ id: 10 })}>registered</button>
  ),
}));

import { TablesPage } from "../TablesPage";

const t = i18n.getFixedT("en");
const HISTORY = {
  businessKey: ["trade_id"],
  validTime: "trade_date",
  recordedTime: "updated_at",
};

beforeEach(() => {
  navigateTo.mockReset();
  TABLES = [
    table(7, "trades", [], HISTORY),
    table(8, "orders", [], { enableGroupBy: true }),
    table(9, "trade_log", [], {
      schemaName: "views",
      viewSql: "SELECT 1",
      materialize: true,
      ...HISTORY,
    }),
  ];
  refetchTables.mockReset().mockImplementation(async () => ({ data: { tables: TABLES } }));
});

describe("HISTORY badge", () => {
  it("a bitemporal table has it, with its tooltip, beside DIMENSION; another has none", async () => {
    render(<TablesPage />);
    const list = await screen.findByTestId("tables-list");
    const badge = within(list).getByTestId("tables-history-trades");
    expect(badge).toHaveTextContent(t("tablesPage.history.badge"));
    expect(badge).toHaveAttribute("title", t("tablesPage.history.title"));
    expect(t("tablesPage.history.title")).toBe(
      "Keeps history: query it as of or as recorded (Time Travel)",
    );
    expect(within(list).getByTestId("tables-implied-dimension-orders")).toBeInTheDocument();
    expect(within(list).queryByTestId("tables-history-orders")).toBeNull();
  });

  it("a bitemporal view has it in the Views list", async () => {
    render(<TablesPage viewsOnly />);
    const list = await screen.findByTestId("tables-list");
    expect(within(list).getByTestId("tables-history-trade_log")).toBeInTheDocument();
  });
});

describe("names listed in other domains", () => {
  it("are shown under the list as domain.table and column names, nothing more", async () => {
    render(<TablesPage />);
    const listed = await screen.findByTestId("tables-listed-names");
    expect(listed).toHaveTextContent(t("tablesPage.listedElsewhere"));
    expect(listed).toHaveTextContent("hr.staff: id, title");
  });

  it("are not shown in the Views list", async () => {
    render(<TablesPage viewsOnly />);
    await screen.findByTestId("tables-list");
    expect(screen.queryByTestId("tables-listed-names")).toBeNull();
  });
});

describe("the detail panel of a table that keeps history", () => {
  it("says so, and links to query it as of or as recorded in the SQL explorer", async () => {
    render(<TablesPage />);
    const list = await screen.findByTestId("tables-list");
    await userEvent.click(within(list).getByText("trades"));
    const note = await screen.findByTestId("table-keeps-history");
    expect(note).toHaveTextContent(t("tableReadView.keepsHistory"));
    await userEvent.click(within(note).getByTestId("table-keeps-history-query"));
    expect(navigateTo).toHaveBeenCalledWith("/sql", {
      state: { sql: 'select * from "sales"."trades"' },
    });
  });

  it("says nothing of history for a table that keeps none", async () => {
    render(<TablesPage />);
    const list = await screen.findByTestId("tables-list");
    await userEvent.click(within(list).getByText("orders"));
    expect(await screen.findByTestId("tables-split-title")).toHaveTextContent("orders");
    expect(screen.queryByTestId("table-keeps-history")).toBeNull();
  });
});
