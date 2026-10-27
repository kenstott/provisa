// Copyright (c) 2026 Kenneth Stott
// Canary: f1df1dfc-3c6b-46ff-b38f-618da394dffa
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The Views page is the Tables page showing only views, so a view's detail is already a panel
// at the right of the list. What is its own: "+ View" asks only what a view needs in order to
// exist and the view it creates is selected with its detail in edit; and a view's definition
// is edited in the panel, with a way back, not in a dialog over the page.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor, within } from "../../test-utils/render";
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
    ensureDomainChecked,
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
const ok = { success: true, message: "" };
const ensureDomainChecked = vi.fn();
// REQ-2109: one Save registers what is created; the server answers with the new table's id.
const registerTable = vi.fn();
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
  useAllRelationships: () => ({ relationships: [], loading: false, refetch: async () => ({}) }),
  useAvailableColumnsMetadataLazy: () => async () => [],
  useRefreshPolicyPreview: () => async () => null,
  useUpdateTable: () => ({ updateTable, loading: false }),
  useRegisterTable: () => ({ registerTable, loading: false }),
  useUpdateTableNaming: () => ({ updateTableNaming, loading: false }),
  useUpdateTableCache: () => ({ updateTableCache, loading: false }),
  useUpdateTableReplicate: () => ({ updateTableReplicate, loading: false }),
  useUpdateTableLoadProtection: () => ({ updateTableLoadProtection, loading: false }),
}));

// The form that creates or redefines a view, as the page sees it: a new view's definition is
// handed on for its settings; a redefinition is saved.
vi.mock("../tables/ViewDefinitionForm", () => ({
  ViewDefinitionForm: (props: {
    editing: RegisteredTable | null;
    onContinue: (registration: unknown) => void;
    onSuccess: () => void;
  }) => (
    <div data-testid="view-form-stub">
      {props.editing ? `redefining ${props.editing.tableName}` : "a new view"}
      <button
        onClick={() =>
          props.editing
            ? props.onSuccess()
            : props.onContinue({
                input: {
                  sourceId: "__derived__",
                  domainId: "sales",
                  schemaName: "views",
                  tableName: "rev_by_region",
                  alias: "rev_by_region",
                  viewMetrics: { metrics: ["revenue"], dimensions: ["region"], filters: [] },
                  columns: [{ name: "region", visibleTo: ["*"] }],
                },
                pagination: null,
                pagingKind: null,
                pagingCeilingRows: null,
                rowRules: [],
              })
        }
      >
        saved
      </button>
    </div>
  ),
}));

// The register step, as the page sees it: it hands on the table it chose for its settings.
vi.mock("../tables/RegisterTableForm", () => ({
  RegisterTableForm: (props: { onContinue: (registration: unknown) => void }) => (
    <button
      onClick={() =>
        props.onContinue({
          input: {
            sourceId: "sales-pg",
            domainId: "sales",
            schemaName: "sales",
            tableName: "returns",
            columns: [{ name: "id", dataType: "integer", visibleTo: ["analyst"], scope: "domain" }],
          },
          pagination: null,
          pagingKind: null,
          pagingCeilingRows: null,
          rowRules: [],
        })
      }
    >
      registered
    </button>
  ),
}));

import { TablesPage } from "../TablesPage";
import { TABLE_SECTIONS } from "../tables/tableSections";

const view = (id: number, name: string, over: Partial<RegisteredTable> = {}) =>
  table(id, name, [], { schemaName: "views", viewSql: "SELECT 1", ...over });
const panel = () => screen.queryByTestId("tables-split-panel");

beforeEach(() => {
  TABLES = [view(7, "sales_by_day"), table(8, "orders", [])];
  ensureDomainChecked.mockReset();
  registerTable.mockReset();
  refetchTables.mockReset().mockImplementation(async () => ({ data: { tables: TABLES } }));
});

describe("the Views page", () => {
  it("lists only views, and shows the chosen one in the panel", async () => {
    render(<TablesPage viewsOnly />);
    const list = await screen.findByTestId("tables-list");
    expect(within(list).getByText("sales_by_day")).toBeInTheDocument();
    expect(within(list).queryByText("orders")).toBeNull();
    expect(panel()).toBeNull();
    await userEvent.click(within(list).getByText("sales_by_day"));
    expect(screen.getByTestId("tables-split-title")).toHaveTextContent("sales_by_day");
  });

  it("asks every setting of a new view in the editor's sections, then registers and selects it", async () => {
    render(<TablesPage viewsOnly />);
    await userEvent.click(await screen.findByTestId("views-add-toggle"));
    expect(within(panel()!).getByTestId("view-form-stub")).toHaveTextContent("a new view");
    await userEvent.click(screen.getByText("saved"));
    // The new view's settings: the same seven sections as its editor, before anything is saved.
    for (const section of TABLE_SECTIONS)
      expect(within(panel()!).getByTestId(`table-section-${section}`)).toBeInTheDocument();
    expect(screen.getByTestId("tables-split-title")).toHaveTextContent("New view");
    expect(registerTable).not.toHaveBeenCalled();
    // A view is materialized on its Load and timeliness section, where it keeps Time Travel.
    await userEvent.click(screen.getByTestId("table-section-load"));
    await userEvent.click(screen.getByRole("checkbox", { name: /Materialized View/ }));
    expect(screen.getByTestId("time-travel-section")).toBeInTheDocument();
    // The list as the server has it once the view exists: a draft, as every new view is.
    registerTable.mockResolvedValue({ ...ok, params: { id: 9 } });
    refetchTables.mockImplementation(async () => {
      TABLES = [...TABLES, view(9, "rev_by_region", { draft: true })];
      return { data: { tables: TABLES } };
    });
    await userEvent.click(within(panel()!).getByTestId("table-edit-save"));
    await waitFor(() =>
      expect(screen.getByTestId("tables-split-title")).toHaveTextContent("rev_by_region"),
    );
    const input = registerTable.mock.calls[0][0];
    expect(input).toMatchObject({
      tableName: "rev_by_region",
      materialize: true,
      viewMetrics: { metrics: ["revenue"], dimensions: ["region"], filters: [] },
    });
    expect(ensureDomainChecked).toHaveBeenCalledWith("sales");
  });

  it("edits a view's definition in the panel, with a way back, not in a dialog", async () => {
    render(<TablesPage viewsOnly />);
    const list = await screen.findByTestId("tables-list");
    await userEvent.click(within(list).getByText("sales_by_day"));
    await userEvent.click(screen.getByTestId("table-read-view-edit-definition"));
    const surface = within(panel()!).getByTestId("view-definition-modal");
    expect(within(surface).getByTestId("view-form-stub")).toHaveTextContent(
      "redefining sales_by_day",
    );
    expect(screen.queryByRole("dialog")).toBeNull();
    await userEvent.click(screen.getByTestId("view-definition-back"));
    expect(screen.queryByTestId("view-form-stub")).toBeNull();
    expect(screen.getByTestId("table-read-view-edit-definition")).toBeInTheDocument();
  });

  it("returns to the view's detail when its definition has been saved", async () => {
    render(<TablesPage viewsOnly />);
    const list = await screen.findByTestId("tables-list");
    await userEvent.click(within(list).getByText("sales_by_day"));
    await userEvent.click(screen.getByTestId("table-read-view-edit-definition"));
    await userEvent.click(screen.getByText("saved"));
    await waitFor(() => expect(screen.queryByTestId("view-form-stub")).toBeNull());
    expect(screen.getByTestId("tables-split-title")).toHaveTextContent("sales_by_day");
  });
});

describe("the Tables page's register step", () => {
  it("asks every setting of the chosen table in the editor's sections, then registers and selects it", async () => {
    render(<TablesPage />);
    await userEvent.click(await screen.findByTestId("tables-add-toggle"));
    await userEvent.click(screen.getByText("registered"));
    // The create form shows every section the editor shows, on Identity first.
    for (const section of TABLE_SECTIONS)
      expect(within(panel()!).getByTestId(`table-section-${section}`)).toBeInTheDocument();
    expect(screen.getByTestId("table-section-identity")).toHaveAttribute("aria-selected", "true");
    expect(screen.getByTestId("table-edit-table")).toHaveValue("returns");
    await userEvent.click(screen.getByTestId("table-section-query"));
    await userEvent.click(screen.getByRole("checkbox", { name: /Enable Aggregates/ }));
    registerTable.mockResolvedValue({ ...ok, params: { id: 10 } });
    // Stored under another schema than the form sent, as an ingest table is: found by its id.
    refetchTables.mockImplementation(async () => {
      TABLES = [...TABLES, table(10, "returns", [], { draft: true, schemaName: "org_default" })];
      return { data: { tables: TABLES } };
    });
    await userEvent.click(within(panel()!).getByTestId("table-edit-save"));
    await waitFor(() =>
      expect(screen.getByTestId("tables-split-title")).toHaveTextContent("returns"),
    );
    expect(registerTable.mock.calls[0][0]).toMatchObject({
      tableName: "returns",
      enableAggregates: true,
    });
    // Registered through the admin a table starts as draft; nothing releases it unasked.
    expect(setTableDraft).not.toHaveBeenCalled();
  });
});
