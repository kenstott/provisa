// Copyright (c) 2026 Kenneth Stott
// Canary: 5831a86c-ddff-4bfe-841e-743d3b1a5a1d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The Tables page is a list with a detail panel on its right: selecting a table shows it there,
// editing happens there, and the create forms open there. Nothing opens inside the list.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "../../test-utils/render";
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
  table(7, "orders", [], {
    region: "eu",
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

async function select(name: string) {
  // The name is in the list and, once selected, in the panel's title: take the list's.
  const list = await screen.findByTestId("tables-list");
  const row = (await within(list).findByText(name)).closest("tr") as HTMLElement;
  await userEvent.click(row.querySelector("td") as HTMLElement);
  return row;
}

describe("TablesPage — the detail panel", () => {
  beforeEach(() => localStorage.clear());

  it("opens on a linked column: its table's Columns section, the row marked", async () => {
    Element.prototype.scrollIntoView = vi.fn();
    render(<TablesPage />, {
      initialEntries: ["/tables?table=7&section=columns&column=email"],
    });
    const panel = await screen.findByTestId("tables-split-panel");
    expect(within(panel).getByTestId("tables-split-title")).toHaveTextContent("orders");
    expect(within(panel).getByTestId("table-read-column-email")).toHaveAttribute(
      "data-linked",
      "true",
    );
  });

  it("has no panel until a table is selected", async () => {
    render(<TablesPage />);
    await screen.findByTestId("tables-list");
    expect(screen.queryByTestId("tables-split-panel")).toBeNull();
  });

  it("shows the selected table in the panel, not inside the list", async () => {
    render(<TablesPage />);
    const row = await select("orders");
    const panel = await screen.findByTestId("tables-split-panel");
    expect(within(panel).getByTestId("tables-split-title")).toHaveTextContent("orders");
    expect(within(panel).getByTestId("table-read-view-edit")).toBeInTheDocument();
    expect(row).toHaveAttribute("aria-selected", "true");
    // The list holds rows only: no detail row opens beneath the selected one.
    expect(screen.getByTestId("tables-list").querySelector("tr.list-expand")).toBeNull();
  });

  it("edits the table in the same panel", async () => {
    render(<TablesPage />);
    await select("orders");
    await userEvent.click(await screen.findByTestId("table-read-view-edit"));
    const panel = screen.getByTestId("tables-split-panel");
    expect(await within(panel).findByTestId("table-draft-checkbox")).toBeInTheDocument();
  });

  it("closes from the panel’s own button, and from a second click on the row", async () => {
    render(<TablesPage />);
    const row = await select("orders");
    await userEvent.click(await screen.findByTestId("tables-split-close"));
    await waitFor(() => expect(screen.queryByTestId("tables-split-panel")).toBeNull());
    expect(row).toHaveAttribute("aria-selected", "false");
    await select("orders");
    await screen.findByTestId("tables-split-panel");
    await select("orders");
    await waitFor(() => expect(screen.queryByTestId("tables-split-panel")).toBeNull());
  });

  it("opens the register-table form in the panel", async () => {
    render(<TablesPage />);
    await userEvent.click(await screen.findByTestId("tables-add-toggle"));
    const panel = await screen.findByTestId("tables-split-panel");
    expect(within(panel).getByTestId("register-table-source-select")).toBeInTheDocument();
    // Titled for what it is, not with the button's words.
    expect(within(panel).getByTestId("tables-split-title")).toHaveTextContent("Register table");
  });

  it("keeps the table's actions in the panel's pinned head", async () => {
    render(<TablesPage />);
    await select("orders");
    const head = (await screen.findByTestId("tables-split-title")).parentElement as HTMLElement;
    for (const id of ["table-read-view-preview", "table-read-view-edit", "table-read-view-draft"]) {
      expect(await within(head).findByTestId(id)).toBeInTheDocument();
    }
  });

  it("puts Save and Cancel in the head while editing, in place of the table's actions", async () => {
    render(<TablesPage />);
    await select("orders");
    const head = screen.getByTestId("tables-split-title").parentElement as HTMLElement;
    expect(await within(head).findByTestId("table-read-view-preview")).toBeInTheDocument();
    await userEvent.click(await screen.findByTestId("table-read-view-edit"));
    await screen.findByTestId("table-draft-checkbox");
    // The name, then Save and Cancel: nothing of the read view's actions is left to grey out.
    const save = await within(head).findByTestId("table-edit-save");
    const cancel = within(head).getByTestId("table-edit-cancel");
    expect(head).toHaveTextContent(/orders.*Save.*Cancel/);
    expect(save.compareDocumentPosition(cancel) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    for (const gone of ["preview", "profile", "policies", "edit", "delete"]) {
      expect(within(head).queryByTestId(`table-read-view-${gone}`)).toBeNull();
    }
    // The panel has no row of buttons at its foot: the form has its whole height.
    const body = screen.getByTestId("tables-split-panel").querySelector(".split-pane__body")!;
    expect(body.querySelector(".form-footer")).toBeNull();
    // Cancel gives the head back to the table's actions.
    await userEvent.click(cancel);
    expect(await within(head).findByTestId("table-read-view-preview")).toBeEnabled();
    expect(within(head).queryByTestId("table-edit-save")).toBeNull();
  });

  it("puts a create form's Continue and Cancel in the head too", async () => {
    render(<TablesPage />);
    await userEvent.click(await screen.findByTestId("tables-add-toggle"));
    const head = (await screen.findByTestId("tables-split-title")).parentElement as HTMLElement;
    // Choosing what is registered continues to its settings, where Save registers it (REQ-2109).
    expect(await within(head).findByTestId("register-table-submit")).toHaveTextContent("Continue");
    expect(within(head).getByTestId("register-table-cancel")).toHaveTextContent("Cancel");
    const body = screen.getByTestId("tables-split-panel").querySelector(".split-pane__body")!;
    expect(body.querySelector(".form-footer")).toBeNull();
  });

  it("closes on Escape while a table is only being looked at, and not while it is edited", async () => {
    render(<TablesPage />);
    await select("orders");
    await userEvent.click(await screen.findByTestId("table-read-view-edit"));
    await screen.findByTestId("table-draft-checkbox");
    fireEvent.keyDown(screen.getByTestId("tables-split-panel"), { key: "Escape" });
    expect(screen.getByTestId("tables-split-panel")).toBeInTheDocument();
  });

  it("shows another table when its row is clicked, one at a time", async () => {
    render(<TablesPage />);
    await select("orders");
    const other = await select("staging");
    expect(screen.getByTestId("tables-split-title")).toHaveTextContent("staging");
    expect(other).toHaveAttribute("aria-selected", "true");
    expect(screen.getAllByRole("complementary")).toHaveLength(1);
  });

  it("opens Preview inside the panel, with a way back to the table's detail", async () => {
    render(<TablesPage />);
    await select("orders");
    await userEvent.click(await screen.findByTestId("table-read-view-preview"));
    const panel = screen.getByTestId("tables-split-panel");
    expect(await within(panel).findByTestId("table-preview-modal")).toBeInTheDocument();
    expect(screen.queryByRole("dialog")).toBeNull();
    await userEvent.click(within(panel).getByTestId("table-preview-back"));
    expect(await within(panel).findByTestId("table-read-view-edit")).toBeInTheDocument();
  });
});
