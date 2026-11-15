// Copyright (c) 2026 Kenneth Stott
// Canary: b44cb5df-cb68-4a7f-a347-d4a463123bb9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect, vi, beforeEach } from "vitest";
// Uses the provider-wrapping render (MantineProvider + i18n) required now that
// the form embeds Mantine components (REQ-1016).
import { render, screen, waitFor, within } from "../../test-utils/render";
import userEvent from "@testing-library/user-event";

// The real router is used here: a MemoryRouter supplies useNavigate/useSearchParams with no mock
// at all, which is one less module this file has to keep in sync with the rest of the suite.

// Spread the real module (shared registry — see the react-router-dom mock above).
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

// Spread the real module (shared registry — see the react-router-dom mock above).
vi.mock("../../context/AuthContext", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../context/AuthContext")>();
  return {
    ...actual,
    useAuth: () => ({
      role: "admin",
      selectedRoles: ["admin"],
      // The org administrator's rights: no capability stands in for them (REQ-1327).
      capabilities: actual.DEFAULT_ADMIN_ROLE.effectiveCapabilities,
      domainAccess: ["*"],
    }),
  };
});

vi.mock("../../components/admin/FilterInput", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../components/admin/FilterInput")>()),
  FilterInput: () => null,
}));

// Only the REST helpers (fetch(), not GraphQL) remain imperative in TablesPage. Spread the real
// module: vmThreads + fileParallelism:false share one module registry, so a replace-everything
// factory here leaks into other files' TableEditForm renders and drops exports they need.
vi.mock("../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/admin")>()),
  fetchSettings: vi.fn().mockResolvedValue({
    redirect: { enabled: false, threshold: 10000, default_format: "json", ttl: 3600 },
    sampling: { default_sample_size: 1000 },
    cache: { default_ttl: 300 },
    naming: { domain_prefix: false, convention: "none" },
  }),
  profileTable: vi.fn().mockResolvedValue({ columns: [], rows: [], rowCount: 0 }),
  fetchTableUniqueConstraints: vi.fn().mockResolvedValue([]),
}));

// Module-level hook spies so tests can assert call args directly.
const mockUseAvailableSchemas = vi
  .fn()
  .mockReturnValue({ schemas: ["public", "private"], loading: false });
const mockUseAvailableTables = vi.fn().mockReturnValue({
  tables: [
    { name: "customers", comment: "Registered customer accounts" },
    { name: "orders", comment: "Customer purchase orders" },
    { name: "products", comment: null },
  ],
  loading: false,
});
const getAvailableColumnsMetadata = vi.fn().mockResolvedValue([
  {
    name: "id",
    dataType: "integer",
    comment: "Primary key",
    nativeFilterType: null,
    isPrimaryKey: true,
  },
  {
    name: "name",
    dataType: "varchar",
    comment: "Customer name",
    nativeFilterType: null,
    isPrimaryKey: false,
  },
]);

const SALES_PG_SOURCE = {
  id: "sales-pg",
  type: "postgresql",
  host: "localhost",
  port: 5432,
  database: "sales",
  username: "admin",
  dialect: "postgresql",
  cacheEnabled: false,
  cacheTtl: null,
  allowedDomains: [],
  namingConvention: null,
  path: null,
  description: "",
};
// Mutable so individual tests can override the source list for one render.
let sourcesData: Array<Record<string, unknown>> = [SALES_PG_SOURCE];

const mutationOk = () => ({ success: true, message: "" });

// Stable identities across renders. TablesPage's reload() is a useCallback keyed on
// the refetch fns and runs in an effect, so a fresh fn each render would loop forever.
// Likewise an effect keyed on `tables` re-runs setState on every new array identity.
const EMPTY_TABLES: never[] = [];
const ROLES = [
  { id: "admin", capabilities: ["admin"], effectiveCapabilities: ["admin"], domainAccess: ["*"] },
];
const DOMAINS = [{ id: "sales", description: "Sales data" }];
const refetchTables = vi.fn();
const refetchSources = vi.fn();
const refetchDomains = vi.fn();
const refetchRoles = vi.fn();
const generateTableDescription = vi.fn().mockResolvedValue("");
const generateColumnDescription = vi.fn().mockResolvedValue("");
const registerTable = vi.fn().mockResolvedValue(mutationOk());
const updateTable = vi.fn().mockResolvedValue(mutationOk());
const deleteTable = vi.fn().mockResolvedValue(mutationOk());
const updateTableCache = vi.fn().mockResolvedValue(mutationOk());
const updateTableReplicate = vi.fn().mockResolvedValue(mutationOk());
const updateTableNaming = vi.fn().mockResolvedValue(mutationOk());
const purgeCacheByTable = vi.fn().mockResolvedValue(mutationOk());
const deployViewToDb = vi.fn().mockResolvedValue(mutationOk());

// Spread the real module: vmThreads + fileParallelism:false share one module registry, so a
// replace-everything factory here leaks into other files. In particular a stubbed
// useMaterializeStoreInfo would win over the real Apollo hook that
// TableEditForm.consistency drives through MockedProvider.
vi.mock("../../hooks/useAdminOpsQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminOpsQueries")>()),
  usePurgeCacheByTable: () => ({ purgeCacheByTable, loading: false }),
}));

vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useTables: () => ({ tables: EMPTY_TABLES, loading: false, refetch: refetchTables }),
  useSources: () => ({ sources: sourcesData, loading: false, refetch: refetchSources }),
  useDomains: () => ({ domains: DOMAINS, loading: false, refetch: refetchDomains }),
  useRoles: () => ({ roles: ROLES, loading: false, refetch: refetchRoles }),
  useAvailableSchemas: (...args: Parameters<typeof mockUseAvailableSchemas>) =>
    mockUseAvailableSchemas(...args),
  useAvailableTables: (...args: unknown[]) => mockUseAvailableTables(...args),
  useAvailableColumnsMetadataLazy: () => ({
    fetchColumns: getAvailableColumnsMetadata,
    late: null,
    starting: false,
    startingTimedOut: false,
  }),
  useGenerateTableDescription: () => ({ generateTableDescription, loading: false }),
  useGenerateColumnDescription: () => ({ generateColumnDescription, loading: false }),
  useRegisterTable: () => ({ registerTable, loading: false }),
  useUpdateTable: () => ({ updateTable, loading: false }),
  useDeleteTable: () => ({ deleteTable, loading: false }),
  useUpdateTableCache: () => ({ updateTableCache, loading: false }),
  useUpdateTableReplicate: () => ({ updateTableReplicate, loading: false }),
  useUpdateTableLoadProtection: () => ({ updateTableLoadProtection: vi.fn(), loading: false }),
  useUpdateTableNaming: () => ({ updateTableNaming, loading: false }),
  useCalendars: () => ({ calendars: [], loading: false, error: undefined, refetch: vi.fn() }),
  useCreateCalendar: () => ({ createCalendar: vi.fn(), loading: false, error: undefined }),
  useDeleteCalendar: () => ({ deleteCalendar: vi.fn(), loading: false, error: undefined }),
  // TableEditForm (rendered on edit) uses these; include them so the module mock is complete and a
  // vmThreads cross-file leak (fileParallelism:false shares one context) can't break later tests.
  useRefreshPolicyPreview: () => async () => null,
  useDeployViewToDb: () => ({ deployViewToDb, loading: false }),
  useAllRelationships: () => ({ relationships: [], loading: false, refetch: vi.fn() }),
  useSuggestTableAlias: () => ({
    suggestTableAlias: async (tableName: string) => tableName,
    loading: false,
  }),
}));

// REQ-2234: the Tables field reads each table's columns, and registers the tables picked in one
// action.
const readColumns = (...args: unknown[]) => getAvailableColumnsMetadata(...args);
vi.mock("../../hooks/useAvailableColumnsMetadataLazy", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAvailableColumnsMetadataLazy")>()),
  useReadColumns: () => readColumns,
}));
const registerTables = vi.fn();
vi.mock("../../hooks/useTableQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useTableQueries")>()),
  useRegisterTables: () => ({ registerTables, loading: false }),
}));

import { TablesPage } from "../TablesPage";

// The source and schema pickers are native <select> elements, each with a leading placeholder
// <option value="">; the schema select stays `disabled` until its fetch resolves, so wait for
// the option to exist and the select to be enabled before choosing.
async function selectOption(select: HTMLElement, name: string) {
  await waitFor(() => {
    const el = select as HTMLSelectElement;
    expect(el.disabled).toBe(false);
    expect(Array.from(el.options).some((o) => o.value === name)).toBe(true);
  });
  await userEvent.selectOptions(select, name);
}

const sourceSelect = () => screen.getByTestId("register-table-source-select");
const schemaSelect = () => screen.getByTestId("register-table-schema-select");
const tablesField = () => screen.getByRole("textbox", { name: "Tables" });

/** `table` picked in the Tables field once its columns are read. */
async function pick(table: string) {
  await waitFor(() => expect(tablesField()).not.toBeDisabled());
  await userEvent.click(tablesField());
  const option = () => screen.getByRole("option", { name: new RegExp(`^${table}`), hidden: true });
  await waitFor(() => expect(option()).not.toHaveAttribute("data-combobox-disabled"));
  await userEvent.click(option());
}

// The description the source's comment gave is not asked on Register Table: it is registered
// with the table (REQ-2234) and edited in its Identity section.
async function registeredDescription(): Promise<unknown> {
  await userEvent.click(screen.getByTestId("register-many-tables-submit"));
  await waitFor(() => expect(registerTables).toHaveBeenCalled());
  return registerTables.mock.calls.at(-1)![0][0].description;
}

async function openForm() {
  render(<TablesPage />);
  await waitFor(() => {
    expect(screen.getByRole("heading", { name: /registered tables/i })).toBeInTheDocument();
  });
  await userEvent.click(screen.getByRole("button", { name: "+ Table" }));
  // One domain is offered here, so it is the domain and is not asked: there is no select.
  expect(screen.queryByTestId("register-table-domain-select")).toBeNull();
}

// clearAllMocks wipes implementations too, so re-seed the module-level lazy-hook
// spies (and reset the per-test source override) before each test.
function resetSpies() {
  vi.clearAllMocks();
  sourcesData = [SALES_PG_SOURCE];
  mockUseAvailableSchemas.mockReturnValue({ schemas: ["public", "private"], loading: false });
  mockUseAvailableTables.mockImplementation((_source: unknown, schema: unknown) => ({
    tables: schema
      ? [
          { name: "customers", comment: "Registered customer accounts" },
          { name: "orders", comment: "Customer purchase orders" },
          { name: "products", comment: null },
        ]
      : [],
    loading: false,
  }));
  getAvailableColumnsMetadata.mockResolvedValue([
    {
      name: "id",
      dataType: "integer",
      comment: "Primary key",
      nativeFilterType: null,
      isPrimaryKey: true,
    },
    {
      name: "name",
      dataType: "varchar",
      comment: "Customer name",
      nativeFilterType: null,
      isPrimaryKey: false,
    },
  ]);
  registerTables.mockResolvedValue([
    { tableName: "t", result: { success: true, message: "", params: { id: 1 } } },
  ]);
}

describe("Table description auto-fill from physical database", () => {
  beforeEach(() => {
    resetSpies();
  });

  it("registers the table's comment as its description", async () => {
    await openForm();
    await selectOption(sourceSelect(), "sales-pg");
    await selectOption(schemaSelect(), "public");
    await pick("customers");
    expect(await registeredDescription()).toBe("Registered customer accounts");
  });

  it("leaves description empty when table has no comment", async () => {
    await openForm();
    await selectOption(sourceSelect(), "sales-pg");
    await selectOption(schemaSelect(), "public");
    await pick("products");
    expect(await registeredDescription()).toBeUndefined();
  });

  it("drops what was picked when the schema changes", async () => {
    await openForm();
    await selectOption(sourceSelect(), "sales-pg");
    await selectOption(schemaSelect(), "public");
    await pick("customers");
    expect(screen.getByTestId("register-many-tables-submit")).toHaveTextContent("Register 1 table");
    // Another schema: nothing of the first is kept, and a table picked there without a comment
    // is registered with none.
    await selectOption(schemaSelect(), "private");
    await waitFor(() =>
      expect(screen.getByTestId("register-many-tables-submit")).toHaveTextContent(
        "Register 0 tables",
      ),
    );
    await pick("products");
    expect(await registeredDescription()).toBeUndefined();
  });
});

describe("Schema population — source type routing", () => {
  beforeEach(() => {
    resetSpies();
  });

  it("calls available-schemas for RDBMS sources", async () => {
    await openForm();
    await selectOption(sourceSelect(), "sales-pg");
    await waitFor(() => {
      expect(mockUseAvailableSchemas).toHaveBeenCalledWith("sales-pg");
    });
  });

  it("populates schema dropdown after API response", async () => {
    await openForm();
    await selectOption(sourceSelect(), "sales-pg");
    // A native <select> renders its options inline, so assert on the element's own option list.
    await waitFor(() => {
      const schemaOptions = within(schemaSelect()).getAllByRole("option", { name: "public" });
      expect(schemaOptions.length).toBeGreaterThan(0);
    });
  });

  it.each([
    ["fixed-schema", "my-gql", "graphql"],
    ["kafka", "my-kafka", "kafka"],
  ])(
    "takes the one schema returned by backend for %s sources, with no Schema field",
    async (_kind, id, type) => {
      sourcesData = [
        {
          id,
          type,
          host: "",
          port: 0,
          database: "",
          username: "",
          dialect: type,
          cacheEnabled: false,
          cacheTtl: null,
          allowedDomains: [],
          namingConvention: null,
          path: null,
          description: "",
        },
      ];
      mockUseAvailableSchemas.mockReturnValue({ schemas: ["default"], loading: false });
      await openForm();
      await selectOption(sourceSelect(), id);
      await waitFor(() => {
        expect(mockUseAvailableSchemas).toHaveBeenCalledWith(id);
        expect(mockUseAvailableTables).toHaveBeenLastCalledWith(id, "default");
      });
      expect(screen.queryByTestId("register-table-schema-select")).toBeNull();
    },
  );

  it("resets schema and tables when source changes", async () => {
    await openForm();
    await selectOption(sourceSelect(), "sales-pg");
    await selectOption(schemaSelect(), "public");
    await pick("customers");

    // Change source — clear it; the schema and tables go with it.
    await userEvent.selectOptions(sourceSelect(), "");
    await waitFor(() => {
      expect(screen.queryByTestId("register-table-schema-select")).toBeNull();
      expect(screen.queryByRole("textbox", { name: "Tables" })).toBeNull();
    });
  });
});
