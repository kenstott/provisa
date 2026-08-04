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
import { MemoryRouter } from "react-router-dom";

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
vi.mock("../../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../context/AuthContext")>()),
  useAuth: () => ({
    role: "admin",
    selectedRoles: ["admin"],
    capabilities: ["admin"],
    domainAccess: ["*"],
  }),
}));

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
const ROLES = [{ id: "admin", capabilities: ["admin"], domainAccess: ["*"] }];
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
const updateTablePreferMaterialized = vi.fn().mockResolvedValue(mutationOk());
const updateTableNaming = vi.fn().mockResolvedValue(mutationOk());
const purgeCacheByTable = vi.fn().mockResolvedValue(mutationOk());
const invalidateFileSource = vi.fn().mockResolvedValue(mutationOk());
const deployViewToDb = vi.fn().mockResolvedValue(mutationOk());

// Spread the real module: vmThreads + fileParallelism:false share one module registry, so a
// replace-everything factory here leaks into other files. In particular a stubbed
// useMaterializeStoreInfo would win over the real Apollo hook that
// TableEditForm.consistency drives through MockedProvider.
vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useTables: () => ({ tables: EMPTY_TABLES, loading: false, refetch: refetchTables }),
  useSources: () => ({ sources: sourcesData, loading: false, refetch: refetchSources }),
  useDomains: () => ({ domains: DOMAINS, loading: false, refetch: refetchDomains }),
  useRoles: () => ({ roles: ROLES, loading: false, refetch: refetchRoles }),
  useAvailableSchemas: (...args: Parameters<typeof mockUseAvailableSchemas>) =>
    mockUseAvailableSchemas(...args),
  useAvailableTables: (...args: unknown[]) => mockUseAvailableTables(...args),
  useAvailableColumnsMetadataLazy: () => getAvailableColumnsMetadata,
  useGenerateTableDescription: () => ({ generateTableDescription, loading: false }),
  useGenerateColumnDescription: () => ({ generateColumnDescription, loading: false }),
  useRegisterTable: () => ({ registerTable, loading: false }),
  useUpdateTable: () => ({ updateTable, loading: false }),
  useDeleteTable: () => ({ deleteTable, loading: false }),
  useUpdateTableCache: () => ({ updateTableCache, loading: false }),
  useUpdateTablePreferMaterialized: () => ({ updateTablePreferMaterialized, loading: false }),
  useUpdateTableLoadProtection: () => ({ updateTableLoadProtection: vi.fn(), loading: false }),
  useUpdateTableNaming: () => ({ updateTableNaming, loading: false }),
  useCalendars: () => ({ calendars: [], loading: false, error: undefined, refetch: vi.fn() }),
  useCreateCalendar: () => ({ createCalendar: vi.fn(), loading: false, error: undefined }),
  useDeleteCalendar: () => ({ deleteCalendar: vi.fn(), loading: false, error: undefined }),
  // TableEditForm (rendered on edit) uses these; include them so the module mock is complete and a
  // vmThreads cross-file leak (fileParallelism:false shares one context) can't break later tests.
  useRefreshPolicyPreview: () => async () => null,
  usePurgeCacheByTable: () => ({ purgeCacheByTable, loading: false }),
  useInvalidateFileSource: () => ({ invalidateFileSource, loading: false }),
  useDeployViewToDb: () => ({ deployViewToDb, loading: false }),
  useAllRelationships: () => ({ relationships: [], loading: false, refetch: vi.fn() }),
  useSuggestTableAlias: () => ({
    suggestTableAlias: async (tableName: string) => tableName,
    loading: false,
  }),
}));

import { TablesPage } from "../TablesPage";

// The RegisterTableForm pickers are native <select> elements (RegisterTableForm.tsx:276-355),
// each with a leading placeholder <option value="">. Option value and label are identical for
// all four, so selecting by value is selecting by visible name. The schema and table selects
// stay `disabled` until their prerequisite fetch resolves and are repopulated when the source
// changes, so wait for the option to exist and the select to be enabled before choosing.
async function selectOption(select: HTMLElement, name: string) {
  await waitFor(() => {
    const el = select as HTMLSelectElement;
    expect(el.disabled).toBe(false);
    expect(Array.from(el.options).some((o) => o.value === name)).toBe(true);
  });
  await userEvent.selectOptions(select, name);
}

// Clearing a native select means selecting its placeholder back — there is no click-the-
// selected-option-again deselect the way a Mantine Select offers.
async function clearOption(select: HTMLElement) {
  await userEvent.selectOptions(select, "");
}

// Each picker carries its own data-testid; keep the original index layout the tests relied on
// (0=source, 1=domain, 2=schema, 3=table) so per-test references stay unchanged.
function formSelects(): HTMLElement[] {
  const arr: HTMLElement[] = [];
  arr[0] = screen.getByTestId("register-table-source-select");
  arr[1] = screen.getByTestId("register-table-domain-select");
  arr[2] = screen.getByTestId("register-table-schema-select");
  arr[3] = screen.getByTestId("register-table-table-select");
  return arr;
}

function renderPage() {
  return render(
    <MemoryRouter>
      <TablesPage />
    </MemoryRouter>,
  );
}

// clearAllMocks wipes implementations too, so re-seed the module-level lazy-hook
// spies (and reset the per-test source override) before each test.
function resetSpies() {
  vi.clearAllMocks();
  sourcesData = [SALES_PG_SOURCE];
  mockUseAvailableSchemas.mockReturnValue({ schemas: ["public", "private"], loading: false });
  mockUseAvailableTables.mockReturnValue({
    tables: [
      { name: "customers", comment: "Registered customer accounts" },
      { name: "orders", comment: "Customer purchase orders" },
      { name: "products", comment: null },
    ],
    loading: false,
  });
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
}

describe("Table description auto-fill from physical database", () => {
  beforeEach(() => {
    resetSpies();
  });

  it("prefills table description from comment when table is selected", async () => {
    renderPage();

    await waitFor(() => {
      expect(screen.getByRole("heading", { name: /registered tables/i })).toBeInTheDocument();
    });

    await userEvent.click(screen.getByRole("button", { name: "+ Table" }));

    const selects = formSelects();
    await selectOption(selects[0], "sales-pg");
    await selectOption(selects[2], "public");
    await selectOption(selects[3], "customers");

    await waitFor(() => {
      const descInput = screen.getByPlaceholderText(/appears in sdl docs/i);
      expect(descInput).toHaveValue("Registered customer accounts");
    });
  });

  it("leaves description empty when table has no comment", async () => {
    renderPage();

    await waitFor(() => {
      expect(screen.getByRole("heading", { name: /registered tables/i })).toBeInTheDocument();
    });

    await userEvent.click(screen.getByRole("button", { name: "+ Table" }));

    const selects = formSelects();
    await selectOption(selects[0], "sales-pg");
    await selectOption(selects[2], "public");
    await selectOption(selects[3], "products");

    await waitFor(() => {
      const descInput = screen.getByPlaceholderText(/appears in sdl docs/i);
      expect(descInput).toHaveValue("");
    });
  });

  it("clears description when schema changes", async () => {
    renderPage();

    await waitFor(() => {
      expect(screen.getByRole("heading", { name: /registered tables/i })).toBeInTheDocument();
    });

    await userEvent.click(screen.getByRole("button", { name: "+ Table" }));

    const selects = formSelects();
    await selectOption(selects[0], "sales-pg");
    await selectOption(selects[2], "public");
    await selectOption(selects[3], "customers");

    await waitFor(() => {
      const descInput = screen.getByPlaceholderText(/appears in sdl docs/i);
      expect(descInput).toHaveValue("Registered customer accounts");
    });

    // Clearing the schema clears table and description with it.
    await clearOption(selects[2]);

    await waitFor(() => {
      const descInput = screen.getByPlaceholderText(/appears in sdl docs/i);
      expect(descInput).toHaveValue("");
    });
  });
});

describe("Schema population — source type routing", () => {
  beforeEach(() => {
    resetSpies();
  });

  it("calls available-schemas for RDBMS sources", async () => {
    renderPage();

    await waitFor(() => screen.getByRole("heading", { name: /registered tables/i }));
    await userEvent.click(screen.getByRole("button", { name: "+ Table" }));

    const selects = formSelects();
    await selectOption(selects[0], "sales-pg");

    await waitFor(() => {
      expect(mockUseAvailableSchemas).toHaveBeenCalledWith("sales-pg");
    });
  });

  it("populates schema dropdown after API response", async () => {
    renderPage();

    await waitFor(() => screen.getByRole("heading", { name: /registered tables/i }));
    await userEvent.click(screen.getByRole("button", { name: "+ Table" }));

    const selects = formSelects();
    await selectOption(selects[0], "sales-pg");

    // A native <select> renders its options inline, so assert on the element's own option list.
    await waitFor(() => {
      const schemaOptions = within(selects[2]).getAllByRole("option", { name: "public" });
      expect(schemaOptions.length).toBeGreaterThan(0);
    });
  });

  it("auto-selects single schema returned by backend for fixed-schema sources", async () => {
    sourcesData = [
      {
        id: "my-gql",
        type: "graphql",
        host: "",
        port: 0,
        database: "",
        username: "",
        dialect: "graphql",
        cacheEnabled: false,
        cacheTtl: null,
        allowedDomains: [],
        namingConvention: null,
        path: null,
        description: "",
      },
    ];
    mockUseAvailableSchemas.mockReturnValue({ schemas: ["default"], loading: false });

    renderPage();

    await waitFor(() => screen.getByRole("heading", { name: /registered tables/i }));
    await userEvent.click(screen.getByRole("button", { name: "+ Table" }));

    const selects = formSelects();
    await selectOption(selects[0], "my-gql");

    await waitFor(() => {
      expect(mockUseAvailableSchemas).toHaveBeenCalledWith("my-gql");
      expect(selects[2]).toHaveValue("default");
    });
  });

  it("auto-selects single schema returned by backend for kafka sources", async () => {
    sourcesData = [
      {
        id: "my-kafka",
        type: "kafka",
        host: "",
        port: 0,
        database: "",
        username: "",
        dialect: "kafka",
        cacheEnabled: false,
        cacheTtl: null,
        allowedDomains: [],
        namingConvention: null,
        path: null,
        description: "",
      },
    ];
    mockUseAvailableSchemas.mockReturnValue({ schemas: ["default"], loading: false });

    renderPage();

    await waitFor(() => screen.getByRole("heading", { name: /registered tables/i }));
    await userEvent.click(screen.getByRole("button", { name: "+ Table" }));

    const selects = formSelects();
    await selectOption(selects[0], "my-kafka");

    await waitFor(() => {
      expect(mockUseAvailableSchemas).toHaveBeenCalledWith("my-kafka");
      expect(selects[2]).toHaveValue("default");
    });
  });

  it("resets schema and table when source changes", async () => {
    renderPage();

    await waitFor(() => screen.getByRole("heading", { name: /registered tables/i }));
    await userEvent.click(screen.getByRole("button", { name: "+ Table" }));

    const selects = formSelects();
    await selectOption(selects[0], "sales-pg");
    await selectOption(selects[2], "public");
    await selectOption(selects[3], "customers");

    // Change source — clear it; schema/table reset.
    await clearOption(selects[0]);

    await waitFor(() => {
      expect(selects[2]).toHaveValue("");
    });
  });
});
