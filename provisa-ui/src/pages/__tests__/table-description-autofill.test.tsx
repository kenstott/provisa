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
import { Fragment } from "react";

// react-router-dom 7 ships .mjs files under a package.json without "type":"module",
// so the vmThreads pool loads them as CommonJS and throws. TablesPage only needs
// useNavigate/useSearchParams; stub them (and a no-op MemoryRouter) to avoid the load.
vi.mock("react-router-dom", () => ({
  MemoryRouter: ({ children }: { children: React.ReactNode }) => <Fragment>{children}</Fragment>,
  useNavigate: () => vi.fn(),
  useSearchParams: () => [new URLSearchParams(), vi.fn()],
}));

import { MemoryRouter } from "react-router-dom";

vi.mock("../../context/DomainFilterContext", () => ({
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

vi.mock("../../context/AuthContext", () => ({
  useAuth: () => ({
    role: "admin",
    selectedRoles: ["admin"],
    capabilities: ["admin"],
    domainAccess: ["*"],
  }),
}));

vi.mock("../../components/admin/FilterInput", () => ({
  FilterInput: () => null,
}));

// Only the REST helpers (fetch(), not GraphQL) remain imperative in TablesPage.
vi.mock("../../api/admin", () => ({
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

vi.mock("../../hooks/useAdminQueries", () => ({
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
  useMaterializeStoreInfo: () => ({
    materializeStoreInfo: null,
    loading: false,
    error: undefined,
    refetch: vi.fn(),
  }),
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

// Mantine Select renders a readonly text input with role="combobox". Options mount
// into a portal (role="listbox"/"option") only once the dropdown is open, so choosing
// a value means: click the input to open, wait for the option, click it. Clicking the
// already-selected option again toggles it off (Mantine's default allowDeselect), which
// is how these tests clear a source/schema selection.
async function selectOption(combobox: HTMLElement, name: string) {
  await userEvent.click(combobox);
  // Mantine mounts each Select's options into its own listbox (referenced by the
  // input's aria-controls). jsdom applies no layout, so the dropdown reads as
  // "hidden" to Testing Library — hence { hidden: true }. Scoping to this Select's
  // listbox keeps a shared label (e.g. "public" also used by scope pickers) unique.
  const listboxId = combobox.getAttribute("aria-controls");
  const listbox = listboxId ? document.getElementById(listboxId) : null;
  if (!listbox) throw new Error(`No listbox for combobox ${combobox.getAttribute("data-testid")}`);
  const option = await within(listbox).findByRole("option", { name, hidden: true });
  await userEvent.click(option);
}

// The RegisterTableForm pickers are Mantine Selects. Each input carries its own
// data-testid; keep the original index layout the tests relied on (0=source,
// 1=domain, 2=schema, 3=table) so per-test references stay unchanged.
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

    // Clicking the selected schema option again deselects it, clearing schema/table/desc.
    await selectOption(selects[2], "public");

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

    // Open the schema dropdown and assert the backend-provided option appears.
    await userEvent.click(selects[2]);
    const schemaListbox = document.getElementById(selects[2].getAttribute("aria-controls")!)!;
    await waitFor(() => {
      const schemaOptions = within(schemaListbox).getAllByRole("option", {
        name: "public",
        hidden: true,
      });
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

    // Change source — deselect it (click the selected option again); schema/table reset.
    await selectOption(selects[0], "sales-pg");

    await waitFor(() => {
      expect(selects[2]).toHaveValue("");
    });
  });
});
