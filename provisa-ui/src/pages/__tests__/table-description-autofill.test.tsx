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
import { render, screen, waitFor } from "@testing-library/react";
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
  useDomainFilter: () => ({ checkedDomains: new Set<string>(), domains: [], setDomains: vi.fn(), selectedDomain: null, setSelectedDomain: vi.fn(), toggleDomain: vi.fn() }),
}));

vi.mock("../../context/AuthContext", () => ({
  useAuth: () => ({ role: "admin", selectedRoles: ["admin"], capabilities: ["admin"], domainAccess: ["*"] }),
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
}));

// Module-level lazy-hook spies so tests can assert call args directly.
const getAvailableSchemas = vi.fn().mockResolvedValue(["public"]);
const getAvailableTables = vi.fn().mockResolvedValue([
  { name: "customers", comment: "Registered customer accounts" },
  { name: "orders", comment: "Customer purchase orders" },
  { name: "products", comment: null },
]);
const getAvailableColumnsMetadata = vi.fn().mockResolvedValue([
  { name: "id", dataType: "integer", comment: "Primary key", nativeFilterType: null, isPrimaryKey: true },
  { name: "name", dataType: "varchar", comment: "Customer name", nativeFilterType: null, isPrimaryKey: false },
]);

const SALES_PG_SOURCE = {
  id: "sales-pg", type: "postgresql", host: "localhost", port: 5432, database: "sales", username: "admin", dialect: "postgresql", cacheEnabled: false, cacheTtl: null, allowedDomains: [], namingConvention: null, path: null, description: "",
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
const updateTableNaming = vi.fn().mockResolvedValue(mutationOk());
const purgeCacheByTable = vi.fn().mockResolvedValue(mutationOk());
const invalidateFileSource = vi.fn().mockResolvedValue(mutationOk());
const deployViewToDb = vi.fn().mockResolvedValue(mutationOk());

vi.mock("../../hooks/useAdminQueries", () => ({
  useTables: () => ({ tables: EMPTY_TABLES, loading: false, refetch: refetchTables }),
  useSources: () => ({ sources: sourcesData, loading: false, refetch: refetchSources }),
  useDomains: () => ({ domains: DOMAINS, loading: false, refetch: refetchDomains }),
  useRoles: () => ({ roles: ROLES, loading: false, refetch: refetchRoles }),
  useAvailableSchemasLazy: () => getAvailableSchemas,
  useAvailableTablesLazy: () => getAvailableTables,
  useAvailableColumnsMetadataLazy: () => getAvailableColumnsMetadata,
  useGenerateTableDescription: () => ({ generateTableDescription, loading: false }),
  useGenerateColumnDescription: () => ({ generateColumnDescription, loading: false }),
  useRegisterTable: () => ({ registerTable, loading: false }),
  useUpdateTable: () => ({ updateTable, loading: false }),
  useDeleteTable: () => ({ deleteTable, loading: false }),
  useUpdateTableCache: () => ({ updateTableCache, loading: false }),
  useUpdateTableNaming: () => ({ updateTableNaming, loading: false }),
  usePurgeCacheByTable: () => ({ purgeCacheByTable, loading: false }),
  useInvalidateFileSource: () => ({ invalidateFileSource, loading: false }),
  useDeployViewToDb: () => ({ deployViewToDb, loading: false }),
}));

import { TablesPage } from "../TablesPage";

function renderPage() {
  return render(
    <MemoryRouter>
      <TablesPage />
    </MemoryRouter>
  );
}

// clearAllMocks wipes implementations too, so re-seed the module-level lazy-hook
// spies (and reset the per-test source override) before each test.
function resetSpies() {
  vi.clearAllMocks();
  sourcesData = [SALES_PG_SOURCE];
  getAvailableSchemas.mockResolvedValue(["public"]);
  getAvailableTables.mockResolvedValue([
    { name: "customers", comment: "Registered customer accounts" },
    { name: "orders", comment: "Customer purchase orders" },
    { name: "products", comment: null },
  ]);
  getAvailableColumnsMetadata.mockResolvedValue([
    { name: "id", dataType: "integer", comment: "Primary key", nativeFilterType: null, isPrimaryKey: true },
    { name: "name", dataType: "varchar", comment: "Customer name", nativeFilterType: null, isPrimaryKey: false },
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

    const selects = screen.getAllByRole("combobox");
    await userEvent.selectOptions(selects[0], "sales-pg");

    await waitFor(() => {
      expect(screen.getByRole("option", { name: "public" })).toBeInTheDocument();
    });
    await userEvent.selectOptions(selects[2], "public");

    await waitFor(() => {
      expect(screen.getByRole("option", { name: "customers" })).toBeInTheDocument();
    });

    await userEvent.selectOptions(selects[3], "customers");

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

    const selects = screen.getAllByRole("combobox");
    await userEvent.selectOptions(selects[0], "sales-pg");

    await waitFor(() => {
      expect(screen.getByRole("option", { name: "public" })).toBeInTheDocument();
    });
    await userEvent.selectOptions(selects[2], "public");

    await waitFor(() => {
      expect(screen.getByRole("option", { name: "products" })).toBeInTheDocument();
    });
    await userEvent.selectOptions(selects[3], "products");

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

    const selects = screen.getAllByRole("combobox");
    await userEvent.selectOptions(selects[0], "sales-pg");

    await waitFor(() => {
      expect(screen.getByRole("option", { name: "public" })).toBeInTheDocument();
    });
    await userEvent.selectOptions(selects[2], "public");

    await waitFor(() => {
      expect(screen.getByRole("option", { name: "customers" })).toBeInTheDocument();
    });
    await userEvent.selectOptions(selects[3], "customers");

    await waitFor(() => {
      const descInput = screen.getByPlaceholderText(/appears in sdl docs/i);
      expect(descInput).toHaveValue("Registered customer accounts");
    });

    await userEvent.selectOptions(selects[2], "");

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

    const selects = screen.getAllByRole("combobox");
    await userEvent.selectOptions(selects[0], "sales-pg");

    await waitFor(() => {
      expect(getAvailableSchemas).toHaveBeenCalledWith("sales-pg");
    });
  });

  it("populates schema dropdown after API response", async () => {
    renderPage();

    await waitFor(() => screen.getByRole("heading", { name: /registered tables/i }));
    await userEvent.click(screen.getByRole("button", { name: "+ Table" }));

    const selects = screen.getAllByRole("combobox");
    await userEvent.selectOptions(selects[0], "sales-pg");

    await waitFor(() => {
      const schemaOptions = screen.getAllByRole("option", { name: "public" });
      expect(schemaOptions.length).toBeGreaterThan(0);
    });
  });

  it("does NOT call available-schemas for graphql sources (uses fixed schema)", async () => {
    sourcesData = [
      { id: "my-gql", type: "graphql", host: "", port: 0, database: "", username: "", dialect: "graphql", cacheEnabled: false, cacheTtl: null, allowedDomains: [], namingConvention: null, path: null, description: "" },
    ];

    renderPage();

    await waitFor(() => screen.getByRole("heading", { name: /registered tables/i }));
    await userEvent.click(screen.getByRole("button", { name: "+ Table" }));

    const selects = screen.getAllByRole("combobox");
    await userEvent.selectOptions(selects[0], "my-gql");

    await waitFor(() => {
      const schemaOpts = screen.getAllByRole("option", { name: "default" });
      expect(schemaOpts.length).toBeGreaterThan(0);
    });
    expect(getAvailableSchemas).not.toHaveBeenCalled();
  });

  it("does NOT call available-schemas for kafka sources (uses fixed schema)", async () => {
    sourcesData = [
      { id: "my-kafka", type: "kafka", host: "", port: 0, database: "", username: "", dialect: "kafka", cacheEnabled: false, cacheTtl: null, allowedDomains: [], namingConvention: null, path: null, description: "" },
    ];

    renderPage();

    await waitFor(() => screen.getByRole("heading", { name: /registered tables/i }));
    await userEvent.click(screen.getByRole("button", { name: "+ Table" }));

    const selects = screen.getAllByRole("combobox");
    await userEvent.selectOptions(selects[0], "my-kafka");

    await waitFor(() => {
      const schemaOpts = screen.getAllByRole("option", { name: "default" });
      expect(schemaOpts.length).toBeGreaterThan(0);
    });
    expect(getAvailableSchemas).not.toHaveBeenCalled();
  });

  it("resets schema and table when source changes", async () => {
    renderPage();

    await waitFor(() => screen.getByRole("heading", { name: /registered tables/i }));
    await userEvent.click(screen.getByRole("button", { name: "+ Table" }));

    const selects = screen.getAllByRole("combobox");
    await userEvent.selectOptions(selects[0], "sales-pg");

    await waitFor(() => screen.getByRole("option", { name: "public" }));
    await userEvent.selectOptions(selects[2], "public");

    await waitFor(() => screen.getByRole("option", { name: "customers" }));
    await userEvent.selectOptions(selects[3], "customers");

    // Change source — schema and table should reset
    await userEvent.selectOptions(selects[0], "");

    await waitFor(() => {
      expect(selects[2]).toHaveValue("");
    });
  });
});
