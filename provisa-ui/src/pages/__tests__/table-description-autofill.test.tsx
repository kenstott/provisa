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

vi.mock("../../api/admin", () => ({
  fetchTables: vi.fn().mockResolvedValue([]),
  fetchSources: vi.fn().mockResolvedValue([
    { id: "sales-pg", type: "postgresql", host: "localhost", port: 5432, database: "sales", username: "admin", dialect: "postgresql", cacheEnabled: false, cacheTtl: null, allowedDomains: [], namingConvention: null, path: null, description: "" },
  ]),
  fetchDomains: vi.fn().mockResolvedValue([{ id: "sales", description: "Sales data" }]),
  fetchRoles: vi.fn().mockResolvedValue([{ id: "admin", capabilities: ["admin"], domainAccess: ["*"] }]),
  fetchSettings: vi.fn().mockResolvedValue({
    redirect: { enabled: false, threshold: 10000, default_format: "json", ttl: 3600 },
    sampling: { default_sample_size: 1000 },
    cache: { default_ttl: 300 },
    naming: { domain_prefix: false, convention: "none" },
  }),
  fetchAvailableSchemas: vi.fn().mockResolvedValue(["public"]),
  fetchAvailableTables: vi.fn().mockResolvedValue([
    { name: "customers", comment: "Registered customer accounts" },
    { name: "orders", comment: "Customer purchase orders" },
    { name: "products", comment: null },
  ]),
  fetchAvailableColumnsMetadata: vi.fn().mockResolvedValue([
    { name: "id", dataType: "integer", comment: "Primary key", nativeFilterType: null, isPrimaryKey: true },
    { name: "name", dataType: "varchar", comment: "Customer name", nativeFilterType: null, isPrimaryKey: false },
  ]),
  registerTable: vi.fn().mockResolvedValue({ success: true, message: "Registered" }),
  deleteTable: vi.fn().mockResolvedValue({ success: true, message: "Deleted" }),
  updateTable: vi.fn().mockResolvedValue({ success: true, message: "Updated" }),
  updateTableCache: vi.fn().mockResolvedValue({ success: true, message: "Updated" }),
  purgeCacheByTable: vi.fn().mockResolvedValue({ success: true, message: "Purged" }),
  invalidateFileSource: vi.fn().mockResolvedValue({ success: true, message: "Invalidated" }),
  updateTableNaming: vi.fn().mockResolvedValue({ success: true, message: "Updated" }),
  profileTable: vi.fn().mockResolvedValue({ columns: [], rows: [], rowCount: 0 }),
  generateTableDescription: vi.fn().mockResolvedValue(""),
  generateColumnDescription: vi.fn().mockResolvedValue(""),
}));

import { TablesPage } from "../TablesPage";
import * as adminApi from "../../api/admin";

function renderPage() {
  return render(
    <MemoryRouter>
      <TablesPage />
    </MemoryRouter>
  );
}

describe("Table description auto-fill from physical database", () => {
  beforeEach(() => {
    vi.clearAllMocks();
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
    vi.clearAllMocks();
  });

  it("calls fetchAvailableSchemas for RDBMS sources", async () => {
    renderPage();

    await waitFor(() => screen.getByRole("heading", { name: /registered tables/i }));
    await userEvent.click(screen.getByRole("button", { name: "+ Table" }));

    const selects = screen.getAllByRole("combobox");
    await userEvent.selectOptions(selects[0], "sales-pg");

    await waitFor(() => {
      expect(adminApi.fetchAvailableSchemas).toHaveBeenCalledWith("sales-pg");
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

  it("does NOT call fetchAvailableSchemas for graphql sources (uses fixed schema)", async () => {
    vi.mocked(adminApi.fetchSources).mockResolvedValueOnce([
      { id: "my-gql", type: "graphql", host: "", port: 0, database: "", username: "", dialect: "graphql", cacheEnabled: false, cacheTtl: null, allowedDomains: [], namingConvention: null, path: null, description: "" },
    ]);

    renderPage();

    await waitFor(() => screen.getByRole("heading", { name: /registered tables/i }));
    await userEvent.click(screen.getByRole("button", { name: "+ Table" }));

    const selects = screen.getAllByRole("combobox");
    await userEvent.selectOptions(selects[0], "my-gql");

    await waitFor(() => {
      const schemaOpts = screen.getAllByRole("option", { name: "default" });
      expect(schemaOpts.length).toBeGreaterThan(0);
    });
    expect(adminApi.fetchAvailableSchemas).not.toHaveBeenCalled();
  });

  it("does NOT call fetchAvailableSchemas for kafka sources (uses fixed schema)", async () => {
    vi.mocked(adminApi.fetchSources).mockResolvedValueOnce([
      { id: "my-kafka", type: "kafka", host: "", port: 0, database: "", username: "", dialect: "kafka", cacheEnabled: false, cacheTtl: null, allowedDomains: [], namingConvention: null, path: null, description: "" },
    ]);

    renderPage();

    await waitFor(() => screen.getByRole("heading", { name: /registered tables/i }));
    await userEvent.click(screen.getByRole("button", { name: "+ Table" }));

    const selects = screen.getAllByRole("combobox");
    await userEvent.selectOptions(selects[0], "my-kafka");

    await waitFor(() => {
      const schemaOpts = screen.getAllByRole("option", { name: "default" });
      expect(schemaOpts.length).toBeGreaterThan(0);
    });
    expect(adminApi.fetchAvailableSchemas).not.toHaveBeenCalled();
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
