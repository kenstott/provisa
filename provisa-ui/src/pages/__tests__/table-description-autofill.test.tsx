import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

// Mock the admin API module before importing the component
vi.mock("../../api/admin", () => ({
  fetchTables: vi.fn().mockResolvedValue([]),
  fetchSources: vi.fn().mockResolvedValue([
    { id: "sales-pg", type: "postgresql", host: "localhost", port: 5432, database: "sales", username: "admin", dialect: "postgresql" },
  ]),
  fetchDomains: vi.fn().mockResolvedValue([{ id: "sales", description: "Sales data" }]),
  fetchRoles: vi.fn().mockResolvedValue([{ id: "admin", capabilities: ["admin"], domainAccess: ["*"] }]),
  fetchAvailableSchemas: vi.fn().mockResolvedValue(["public"]),
  fetchAvailableTables: vi.fn().mockResolvedValue([
    { name: "customers", comment: "Registered customer accounts" },
    { name: "orders", comment: "Customer purchase orders" },
    { name: "products", comment: null },
  ]),
  fetchAvailableColumnsMetadata: vi.fn().mockResolvedValue([
    { name: "id", dataType: "integer", comment: "Primary key" },
    { name: "name", dataType: "varchar", comment: "Customer name" },
  ]),
  registerTable: vi.fn().mockResolvedValue({ success: true, message: "Registered" }),
  deleteTable: vi.fn().mockResolvedValue({ success: true, message: "Deleted" }),
  updateTable: vi.fn().mockResolvedValue({ success: true, message: "Updated" }),
}));

import { TablesPage } from "../TablesPage";

describe("Table description auto-fill from physical database", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("prefills table description from comment when table is selected", async () => {
    render(<TablesPage />);

    // Wait for initial data load
    await waitFor(() => {
      expect(screen.getByRole("heading", { name: /registered tables/i })).toBeInTheDocument();
    });

    // Open register form
    await userEvent.click(screen.getByRole("button", { name: /register table/i }));

    // Select source
    const selects = screen.getAllByRole("combobox");
    await userEvent.selectOptions(selects[0], "sales-pg");

    // Wait for schemas, select one
    await waitFor(() => {
      expect(screen.getByRole("option", { name: "public" })).toBeInTheDocument();
    });
    const schemaSelect = selects[2];
    await userEvent.selectOptions(schemaSelect, "public");

    // Wait for tables to load
    await waitFor(() => {
      expect(screen.getByRole("option", { name: "customers" })).toBeInTheDocument();
    });

    // Select "customers" table
    const tableSelect = selects[3];
    await userEvent.selectOptions(tableSelect, "customers");

    // Verify description was auto-populated
    await waitFor(() => {
      const descInput = screen.getByPlaceholderText(/appears in sdl docs/i);
      expect(descInput).toHaveValue("Registered customer accounts");
    });
  });

  it("leaves description empty when table has no comment", async () => {
    render(<TablesPage />);

    await waitFor(() => {
      expect(screen.getByRole("heading", { name: /registered tables/i })).toBeInTheDocument();
    });

    await userEvent.click(screen.getByRole("button", { name: /register table/i }));

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

    // Description should remain empty since products has no comment
    await waitFor(() => {
      const descInput = screen.getByPlaceholderText(/appears in sdl docs/i);
      expect(descInput).toHaveValue("");
    });
  });

  it("clears description when schema changes", async () => {
    render(<TablesPage />);

    await waitFor(() => {
      expect(screen.getByRole("heading", { name: /registered tables/i })).toBeInTheDocument();
    });

    await userEvent.click(screen.getByRole("button", { name: /register table/i }));

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

    // Verify description is populated
    await waitFor(() => {
      const descInput = screen.getByPlaceholderText(/appears in sdl docs/i);
      expect(descInput).toHaveValue("Registered customer accounts");
    });

    // Change schema — should clear description
    await userEvent.selectOptions(selects[2], "");

    await waitFor(() => {
      const descInput = screen.getByPlaceholderText(/appears in sdl docs/i);
      expect(descInput).toHaveValue("");
    });
  });
});
