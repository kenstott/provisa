// Copyright (c) 2026 Kenneth Stott
// Canary: b2c0d8a7-7058-4c04-91dc-45272d4ecb2a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * A batch on the SQL page: one result per statement, each under its own tab ("Result 1 ·
 * orders"), each with its row count; a statement that failed says why under its own tab while
 * the others' rows still show. Each statement is sampled on its own.
 */

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "../../test-utils/render";
import userEvent from "@testing-library/user-event";
import { Fragment } from "react";

const navigateTo = vi.fn();
vi.mock("react-router-dom", () => ({
  MemoryRouter: ({ children }: { children: React.ReactNode }) => <Fragment>{children}</Fragment>,
  useNavigate: () => navigateTo,
  useLocation: () => ({ state: null, pathname: "/sql", search: "", hash: "", key: "x" }),
}));
import { MemoryRouter } from "react-router-dom";

const idbStore = new Map<string, unknown>();
vi.mock("idb-keyval", () => ({
  get: vi.fn(async (k: string) => idbStore.get(k)),
  set: vi.fn(async (k: string, v: unknown) => void idbStore.set(k, v)),
  del: vi.fn(async (k: string) => void idbStore.delete(k)),
}));
vi.mock("@uiw/react-codemirror", () => ({
  default: ({ value, onChange }: { value: string; onChange?: (v: string) => void }) => (
    <textarea data-testid="sql-editor" value={value} onChange={(e) => onChange?.(e.target.value)} />
  ),
}));
vi.mock("@codemirror/lang-sql", () => ({ sql: () => [], PostgreSQL: {} }));
vi.mock("@codemirror/theme-one-dark", () => ({ oneDark: [] }));
vi.mock("@codemirror/view", () => ({ EditorView: { lineWrapping: [] } }));
vi.mock("../../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({ checkedDomains: new Set<string>() }),
}));

const analyst = { id: "analyst", capabilities: [], domain_access: ["*"] };
const orgAdmin = { id: "org_admin", capabilities: [], domain_access: ["*"] };
vi.mock("../../context/AuthContext", () => ({
  // "Role: All": the context exposes the first role as `role` and the whole set as selectedRoles.
  useAuth: () => ({ role: analyst, selectedRoles: [analyst, orgAdmin] }),
}));
vi.mock("../../hooks/useCapability", () => ({
  useCapability: () => true,
}));

const runSql = vi.fn().mockResolvedValue({
  answers: [
    {
      statement: "SELECT id FROM orders\nLIMIT 100",
      columns: ["id"],
      column_types: null,
      rows: [{ id: 1 }, { id: 2 }],
      warnings: [],
    },
    {
      statement: "SELECT id FROM secret\nLIMIT 100",
      columns: [],
      column_types: null,
      rows: [],
      warnings: [],
      error: "Table secret is not in your scope",
    },
  ],
});
const registerTable = vi.fn().mockResolvedValue({ success: true, message: "View created (id=7)" });
vi.mock("../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/admin")>()),
  runSqlEach: (...a: unknown[]) => runSql(...a),
  // As the server splits it: the ';' inside the string does not split.
  splitSql: async () => ["SELECT id FROM orders WHERE note = 'a;b'", "SELECT id FROM secret"],
}));
vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useRoles: () => ({
    roles: [{ id: "analyst" }, { id: "org_admin" }],
    loading: false,
    refetch: vi.fn(),
  }),
  useDomains: () => ({
    domains: [{ id: "pet-store", description: "Pet store" }],
    loading: false,
    refetch: vi.fn(),
  }),
  useTables: () => ({ tables: [], loading: false, refetch: vi.fn() }),
  useRelationships: () => ({ relationships: [], loading: false, refetch: vi.fn() }),
  useRegisterTable: () => ({
    registerTable: (...a: unknown[]) => registerTable(...a),
    loading: false,
  }),
  useUpdateTable: () => ({ updateTable: vi.fn(), loading: false }),
}));

import { SqlPage } from "../SqlPage";

describe("a batch of two statements", () => {
  beforeEach(() => {
    localStorage.clear();
    idbStore.clear();
    vi.clearAllMocks();
  });

  it("samples each statement, and shows each result under its own tab", async () => {
    render(
      <MemoryRouter>
        <SqlPage />
      </MemoryRouter>,
    );
    const editor = await screen.findByTestId("sql-editor");
    await userEvent.clear(editor);
    await userEvent.type(editor, "SELECT 1");
    await userEvent.click(screen.getByTestId("sql-run"));
    await waitFor(() => expect(runSql).toHaveBeenCalled());
    expect(runSql.mock.calls[0][0]).toBe(
      "SELECT id FROM orders WHERE note = 'a;b'\nLIMIT 100\n;\nSELECT id FROM secret\nLIMIT 100",
    );

    const first = await screen.findByTestId("sql-statement-tab-0");
    expect(first).toHaveTextContent("Result 1 · orders");
    expect(screen.getByTestId("sql-statement-rows-0")).toHaveTextContent("2");
    expect(screen.getByTestId("sql-statement-tab-1")).toHaveTextContent("Result 2 · secret");
    expect(screen.getByTestId("sql-statement-failed-1")).toBeInTheDocument();
    // The first statement's rows, though the second failed.
    expect(screen.queryByTestId("sql-results-error")).toBeNull();
    expect(screen.getByTestId("results-tab-results")).toHaveTextContent("2");

    await userEvent.click(screen.getByTestId("sql-statement-tab-1"));
    expect(await screen.findByTestId("sql-results-error")).toHaveTextContent(
      "Table secret is not in your scope",
    );
    await userEvent.click(screen.getByTestId("sql-statement-tab-0"));
    await waitFor(() => expect(screen.queryByTestId("sql-results-error")).toBeNull());
  });
});
