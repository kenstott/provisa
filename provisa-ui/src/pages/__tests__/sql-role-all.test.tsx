// Copyright (c) 2026 Kenneth Stott
// Canary: 7c2e9b48-3a5d-4f16-b8e0-9d4c1a67e2f5
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-1620 in the SQL Explorer: under the app-wide "Role: All" the picker seeds to the whole
 * active set and a run sends it comma-separated in X-Provisa-Role, the way Cypher and GraphQL do.
 * Seeding the first role by id (analyst in the demo) had the server refuse the run with V003 for
 * columns the union can see.
 */

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor, within } from "../../test-utils/render";
import userEvent from "@testing-library/user-event";
import { Fragment } from "react";

vi.mock("react-router-dom", () => ({
  MemoryRouter: ({ children }: { children: React.ReactNode }) => <Fragment>{children}</Fragment>,
  useNavigate: () => vi.fn(),
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

const runSql = vi.fn().mockResolvedValue({ columns: ["id"], rows: [{ id: 1 }] });
const registerTable = vi.fn().mockResolvedValue({ success: true, message: "View created (id=7)" });
vi.mock("../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/admin")>()),
  runSql: (...a: unknown[]) => runSql(...a),
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
  useRegisterTable: () => ({ registerTable: (...a: unknown[]) => registerTable(...a), loading: false }),
  useUpdateTable: () => ({ updateTable: vi.fn(), loading: false }),
}));

import { SqlPage } from "../SqlPage";

describe("SQL Explorer under Role: All (REQ-1620)", () => {
  beforeEach(() => {
    localStorage.clear();
    idbStore.clear();
    vi.clearAllMocks();
  });

  it("seeds the picker to the whole active set and runs with it", async () => {
    render(
      <MemoryRouter>
        <SqlPage />
      </MemoryRouter>,
    );
    await waitFor(() => expect(screen.getByTestId("sql-role")).toHaveValue("All"));
    const editor = screen.getByTestId("sql-editor") as HTMLTextAreaElement;
    await userEvent.clear(editor);
    await userEvent.type(editor, "SELECT 1");
    await userEvent.click(screen.getByTestId("sql-run"));
    await waitFor(() => expect(runSql).toHaveBeenCalled());
    expect(runSql.mock.calls[0][1]).toBe("analyst,org_admin");
  });

  it("a new view's default visibleTo is the role ids, never the picker's All entry", async () => {
    render(
      <MemoryRouter>
        <SqlPage />
      </MemoryRouter>,
    );
    await waitFor(() => expect(screen.getByTestId("sql-role")).toHaveValue("All"));
    const editor = screen.getByTestId("sql-editor") as HTMLTextAreaElement;
    await userEvent.clear(editor);
    await userEvent.type(editor, "SELECT 1 AS id");
    await userEvent.click(screen.getByTestId("sql-run"));
    await waitFor(() => expect(runSql).toHaveBeenCalled());
    await userEvent.click(await screen.findByTestId("sql-open-view-modal"));
    await userEvent.type(await screen.findByTestId("view-alias-input"), "e2e_mv_a");
    // Mantine Select portals its listbox; jsdom applies no layout so it reads as hidden.
    const combobox = screen.getByTestId("view-domain-select");
    await userEvent.click(combobox);
    const listboxId = combobox.getAttribute("aria-controls");
    const listbox = listboxId ? document.getElementById(listboxId) : null;
    if (!listbox) throw new Error("no listbox for the domain select");
    await userEvent.click(
      await within(listbox).findByRole("option", { name: /pet-store/, hidden: true }),
    );
    await userEvent.click(screen.getByTestId("save-view-button"));
    await waitFor(() => expect(registerTable).toHaveBeenCalled());
    const input = registerTable.mock.calls[0][0] as { columns: { visibleTo: unknown }[] };
    expect(input.columns[0].visibleTo).toEqual(["analyst", "org_admin"]);
  });
});
