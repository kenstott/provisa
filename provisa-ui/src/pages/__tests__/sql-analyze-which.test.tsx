// Copyright (c) 2026 Kenneth Stott
// Canary: 6bc2ac12-a3a2-4070-bf41-3c71b5e1b5c9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-1519: Analyze with several statements in the editor asks which one to analyze; one
 * statement is analyzed directly; a commented-out statement is not offered; leading comments
 * (provisa-params lines) travel with their statement.
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

const registerTable = vi.fn().mockResolvedValue({ success: true, message: "" });
const explain = vi.fn().mockResolvedValue({
  route: "direct",
  route_reason: "single source",
  dialect: "postgres",
  analyzed: false,
  sources: [],
  optimizations: [],
  sql: "SELECT 1",
  semantic_sql: null,
  plan: [],
  mermaid: "",
});
let split: string[] = [];
vi.mock("../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/admin")>()),
  explainSql: (...a: unknown[]) => explain(...a),
  splitSql: async () => split,
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

async function analyzeWith(text: string) {
  render(
    <MemoryRouter>
      <SqlPage />
    </MemoryRouter>,
  );
  const editor = await screen.findByTestId("sql-editor");
  await userEvent.clear(editor);
  await userEvent.type(editor, text);
  await userEvent.click(screen.getByTestId("sql-analyze"));
}

describe("Analyze with several statements", () => {
  beforeEach(() => {
    localStorage.clear();
    idbStore.clear();
    vi.clearAllMocks();
  });

  it("asks which one, and sends the chosen one", async () => {
    split = [
      "SELECT id FROM orders",
      "-- provisa-params: 1\n-- note\nSELECT id FROM pets WHERE id = $1",
    ];
    await analyzeWith("x");
    expect(explain).not.toHaveBeenCalled();
    const first = await screen.findByTestId("sql-analyze-choice-0");
    expect(first).toHaveTextContent("SELECT id FROM orders");
    // Labelled by the first non-comment line; the params comments are not the label.
    expect(screen.getByTestId("sql-analyze-choice-1")).toHaveTextContent(
      "SELECT id FROM pets WHERE id = $1",
    );
    await userEvent.click(screen.getByTestId("sql-analyze-choice-1"));
    await waitFor(() => expect(explain).toHaveBeenCalled());
    // The leading comments travel with the statement.
    expect(explain.mock.calls[0][0]).toBe(split[1]);
  });

  it("analyzes a single statement directly", async () => {
    split = ["SELECT 1"];
    await analyzeWith("x");
    await waitFor(() => expect(explain).toHaveBeenCalled());
    expect(explain.mock.calls[0][0]).toBe("SELECT 1");
    expect(screen.queryByTestId("sql-analyze-choice-0")).toBeNull();
  });

  it("does not offer a commented-out statement", async () => {
    // The server's split drops a segment of comments alone: one statement remains.
    split = ["SELECT 2"];
    await analyzeWith("SELECT 2; -- SELECT 3");
    await waitFor(() => expect(explain).toHaveBeenCalled());
    expect(screen.queryByTestId("sql-analyze-choice-0")).toBeNull();
    expect(explain.mock.calls[0][0]).toBe("SELECT 2");
  });
});
