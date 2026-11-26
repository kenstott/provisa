// Copyright (c) 2026 Kenneth Stott
// Canary: 8c41d7a2-5e93-4b06-a1f8-0d2b7c9e4f63
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "../../test-utils/render";
import { Fragment } from "react";

let handed: { key: string; state: unknown } = { key: "k0", state: null };
vi.mock("react-router-dom", () => ({
  MemoryRouter: ({ children }: { children: React.ReactNode }) => <Fragment>{children}</Fragment>,
  useNavigate: () => vi.fn(),
  useLocation: () => ({
    state: handed.state,
    pathname: "/sql",
    search: "",
    hash: "",
    key: handed.key,
  }),
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
vi.mock("../../context/AuthContext", () => ({
  useAuth: () => ({ role: { id: "admin", capabilities: [] }, selectedRoles: [] }),
}));
vi.mock("../../hooks/useCapability", () => ({ useCapability: () => true }));

const runSqlEach = vi.fn();
vi.mock("../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/admin")>()),
  runSql: vi.fn().mockResolvedValue({ columns: ["id"], rows: [{ id: 1 }] }),
  runSqlEach: (...a: unknown[]) => runSqlEach(...a),
  splitSql: async (sql: string) => [sql],
}));
vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useRoles: () => ({ roles: [{ id: "admin" }], loading: false, refetch: vi.fn() }),
  useDomains: () => ({ domains: [], loading: false, refetch: vi.fn() }),
  useTables: () => ({ tables: [], loading: false, refetch: vi.fn() }),
  useRelationships: () => ({ relationships: [], loading: false, refetch: vi.fn() }),
  useRegisterTable: () => ({ registerTable: vi.fn(), loading: false }),
  useUpdateTable: () => ({ updateTable: vi.fn(), loading: false }),
}));

import { SqlPage } from "../SqlPage";

const editor = () => screen.getByTestId("sql-editor") as HTMLTextAreaElement;

beforeEach(() => {
  localStorage.clear();
  idbStore.clear();
  vi.clearAllMocks();
  runSqlEach.mockResolvedValue({
    answers: [
      { statement: "x", columns: ["id"], column_types: null, rows: [{ id: 4242 }], warnings: [] },
    ],
  });
});

function open(state: unknown) {
  handed = { key: `k${Math.random()}`, state };
  return render(
    <MemoryRouter>
      <SqlPage />
    </MemoryRouter>,
  );
}

describe("SQL Explorer hand-off", () => {
  it("opens handed SQL formatted in a new tab", async () => {
    open({ sql: "select a from t where a = 1" });
    await waitFor(() => expect(editor()).toHaveValue("SELECT\n  a\nFROM\n  t\nWHERE\n  a = 1"));
    expect(screen.getByText("Query 2")).toBeInTheDocument();
    expect(runSqlEach).not.toHaveBeenCalled();
  });

  it("keeps a leading provisa-params comment intact", async () => {
    open({ sql: "-- provisa-params: $1=5\nselect a from t where a = $1" });
    await waitFor(() =>
      expect(editor()).toHaveValue(
        "-- provisa-params: $1=5\nSELECT\n  a\nFROM\n  t\nWHERE\n  a = $1",
      ),
    );
  });

  it("runs handed SQL at once when asked, once, on the formatted text", async () => {
    open({ sql: "select a from t", autoRun: true });
    await waitFor(() => expect(screen.getByText("4242")).toBeInTheDocument());
    // The run's own alias pass may re-space the text; the keywords stay upper case.
    expect(editor().value).toMatch(/^SELECT\s+a\s+FROM\s+t$/);
    expect(runSqlEach).toHaveBeenCalledTimes(1);
  });
});
