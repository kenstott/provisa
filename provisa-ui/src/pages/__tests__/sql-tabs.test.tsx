// Copyright (c) 2026 Kenneth Stott
// Canary: ccf8c752-0a99-4955-a211-cb67801e4cca
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "../../test-utils/render";
import userEvent from "@testing-library/user-event";
import { Fragment } from "react";
import i18n from "../../i18n";

const t = i18n.getFixedT("en");

vi.mock("react-router-dom", () => ({
  MemoryRouter: ({ children }: { children: React.ReactNode }) => <Fragment>{children}</Fragment>,
  useNavigate: () => vi.fn(),
  useLocation: () => ({ state: null, pathname: "/sql", search: "", hash: "", key: "x" }),
}));

import { MemoryRouter } from "react-router-dom";

// In-memory idb-keyval.
const idbStore = new Map<string, unknown>();
vi.mock("idb-keyval", () => ({
  get: vi.fn(async (k: string) => idbStore.get(k)),
  set: vi.fn(async (k: string, v: unknown) => void idbStore.set(k, v)),
  del: vi.fn(async (k: string) => void idbStore.delete(k)),
}));

// CodeMirror → plain textarea bound to value/onChange.
vi.mock("@uiw/react-codemirror", () => ({
  default: ({ value, onChange }: { value: string; onChange?: (v: string) => void }) => (
    <textarea
      data-testid="sql-editor"
      value={value}
      onChange={(e) => onChange?.(e.target.value)}
    />
  ),
}));
vi.mock("@codemirror/lang-sql", () => ({ sql: () => [], PostgreSQL: {} }));
vi.mock("@codemirror/theme-one-dark", () => ({ oneDark: [] }));
vi.mock("@codemirror/view", () => ({ EditorView: { lineWrapping: [] } }));

vi.mock("../../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({ checkedDomains: new Set<string>() }),
}));

vi.mock("../../hooks/useCapability", () => ({
  useCapability: () => true,
}));

const runSql = vi.fn().mockResolvedValue({ columns: ["id"], rows: [{ id: 1 }] });
const nlToSql = vi.fn().mockResolvedValue({ sql: "select 1", attempts: 1 });
vi.mock("../../api/admin", () => ({
  runSql: (...a: unknown[]) => runSql(...a),
  nlToSql: (...a: unknown[]) => nlToSql(...a),
}));

vi.mock("../../hooks/useAdminQueries", () => ({
  useRoles: () => ({ roles: [{ id: "admin" }], loading: false, refetch: vi.fn() }),
  useDomains: () => ({ domains: [], loading: false, refetch: vi.fn() }),
  useTables: () => ({ tables: [], loading: false, refetch: vi.fn() }),
  useRelationships: () => ({ relationships: [], loading: false, refetch: vi.fn() }),
  useRegisterTable: () => ({ registerTable: vi.fn(), loading: false }),
  useUpdateTable: () => ({ updateTable: vi.fn(), loading: false }),
  // Keep the module mock complete so it can't leak an undefined hook into form-rendering tests
  // (vmThreads + fileParallelism:false share one module context).
  useMaterializeStoreInfo: () => ({
    materializeStoreInfo: null,
    loading: false,
    error: undefined,
    refetch: vi.fn(),
  }),
}));

import { SqlPage } from "../SqlPage";

const TABS_KEY = "provisa.sql.tabs";

function renderPage() {
  return render(
    <MemoryRouter>
      <SqlPage />
    </MemoryRouter>,
  );
}

function editor() {
  return screen.getByTestId("sql-editor") as HTMLTextAreaElement;
}

beforeEach(() => {
  localStorage.clear();
  idbStore.clear();
  vi.clearAllMocks();
});

describe("SQL explore query tabs", () => {
  it("starts with a single Query 1 tab", async () => {
    renderPage();
    await waitFor(() => expect(screen.getByText("Query 1")).toBeInTheDocument());
    expect(screen.queryByText("Query 2")).not.toBeInTheDocument();
  });

  it("adds a new tab and isolates editor content per tab", async () => {
    const user = userEvent.setup();
    renderPage();
    await waitFor(() => screen.getByText("Query 1"));

    await user.type(editor(), "select a");
    expect(editor()).toHaveValue("select a");

    await user.click(screen.getByRole("button", { name: t("sqlEditorPanel.newTab") }));
    await waitFor(() => screen.getByText("Query 2"));
    expect(editor()).toHaveValue(""); // new tab is blank

    await user.type(editor(), "select b");
    expect(editor()).toHaveValue("select b");

    // switch back to Query 1 — original content restored
    await user.click(screen.getByText("Query 1"));
    expect(editor()).toHaveValue("select a");
  });

  it("persists tabs and per-tab sql to localStorage", async () => {
    const user = userEvent.setup();
    renderPage();
    await waitFor(() => screen.getByText("Query 1"));
    await user.type(editor(), "select persisted");

    await waitFor(() => {
      const meta = JSON.parse(localStorage.getItem(TABS_KEY) ?? "null");
      expect(meta?.tabs?.length).toBe(1);
      const id = meta.tabs[0].id;
      expect(localStorage.getItem(`provisa.sql.tab.${id}`)).toBe("select persisted");
    });
  });

  it("restores tabs from localStorage on remount", async () => {
    const user = userEvent.setup();
    const first = renderPage();
    await waitFor(() => screen.getByText("Query 1"));
    await user.type(editor(), "select restored");
    await user.click(screen.getByRole("button", { name: t("sqlEditorPanel.newTab") }));
    await waitFor(() => screen.getByText("Query 2"));
    first.unmount();

    renderPage();
    await waitFor(() => screen.getByText("Query 2"));
    // Query 1 is restored with its content (active tab is Query 2 → blank editor)
    await userEvent.click(screen.getByText("Query 1"));
    expect(editor()).toHaveValue("select restored");
  });

  it("closes a tab and keeps at least one", async () => {
    const user = userEvent.setup();
    renderPage();
    await waitFor(() => screen.getByText("Query 1"));
    await user.click(screen.getByRole("button", { name: t("sqlEditorPanel.newTab") }));
    await waitFor(() => screen.getByText("Query 2"));

    await user.click(screen.getByRole("button", { name: t("sqlEditorPanel.closeTab", { title: "Query 2" }) }));
    await waitFor(() => expect(screen.queryByText("Query 2")).not.toBeInTheDocument());
    expect(screen.getByText("Query 1")).toBeInTheDocument();

    // closing the last tab resets it rather than removing it
    await user.click(screen.getByRole("button", { name: t("sqlEditorPanel.closeTab", { title: "Query 1" }) }));
    expect(screen.getByText("Query 1")).toBeInTheDocument();
  });

  it("renames a tab via double-click", async () => {
    const user = userEvent.setup();
    renderPage();
    await waitFor(() => screen.getByText("Query 1"));

    await user.dblClick(screen.getByText("Query 1"));
    const input = screen.getByDisplayValue("Query 1");
    await user.clear(input);
    await user.type(input, "My Report{Enter}");

    await waitFor(() => expect(screen.getByText("My Report")).toBeInTheDocument());
    expect(screen.queryByText("Query 1")).not.toBeInTheDocument();
  });
});
