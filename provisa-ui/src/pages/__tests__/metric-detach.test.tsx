// Copyright (c) 2026 Kenneth Stott
// Canary: ccf8c752-0a99-4955-a211-cb67801e4cca
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1322: one-way detach — confirm modal gates the editor-text replacement.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor, fireEvent } from "../../test-utils/render";
import userEvent from "@testing-library/user-event";
import { Fragment } from "react";

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
    <textarea data-testid="sql-editor" value={value} onChange={(e) => onChange?.(e.target.value)} />
  ),
}));
vi.mock("@codemirror/lang-sql", () => ({ sql: () => [], PostgreSQL: {} }));
vi.mock("@codemirror/theme-one-dark", () => ({ oneDark: [] }));
vi.mock("@codemirror/view", () => ({ EditorView: { lineWrapping: [] } }));

vi.mock("../../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({ checkedDomains: new Set<string>(), ensureDomainChecked: vi.fn() }),
}));

vi.mock("../../hooks/useCapability", () => ({
  useCapability: () => true,
}));

const runSql = vi.fn().mockResolvedValue({ columns: [], rows: [] });
// REQ-1322: the expansion is the compiler-derived physical SQL the explain endpoint reports, not
// a plan tree — and not `EXPLAIN <sql>` through /data/sql, which the parser takes as an opaque
// command, leaving `metrics.<name>` unexpanded for the engine to reject.
const explainSql = vi.fn().mockResolvedValue({ sql: "PHYSICAL EXPANSION" });
// Spread the real module: vmThreads + fileParallelism:false share one module registry, so a
// replace-everything factory here leaks into other files and drops exports they need.
vi.mock("../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/admin")>()),
  runSql: (...a: unknown[]) => runSql(...a),
  explainSql: (...a: unknown[]) => explainSql(...a),
}));

// Spread the real module (same registry-sharing reason as above).
vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useRoles: () => ({ roles: [{ id: "admin" }], loading: false, refetch: vi.fn() }),
  useDomains: () => ({ domains: [], loading: false, refetch: vi.fn() }),
  useTables: () => ({ tables: [], loading: false, refetch: vi.fn() }),
  useRelationships: () => ({ relationships: [], loading: false, refetch: vi.fn() }),
  useMetrics: () => ({ metrics: [], loading: false, refetch: vi.fn() }),
  useRegisterTable: () => ({ registerTable: vi.fn(), loading: false }),
  useUpdateTable: () => ({ updateTable: vi.fn(), loading: false }),
}));

import { SqlPage } from "../SqlPage";

const METRIC_SQL = "SELECT value FROM metrics.revenue";

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
  explainSql.mockResolvedValue({ sql: "PHYSICAL EXPANSION" });
});

describe("metric expansion + one-way detach (REQ-1322)", () => {
  it("shows the expansion/detach affordances only when SQL references metrics.", async () => {
    renderPage();
    await waitFor(() => screen.getByText("Query 1"));
    expect(screen.queryByTestId("sql-detach")).not.toBeInTheDocument();
    expect(screen.queryByTestId("sql-show-expansion")).not.toBeInTheDocument();

    fireEvent.change(editor(), { target: { value: METRIC_SQL } });
    expect(screen.getByTestId("sql-detach")).toBeInTheDocument();
    expect(screen.getByTestId("sql-show-expansion")).toBeInTheDocument();
  });

  it("shows the server expansion read-only in a drawer", async () => {
    const user = userEvent.setup();
    renderPage();
    await waitFor(() => screen.getByText("Query 1"));
    fireEvent.change(editor(), { target: { value: METRIC_SQL } });

    await user.click(screen.getByTestId("sql-show-expansion"));
    await waitFor(() =>
      expect(screen.getByTestId("sql-expansion-text")).toHaveTextContent("PHYSICAL EXPANSION"),
    );
    expect(explainSql).toHaveBeenCalledWith(METRIC_SQL, "admin", false);
    // Editor untouched by the preview
    expect(editor()).toHaveValue(METRIC_SQL);
  });

  it("detach is gated behind the confirm modal and replaces the editor only on confirm", async () => {
    const user = userEvent.setup();
    renderPage();
    await waitFor(() => screen.getByText("Query 1"));
    fireEvent.change(editor(), { target: { value: METRIC_SQL } });

    // Open the confirm modal — nothing replaced yet.
    await user.click(screen.getByTestId("sql-detach"));
    await waitFor(() => screen.getByTestId("sql-detach-confirm"));
    expect(editor()).toHaveValue(METRIC_SQL);
    expect(explainSql).not.toHaveBeenCalled();

    // Cancel keeps the semantic SQL.
    await user.click(screen.getByTestId("sql-detach-cancel"));
    expect(editor()).toHaveValue(METRIC_SQL);
    expect(explainSql).not.toHaveBeenCalled();

    // Confirm requests the expansion and replaces the editor text.
    await user.click(screen.getByTestId("sql-detach"));
    await waitFor(() => screen.getByTestId("sql-detach-confirm"));
    await user.click(screen.getByTestId("sql-detach-confirm"));
    await waitFor(() => expect(editor()).toHaveValue("PHYSICAL EXPANSION"));
    expect(explainSql).toHaveBeenCalledWith(METRIC_SQL, "admin", false);

    // Tab is badged detached.
    expect(screen.getByText("detached")).toBeInTheDocument();
  });
});
