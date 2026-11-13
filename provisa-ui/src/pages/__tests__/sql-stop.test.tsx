// Copyright (c) 2026 Kenneth Stott
// Canary: 0e7b3c58-92a1-4d6f-b8e4-1c5a7f2d9b60
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * A statement running in the SQL editor can be stopped. While it runs, Stop stands where Run is;
 * Stop aborts the request and the results pane says "Stopped": not an error, no longer running.
 * An answer that comes after Stop is dropped.
 */

import { describe, it, expect, vi, beforeEach } from "vitest";
import { act, render, screen, waitFor } from "../../test-utils/render";
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
vi.mock("../../context/AuthContext", () => ({
  useAuth: () => ({ role: analyst, selectedRoles: [analyst] }),
}));
vi.mock("../../hooks/useCapability", () => ({ useCapability: () => true }));
vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useRoles: () => ({ roles: [{ id: "analyst" }], loading: false, refetch: vi.fn() }),
  useDomains: () => ({ domains: [], loading: false, refetch: vi.fn() }),
  useTables: () => ({ tables: [], loading: false, refetch: vi.fn() }),
  useRelationships: () => ({ relationships: [], loading: false, refetch: vi.fn() }),
  useRegisterTable: () => ({ registerTable: vi.fn(), loading: false }),
  useUpdateTable: () => ({ updateTable: vi.fn(), loading: false }),
}));

import { SqlPage } from "../SqlPage";

const json = (body: unknown) =>
  new Response(JSON.stringify(body), { headers: { "content-type": "application/json" } });

interface Held {
  signal?: AbortSignal;
  answer: (r: Response) => void;
}

/** /data/sql is held until the test answers it; the split answers at once. */
function serve(honorAbort = true): Held[] {
  const held: Held[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string, init?: RequestInit) => {
      const u = String(url);
      if (u.endsWith("/data/sql/statements"))
        return json({ statements: [JSON.parse(String(init?.body)).sql] });
      if (u.endsWith("/data/sql") && init?.method === "POST")
        return new Promise<Response>((resolve, reject) => {
          held.push({ signal: init.signal ?? undefined, answer: resolve });
          // As fetch does, unless the answer is to come regardless (one already on its way).
          if (honorAbort) init.signal?.addEventListener("abort", () => reject(init.signal?.reason));
        });
      if (u.endsWith("/data/sql/approved-joins")) return json({ joins: [] });
      return json({});
    }),
  );
  return held;
}

const LATE = () =>
  json({
    results: [
      { statement: "SELECT 1", warnings: [], columns: ["late"], column_types: null, data: [[1]] },
    ],
  });

async function startRun() {
  render(
    <MemoryRouter>
      <SqlPage />
    </MemoryRouter>,
  );
  const editor = await screen.findByTestId("sql-editor");
  await userEvent.clear(editor);
  await userEvent.type(editor, "SELECT 1");
  await userEvent.click(screen.getByTestId("sql-run"));
}

describe("stopping a running SQL statement", () => {
  beforeEach(() => {
    localStorage.clear();
    idbStore.clear();
  });

  it("shows Stop in place of Run while the statement runs", async () => {
    const held = serve();
    await startRun();
    expect(await screen.findByTestId("sql-stop")).toBeInTheDocument();
    expect(screen.queryByTestId("sql-run")).toBeNull();
    await waitFor(() => expect(held).toHaveLength(1));
    await act(async () => held[0].answer(LATE()));
    expect(await screen.findByTestId("sql-run")).toBeInTheDocument();
    expect(screen.queryByTestId("sql-stop")).toBeNull();
  });

  it("aborts the request, and the results say Stopped, not an error and not running", async () => {
    const held = serve();
    await startRun();
    await waitFor(() => expect(held).toHaveLength(1));
    await userEvent.click(await screen.findByTestId("sql-stop"));
    expect(held[0].signal?.aborted).toBe(true);
    expect(await screen.findByTestId("sql-results-stopped")).toHaveTextContent("Stopped");
    expect(screen.queryByTestId("sql-results-error")).toBeNull();
    expect(screen.queryByText("Running…")).toBeNull();
    expect(screen.getByTestId("sql-run")).toBeInTheDocument();
  });

  it("drops an answer that comes after Stop", async () => {
    const held = serve(false);
    await startRun();
    await waitFor(() => expect(held).toHaveLength(1));
    await userEvent.click(await screen.findByTestId("sql-stop"));
    await act(async () => held[0].answer(LATE()));
    await act(async () => new Promise((r) => setTimeout(r, 20)));
    expect(screen.getByTestId("sql-results-stopped")).toBeInTheDocument();
    expect(screen.queryByText("late")).toBeNull();
    expect(screen.queryByTestId("sql-results-error")).toBeNull();
  });
});
