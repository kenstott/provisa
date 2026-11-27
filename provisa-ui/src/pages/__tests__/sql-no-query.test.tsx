// Copyright (c) 2026 Kenneth Stott
// Canary: 7e2a9d41-35bc-4c08-a6f7-8b14d0c3e592
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * A SQL editor run with no statement in it (blank, or only comments) says so in the results,
 * before anything is sent. A real statement still runs.
 */

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "../../test-utils/render";
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

function serve() {
  const fetchMock = vi.fn(async (url: string, init?: RequestInit) => {
    const u = String(url);
    if (u.endsWith("/data/sql/statements"))
      return json({ statements: [JSON.parse(String(init?.body)).sql] });
    if (u.endsWith("/data/sql") && init?.method === "POST")
      return json({
        results: [
          { statement: "SELECT 1", warnings: [], columns: ["n"], column_types: null, data: [[1]] },
        ],
      });
    if (u.endsWith("/data/sql/approved-joins")) return json({ joins: [] });
    return json({});
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

const sent = (f: ReturnType<typeof serve>) =>
  f.mock.calls.filter(([u]) => String(u).includes("/data/sql") && !String(u).includes("approved"));

async function runWith(text: string) {
  render(
    <MemoryRouter>
      <SqlPage />
    </MemoryRouter>,
  );
  const editor = await screen.findByTestId("sql-editor");
  await userEvent.clear(editor);
  if (text) await userEvent.type(editor, text);
  await userEvent.click(screen.getByTestId("sql-run"));
}

describe("SQL editor with no statement", () => {
  beforeEach(() => {
    localStorage.clear();
    idbStore.clear();
  });

  it.each([
    ["blank", ""],
    ["comment-only", "-- nothing here"],
  ])("%s: says there is no query and sends no request", async (_n, text) => {
    const f = serve();
    await runWith(text);
    expect(await screen.findByTestId("sql-no-query")).toHaveTextContent(
      "There is no query to run.",
    );
    expect(screen.queryByTestId("sql-results-error")).toBeNull();
    expect(sent(f)).toHaveLength(0);
  });

  it("a real statement still runs", async () => {
    const f = serve();
    await runWith("SELECT 1");
    await waitFor(() => expect(sent(f).length).toBeGreaterThan(0));
    expect(screen.queryByTestId("sql-no-query")).toBeNull();
  });
});
