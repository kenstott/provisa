// Copyright (c) 2026 Kenneth Stott
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2253: natural-language query works only within the navbar's selected domains. The NL page
// and the SQL page's NL-to-SQL send useViewDomains() with the question.

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

// The navbar narrowed to "sales": the selection both NL entry points must send.
const SELECTED = new Set(["sales"]);
vi.mock("../../lib/viewDomains", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../lib/viewDomains")>()),
  useViewDomains: () => SELECTED,
}));

vi.mock("../../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({ checkedDomains: new Set<string>() }),
}));

vi.mock("../../context/AuthContext", () => ({
  useAuth: () => ({
    role: { id: "admin", capabilities: [] },
    selectedRoles: [],
    actingRoles: "admin",
  }),
}));

vi.mock("../../hooks/useCapability", () => ({
  useCapability: () => true,
}));

const runSql = vi.fn().mockResolvedValue({ columns: ["id"], rows: [{ id: 1 }] });
const submitNlQuery = vi.fn().mockReturnValue(new Promise(() => {}));
const nlToSql = vi.fn().mockResolvedValue({ sql: "select 1", attempts: 1 });
// Spread the real module: vmThreads + fileParallelism:false share one module registry, so a
// replace-everything factory here leaks into other files and drops exports they need.
vi.mock("../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/admin")>()),
  runSql: (...a: unknown[]) => runSql(...a),
  nlToSql: (...a: unknown[]) => nlToSql(...a),
  fetchNlMode: async () => ({ non_strict: false }),
  submitNlQuery: (...a: unknown[]) => submitNlQuery(...a),
  runSqlEach: async () => ({
    answers: [
      {
        statement: "select ran",
        columns: ["id"],
        column_types: null,
        rows: [{ id: 4242 }],
        warnings: [],
      },
    ],
  }),
  splitSql: async (sql: string) => [sql],
}));

// Spread the real module: vmThreads + fileParallelism:false share one module registry, so a
// replace-everything factory here leaks into other files. In particular a stubbed
// useMaterializeStoreInfo would win over the real Apollo hook that
// TableEditForm.consistency drives through MockedProvider.
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
import { NlPage } from "../NlPage";

beforeEach(() => {
  localStorage.clear();
  idbStore.clear();
  vi.clearAllMocks();
});

describe("NL entry points send the selected domains", () => {
  it("the NL page asks within the selection", async () => {
    render(<NlPage />);
    await userEvent.type(screen.getByTestId("nl-question-input"), "how many pets");
    await userEvent.click(screen.getByTestId("nl-submit-button"));
    await waitFor(() => expect(submitNlQuery).toHaveBeenCalled());
    expect(submitNlQuery.mock.calls[0][3]).toBe(SELECTED);
  });

  it("the SQL page's NL-to-SQL asks within the selection", async () => {
    render(
      <MemoryRouter>
        <SqlPage />
      </MemoryRouter>,
    );
    await userEvent.type(await screen.findByTestId("sql-nl-input"), "how many pets");
    await userEvent.click(screen.getByTestId("sql-nl-generate"));
    await waitFor(() => expect(nlToSql).toHaveBeenCalled());
    expect(nlToSql.mock.calls[0][3]).toBe(SELECTED);
  });
});
