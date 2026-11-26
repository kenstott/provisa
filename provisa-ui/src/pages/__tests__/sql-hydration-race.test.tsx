// Copyright (c) 2026 Kenneth Stott
// Canary: 5dfe4c29-e035-4535-ba68-317e78f861a1
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { act, render, screen, waitFor } from "../../test-utils/render";
import userEvent from "@testing-library/user-event";
import { Fragment } from "react";
import i18n from "../../i18n";

const t = i18n.getFixedT("en");

// The location a render sees: a hand-off's payload, under its own key.
const nav = vi.hoisted(() => ({ state: null as unknown, key: "x" }));

vi.mock("react-router-dom", () => ({
  MemoryRouter: ({ children }: { children: React.ReactNode }) => <Fragment>{children}</Fragment>,
  useNavigate: () => vi.fn(),
  useLocation: () => ({ state: nav.state, pathname: "/sql", search: "", hash: "", key: nav.key }),
}));

import { MemoryRouter } from "react-router-dom";

// In-memory idb-keyval. A read takes the value stored when it is made and, while idbGate.wait is set,
// answers only when it resolves: the kept results arrive late.
const idbGate = vi.hoisted(() => ({ wait: null as Promise<void> | null }));
const idbStore = new Map<string, unknown>();
vi.mock("idb-keyval", () => ({
  get: vi.fn(async (k: string) => {
    const value = idbStore.get(k);
    if (idbGate.wait) await idbGate.wait;
    return value;
  }),
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
  useDomainFilter: () => ({ checkedDomains: new Set<string>() }),
}));

vi.mock("../../context/AuthContext", () => ({
  useAuth: () => ({ role: { id: "admin", capabilities: [] }, selectedRoles: [] }),
}));

vi.mock("../../hooks/useCapability", () => ({
  useCapability: () => true,
}));

// A run answers only when runGate.wait resolves, while it is set.
const runGate = vi.hoisted(() => ({ wait: null as Promise<void> | null }));
const petsBreed = vi.hoisted(() => ({ value: "Labrador" }));
const runSql = vi.fn().mockResolvedValue({ columns: ["id"], rows: [{ id: 1 }] });
const nlToSql = vi.fn().mockResolvedValue({ sql: "select 1", attempts: 1 });
// Spread the real module: vmThreads + fileParallelism:false share one module registry, so a
// replace-everything factory here leaks into other files and drops exports they need.
vi.mock("../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/admin")>()),
  runSql: (...a: unknown[]) => runSql(...a),
  nlToSql: (...a: unknown[]) => nlToSql(...a),
  // The pets query answers its breed; every other statement answers id 4242.
  runSqlEach: async (sql: string) => {
    if (runGate.wait) await runGate.wait;
    return {
      answers: [
        sql.includes("pets")
          ? {
              statement: sql,
              columns: ["breed_name"],
              column_types: null,
              rows: [{ breed_name: petsBreed.value }],
              warnings: [],
            }
          : {
              statement: sql,
              columns: ["id"],
              column_types: null,
              rows: [{ id: 4242 }],
              warnings: [],
            },
      ],
    };
  },
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
  nav.state = null;
  nav.key = "x";
  idbGate.wait = null;
  runGate.wait = null;
  petsBreed.value = "Labrador";
  localStorage.clear();
  idbStore.clear();
  vi.clearAllMocks();
});

/** Holds every kept-results read until the returned release is called. */
function holdIdbReads(): () => Promise<void> {
  let release!: () => void;
  idbGate.wait = new Promise<void>((r) => (release = r));
  return async () => {
    idbGate.wait = null;
    await act(async () => {
      release();
      await new Promise((r) => setTimeout(r, 50));
    });
  };
}

/** Runs the pets query in the one tab, so its rows are kept, and unmounts. */
async function seedPetsTab() {
  const user = userEvent.setup();
  const first = renderPage();
  await waitFor(() => screen.getByText("Query 1"));
  await user.type(editor(), "select pets");
  await user.click(screen.getByTestId("sql-run"));
  await waitFor(() => expect(screen.getByText("Labrador")).toBeInTheDocument());
  first.unmount();
}

/** Remounts the page with a hand-off payload, as the ontology graph's Open as Query does. */
function openHandoff(state: Record<string, unknown>) {
  nav.state = state;
  nav.key = "handoff";
  return renderPage();
}

describe("SQL explore results hydration", () => {
  it("a hand-off tab never shows the previous tab's kept results", async () => {
    await seedPetsTab();
    const release = holdIdbReads();
    openHandoff({ sql: "select glossary_terms" });
    await waitFor(() => expect(editor()).toHaveValue("select glossary_terms"));
    await waitFor(() => expect(screen.getByText("Query 2")).toBeInTheDocument());
    await release();
    expect(editor()).toHaveValue("select glossary_terms");
    expect(screen.queryByText("breed_name")).not.toBeInTheDocument();
    expect(screen.queryByText("Labrador")).not.toBeInTheDocument();
  });

  it("a hand-off tab keeps the previous tab's results with that tab", async () => {
    const user = userEvent.setup();
    await seedPetsTab();
    const release = holdIdbReads();
    openHandoff({ sql: "select glossary_terms" });
    await waitFor(() => expect(screen.getByText("Query 2")).toBeInTheDocument());
    await release();
    await user.click(screen.getByText("Query 1"));
    expect(editor()).toHaveValue("select pets");
    expect(await screen.findByText("Labrador")).toBeInTheDocument();
  });

  it("a handed run's rows stay when the kept results arrive after it", async () => {
    await seedPetsTab();
    const release = holdIdbReads();
    openHandoff({ sql: "select glossary_terms", autoRun: true });
    await waitFor(() => expect(screen.getByText("4242")).toBeInTheDocument());
    await release();
    expect(screen.getByText("4242")).toBeInTheDocument();
    expect(screen.queryByText("breed_name")).not.toBeInTheDocument();
    expect(screen.queryByText("Labrador")).not.toBeInTheDocument();
  });

  it("a run started before the kept results arrive keeps its rows", async () => {
    const user = userEvent.setup();
    await seedPetsTab();
    const release = holdIdbReads();
    renderPage();
    await waitFor(() => expect(editor()).toHaveValue("select pets"));
    petsBreed.value = "Poodle";
    await user.click(screen.getByTestId("sql-run"));
    await waitFor(() => expect(screen.getByText("Poodle")).toBeInTheDocument());
    await release();
    expect(screen.getByText("Poodle")).toBeInTheDocument();
    expect(screen.queryByText("Labrador")).not.toBeInTheDocument();
  });

  it("switching tabs before the kept results arrive shows the switched-to tab's results", async () => {
    const user = userEvent.setup();
    const first = renderPage();
    await waitFor(() => screen.getByText("Query 1"));
    await user.type(editor(), "select pets");
    await user.click(screen.getByTestId("sql-run"));
    await waitFor(() => expect(screen.getByText("Labrador")).toBeInTheDocument());
    await user.click(screen.getByRole("button", { name: t("sqlEditorPanel.newTab") }));
    await waitFor(() => screen.getByText("Query 2"));
    await user.type(editor(), "select other");
    await user.click(screen.getByText("Query 1"));
    await waitFor(() => expect(editor()).toHaveValue("select pets"));
    first.unmount();

    const release = holdIdbReads();
    renderPage();
    await waitFor(() => expect(editor()).toHaveValue("select pets"));
    await user.click(screen.getByText("Query 2"));
    expect(editor()).toHaveValue("select other");
    await release();
    expect(editor()).toHaveValue("select other");
    expect(screen.queryByText("Labrador")).not.toBeInTheDocument();
    await user.click(screen.getByText("Query 1"));
    expect(await screen.findByText("Labrador")).toBeInTheDocument();
  });

  it("an edit before the kept results arrive drops them", async () => {
    const user = userEvent.setup();
    await seedPetsTab();
    const release = holdIdbReads();
    renderPage();
    await waitFor(() => expect(editor()).toHaveValue("select pets"));
    await user.type(editor(), " where 1=1");
    await release();
    expect(screen.queryByText("Labrador")).not.toBeInTheDocument();
  });

  it("a run that finishes after a tab switch shows in the tab that started it", async () => {
    const user = userEvent.setup();
    renderPage();
    await waitFor(() => screen.getByText("Query 1"));
    await user.type(editor(), "select pets");
    let answer!: () => void;
    runGate.wait = new Promise<void>((r) => (answer = r));
    await user.click(screen.getByTestId("sql-run"));
    await user.click(screen.getByRole("button", { name: t("sqlEditorPanel.newTab") }));
    await waitFor(() => screen.getByText("Query 2"));
    expect(editor()).toHaveValue("");
    await act(async () => {
      answer();
      await new Promise((r) => setTimeout(r, 50));
    });
    expect(screen.queryByText("Labrador")).not.toBeInTheDocument();
    expect(screen.queryByText("breed_name")).not.toBeInTheDocument();
    await user.click(screen.getByText("Query 1"));
    expect(editor()).toHaveValue("select pets");
    expect(await screen.findByText("Labrador")).toBeInTheDocument();
  });
});
