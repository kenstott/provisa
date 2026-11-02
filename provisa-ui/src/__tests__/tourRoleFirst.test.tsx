// Copyright (c) 2026 Kenneth Stott
// Canary: 9a9e1596-ddde-4e89-8e4c-3e9487c78904
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A demo step that names a role and what to run: the role is picked as the step opens, on the
// step's page, and what the step names then runs as it, so no step announces a role over an empty
// editor.

import { useState } from "react";
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, act, fireEvent, waitFor } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { MantineProvider } from "@mantine/core";
import { useNavPayload } from "../hooks/useNavPayload";
import { demoTourSteps } from "../tour/demoTour";
import { NamedDemoContext } from "../context/NamedDemoContext";

vi.mock("../hooks/useOrgHas", () => {
  const read = () =>
    Promise.resolve({
      sources: new Set<string>(),
      tables: new Set<string>(),
      dataProducts: 0,
      pollyConfigured: true,
    });
  return { useOrgHas: () => read };
});
// One value across renders: a new `t`, or new rights, re-runs the tour's step.
const stable = vi.hoisted(() => ({
  t: (key: string) => key,
  auth: { loading: false, capabilities: ["query_development"] },
}));
vi.mock("react-i18next", () => ({ useTranslation: () => ({ t: stable.t }) }));
vi.mock("../pageChunks", () => ({ prefetchAllPageChunks: () => Promise.resolve() }));
vi.mock("../hooks/useAdminQueries", () => ({ useTourPrefetch: () => () => Promise.resolve() }));
vi.mock("../context/pollyState", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../context/pollyState")>()),
  usePolly: () => ({
    open: false,
    checkingConfig: false,
    unconfiguredReason: null,
    setUnconfiguredReason: () => {},
    openPolly: () => Promise.resolve(),
    closePolly: () => {},
    showExchange: () => {},
    shownExchange: null,
  }),
}));
vi.mock("../context/AuthContext", () => ({
  useAuth: () => stable.auth,
}));

const { TourProvider, useTour } = await import("../tour/useTour");

const EXAMPLES = [{ name: "first", query: "{ first }" }];

function Launcher() {
  const { startTour } = useTour();
  return (
    <button type="button" onClick={() => startTour()}>
      launch
    </button>
  );
}

/** The SQL explorer as far as the step reaches it: its run-as picker and its results. */
function Explorer({ log }: { log: string[] }) {
  const [open, setOpen] = useState(false);
  useNavPayload<{ sql?: string }>((payload) => {
    if (payload.sql) log.push(`run ${payload.sql}`);
  });
  return (
    <>
      <button
        type="button"
        data-testid="sql-role"
        onClick={() => {
          log.push("open");
          setOpen(!open);
        }}
      >
        run as
      </button>
      {open && (
        <button
          type="button"
          data-sql-role-option="analyst"
          onClick={() => {
            log.push("pick analyst");
            setOpen(false);
          }}
        >
          analyst
        </button>
      )}
      <table className="sql-results-table">
        <tbody>
          <tr>
            <td>row</td>
          </tr>
        </tbody>
      </table>
    </>
  );
}

describe("a step's role, as the runner picks it", () => {
  beforeEach(() => {
    localStorage.clear();
    sessionStorage.clear();
  });

  it("is picked once on the step's page, before its statement is handed over", async () => {
    const log: string[] = [];
    render(
      <MantineProvider>
        <MemoryRouter initialEntries={["/sources"]}>
          <NamedDemoContext.Provider
            value={{
              name: "weblogs",
              exampleQueries: [],
              tour: [
                {
                  key: "s1",
                  route: "/sql",
                  capability: "query_development",
                  role: "analyst",
                  sql: "SELECT 1",
                },
              ],
              landing: null,
              notebook: [],
              orgFiles: [],
            }}
          >
            <TourProvider>
              <Launcher />
              <Routes>
                <Route path="/sql" element={<Explorer log={log} />} />
              </Routes>
            </TourProvider>
          </NamedDemoContext.Provider>
        </MemoryRouter>
      </MantineProvider>,
    );
    fireEvent.click(screen.getByText("launch"));
    await waitFor(() => expect(log).toContain("run SELECT 1"), { timeout: 8000 });
    // The step's page re-renders as it runs: the picker is opened, and the statement sent, once.
    await act(async () => new Promise((r) => setTimeout(r, 300)));
    expect(log).toEqual(["open", "pick analyst", "run SELECT 1"]);
  });
});

describe("a demo step naming a role and what to run", () => {
  it("a role beside what the step runs is picked as it opens, in the page's picker", () => {
    const [example, sql, mcp] = demoTourSteps(
      [
        {
          key: "e",
          route: "/query",
          capability: "query_development",
          role: "customer",
          example: EXAMPLES[0].name,
        },
        {
          key: "s",
          route: "/sql",
          capability: "query_development",
          role: "analyst",
          sql: "SELECT 1",
        },
        {
          key: "m",
          route: "/explore",
          capability: "query_development",
          role: "support_west",
          mcp: "Who ordered?",
        },
      ],
      EXAMPLES,
    );
    expect(example.chooseFirst).toEqual({
      open: '[data-testid="role-selector-trigger"]',
      option: '[data-role-option="customer"]',
    });
    expect(example.chooseOnNext).toBeUndefined();
    expect(example.openQuery).toEqual({ stateKey: "query", text: EXAMPLES[0].query });
    expect(example.element).toBe(".graphiql-response");
    expect(sql.chooseFirst).toEqual({
      open: '[data-testid="sql-role"]',
      option: '[data-sql-role-option="analyst"]',
    });
    expect(sql.openQuery).toEqual({ stateKey: "sql", text: "SELECT 1" });
    expect(mcp.chooseFirst?.option).toBe('[data-role-option="support_west"]');
    expect(mcp.openQuery).toEqual({ stateKey: "mcpQuestion", text: "Who ordered?" });
    expect(mcp.element).toBe(".mcp-chat-wrap");
  });
});
