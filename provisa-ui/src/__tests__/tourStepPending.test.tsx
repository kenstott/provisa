// Copyright (c) 2026 Kenneth Stott
// Canary: 384624e5-6b85-4693-bd04-2a2d74585ad0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A step that is waiting or stuck is this step's, and the card says so alone: left on screen under
// it, the step before's popover read as the step in trouble. On a live profiling sandbox "step 3 of
// 19" sat under the notice of step 4, whose statement had answered no rows. A statement that
// answers no rows is what the page says of it (REQ-2052): the step stops at once, that its reason,
// instead of waiting out its anchor.

import { describe, it, expect, vi, afterEach } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { render, screen, act, fireEvent } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { MantineProvider } from "@mantine/core";
import { NamedDemoContext, type NamedDemo } from "../context/NamedDemoContext";

const t = (key: string) => key;
vi.mock("../hooks/useOrgHas", () => {
  const has = {
    sources: new Set(["crm"]),
    tables: new Set(["crm.customers"]),
    dataProducts: 0,
    pollyConfigured: false,
  };
  const read = () => Promise.resolve(has);
  return { useOrgHas: () => read };
});
vi.mock("react-i18next", () => ({ useTranslation: () => ({ t }) }));
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
  useAuth: () => ({
    loading: false,
    capabilities: ["source_registration", "table_registration", "query_development"],
  }),
}));

const { TourProvider, useTour } = await import("../tour/useTour");

const DEMO: NamedDemo = {
  name: "profiling",
  exampleQueries: [],
  tour: [
    { key: "profilingScenario", route: "/sources", capability: "source_registration", source: "crm" },
    { key: "profilingRuns", route: "/sql", capability: "query_development", sql: "SELECT 1" },
  ],
  landing: null,
  notebook: [],
  orgFiles: [],
};

function Launcher() {
  const { startTour } = useTour();
  return (
    <button type="button" onClick={() => startTour()}>
      launch
    </button>
  );
}

async function onSecondStep(page: React.ReactNode) {
  vi.useFakeTimers({ shouldAdvanceTime: true });
  render(
    <MantineProvider>
      <MemoryRouter>
        <NamedDemoContext.Provider value={DEMO}>
          <TourProvider>
            <Launcher />
            <table>
              <tbody>
                <tr data-testid="sources-row-crm" />
              </tbody>
            </table>
            {page}
          </TourProvider>
        </NamedDemoContext.Provider>
      </MemoryRouter>
    </MantineProvider>,
  );
  fireEvent.click(screen.getByText("launch"));
  expect(await screen.findByText("tour.steps.profilingScenario.title")).toBeInTheDocument();
  fireEvent.click(document.querySelector<HTMLElement>(".driver-popover-next-btn")!);
}

const advance = (ms: number) =>
  act(async () => {
    await vi.advanceTimersByTimeAsync(ms);
  });

const sheet = (path: string) => readFileSync(resolve(__dirname, path), "utf8");

afterEach(() => {
  vi.useRealTimers();
  document.head.querySelectorAll("style[data-sheet]").forEach((s) => s.remove());
});

describe("a step waiting or stuck, over the step before", () => {
  it("does not leave the step before's popover on screen", async () => {
    for (const css of [
      sheet("../../node_modules/driver.js/dist/driver.css"),
      sheet("../tour/tour.css"),
    ]) {
      const style = document.createElement("style");
      style.dataset.sheet = "";
      style.textContent = css;
      document.head.appendChild(style);
    }
    await onSecondStep(null);
    const popover = document.querySelector<HTMLElement>(".driver-popover")!;
    await advance(2000);
    expect(screen.getByText("tour.status.waiting")).toBeInTheDocument();
    expect(getComputedStyle(popover).display).toBe("none");
    // RESULT_WAIT_MS and past it: stuck, and still alone.
    await advance(61000);
    expect(screen.getByText("tour.status.stuck")).toBeInTheDocument();
    expect(getComputedStyle(popover).display).toBe("none");
  });
});

describe("a statement step whose statement answers no rows", () => {
  it("stops at once, the page's words its reason, offering Skip", async () => {
    await onSecondStep(
      <div data-testid="sql-results-empty" data-answered="">
        No results.
      </div>,
    );
    // Well inside the anchor's window (RESULT_WAIT_MS): the step did not wait it out.
    await advance(3000);
    expect(
      screen.getByText("No results.", { selector: ".tour-status-message" }),
    ).toBeInTheDocument();
    expect(screen.getByText("tour.status.skip")).toBeInTheDocument();
  });

  it("waits while the tab has only not been run", async () => {
    await onSecondStep(<div data-testid="sql-results-empty">No results.</div>);
    await advance(3000);
    expect(screen.queryByText("No results.", { selector: ".tour-status-message" })).toBeNull();
    expect(screen.getByText("tour.status.waiting")).toBeInTheDocument();
  });
});
